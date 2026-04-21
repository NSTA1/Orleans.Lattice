# TTL on `SetAsync`

Orleans.Lattice supports **per-entry time-to-live (TTL)** on writes. An entry written with a TTL is visible to every read until its absolute expiry instant, after which it becomes invisible to reads and is eventually reaped by tombstone compaction.

## Top-level behaviour

The public API is an overload on `ILattice`:

```csharp
Task SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken ct = default);
```

Typed convenience overloads are provided on `TypedLatticeExtensions`:

```csharp
await lattice.SetAsync("session:42", session, TimeSpan.FromMinutes(30));
await lattice.SetAsync("session:42", session, TimeSpan.FromMinutes(30), serializer);
```

Behavior:

- **Absolute UTC expiry, resolved server-side.** The silo handling the `SetAsync` call computes `DateTimeOffset.UtcNow.Add(ttl).UtcTicks` and stores that absolute instant on the entry. Client clock skew does **not** shift individual entry lifetimes — all entries expire relative to the server that accepted them.
- **Validation.** `SetAsync` throws `ArgumentOutOfRangeException` if `ttl` is zero, negative, or large enough to overflow `DateTimeOffset.MaxValue` when added to `DateTimeOffset.UtcNow`.
- **Non-TTL writes are unaffected.** Existing `SetAsync(key, value)` calls and bulk-load entries carry no expiry (`ExpiresAtTicks == 0`) and never expire on their own.
- **Overwrite semantics.** A later `SetAsync` on the same key wins by hybrid-logical-clock timestamp regardless of whether either write carried a TTL. Writing a non-TTL value over a TTL value clears the expiry; writing a TTL value over a non-TTL value introduces one.

## Internal representation

Every entry is stored as an `LwwValue<byte[]>`:

| Field | Purpose |
|---|---|
| `Value` | The stored bytes (or tombstone marker). |
| `Timestamp` | HLC timestamp used for last-writer-wins conflict resolution. |
| `Deleted` | Tombstone flag. |
| `ExpiresAtTicks` | Absolute UTC ticks at which the entry expires. `0` means "no expiry" — this is the default and keeps pre-TTL snapshots wire-compatible. |

`LwwValue<T>.IsExpired(long nowUtcTicks)` is the single predicate used by every read path to decide whether to hide an entry.

## Read paths

All read entry points filter expired entries before returning results to callers:

| Operation | Behaviour when entry is expired |
|---|---|
| `GetAsync` | Returns `null`. |
| `GetWithVersionAsync` | Returns `VersionedValue.NotFound`. |
| `ExistsAsync` | Returns `false`. |
| `GetManyAsync` | Key is absent from the returned dictionary. |
| `GetOrSetAsync` | Treats the expired entry as absent and proceeds to write the supplied factory value. |
| `SetIfVersionAsync` | Treats the expired entry as `HybridLogicalClock.Zero` for CAS comparison. |

The effective "now" is captured once per grain call so all keys in a single `GetManyAsync` / `CountAsync` / scan observe the same expiry instant.

## Scans and counts

| Operation | Behaviour |
|---|---|
| `KeysAsync` / `EntriesAsync` | Expired entries are omitted. |
| `CountAsync` / `CountPerShardAsync` | Expired entries are excluded from the count. |
| `DeleteRangeAsync` | Expired entries are skipped (no tombstone is written for them — compaction will reap them). |

## Durable cursors

`OpenKeyCursorAsync` and `OpenEntryCursorAsync` inherit the filtering rules above: each page fetched by the cursor reads through the same expired-entry filter applied to ad-hoc scans. A long-lived cursor iterating a shard whose entries expire mid-iteration will simply stop seeing those keys on subsequent pages.

## Read cache

`LeafCacheGrain` (the per-silo `[StatelessWorker]` cache described in [Read Caching](caching.md)) stores the raw `LwwValue<byte[]>` including `ExpiresAtTicks`. On a cache hit it applies `IsExpired(nowUtcTicks)` before returning, so expired entries are never served from cache even if the primary leaf has not yet had them compacted.

## Atomic writes (saga compensation)

`SetManyAtomicAsync` runs a two-phase saga: `Prepare` captures the pre-image of each target entry, `Apply` writes the new values, and on failure `Compensate` restores each pre-image.

TTL preservation is end-to-end:

- `Prepare` reads the full `LwwValue` (including `ExpiresAtTicks`) via the guarded internal `IShardRootGrain.GetRawEntryAsync` — not the filtered `VersionedValue` surface.
- `Compensate` restores each pre-image through the TTL-aware `SetAsync(key, value, TimeSpan)` overload, reconstructing the original expiry relative to the pre-image's absolute expiry instant.
- If a pre-image's expiry has already passed by compensation time, it is restored as a tombstone rather than a live value — matching what a read would have seen.

## Shard splits

Online shard splits must carry TTL metadata across two distinct code paths:

- **Shadow-forward writes** during the shadow phase read the source leaf's raw `LwwValue` via `GetRawEntryAsync` (not the filtered `VersionedValue`) so the shadow receives the full record including `ExpiresAtTicks`.
- **Drain** uses `MergeManyAsync(batch)`, which carries the complete `LwwValue` for each entry. Expiry is preserved verbatim on the target shard.

See [Shard Splitting](shard-splitting.md) for the full split lifecycle.

## Snapshots

`TreeSnapshotGrain.CopyShardAsync` drains each source shard via `GetLiveRawEntriesAsync` and bulk-loads the destination via `BulkLoadRawAsync`. Both sides transport `LwwValue<byte[]>` directly, so HLC timestamps and `ExpiresAtTicks` cross the snapshot boundary unchanged. This applies to both offline and online snapshot modes. See [Snapshots](snapshots.md).

## Resize

`ResizeAsync` is implemented on top of the snapshot pipeline (copy to a new physical tree, then swap the alias). TTLs are preserved verbatim for the same reason snapshots preserve them. See [Tree Sizing](tree-sizing.md) for the resize phase machine and [Tree Storage](tree-storage.md) for sizing guidance.

## Merge (`MergeAsync`)

Merging one tree into another flows entries through `GetDeltaSinceAsync` + `MergeManyAsync`. Conflict resolution is LWW by HLC timestamp; the winning `LwwValue` carries its `ExpiresAtTicks` unchanged. If the winner was written without a TTL, the target entry ends up with no expiry — even if the loser had one.

## Tombstone compaction

Expired live entries past the configured `TombstoneGracePeriod` are reaped alongside regular tombstones by the reminder-driven compaction path. The grace window prevents a peer with a lagging clock from resurrecting an entry via a delayed merge: a remote replica that has not yet seen the expiry will continue to propagate the entry as "live" (see CRDT replication below), and the grace period ensures local compaction waits long enough for those stragglers to converge before permanent removal. See [Tombstone Compaction](tombstone-compaction.md).

## CRDT replication invariant

Expired entries are **intentionally preserved** on the replication-layer code paths:

- `BPlusLeafGrain.GetDeltaSinceAsync` — returns expired entries to requesting peers.
- `BPlusLeafGrain.MergeEntriesAsync` / `MergeManyAsync` — stores expired entries received from peers.

This is required for CRDT convergence: two replicas with different views of "now" must still agree on the last-writer-wins value for a key, and that resolution needs access to the entry's HLC timestamp even after its `ExpiresAtTicks` has passed. User-facing read paths apply the expiry filter; the replication layer does not. Read-cache deltas (`StateDelta` from the primary to `LeafCacheGrain`) follow the same rule — the cache receives expired entries verbatim and filters them at read time.

## Bulk load

`BulkLoadAsync` and the streaming bulk-load overloads do not currently accept a TTL. Entries loaded via bulk load have no expiry. Follow-on `SetAsync(..., TimeSpan)` calls can introduce a TTL on specific keys if needed.

## Operational notes

- **Clock skew.** Because expiry is resolved on the silo that accepts the write, TTL consistency depends on reasonably synchronised silo clocks — not client clocks. Two writes with the same `ttl` made simultaneously from different clients will expire at nearly the same instant regardless of client time, provided the silos they land on share the same wall clock.
- **Grace period.** Tune `LatticeOptions.TombstoneGracePeriod` to be at least the worst-case clock drift plus replication delay you expect between silos. Setting it too low risks a lagging replica resurrecting an expired entry; setting it too high delays physical space reclamation.
- **`CacheTtl` is independent of entry TTL.** `CacheTtl` controls how long a `LeafCacheGrain` may serve reads without re-polling the primary for a delta; it has no effect on an entry's own expiry. An expired entry is filtered at the cache regardless of `CacheTtl`.
