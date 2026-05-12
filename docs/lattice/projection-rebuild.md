# Leaf-Projection Rebuild & Digest

Orleans.Lattice's per-shard write-ahead log (WAL) is, in a fully replicated
deployment, the canonical durable record of every leaf mutation. Each leaf
grain materialises that log into an in-memory + persisted projection
(its `Entries` dictionary) and advances a per-leaf checkpoint offset as the
WAL grows. Two operational concerns naturally arise:

1. **Drift detection.** If a silo's leaf projection diverges from the
   WAL prefix it claims to have applied - a cosmic-ray bit flip, a
   storage-provider read-after-write anomaly, a bug in `ILeafProjection.Apply` -
   how does an operator notice before downstream readers do?
2. **Recovery from WAL trim.** If a leaf has been cold long enough that the
   WAL has been trimmed past its last persisted checkpoint, the leaf cannot
   resume by tail-replay alone. What does activation do?

This document covers the two surfaces that answer those questions:
`ILattice.GetLeafProjectionDigestAsync` (drift detection) and
`ProjectionRebuildPolicy` together with the supporting
`MaxLeafReplayEntries` / `LeafProjectionRetention`
options (recovery).

## Drift detection: `GetLeafProjectionDigestAsync`

```csharp verify
LeafProjectionDigest digest = await tree.GetLeafProjectionDigestAsync(
    shardIndex: 0,
    cancellationToken);

// digest.Hash             - 16-byte XxHash128 fingerprint of the shard's projection
// digest.EntryCount       - entries (live + tombstoned) folded into the hash
// digest.CheckpointOffset - sum of per-leaf projection-checkpoint offsets
```

`GetLeafProjectionDigestAsync` walks the leaf chain of the requested
**physical shard** and chains every leaf's XxHash128 digest into a single
shard-level fingerprint. The shard hash is

```text
XxHash128( leaf_1.Hash || leaf_2.Hash || ... || leaf_N.Hash )
```

so a single-byte difference at any leaf - a stale tombstone, a missing
TTL stamp, a divergent vector clock - surfaces as a different shard
hash. Operators running multiple silos against the same WAL can poll
the digest from each silo and compare bytes; equality is the strongest
possible cross-silo state-equivalence check the library provides.

XxHash128 is a non-cryptographic hash: it is chosen for ~10x lower CPU
cost than SHA-256 on the per-mutation hot path and for its uniformly
distributed output (which the XOR-fold algebra requires). The digest is
a drift-detection fingerprint, not an authentication tag - a malicious
operator with write access to the projection state could craft a
collision, but the digest's job is to catch silent corruption, not to
defend against forgery.

### What is folded into the leaf hash

For every entry in the leaf's sorted `Entries` dictionary the implementation
computes a 16-byte XxHash128 contribution over the following fields, in this
order:

1. `key` (length-prefixed UTF-8)
2. `lww.Timestamp.WallClockTicks` (`Int64`, little-endian)
3. `lww.Timestamp.Counter` (`Int32`, little-endian)
4. `lww.IsTombstone` (`byte`, `0x00` or `0x01`)
5. `lww.ExpiresAtTicks` (`Int64`, little-endian - `0` when unset)
6. `lww.OriginClusterId` (length-prefixed UTF-8, `-1` sentinel for null)
7. `lww.VectorClock` (a deterministic ordinal-sorted feed of every
   `(replicaId, hlc.WallClockTicks, hlc.Counter)` triple, or `-1` sentinel
   when null/empty)
8. `lww.Value` (length-prefixed bytes - `-1` sentinel for tombstones)

The per-entry contributions are XOR-folded into a 16-byte running hash
that is **maintained incrementally on every mutation** and persisted on
the leaf state. Insert XORs the new contribution in; replace XORs the
old contribution out and the new one in (the old contribution cancels
under self-inverse XOR); delete XORs the contribution out. Because XOR
is commutative, associative, and self-inverse, the running hash is
independent of insertion order and idempotent re-application of the
same mutation is a no-op - exactly the algebra LWW already provides
for entry state.

The public digest is the XxHash128 of `(running_xor || entryCount ||
checkpointOffset)`, so two silos at different replay positions report
distinct digests even if their post-state happens to coincide. The
shard-level hasher then absorbs each leaf's resulting 16-byte hash, in
leaf-chain order.

### Determinism contract

The digest is byte-stable across silos because every input is canonicalised:

- The leaf's `Entries` is a `SortedDictionary<string, LwwValue<byte[]>>`
  built with `StringComparer.Ordinal`, so the per-entry contributions
  are identical on every silo regardless of insertion order.
- All numeric fields use little-endian framing via `BinaryPrimitives`.
- All strings use `Encoding.UTF8`, length-prefixed with an `Int32`.
- Length-prefix sentinels (`-1`) distinguish tombstone from empty value
  and null-string from empty-string so adjacent variable-length fields
  cannot collide.
- `VersionVector` keys are sorted with `StringComparer.Ordinal` before
  feeding so dictionary insertion order does not perturb the output.

### Cost and where to call it

Because the per-entry XOR fold is maintained incrementally on every
mutation, `GetLeafProjectionDigestAsync` does **not** re-walk the leaf's
`Entries` on each call - the running hash is already on the leaf's
in-memory state, so the per-leaf computation collapses to a single
fixed-size XxHash128 over `(running_xor || entryCount || checkpointOffset)`.
The call still flows through grain activation: a cold leaf will be
activated (and its persisted state, including the running hash, loaded
from storage) the first time it is queried, exactly as for any other
RPC. The win versus the original O(N) walk is in the steady state:
once a shard's leaves are warm, repeated digest polls cost one grain
hop per leaf plus a constant-time hash, regardless of how many entries
each leaf holds. Per-shard cost is therefore one grain hop per leaf in
the chain plus one final XxHash128 chain through the leaf hashes - the
digest visits exactly the leaves a normal scan would visit, and never
re-hashes their entries.

Heap allocations on the hot path are bounded:

| Allocation                              | Per call    |
|-----------------------------------------|-------------|
| `XxHash128` (one per shard, plus one cached per leaf grain activation) | reused via `TryGetHashAndReset` |
| `byte[16]` XxHash128 hash from `GetHashAndReset()` | unavoidable (the result) |
| String / VC scratch buffers             | pooled (`stackalloc 256` fast path; `ArrayPool<byte>.Shared` and `ArrayPool<string>.Shared` for the rare overflow) |

The `O(1)` per-leaf cost makes the digest cheap enough for steady-state
monitoring - including periodic cross-silo equality canaries - not just
on-demand diagnostics. It is safe to call against a live shard under
load: it observes the current in-memory projection without taking any
kind of consistency freeze. The result is necessarily a snapshot at one
wall-clock instant, however, so two calls under sustained writes will
report different digests; equality is meaningful only between
**quiescent observations** (no in-flight writes to the shard between
the two reads being compared).

> **Forward-looking note.** The per-shard cost above (one grain hop per
> leaf in the chain) is a property of the current shipping topology -
> a whole-tree poll still activates every leaf in every shard. A planned
> follow-up promotes the same XOR running-hash up through
> `IBPlusInternalGrain`: each internal node maintains a
> `SubtreeProjectionHash` over its descendants, updated incrementally
> on the same call path that already persists each leaf write. Once
> shipped, `GetLeafProjectionDigestAsync(shardIndex, ct)` collapses to
> one grain call per shard root regardless of leaf count, and a
> whole-tree poll costs `O(shardCount)` rather than
> `O(shardCount × leafCount)`. The public surface - return type, byte
> framing, cross-silo equality semantics - is preserved verbatim, so
> operator tooling written against today's API needs no change.

### Cross-silo divergence example

```csharp verify
// On every silo hosting the cluster, schedule a periodic poll
// over every shard and compare digests. Divergence here means
// at least one silo's projection has drifted from the WAL.
var routing = await tree.GetRoutingAsync();
foreach (var shardIndex in routing.Map.GetPhysicalShardIndices())
{
    LeafProjectionDigest digest = await tree.GetLeafProjectionDigestAsync(
        shardIndex,
        cancellationToken);
    // emit (silo, treeId, shardIndex, digest.Hash, digest.EntryCount, digest.CheckpointOffset)
    // to your telemetry pipeline.
}
```

### Error surface

| Condition                                                    | Exception                          |
|--------------------------------------------------------------|------------------------------------|
| `shardIndex` is not a physical shard of the per-tree map     | `ArgumentOutOfRangeException`      |
| The activation's tree id starts with the reserved system prefix | `InvalidOperationException`     |
| `cancellationToken` was already cancelled                    | `OperationCanceledException`       |

## Recovery: fall-off-log triggers and `ProjectionRebuildPolicy`

When a leaf grain reactivates it consults its persisted
`ProjectionCheckpointOffset` and decides how to recover. Three triggers
classify the activation as **fall-off-log** - the leaf cannot or should
not resume by tail-replay alone:

1. **WAL trimmed past checkpoint.** The per-shard WAL has GC'd entries
   the leaf still considers unapplied. A tail replay would skip those
   entries and converge to the wrong state.
2. **Replay budget exceeded.** The gap `walHead - checkpoint` exceeds
   `LatticeOptions.MaxLeafReplayEntries` (default `10 000`). Replaying
   in the activation path would produce a long cold-start; the operator
   has elected to take the snapshot-then-WAL path instead.
3. **Cold past retention.** The persisted projection age exceeds
   `LatticeOptions.LeafProjectionRetention` (default 7 days) - long
   enough that even a healthy WAL has likely been trimmed beneath the
   leaf's checkpoint. Forcing a snapshot-based recovery here avoids a
   silent miss of trim-induced gaps.

The `ProjectionRebuildPolicy` enum on `LatticeOptions` selects what
the leaf does once a trigger fires:

| Policy | Behaviour |
|---|---|
| `SnapshotThenWal` *(default)* | Drains the per-leaf snapshot via `ILeafSnapshotProvider` as the recovery base, persists the snapshot offset as the new checkpoint, then tail-replays the remaining WAL entries since the snapshot. Reliable: works even when the WAL has been trimmed below the leaf's previous checkpoint. |
| `FullRebuildFromWal` | Replays from the absolute tail of the WAL. Fails fast with `LeafProjectionStaleException` if the WAL has been trimmed and a complete history is unavailable. Diagnostic. |
| `Fail` | Surfaces a `LeafProjectionStaleException` at activation time and waits for an operator-driven rebuild. |

### Configuration

```csharp verify
siloBuilder.ConfigureLattice(o =>
{
    // Allow a cold leaf to replay up to 100 000 entries before
    // taking the snapshot-then-WAL path:
    o.MaxLeafReplayEntries = 100_000;

    // Trust the WAL retention for a full month before forcing
    // a snapshot-based recovery on stale projections:
    o.LeafProjectionRetention = TimeSpan.FromDays(30);

    // Strictest default - try the snapshot path before tailing:
    o.ProjectionRebuildPolicy = ProjectionRebuildPolicy.SnapshotThenWal;
});
```

## Related surfaces

- `ILattice.GetLeafProjectionDigestAsync` - the public surface.
- `LeafProjectionDigest` - the returned `readonly record struct`.
- `ProjectionRebuildPolicy` - the activation-time recovery policy.
- `LatticeOptions.MaxLeafReplayEntries`, `LatticeOptions.LeafProjectionRetention`,
  `LatticeOptions.MaterialiserCheckpointInterval`,
  `LatticeOptions.MaterialiserCheckpointEntries` - see [Configuration](configuration.md).
- `LeafProjectionStaleException` - thrown by `ProjectionRebuildPolicy.Fail`
  and by `ProjectionRebuildPolicy.FullRebuildFromWal` when the WAL has
  been trimmed.
