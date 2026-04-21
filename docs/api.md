# Lattice Public API Reference

Lattice is a distributed, CRDI-based B+ tree built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It exposes a single entry-point grain interface, `ILattice`, that routes operations to sharded internal storage. See [Architecture](architecture.md) for the full grain layer design.

## Setup

Install the NuGet package:

```shell
dotnet add package Orleans.Lattice
```

Add the namespace import:

```csharp verify
using Orleans.Lattice;
```

Register Lattice on the silo, providing a storage provider:

```csharp verify
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));
```

Per-tree options can be configured with the `ConfigureLattice` extension method (see [Configuration](configuration.md) for the full options reference and per-tree override semantics):

```csharp verify
siloBuilder.ConfigureLattice("my-tree", o =>
{
    o.CacheTtl = TimeSpan.FromMilliseconds(100);
    o.HotShardOpsPerSecondThreshold = 500;
});
```

> Structural sizing (`MaxLeafKeys`, `MaxInternalChildren`, `ShardCount`) is **not** configured here — those values are pinned per-tree in the registry. See [Tree Sizing](tree-sizing.md) for how to set them on a new or existing tree.

### Basic usage

Once Lattice is registered on the silo, resolve an `ILattice` grain from the client's (or a grain's) `IGrainFactory` using the tree's logical name as the string key, then call its methods directly:

```csharp verify
// Resolve the tree (idempotent — the same logical name always routes to the same tree).
var tree = grainFactory.GetGrain<ILattice>("my-tree");

// Write a value.
await tree.SetAsync("user:1", "Alice"u8.ToArray());

// Read it back (returns null if the key is absent or tombstoned).
byte[]? value = await tree.GetAsync("user:1");

// Conditional write — only insert if the key is not already present.
byte[]? existing = await tree.GetOrSetAsync("user:1", "Bob"u8.ToArray());

// Delete (tombstones the key; returns true if it was live).
bool deleted = await tree.DeleteAsync("user:1");

// Stream a key range in strict lexicographic order.
await foreach (var key in tree.KeysAsync(startInclusive: "user:", endExclusive: "user;"))
{
    Console.WriteLine(key);
}
```

Keys are `string`; values are `byte[]`. For typed payloads (POCOs, records, DTOs) use the serializer-aware overloads in [`TypedLatticeExtensions`](#typedlatticeextensions) — they accept any `T` and default to `JsonLatticeSerializer<T>`:

```csharp verify
await tree.SetAsync("user:1", new User("Alice", 30));
var user = await tree.GetAsync<User>("user:1");
```
For the full set of runtime and maintenance operations, see [`ILattice`](#ilattice) below.

## `ILattice`

Obtain an `ILattice` grain from the grain factory using the tree's logical name as the string key:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
```

> For the consistency contract of each method below — linearizable,
> strongly consistent, snapshot, or eventually consistent — see the
> per-operation matrix in [Consistency](consistency.md). Per-row notes
> here describe behaviour under specific conditions (e.g. concurrent
> shard splits); they do not replace the classification in that doc.

### Cancellation

Every method on `ILattice` — including scans (`KeysAsync`, `EntriesAsync`),
range deletes, counts, fan-out batch operations, bulk load, stateful
cursors, and all tree-lifecycle orchestrators — accepts an optional
trailing `CancellationToken cancellationToken = default` parameter. The
signatures in the tables below omit the parameter for readability. The
orchestrator checks the token at method entry, inside retry
`when` filters, and at fan-out / pagination checkpoints, so a
pre-cancelled token fails fast without contacting any shard. Scan
iterators apply `[EnumeratorCancellation]` so
`await foreach (...).WithCancellation(ct)` propagates into the
orchestrator. Cooperative cancellation is scoped to the `LatticeGrain`
orchestrator — once a long-running coordinator (saga, resize, snapshot,
merge) has accepted a request it drives itself to a terminal state via
reminders and is not cooperatively cancelled.

The typed extensions in `TypedLatticeExtensions` and both streaming
`BulkLoadAsync` overloads in `LatticeExtensions` also thread the token.

### Runtime Operations

These methods are used during normal application flow to read, write, and enumerate data. They are safe to call concurrently and do not affect tree availability.

#### Single-Key

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync` | `Task<byte[]?> GetAsync(string key)` | Returns the value for `key`, or `null` if absent or tombstoned. Reads are served via a [stateless-worker cache](caching.md); `CacheTtl` controls how long cached entries are served before refreshing from the primary leaf. |
| `GetWithVersionAsync` | `Task<VersionedValue> GetWithVersionAsync(string key)` | Returns the value and its `HybridLogicalClock` version for `key`, or a default `VersionedValue` with `null` value and zero version when absent/tombstoned. Use the returned version with `SetIfVersionAsync` for optimistic concurrency (CAS). Reads directly from the primary leaf (not cached) to ensure version freshness. |
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` if `key` exists and is not tombstoned. |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `SetAsync` (TTL) | `Task SetAsync(string key, byte[] value, TimeSpan ttl)` | Inserts or updates the value for `key` with a time-to-live. The entry is treated as tombstoned on all reads (`GetAsync`, `ExistsAsync`, `GetManyAsync`, `KeysAsync`, `EntriesAsync`, `CountAsync`, etc.) once `ttl` has elapsed since the server-side write, and is reaped by background tombstone compaction after the configured `LatticeOptions.TombstoneGracePeriod`. The TTL is converted to an absolute UTC expiry on the silo handling the call, so clock skew between clients does not shift individual entries' lifetimes. Throws `ArgumentOutOfRangeException` when `ttl` is zero or negative. Typed overload: `SetAsync<T>(this ILattice, string, T, TimeSpan, ILatticeSerializer<T>)`. |
| `SetIfVersionAsync` | `Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)` | Sets `key` to `value` only if the entry's current `HybridLogicalClock` matches `expectedVersion`. Returns `true` if the write was applied, `false` if the version did not match (another writer updated the key). For a new key, pass `HybridLogicalClock.Zero` as the expected version. Enables safe read-modify-write patterns without distributed locks. |
| `GetOrSetAsync` | `Task<byte[]?> GetOrSetAsync(string key, byte[] value)` | Sets `key` to `value` only if the key does not already exist (or is tombstoned). Returns the existing value when the key is live, or `null` when the value was newly written. Avoids a read-then-write roundtrip by short-circuiting at the leaf grain. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if it was live. Tombstones are removed by [background compaction](tombstone-compaction.md). |

#### Batch

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel across shards. Missing/tombstoned keys are omitted from the result. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Inserts or updates multiple entries in parallel across shards. **Not atomic** — a partial failure leaves the batch half-applied with no compensating rollback. Use `SetManyAtomicAsync` when all-or-nothing semantics are required. |
| `SetManyAtomicAsync` | `Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries)` | Atomically writes multiple entries via a saga coordinator grain. Reads each key's pre-saga value up front (including any TTL via `GetRawEntryAsync`), applies writes sequentially, and compensates (reverts) already-committed entries if a subsequent write fails — pre-saga values with a TTL are restored through the TTL-aware `SetAsync(key, value, TimeSpan)` overload so `ExpiresAtTicks` survives the rollback; pre-saga entries whose TTL has already elapsed are tombstoned; previously-absent keys are tombstoned. Crash-recovery is reminder-driven. Throws `ArgumentException` on duplicate keys or null values, and `InvalidOperationException` after compensation completes for a failed write. **Partial-visibility window:** readers between the first and last committed write may see a partial view of the batch; layer version-guarded reads (`GetWithVersionAsync` + `SetIfVersionAsync`) on top for strict isolation. After completion, saga state is retained for `LatticeOptions.AtomicWriteRetention` (default 48h) for idempotent re-invocation, then automatically cleared by a retention reminder. |
| `SetManyAtomicAsync` (idempotency key) | `Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId)` | Caller-supplied idempotency-key overload. The saga grain is keyed `{treeId}/{operationId}`; re-submitting the same `operationId` re-attaches to the original saga and inherits its outcome, turning a transport-level failure (silo restart, client timeout) into a safe client retry. The `operationId` is bound to the exact sorted key set submitted on its first call via a persisted SHA-256 fingerprint; mismatched key sets throw `InvalidOperationException`. Reordering keys or changing values is allowed. `operationId` must be non-empty and must not contain `'/'` (reserved grain-key separator) — throws `ArgumentException` otherwise. See [Atomic Writes — Caller-supplied idempotency keys](atomic-writes.md#caller-supplied-idempotency-keys). |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)` | Tombstones all live keys in the lexicographic range [`startInclusive`, `endExclusive`] by walking the leaf chain in each shard. Returns the total number of keys tombstoned. |
| `CountAsync` | `Task<int> CountAsync()` | Returns the total number of live (non-tombstoned) keys across all shards. **Strongly consistent during shard splits**: each physical shard is asked to count only the virtual slots it currently owns per the authoritative `ShardMap` (via `IShardRootGrain.CountForSlotsAsync`), then the map version is re-read and the count is retried on any version change. Every virtual slot is therefore counted exactly once — against whichever shard the map identifies as its current owner — regardless of where the split coordinator is in its per-phase machine. Bounded by `LatticeOptions.MaxScanRetries` (default 3); throws `InvalidOperationException` if the topology keeps mutating beyond the retry budget. |
| `CountPerShardAsync` | `Task<IReadOnlyList<int>> CountPerShardAsync()` | Returns the number of live keys in each shard as an ordered list (index = shard index). Uses the same per-slot routing as `CountAsync` so the per-shard counts are also topology-consistent with the observed `ShardMap` snapshot. Useful for diagnostics and load-balancing analysis. |

#### Enumeration

| Method | Signature | Description |
|--------|-----------|-------------|
| `KeysAsync` | `IAsyncEnumerable<string> KeysAsync(string? startInclusive, string? endExclusive, bool reverse, bool? prefetch)` | Streams live keys in strict lexicographic order via paginated k-way merge across shards. When `prefetch` is `true` (or `null` with `PrefetchKeysScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed, hiding grain-call latency during large scans. **Strongly consistent and strictly ordered during shard splits**: when a shard reports moved-away slots or the `ShardMap` version advances mid-scan, the orchestrator drains the affected slots from their current owners into a sorted in-memory cursor and injects it into the same k-way merge priority queue, so output remains globally sorted end-to-end. A per-call `HashSet<string>` suppresses duplicates across pre- and post-swap views. Reconciliation is bounded by `LatticeOptions.MaxScanRetries`; throws `InvalidOperationException` if the topology keeps mutating beyond that budget (see [`docs/shard-splitting.md`](shard-splitting.md)). |
| `EntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive, string? endExclusive, bool reverse, bool? prefetch)` | Streams live key-value entries in strict lexicographic key order via paginated k-way merge across shards. When `prefetch` is `true` (or `null` with `PrefetchEntriesScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed. Because entries carry `byte[]` values, pre-fetched pages hold extra in-flight memory proportional to `shardCount × KeysPageSize × avgValueSize`, so this flag is gated separately from `PrefetchKeysScan`. Useful for exports, migrations, and analytics without a separate `GetAsync` per key. **Strongly consistent and strictly ordered during shard splits** with the same reconciliation-cursor injection algorithm as `KeysAsync`. Subject to the same `InvalidOperationException` contract on retry exhaustion — see [Scan reliability](#scan-reliability). |

### Scan reliability

`CountAsync`, `KeysAsync`, and `EntriesAsync` use bounded optimistic
retry (`LatticeOptions.MaxScanRetries`, default 3) to reconcile against
concurrent shard splits. If the shard topology keeps mutating after
every reconciliation step, the scan throws
`InvalidOperationException` rather than returning a silently
incomplete result.

Under the default configuration (`MaxConcurrentAutoSplits = 2`,
`HotShardSplitCooldown = 2 minutes`) this is not a realistic
operational concern: splits are rate-limited well below the retry
budget. Point operations (`GetAsync` / `SetAsync` / `DeleteAsync` /
`SetIfVersionAsync`) transparently retry on `StaleShardRoutingException`
during shard-map swaps and never surface this exception to callers.

Callers running multi-minute export scans in aggressively split-prone
workloads have three options:

1. Raise `LatticeOptions.MaxScanRetries` — cheap, addresses transient
   topology churn.
2. Wrap the scan in an application-level retry with exponential backoff
   and resume from the last successfully yielded key (using
   `startInclusive`).
3. Use **stateful cursors** — `OpenKeyCursorAsync` /
   `OpenEntryCursorAsync` return a server-side checkpointed iterator
   that survives topology changes, silo failover, and client restarts
   without caller retry code. See
   [Stateful cursors](#stateful-cursors) below.

### Stateful Cursors

`ILattice` exposes a stateful cursor API for long-running scans and
resumable range deletes that survive silo failovers, client restarts,
and topology changes (shard splits). Unlike the stateless `KeysAsync`
/ `EntriesAsync` / `DeleteRangeAsync` methods — bounded by
`LatticeOptions.MaxScanRetries` — a cursor checkpoints its progress
server-side after every page: a new grain activation reads its
persisted state and resumes from the last yielded key.

Each cursor is backed by a single per-tree grain activation keyed
`{treeId}/{cursorId}`. The grain persists the scan spec, current
phase (`NotStarted` / `Open` / `Exhausted` / `Closed`), and the last
key it yielded (or tombstoned). On reactivation it rebuilds the
next-step range from that checkpoint and continues.

#### Method reference

| Method | Signature | Description |
|--------|-----------|-------------|
| `OpenKeyCursorAsync` | `Task<string> OpenKeyCursorAsync(string? startInclusive, string? endExclusive, bool reverse)` | Opens a key-enumeration cursor and returns a server-assigned opaque cursor ID (GUID). `startInclusive` / `endExclusive` may be `null` for unbounded; `reverse=true` walks keys in descending lex order. |
| `OpenEntryCursorAsync` | `Task<string> OpenEntryCursorAsync(string? startInclusive, string? endExclusive, bool reverse)` | Opens an entry-enumeration cursor (key + value pairs). Same bounds / direction semantics as `OpenKeyCursorAsync`. |
| `OpenDeleteRangeCursorAsync` | `Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive)` | Opens a resumable range-delete cursor. Both bounds are **required** (non-null). Reverse is not supported — range deletes are always forward. Throws `ArgumentException` for null bounds or a reverse spec. |
| `NextKeysAsync` | `Task<LatticeCursorKeysPage> NextKeysAsync(string cursorId, int pageSize)` | Returns up to `pageSize` keys and advances the cursor. Throws `ArgumentOutOfRangeException` for non-positive `pageSize`; throws `InvalidOperationException` if the cursor was opened for a different kind (e.g. `Entries` or `DeleteRange`), or if it has been closed. `HasMore=false` signals exhaustion. |
| `NextEntriesAsync` | `Task<LatticeCursorEntriesPage> NextEntriesAsync(string cursorId, int pageSize)` | Returns up to `pageSize` key-value entries and advances the cursor. Same error surface as `NextKeysAsync` with the expected kind `Entries`. |
| `DeleteRangeStepAsync` | `Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete)` | Deletes up to `maxToDelete` keys in a single step and returns progress. `IsComplete=true` when the full range has been drained; subsequent calls are idempotent no-ops returning `DeletedThisStep=0`. Throws `InvalidOperationException` if the cursor is not of kind `DeleteRange`. |
| `CloseCursorAsync` | `Task CloseCursorAsync(string cursorId)` | Closes the cursor, clears its persisted state, unregisters its idle-TTL reminder, and deactivates the grain. Idempotent — safe to call on an already-closed or never-opened cursor. |

#### Return types

| Type | Members | Description |
|------|---------|-------------|
| `LatticeCursorKeysPage` | `IReadOnlyList<string> Keys`, `bool HasMore` | A page returned by `NextKeysAsync`. Keys are in the cursor's scan order. When `HasMore=false` the cursor has reached its end; no further keys remain. |
| `LatticeCursorEntriesPage` | `IReadOnlyList<KeyValuePair<string, T>> Entries`, `bool HasMore` | A page returned by `NextEntriesAsync`. Same semantics as `LatticeCursorKeysPage`. |
| `LatticeCursorDeleteProgress` | `int DeletedThisStep`, `int DeletedTotal`, `bool IsComplete` | Returned by `DeleteRangeStepAsync`. `DeletedTotal` accumulates across every step; check `IsComplete` to terminate the loop. |
| `LatticeCursorKind` | `Keys`, `Entries`, `DeleteRange` | The kind of scan a cursor performs. Returned via `LatticeCursorSpec.Kind` on internal flows; most callers use the typed `Open*` helpers and never touch this directly. |
| `LatticeCursorSpec` | `Kind`, `StartInclusive`, `EndExclusive`, `Reverse` | Immutable scan specification. Captured at `Open*Async` time and persisted by the cursor grain for resumption after silo failover. |

#### Ordering and consistency

- Each step goes through the normal `ILattice.KeysAsync` /
  `EntriesAsync` / `DeleteRangeAsync` path, so strict lexicographic
  ordering under concurrent shard splits applies within each page.
- Across steps, global ordering is preserved because the step's
  effective range strictly excludes every previously-yielded key
  (`startInclusive = LastYieldedKey + "\0"` for forward scans,
  `endExclusive = LastYieldedKey` for reverse scans).
- Values are snapshot-as-read per step, not per cursor — a value
  updated between two steps will reflect the new value when the entry
  is re-visited on a subsequent open of a new cursor, but once a key
  has been yielded by a cursor it is never re-yielded by the same
  cursor.

#### Idle TTL and self-cleanup

To prevent cursor state leaking when a client forgets to call
`CloseCursorAsync`, every cursor grain registers a sliding idle-TTL
reminder on each successful call. If the reminder fires without any
intervening activity, the grain clears its persisted state, unregisters
the reminder, and deactivates. The default window is **48 hours** and
is configurable:

```csharp verify
siloBuilder.ConfigureLattice(o => o.CursorIdleTtl = TimeSpan.FromHours(6));
```

Minimum effective interval is **1 minute** (Orleans reminder
granularity); smaller values are clamped to the floor. Set
`CursorIdleTtl = Timeout.InfiniteTimeSpan` to disable automatic
cleanup (cursors then live until `CloseCursorAsync` is called).

#### Example — resumable export across a silo failover

```csharp verify
var cursorId = await tree.OpenEntryCursorAsync();
while (true)
{
    var page = await tree.NextEntriesAsync(cursorId, pageSize: 500);
    foreach (var (k, v) in page.Entries)
        Console.WriteLine($"{k}={v.Length} bytes");
    if (!page.HasMore) break;
}
await tree.CloseCursorAsync(cursorId);
```

If the client crashes mid-export it can persist the `cursorId` and
resume on restart — the cursor grain reactivates on demand and
continues from its persisted last-yielded key.

#### Example — bounded, resumable range delete

```csharp verify
var cursorId = await tree.OpenDeleteRangeCursorAsync("2024/", "2025/");
int total = 0;
while (true)
{
    var progress = await tree.DeleteRangeStepAsync(cursorId, maxToDelete: 1000);
    total = progress.DeletedTotal;
    if (progress.IsComplete) break;
}
await tree.CloseCursorAsync(cursorId);
Console.WriteLine($"Deleted {total} keys.");
```

#### Error surface

| Call | Condition | Exception |
|------|-----------|-----------|
| `OpenDeleteRangeCursorAsync` | `startInclusive` or `endExclusive` is `null` | `ArgumentException` |
| `OpenDeleteRangeCursorAsync` | `reverse=true` passed through the internal spec | `ArgumentException` |
| Any `Open*` | Re-open with a different spec on the same cursor ID | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | Kind mismatch (e.g. `NextEntriesAsync` on a key cursor) | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | Cursor was closed | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | `pageSize` / `maxToDelete` ≤ 0 | `ArgumentOutOfRangeException` |

### Maintenance Operations

These methods manage tree structure and lifecycle. Several of them **take the tree offline** — reads and writes will throw `InvalidOperationException` while the operation is in progress. Plan maintenance windows accordingly.

#### Bulk Loading

| Method | Signature | Description |
|--------|-----------|-------------|
| `BulkLoadAsync` | `Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)` | Bottom-up bulk load into an empty tree. Entries are sorted internally. Throws if any shard already has data. See [Bulk Loading](bulk-loading.md) for streaming ingestion and recovery guarantees. |

#### Tree Lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `TreeExistsAsync` | `Task<bool> TreeExistsAsync()` | Returns `true` if this tree is registered in the internal [tree registry](tree-registry.md). |
| `GetAllTreeIdsAsync` | `Task<IReadOnlyList<string>> GetAllTreeIdsAsync()` | Returns all registered tree IDs in sorted order. System trees (prefixed with `_lattice_`) are excluded. Physical trees created by `ResizeAsync` and `SnapshotAsync` are included. |
| `DeleteTreeAsync` | `Task DeleteTreeAsync()` | Soft-deletes the tree. Data is retained for `SoftDeleteDuration` before purge. Idempotent. ⚠️ **Takes the tree offline** — all reads and writes will throw `InvalidOperationException` until the tree is recovered. See [Tree Deletion](tree-deletion.md). |
| `RecoverTreeAsync` | `Task RecoverTreeAsync()` | Recovers a soft-deleted tree before purge completes. |
| `PurgeTreeAsync` | `Task PurgeTreeAsync()` | Immediately purges a soft-deleted tree without waiting for the retention window. ⚠️ **Permanently destroys all data** — this cannot be undone. |

#### Resize

| Method | Signature | Description |
|--------|-----------|-------------|
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | **Online** — resizes the tree's node fan-out by draining every live entry into a newly-provisioned destination physical tree while mirroring live writes end-to-end via the shadow-forward primitive, then atomically swapping the registry alias. Reads and writes remain available throughout; the per-shard `Rejecting` phase at swap is absorbed transparently by the stateless-worker `LatticeGrain` via `StaleTreeRoutingException` retry. Zero data loss under concurrent load (LWW commutativity). Undoable within the `SoftDeleteDuration` retention window. Crash-safe via reminder-anchored `TreeResizeGrain`. See [Tree Sizing](tree-sizing.md#resizing-an-existing-tree). |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize. Behaviour depends on when undo is invoked: **before swap** — aborts the snapshot coordinator, clears every source shard's `ShadowForwardState`, and returns the source tree to a fully-writable state; **after swap** — recovers the old physical tree, removes the alias, restores the original registry configuration, clears any residual `Rejecting` phase on source shards, defensively aborts any post-swap snapshot still attached, and deletes the new snapshot tree. Only available while the old tree is still within its `SoftDeleteDuration` window (before purge completes). |
| `ReshardAsync` | `Task ReshardAsync(int newShardCount, CancellationToken cancellationToken = default)` | **Online** — grows the tree's physical shard count to at least `newShardCount` while the tree continues to serve reads and writes. Internally dispatches up to `LatticeOptions.MaxConcurrentMigrations` (default 4) concurrent per-shard splits, each of which atomically grows the `ShardMap` via its own shadow-write + swap phases. Grow-only: `newShardCount` must be strictly greater than the current distinct-shard count and ≤ `LatticeConstants.DefaultVirtualShardCount` (4096, compile-time constant) (`ArgumentOutOfRangeException` otherwise). Idempotent for the same target while in progress; `InvalidOperationException` when a different target is already running. Returns once the intent is persisted — use `IsReshardCompleteAsync` to poll for completion. Crash-safe via reminder-anchored coordinator. See [Online Reshard](online-reshard.md). |

#### Merge

| Method | Signature | Description |
|--------|-----------|-------------|
| `MergeAsync` | `Task MergeAsync(string sourceTreeId)` | Merges all entries from `sourceTreeId` into this tree using LWW semantics, preserving original timestamps. For each key present in both trees, the entry with the higher `HybridLogicalClock` timestamp wins. Tombstones are also merged, ensuring deletes propagate correctly. The source tree remains unmodified. Source and target trees may have different shard counts — entries are re-hashed to the correct target shard during merge. See [Architecture](architecture.md) for details on the CRDT merge primitives. |

#### Snapshots

| Method | Signature | Description |
|--------|-----------|-------------|
| `SnapshotAsync` | `Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys, int? maxInternalChildren)` | Creates a point-in-time copy of the tree into `destinationTreeId`. In `Offline` mode the source is locked during the copy; in `Online` mode it remains available (best-effort consistency). Optional sizing overrides apply to the destination tree. TTL metadata (`ExpiresAtTicks`) and source HLC versions are preserved verbatim on the destination — a key with remaining TTL reappears on the destination with the same absolute expiry, not a fresh zero-expiry entry. ⚠️ **`Offline` mode takes the tree offline** — all reads and writes throw `InvalidOperationException` until the snapshot completes and shards are unmarked. See [Snapshots](snapshots.md). |

#### Operation Status

| Method | Signature | Description |
|--------|-----------|-------------|
| `IsMergeCompleteAsync` | `Task<bool> IsMergeCompleteAsync()` | Returns `true` if no merge operation is in progress — either the most recent merge has completed or no merge has ever been initiated (vacuously complete). |
| `IsSnapshotCompleteAsync` | `Task<bool> IsSnapshotCompleteAsync()` | Returns `true` if no snapshot operation is in progress — either the most recent snapshot has completed or no snapshot has ever been initiated (vacuously complete). |
| `IsResizeCompleteAsync` | `Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default)` | Returns `true` if no resize operation is in progress — either the most recent resize has completed or no resize has ever been initiated (vacuously complete). |
| `IsReshardCompleteAsync` | `Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default)` | Returns `true` if no online reshard operation is in progress — either the most recent reshard has completed or no reshard has ever been initiated (vacuously complete). |

## `SnapshotMode`

Controls source-tree availability during a snapshot operation.

| Value | Description |
|-------|-------------|
| `Offline` | Source tree is locked (marked deleted) during the copy, guaranteeing a fully consistent snapshot. |
| `Online` | Source tree remains available and **strictly consistent**. The snapshot runs under the shadow-forward primitive: every live mutation accepted during drain is mirrored to the destination with its original HLC, and the drain reader copies residual entries with their HLCs intact. LWW commutativity guarantees convergence on the destination regardless of the interleaving between drain reads and live forwards. See [Tree Sizing](tree-sizing.md#resizing-an-existing-tree) for the design rationale. |

## `LatticeExtensions`

| Method | Description |
|--------|-------------|
| `BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, int chunkSize)` | Streaming bulk load for large datasets. Input **must** be pre-sorted in ascending key order. Routing is resolved via `ILattice.GetRoutingAsync()` so the per-tree `ShardMap` is honoured. See [Bulk Loading](bulk-loading.md). |
| `BulkLoadAsync(..., int shardCount, int chunkSize)` *(obsolete)* | Legacy streaming overload that takes an explicit `shardCount`. Bypasses the persisted `ShardMap` and will mis-route entries on trees with non-default maps. Use the overload above. |

## `TypedLatticeExtensions`

Extension methods that serialize/deserialize values via an `ILatticeSerializer<T>`, eliminating per-caller `byte[]` boilerplate. Each method has two overloads: one accepting an explicit serializer and one that defaults to `JsonLatticeSerializer<T>` (System.Text.Json with UTF-8 encoding).

```csharp verify
// Default (System.Text.Json):
await tree.SetAsync("user:1", new User("Alice", 30));
var user = await tree.GetAsync<User>("user:1");

// Custom serializer:
var serializer = new JsonLatticeSerializer<User>(new JsonSerializerOptions { WriteIndented = false });
await tree.SetAsync("user:1", new User("Alice", 30), serializer);

// Compare-and-swap (CAS):
var versioned = await tree.GetWithVersionAsync<User>("user:1");
var updated = versioned.Value! with { Age = 31 };
bool success = await tree.SetIfVersionAsync("user:1", updated, versioned.Version);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync<T>` | `Task<T?> GetAsync<T>(this ILattice, string key, ILatticeSerializer<T>)` | Returns the deserialized value for `key`, or `default` if absent/tombstoned. |
| `GetWithVersionAsync<T>` | `Task<Versioned<T>> GetWithVersionAsync<T>(this ILattice, string key, ILatticeSerializer<T>)` | Returns the deserialized value and its `HybridLogicalClock` version. Returns a `Versioned<T>` with `default` value and zero version when absent/tombstoned. |
| `GetOrSetAsync<T>` | `Task<T?> GetOrSetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Sets `key` to `value` only if absent/tombstoned. Returns the existing deserialized value when live, or `default` when newly written. |
| `SetAsync<T>` | `Task SetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Serializes and stores `value` under `key`. |
| `SetAsync<T>` (TTL) | `Task SetAsync<T>(this ILattice, string key, T value, TimeSpan ttl, ILatticeSerializer<T>)` | Serializes and stores `value` under `key` with a time-to-live. See `ILattice.SetAsync(string, byte[], TimeSpan)` for expiry semantics. |
| `SetIfVersionAsync<T>` | `Task<bool> SetIfVersionAsync<T>(this ILattice, string key, T value, HybridLogicalClock expectedVersion, ILatticeSerializer<T>)` | Serializes and conditionally writes `value` only if the entry's current version matches `expectedVersion`. Returns `true` if applied. |
| `GetManyAsync<T>` | `Task<Dictionary<string, T>> GetManyAsync<T>(this ILattice, List<string> keys, ILatticeSerializer<T>)` | Fetches and deserializes multiple keys. Missing/tombstoned keys are omitted. |
| `SetManyAsync<T>` | `Task SetManyAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, ILatticeSerializer<T>)` | Serializes and inserts/updates multiple entries in parallel. Not atomic — see `SetManyAtomicAsync<T>`. |
| `SetManyAtomicAsync<T>` | `Task SetManyAtomicAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, ILatticeSerializer<T>)` | Serializes and atomically writes multiple entries via the saga coordinator. See `ILattice.SetManyAtomicAsync` for full semantics. |
| `SetManyAtomicAsync<T>` (idempotency key) | `Task SetManyAtomicAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, string operationId, ILatticeSerializer<T>)` | Caller-supplied idempotency-key overload of the typed variant; delegates to `ILattice.SetManyAtomicAsync(entries, operationId)` after serializing each value. |
| `BulkLoadAsync<T>` | `Task BulkLoadAsync<T>(this ILattice, IReadOnlyList<KeyValuePair<string, T>> entries, ILatticeSerializer<T>)` | Serializes and bulk-loads entries into an empty tree. |
| `EntriesAsync<T>` | `IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(this ILattice, ILatticeSerializer<T>, string?, string?, bool)` | Streams live entries, deserializing values via the provided serializer. |

Each method also has a parameterless overload that defaults to `JsonLatticeSerializer<T>.Default`.

## `ILatticeSerializer<T>`

Implement this interface to provide a custom serialization strategy. The library ships with `JsonLatticeSerializer<T>` as the default.

| Member | Signature | Description |
|--------|-----------|-------------|
| `Serialize` | `byte[] Serialize(T value)` | Converts a value to bytes for storage. |
| `Deserialize` | `T Deserialize(byte[] bytes)` | Converts bytes back to a value. |

## `LatticeOptions`

See [Configuration](configuration.md) for detailed guidance on each option, immutability constraints, and per-tree overrides via the [tree registry](tree-registry.md).

> **Structural sizing is pinned per-tree in the registry, not in `LatticeOptions`.** `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` are seeded into the `TreeRegistryEntry` on first tree use from the canonical defaults in `LatticeConstants` (128 / 128 / 64). After seeding, they are mutable only through `ILattice.ResizeAsync` (leaf / internal capacity) and `ILattice.ReshardAsync` (shard count), both of which run online and update the pin atomically. Callers who want non-default sizing should either (a) call `ResizeAsync` / `ReshardAsync` on a freshly-created tree (empty-tree fast-path — no coordinator machinery) or (b) pre-register the pin via `ILatticeRegistry.RegisterAsync` before first use.

> **The virtual shard space is also not a runtime option.** It is a compile-time constant, `LatticeConstants.DefaultVirtualShardCount = 4096`. Persisted `ShardMap` instances index into this space by integer slot, so changing it would invalidate every stored map. The pinned `ShardCount` must divide this constant evenly (enforced by `ShardMap.CreateDefault`).

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `KeysPageSize` | `int` | 512 | Keys per page in `KeysAsync` pagination. |
| `TombstoneGracePeriod` | `TimeSpan` | 24 h | Minimum age before a tombstone is eligible for compaction. `InfiniteTimeSpan` disables compaction. |
| `SoftDeleteDuration` | `TimeSpan` | 72 h | Retention window after soft-delete before purge fires. |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` | Minimum time between delta refreshes in `LeafCacheGrain`. Zero means refresh on every read. |
| `PrefetchKeysScan` | `bool` | `false` | When `true`, `KeysAsync` pre-fetches the next page from each shard in the background. Overridable per-call via the `prefetch` parameter. |
| `PrefetchEntriesScan` | `bool` | `false` | When `true`, `EntriesAsync` pre-fetches the next page from each shard in the background. Gated separately from `PrefetchKeysScan` because entry pages carry `byte[]` values and increase in-flight memory by `shardCount × KeysPageSize × avgValueSize`. Overridable per-call via the `prefetch` parameter. |
| `AutoSplitEnabled` | `bool` | `true` | Master switch for autonomic shard splitting. When `false`, `HotShardMonitorGrain` will not trigger any splits. |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Ops/sec on a single shard that triggers an adaptive split. |
| `HotShardSampleInterval` | `TimeSpan` | 30 s | How often `HotShardMonitorGrain` polls shard hotness counters. |
| `HotShardSplitCooldown` | `TimeSpan` | 2 min | Minimum time between consecutive splits of the same shard. |
| `MaxConcurrentAutoSplits` | `int` | 2 | Maximum in-flight adaptive splits per tree. |
| `SplitDrainBatchSize` | `int` | 1024 | Entries per batch during the shadow-write drain phase of a split. |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 s | Minimum tree age before the hot-shard monitor begins sampling. Prevents splits during initial bulk-load bursts. |
| `MaxScanRetries` | `int` | 3 | Maximum bounded-retry passes for `CountAsync` / `KeysAsync` / `EntriesAsync` when the shard topology changes mid-scan. |
| `CursorIdleTtl` | `TimeSpan` | 48 h | Sliding idle timeout for stateful cursors. `InfiniteTimeSpan` disables auto-cleanup. |
| `AtomicWriteRetention` | `TimeSpan` | 48 h | Retention window for completed `SetManyAtomicAsync` saga state (idempotency window). `InfiniteTimeSpan` disables auto-cleanup. |

## Serializable Types

All serializable types — and every grain interface, including the public `ILattice` — carry stable `[Alias]` attributes (prefixed with `ol.`) to ensure wire-format and grain-manifest compatibility across versions and prevent collisions when Lattice is hosted alongside other Orleans grains. Alias constants live in `TypeAliases` and must never be renamed or removed: they are part of the public wire format.

Public types below are annotated with `[EditorBrowsable(EditorBrowsableState.Never)]` — they remain `public` for Orleans code generation but are hidden from IntelliSense because they are internal implementation details not intended for direct use.

| Type | Alias | Visibility | Description |
|------|-------|------------|-------------|
| `HybridLogicalClock` | `ol.hlc` | public (hidden) | Hybrid logical clock for conflict-free timestamps. See [State Primitives](state-primitives.md). |
| `LwwValue<T>` | `ol.lwv` | public (hidden) | Last-writer-wins register. |
| `VersionVector` | `ol.vv` | public (hidden) | Causal version vector (pointwise-max merge). |
| `StateDelta` | `ol.sd` | public (hidden) | Delta of changed entries for replication. |
| `SplitResult` | `ol.sr` | public (hidden) | Result of a node split (promoted key + new sibling). |
| `KeysPage` | `ol.kp` | public (hidden) | Paginated batch of keys from a shard scan. |
| `EntriesPage` | `ol.ep` | public (hidden) | Paginated batch of key-value entries from a shard scan. |
| `TreeRegistryEntry` | `ol.tre` | public (hidden) | Per-tree metadata record (config overrides, physical tree alias). |
| `SnapshotMode` | `ol.snm` | public | Enum: `Offline`, `Online`. Controls source-tree availability during a snapshot. |
| `TreeResizeState` | `ol.trs` | internal | Persistent state tracking resize progress across phases. |
| `ResizePhase` | `ol.rp` | internal | Enum: `Snapshot`, `Swap`, `Cleanup`. |
| `TreeSnapshotState` | `ol.tss` | internal | Persistent state tracking snapshot progress across shards. |
| `SnapshotPhase` | `ol.snp` | internal | Enum: `Locking`, `Copying`, `Unlocking`, `Completed`. |
| `TreeDeletionState` | `ol.tds` | internal | Persistent state for soft-delete / purge tracking. |
| `TreeMergeState` | `ol.tms` | internal | Persistent state tracking merge progress across source shards. |
| `CasResult` | `ol.cas` | public (hidden) | Result of a compare-and-swap operation (success, current version, optional split). |
| `VersionedValue` | `ol.vvl` | public (hidden) | A `byte[]` value paired with its `HybridLogicalClock` version for CAS reads. |
| `Versioned<T>` | `ol.ver` | public (hidden) | A typed value paired with its `HybridLogicalClock` version (used by typed extensions). |
| `ShardHotness` | `ol.sh` | public (hidden) | Volatile shard hotness counters (reads, writes, window) returned by `IShardRootGrain.GetHotnessAsync()`. |
| `ShardMap` | `ol.sm` | public (hidden) | Per-tree mapping from virtual shard slots to physical shard indices. Persisted on `TreeRegistryEntry`. |
| `RoutingInfo` | `ol.ri` | public (hidden) | Per-activation routing snapshot returned by `ILattice.GetRoutingAsync()`: physical tree id plus the resolved `ShardMap`. Used by infrastructure (e.g. streaming bulk load) that must route to the same physical shards as `LatticeGrain`. |
| `ShardCountResult` | `ol.scr` | internal | Per-shard count plus the set of virtual slots the shard observed in its `MovedAwaySlots` table during the count. Used by `IShardRootGrain.CountWithMovedAwayAsync` to coordinate strongly-consistent scans during shard splits. |

## Internal Grain Access Control

Lattice exposes a single public entry-point — `ILattice`. All other grain interfaces (`IShardRootGrain`, `IBPlusLeafGrain`, `IBPlusInternalGrain`, `ILeafCacheGrain`, `ILatticeRegistry`, `ITombstoneCompactionGrain`, `ITreeDeletionGrain`, `ITreeResizeGrain`, `ITreeSnapshotGrain`) are internal implementation details.

Two mechanisms prevent accidental direct use of internal grains:

1. **IntelliSense exclusion** — All internal grain interfaces and public serializable model types (e.g. `HybridLogicalClock`, `SplitResult`, `KeysPage`, `LatticeConstants`) are annotated with `[EditorBrowsable(EditorBrowsableState.Never)]`, hiding them from auto-complete in IDEs. They remain `public` for Orleans code generation but are invisible during normal development.

2. **Grain call filters** — `AddLattice` registers a pair of grain call filters:
   - An **outgoing filter** (`LatticeCallContextFilter`) resolves the current grain context at call time via `IGrainContextAccessor` and checks whether the calling grain implements a Lattice interface (via direct `Type.IsAssignableFrom`). If so, it stamps the outgoing call with a `RequestContext` token. If the caller is **not** a Lattice grain, the filter **clears** the token to prevent it from leaking through a non-Lattice intermediary.
   - An **incoming filter** (`InternalGrainGuardFilter`) rejects calls to internal Lattice grains that do not carry the token, throwing `InvalidOperationException`.

   External client calls never carry the token and are blocked. Calls from non-Lattice grains co-hosted in the same silo are also blocked because the outgoing filter only stamps calls originating from Lattice grains. Both filters cache `Type.IsAssignableFrom` results in a `ConcurrentDictionary<Type, bool>`, so repeated calls from the same grain type cost a single dictionary lookup (~20 ns) rather than a linear scan. All type checks use direct .NET type comparison — there is no dependency on Orleans grain-type name conventions.

> **Note:** The token is not a security credential — it prevents accidental misuse, not malicious access. A determined caller within the silo process could set the `RequestContext` value manually.
