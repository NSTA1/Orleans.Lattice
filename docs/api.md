# Lattice Public API Reference

Lattice is a distributed, CRDI-bsaed B+ tree built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It exposes a single entry-point grain interface, `ILattice`, that routes operations to sharded internal storage. See [Architecture](architecture.md) for the full grain layer design.

## Setup

Install the NuGet package:

```shell
dotnet add package Orleans.Lattice
```

Add the namespace import:

```csharp
using Orleans.Lattice;
```

Register Lattice on the silo, providing a storage provider:

```csharp
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));
```

Per-tree options can be configured with the `ConfigureLattice` extension method (see [Configuration](configuration.md) for the full options reference and per-tree override semantics):

```csharp
siloBuilder.ConfigureLattice("my-tree", o =>
{
    o.MaxLeafKeys = 256;
    o.ShardCount = 32;
});
```

## `ILattice`

Obtain an `ILattice` grain from the grain factory using the tree's logical name as the string key:

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");
```

### Runtime Operations

These methods are used during normal application flow to read, write, and enumerate data. They are safe to call concurrently and do not affect tree availability.

#### Single-Key

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync` | `Task<byte[]?> GetAsync(string key)` | Returns the value for `key`, or `null` if absent or tombstoned. Reads are served via a [stateless-worker cache](caching.md); `CacheTtl` controls how long cached entries are served before refreshing from the primary leaf. |
| `GetWithVersionAsync` | `Task<VersionedValue> GetWithVersionAsync(string key)` | Returns the value and its `HybridLogicalClock` version for `key`, or a default `VersionedValue` with `null` value and zero version when absent/tombstoned. Use the returned version with `SetIfVersionAsync` for optimistic concurrency (CAS). Reads directly from the primary leaf (not cached) to ensure version freshness. |
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` if `key` exists and is not tombstoned. |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `SetIfVersionAsync` | `Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)` | Sets `key` to `value` only if the entry's current `HybridLogicalClock` matches `expectedVersion`. Returns `true` if the write was applied, `false` if the version did not match (another writer updated the key). For a new key, pass `HybridLogicalClock.Zero` as the expected version. Enables safe read-modify-write patterns without distributed locks. |
| `GetOrSetAsync` | `Task<byte[]?> GetOrSetAsync(string key, byte[] value)` | Sets `key` to `value` only if the key does not already exist (or is tombstoned). Returns the existing value when the key is live, or `null` when the value was newly written. Avoids a read-then-write roundtrip by short-circuiting at the leaf grain. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if it was live. Tombstones are removed by [background compaction](tombstone-compaction.md). |

#### Batch

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel across shards. Missing/tombstoned keys are omitted from the result. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Inserts or updates multiple entries in parallel across shards. **Not atomic** — a partial failure leaves the batch half-applied with no compensating rollback. Use `SetManyAtomicAsync` when all-or-nothing semantics are required. |
| `SetManyAtomicAsync` | `Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries)` | Atomically writes multiple entries via a saga coordinator grain (F-031). Reads each key's pre-saga value up front, applies writes sequentially, and compensates (reverts) already-committed entries with a freshly-ticked HLC if a subsequent write fails — LWW merge guarantees the rollback wins. Crash-recovery is reminder-driven. Throws `ArgumentException` on duplicate keys or null values, and `InvalidOperationException` after compensation completes for a failed write. **Partial-visibility window:** readers between the first and last committed write may see a partial view of the batch; layer version-guarded reads (`GetWithVersionAsync` + `SetIfVersionAsync`) on top for strict isolation. After completion, saga state is retained for `LatticeOptions.AtomicWriteRetention` (default 48h) for idempotent re-invocation, then automatically cleared by a retention reminder. |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)` | Tombstones all live keys in the lexicographic range [`startInclusive`, `endExclusive`] by walking the leaf chain in each shard. Returns the total number of keys tombstoned. |
| `CountAsync` | `Task<int> CountAsync()` | Returns the total number of live (non-tombstoned) keys across all shards. **Strongly consistent during shard splits**: each physical shard is asked to count only the virtual slots it currently owns per the authoritative `ShardMap` (via `IShardRootGrain.CountForSlotsAsync`), then the map version is re-read and the count is retried on any version change. Every virtual slot is therefore counted exactly once — against whichever shard the map identifies as its current owner — regardless of where the split coordinator is in its per-phase machine. Bounded by `LatticeOptions.MaxScanRetries` (default 3); throws `InvalidOperationException` if the topology keeps mutating beyond the retry budget. |
| `CountPerShardAsync` | `Task<IReadOnlyList<int>> CountPerShardAsync()` | Returns the number of live keys in each shard as an ordered list (index = shard index). Uses the same per-slot routing as `CountAsync` so the per-shard counts are also topology-consistent with the observed `ShardMap` snapshot. Useful for diagnostics and load-balancing analysis. |

#### Enumeration

| Method | Signature | Description |
|--------|-----------|-------------|
| `KeysAsync` | `IAsyncEnumerable<string> KeysAsync(string? startInclusive, string? endExclusive, bool reverse, bool? prefetch)` | Streams live keys in strict lexicographic order via paginated k-way merge across shards. When `prefetch` is `true` (or `null` with `PrefetchKeysScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed, hiding grain-call latency during large scans. **Strongly consistent and strictly ordered during shard splits**: when a shard reports moved-away slots or the `ShardMap` version advances mid-scan, the orchestrator drains the affected slots from their current owners into a sorted in-memory cursor and injects it into the same k-way merge priority queue, so output remains globally sorted end-to-end. A per-call `HashSet<string>` suppresses duplicates across pre- and post-swap views. Reconciliation is bounded by `LatticeOptions.MaxScanRetries`; throws `InvalidOperationException` if the topology keeps mutating beyond that budget (see [Scan reliability](#scan-reliability) below). See [`docs/shard-splitting.md`](shard-splitting.md). |
| `EntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive, string? endExclusive, bool reverse)` | Streams live key-value entries in strict lexicographic key order via paginated k-way merge across shards. Useful for exports, migrations, and analytics without a separate `GetAsync` per key. **Strongly consistent and strictly ordered during shard splits** with the same reconciliation-cursor injection algorithm as `KeysAsync`. Subject to the same `InvalidOperationException` contract on retry exhaustion — see [Scan reliability](#scan-reliability). |

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

```csharp
siloBuilder.ConfigureLattice(o => o.CursorIdleTtl = TimeSpan.FromHours(6));
```

Minimum effective interval is **1 minute** (Orleans reminder
granularity); smaller values are clamped to the floor. Set
`CursorIdleTtl = Timeout.InfiniteTimeSpan` to disable automatic
cleanup (cursors then live until `CloseCursorAsync` is called).

#### Example — resumable export across a silo failover

```csharp
var cursorId = await tree.OpenEntryCursorAsync();
while (true)
{
    var page = await tree.NextEntriesAsync(cursorId, pageSize: 500);
    foreach (var (k, v) in page.Entries)
        await sink.WriteAsync(k, v);
    if (!page.HasMore) break;
}
await tree.CloseCursorAsync(cursorId);
```

If the client crashes mid-export it can persist the `cursorId` and
resume on restart — the cursor grain reactivates on demand and
continues from its persisted last-yielded key.

#### Example — bounded, resumable range delete

```csharp
var cursorId = await tree.OpenDeleteRangeCursorAsync("2024/", "2025/");
int total = 0;
while (true)
{
    var progress = await tree.DeleteRangeStepAsync(cursorId, maxToDelete: 1000);
    total = progress.DeletedTotal;
    if (progress.IsComplete) break;
}
await tree.CloseCursorAsync(cursorId);
logger.LogInformation("Deleted {Count} keys.", total);
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
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | Resizes the tree by creating an offline snapshot with new sizing into a new physical tree, swapping the tree alias, and soft-deleting the old data. The tree is unavailable during the snapshot phase but immediately accessible after the swap. Undoable within the `SoftDeleteDuration` retention window. See [Tree Sizing](tree-sizing.md#resizing-an_existing_tree). ⚠️ **Takes the tree offline** during the snapshot phase. |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize by recovering the old physical tree, removing the alias, restoring the original registry configuration, and deleting the new snapshot tree. Only available while the old tree is still within its `SoftDeleteDuration` window (before purge completes). |

#### Merge

| Method | Signature | Description |
|--------|-----------|-------------|
| `MergeAsync` | `Task MergeAsync(string sourceTreeId)` | Merges all entries from `sourceTreeId` into this tree using LWW semantics, preserving original timestamps. For each key present in both trees, the entry with the higher `HybridLogicalClock` timestamp wins. Tombstones are also merged, ensuring deletes propagate correctly. The source tree remains unmodified. Source and target trees may have different shard counts — entries are re-hashed to the correct target shard during merge. See [Architecture](architecture.md) for details on the CRDT merge primitive
