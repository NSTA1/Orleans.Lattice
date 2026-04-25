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

> Prefer a runnable end-to-end example? See [Samples](samples.md) for in-process console apps that stand up a silo and exercise the tree.

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
await foreach (var key in tree.ScanKeysAsync(startInclusive: "user:", endExclusive: "user;"))
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

Every method on `ILattice` — including scans (via the
`ScanKeysAsync` / `ScanEntriesAsync` extension wrappers),
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
| `SetAsync` (TTL) | `Task SetAsync(string key, byte[] value, TimeSpan ttl)` | Inserts or updates the value for `key` with a time-to-live. The entry is treated as tombstoned on all reads (`GetAsync`, `ExistsAsync`, `GetManyAsync`, `ScanKeysAsync`, `ScanEntriesAsync`, `CountAsync`, etc.) once `ttl` has elapsed since the server-side write, and is reaped by background tombstone compaction after the configured `LatticeOptions.TombstoneGracePeriod`. The TTL is converted to an absolute UTC expiry on the silo handling the call, so clock skew between clients does not shift individual entries' lifetimes. Throws `ArgumentOutOfRangeException` when `ttl` is zero or negative. Typed overload: `SetAsync<T>(this ILattice, string, T, TimeSpan, ILatticeSerializer<T>)`. |
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

Long-running scans use the resilient extension wrappers
`ScanKeysAsync` / `ScanEntriesAsync` on `ILattice`. The wrapper tracks
the last yielded key and — if the remote enumerator on the
orchestrator grain is reclaimed mid-scan (silo failover, cold start,
idle expiry, scale-down) — transparently reopens with a tightened
bound and resumes. Because the tree is strongly consistent and
key-ordered, the result stream is deterministic: no duplicates, no
gaps, original order preserved. The retry budget defaults to
`LatticeExtensions.DefaultScanReconnectAttempts` (8) and can be
overridden per call via `maxAttempts`; the first reconnect is
immediate, subsequent reconnects apply a short linear backoff
(10 ms × attempt, capped at 100 ms).

| Method | Signature | Description |
|--------|-----------|-------------|
| `ScanKeysAsync` | `IAsyncEnumerable<string> ScanKeysAsync(this ILattice, string? startInclusive, string? endExclusive, bool reverse, bool? prefetch, int? maxAttempts)` | Streams live keys in strict lexicographic order via paginated k-way merge across shards. When `prefetch` is `true` (or `null` with `PrefetchKeysScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed. **Strongly consistent and strictly ordered during shard splits**: when a shard reports moved-away slots or the `ShardMap` version advances mid-scan, the orchestrator drains the affected slots from their current owners into a sorted in-memory cursor and injects it into the same k-way merge priority queue, so output remains globally sorted end-to-end. A per-call `HashSet<string>` suppresses duplicates across pre- and post-swap views. Reconciliation is bounded by `LatticeOptions.MaxScanRetries`; throws `InvalidOperationException` if the topology keeps mutating beyond that budget (see [`docs/lattice/shard-splitting.md`](shard-splitting.md)). |
| `ScanEntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(this ILattice, string? startInclusive, string? endExclusive, bool reverse, bool? prefetch, int? maxAttempts)` | Streams live key-value entries in strict lexicographic key order. When `prefetch` is `true` (or `null` with `PrefetchEntriesScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed. Because entries carry `byte[]` values, pre-fetched pages hold extra in-flight memory proportional to `shardCount × KeysPageSize × avgValueSize`, so this flag is gated separately from `PrefetchKeysScan`. Useful for exports, migrations, and analytics without a separate `GetAsync` per key. **Strongly consistent and strictly ordered during shard splits** with the same reconciliation-cursor injection algorithm as `ScanKeysAsync`. Subject to the same `InvalidOperationException` contract on retry exhaustion — see [Scan reliability](#scan-reliability). |

### Scan reliability

`CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` use bounded
optimistic retry (`LatticeOptions.MaxScanRetries`, default 3) to
reconcile against concurrent shard splits. If the shard topology keeps
mutating after every reconciliation step, the scan throws
`InvalidOperationException` rather than returning a silently
incomplete result.

Under the default configuration (`MaxConcurrentAutoSplits = 2`,
`HotShardSplitCooldown = 2 minutes`) this is not a realistic
operational concern: splits are rate-limited well below the retry
budget. Point operations (`GetAsync` / `SetAsync` / `DeleteAsync` /
`SetIfVersionAsync`) transparently retry on `StaleShardRoutingException`
during shard-map swaps and never surface this exception to callers.

The resilient scan wrappers additionally recover from
`Orleans.Runtime.EnumerationAbortedException` — raised when the
remote enumerator on the orchestrator grain is reclaimed mid-scan
(silo failover, idle expiry). This is distinct from topology churn
and is bounded separately by the `maxAttempts` parameter on
`ScanKeysAsync` / `ScanEntriesAsync` (default 8; see
`LatticeExtensions.DefaultScanReconnectAttempts`)

Callers running multi-minute export scans in aggressively split-prone
workloads have three options:

1. Raise `LatticeOptions.MaxScanRetries` — cheap, addresses transient
   topology churn.
2. Wrap the scan in an application-level retry with exponential backoff
   and resume from the last successfully yielded key (using
   `startInclusive`).
3. Use **stateful cursors** — `OpenKeyCursorAsync` / `OpenEntryCursorAsync` return a server-side checkpointed iterator
   that survives topology changes, silo failover, and client restarts
   without caller retry code. See
   [Stateful cursors](#stateful-cursors) below.

### Stateful Cursors

`ILattice` exposes a stateful cursor API for long-running scans and
resumable range deletes that survive silo failovers, client restarts,
and topology changes (shard splits). Unlike the stateless
`ScanKeysAsync` / `ScanEntriesAsync` / `DeleteRangeAsync` methods — 
bounded by `LatticeOptions.MaxScanRetries` — a cursor checkpoints its
progress server-side after every page: a new grain activation reads
its persisted state and resumes from the last yielded key.

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

- Each step goes through the normal `ScanKeysAsync` / 
  `ScanEntriesAsync` / `DeleteRangeAsync` path, so strict lexicographic
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

#### Diagnostics

| Method | Signature | Description |
|--------|-----------|-------------|
| `DiagnoseAsync` | `Task<TreeDiagnosticReport> DiagnoseAsync(bool deep = false, CancellationToken cancellationToken = default)` | Returns a per-shard health snapshot — depth, root-is-leaf, live-key count, tombstone count (deep only), hotness counters, ops/sec, and split/bulk state — plus a bounded ring buffer of recent adaptive-split events. Repeated calls within `LatticeOptions.DiagnosticsCacheTtl` (default 5 s) are served from an in-memory cache; shallow and deep reports are cached independently. When `deep: true`, each shard walks its leaf chain to aggregate tombstone counts (one RPC per leaf). See [Diagnostics](diagnostics.md) for the full DTO reference and [Consistency](consistency.md#maintenance-operations) for the consistency classification. |

```csharp verify
var report = await tree.DiagnoseAsync(deep: true, cancellationToken);
Console.WriteLine($"Tree {report.TreeId}: {report.TotalLiveKeys} live, {report.TotalTombstones} tombstones across {report.ShardCount} shards.");
foreach (var shard in report.Shards)
{
    Console.WriteLine($"  shard {shard.ShardIndex}: depth={shard.Depth}, live={shard.LiveKeys}, ops/s={shard.OpsPerSecond:F1}");
}
```

#### Events

| Method | Signature | Description |
|--------|-----------|-------------|
| `SetPublishEventsEnabledAsync` | `Task SetPublishEventsEnabledAsync(bool? enabled, CancellationToken cancellationToken = default)` | Sets or clears the per-tree override for event publication. `true` forces publication on, `false` forces it off, `null` removes the override so the tree inherits the silo-wide `LatticeOptions.PublishEvents` default. The override is persisted on the tree's registry entry and survives silo restarts. Propagation is best-effort: the handling activation observes the change immediately; other activations refresh within a few seconds. See [Events — Per-tree override](events.md#per-tree-override) and [Configuration → `PublishEvents`](configuration.md#publishevents). |

```csharp verify
// Force events on for this tree regardless of the silo default:
await tree.SetPublishEventsEnabledAsync(true, cancellationToken);

// Clear the override and inherit whatever the silo is configured for:
await tree.SetPublishEventsEnabledAsync(null, cancellationToken);
```

For subscribing to the published events on the cluster client, see `SubscribeToEventsAsync` under [`LatticeExtensions`](#latticeextensions).

## Mutation observers

`IMutationObserver` is a grain-side extensibility hook invoked synchronously after every durably-committed mutation, before the grain method returns to the caller. It is the primary seam for building replication write-ahead logs, change-feed producers, and external audit consumers without reaching into grain internals.

> **Mutation observers vs. [tree events](events.md).** Both surface "something changed" notifications, but they target different consumers:
>
> - **`IMutationObserver` is in-process, synchronous, and carries the full value bytes.** It runs on the grain's scheduler before the write returns, so the caller's latency is the observer's latency. Use it when a downstream component (replication WAL, outbox) must see the *value* at commit time and must be on the write path — typically another library, not application code.
> - **Tree events are out-of-process, asynchronous, and metadata-only** (key + kind + HLC — *no value bytes*). They ride Orleans Streams and are fire-and-forget from the grain's perspective. Use them for UI updates, cache invalidation, dashboards, audit projections — anything that can tolerate at-most-once delivery and is willing to `GetAsync` the value itself if needed.
>
> A single write typically fires both: the observer first (inline, with value), then an event (post-commit, metadata-only). Choose observers when you need the value and the write path; choose events for everything else.

Register one or more observers in the silo DI container — they are resolved as `IEnumerable<IMutationObserver>`, so multiple can coexist. When no observer is registered the hook is zero-cost: the grain checks `HasObservers` and short-circuits before allocating the payload.

```csharp
// Implement IMutationObserver as a singleton service:
public sealed class MyReplicationObserver : IMutationObserver
{
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken ct)
    {
        // Inspect mutation.TreeId, mutation.Kind, mutation.Key, mutation.Value,
        // mutation.Timestamp, mutation.IsTombstone, mutation.ExpiresAtTicks,
        // and for DeleteRange also mutation.EndExclusiveKey.
        return Task.CompletedTask;
    }
}
```

Register it on the silo:

```csharp
siloBuilder.ConfigureServices(services =>
    services.AddSingleton<IMutationObserver, MyReplicationObserver>());
```

### Emission points and shape

| Mutation | Emitted by | `Kind` | Notes |
|----------|------------|--------|-------|
| `SetAsync` (all overloads) | `BPlusLeafGrain` | `Set` | One event per key. `Value` is the committed bytes; `Timestamp` is the stamped HLC; `ExpiresAtTicks` carries the TTL deadline verbatim (or `0` for no-expiry). |
| `DeleteAsync` | `BPlusLeafGrain` | `Delete` | One event per tombstoned key. `IsTombstone` is `true`, `Value` is `null`. Absent-key deletes publish nothing. |
| `DeleteRangeAsync` | `ShardRootGrain` | `DeleteRange` | One event **per shard** that received the range (not per key and not per user call), emitted **even when the shard matched zero live keys** so replication consumers propagate the range to peer clusters unconditionally. A single `ILattice.DeleteRangeAsync` call against an N-shard tree produces up to N identical-payload `DeleteRange` mutations; consumers that need exactly-once delivery per user call must dedup on `(TreeId, Key, EndExclusiveKey)`. `Key` carries `startInclusive`; `EndExclusiveKey` carries `endExclusive`; `Timestamp` is `HybridLogicalClock.Zero` because a single range may produce many per-leaf HLCs. |

`MergeEntriesAsync` / `MergeManyAsync` and other internal convergence paths deliberately do not publish — they are downstream of the originating write (which already published) and surfacing them would double-count.

### Failure semantics

The dispatcher wraps every observer call in a `try` / `catch`. Exceptions are logged as a warning with `ObserverType`, `TreeId`, `Key`, and `Kind`, and the dispatcher continues with the remaining observers — a faulty observer cannot short-circuit its peers or the write path. Strict-vs-best-effort propagation is the consumer's concern: an observer that wants strict semantics can itself queue durably and rethrow from a background flush, but the grain write is already durable by the time the hook fires.

### Ordering and durability

Observers see a mutation only after `PersistAsync()` has returned — the mutation is guaranteed durable before any observer is invoked. The hook runs on the grain's single-threaded scheduler, so observer invocations for a given key are serialised relative to subsequent mutations on that key.

### Pitfalls and recommended pattern

`IMutationObserver` is an intentionally thin, inline hook — it is not a safe place to do real I/O. The following sharp edges are inherent to the design; implementations that ignore them will add latency to every write in the silo.

- **Every millisecond inside the observer is a millisecond added to the caller's write latency**, because the hook runs on the grain's single-threaded scheduler and is awaited before the grain method returns. Do not issue synchronous HTTP calls, database writes, or cross-cluster sends directly from `OnMutationAsync`.
- **Exceptions are logged and swallowed.** The write has already been persisted and cannot be rolled back; if the observer throws, the caller is never told. Consumers that need at-least-once delivery must durably record the mutation themselves (local WAL / outbox) before returning and retry out-of-band.
- **Merge paths are silent by design.** `MergeEntriesAsync` / `MergeManyAsync`, shard-split shadow-forward, saga compensation rollback, and snapshot / restore bulk loads do not fire the hook — they are replays of mutations already published at their origin.
- **`DeleteRange` fires once per shard, not per user call**, with `Key = startInclusive`, `EndExclusiveKey = endExclusive`, and `Timestamp = HybridLogicalClock.Zero`. A single `ILattice.DeleteRangeAsync` call against an N-shard tree produces up to N identical-payload `DeleteRange` mutations — idempotent by design but noisy. Consumers that need exactly-once delivery per user call must dedup on `(TreeId, Key, EndExclusiveKey)`; observers that need per-key granularity must expand the range themselves.
- **Ordering is per-grain, not global.** Successive mutations on the same key observe strict order; mutations across different keys, leaves, or trees do not. Impose a global order downstream if you need one (the per-mutation HLC is designed for this).
- **A single registration fires for every tree in the silo.** In multi-tenant deployments, filter on `LatticeMutation.TreeId` in the first line of `OnMutationAsync` before doing any real work.

**Recommended pattern.** Enqueue the mutation onto a bounded `Channel<LatticeMutation>` and drain it from a background `IHostedService`. The grain write path then only pays the cost of a channel write; all real I/O (replication, change-feed, audit) runs off the grain scheduler and can apply its own batching, retry, and backpressure policies independently.

```csharp
public sealed class QueueingObserver(Channel<LatticeMutation> queue) : IMutationObserver
{
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken ct)
    {
        // TryWrite is non-blocking; the drain service handles backpressure.
        queue.Writer.TryWrite(mutation);
        return Task.CompletedTask;
    }
}
```

## Metrics

Orleans.Lattice publishes `System.Diagnostics.Metrics` instruments on a single static meter named `orleans.lattice`, exposed via `Orleans.Lattice.LatticeMetrics`. Instruments are grouped into five tiers: shard-level ops (`shard.reads`, `shard.writes`, `shard.splits_committed`), leaf-level latencies and counters (`leaf.write.duration`, `leaf.scan.duration`, `leaf.compaction.duration`, `leaf.tombstones.created`, `leaf.tombstones.reaped`, `leaf.tombstones.expired`, `leaf.splits`), the read cache (`cache.hits`, `cache.misses`), saga / coordinator / lifecycle outcomes (`atomic_write.completed`, `coordinator.completed`, `tree.lifecycle`), and events / configuration (`events.published`, `events.dropped`, `config.changed`). Every measurement is tagged with `tree` (logical tree id); shard instruments additionally carry `shard` (physical shard index), scan histograms carry `operation` (`keys` or `entries`), saga / coordinator / lifecycle / event counters carry `outcome`, `kind`, or `reason`, and `config.changed` carries `config` (e.g. `publish_events`). Subscribe once with `.AddMeter("orleans.lattice")` on your OpenTelemetry `MeterProviderBuilder`; see [Metrics](metrics.md) for the full instrument catalog, tag conventions, and registration example.

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
| `SubscribeToEventsAsync(this ILattice, IClusterClient, Func<LatticeTreeEvent, Task>, string providerName = "Default", CancellationToken)` | Subscribes to the per-tree `LatticeTreeEvent` stream on the cluster client. Returns a `StreamSubscriptionHandle<LatticeTreeEvent>`; call `UnsubscribeAsync()` on it to stop receiving events. Throws `InvalidOperationException` when `providerName` is not registered on the client. See [Events](events.md). |

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
| `ScanEntriesAsync<T>` | `IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(this ILattice, ILatticeSerializer<T>, string?, string?, bool, bool?, int?)` | Streams live entries, deserializing values via the provided serializer. Transparently recovers from `Orleans.Runtime.EnumerationAbortedException` — see [Scan reliability](#scan-reliability). |

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
| `KeysPageSize` | `int` | 512 | Keys per page in enumeration pagination. |
| `TombstoneGracePeriod` | `TimeSpan` | 24 h | Minimum age before a tombstone is eligible for compaction. `InfiniteTimeSpan` disables compaction. |
| `SoftDeleteDuration` | `TimeSpan` | 72 h | Retention window after soft-delete before purge fires. |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` | Minimum time between delta refreshes in `LeafCacheGrain`. Zero means refresh on every read. |
| `PrefetchKeysScan` | `bool` | `false` | When `true`, `ScanKeysAsync` pre-fetches the next page from each shard in the background. Overridable per-call via the `prefetch` parameter. |
| `PrefetchEntriesScan` | `bool` | `false` | When `true`, `ScanEntriesAsync` pre-fetches the next page from each shard in the background. Gated separately from `PrefetchKeysScan` because entry pages carry `byte[]` values and increase in-flight memory by `shardCount × KeysPageSize × avgValueSize`. Overridable per-call via the `prefetch` parameter. |
| `AutoSplitEnabled` | `bool` | `true` | Master switch for autonomic shard splitting. When `false`, `HotShardMonitorGrain` will not trigger any splits. |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Ops/sec on a single shard that triggers an adaptive split. |
| `HotShardSampleInterval` | `TimeSpan` | 30 s | How often `HotShardMonitorGrain` polls shard hotness counters. |
| `HotShardSplitCooldown` | `TimeSpan` | 2 min | Minimum time between consecutive splits of the same shard. |
| `MaxConcurrentAutoSplits` | `int` | 2 | Maximum in-flight adaptive splits per tree. |
| `SplitDrainBatchSize` | `int` | 1024 | Entries per batch during the shadow-write drain phase of a split. |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 s | Minimum tree age before the hot-shard monitor begins sampling. Prevents splits during initial bulk-load bursts. |
| `MaxScanRetries` | `int` | 3 | Maximum bounded-retry passes for `CountAsync` / `ScanKeysAsync` / `ScanEntriesAsync` when the shard topology changes mid-scan. |
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

Lattice exposes a single public entry-point — `ILattice`. All other grain interfaces (`IShardRootGrain`, `IBPlusLeafGrain`, `IBPlusInternalGrain`, `ILeafCacheGrain`, `ILatticeRegistry`, `ITombstoneCompactionGrain`, `ITreeDeletionGrain`, `ITreeResizeGrain`, `ITreeSnapshotGrain`, `ITreeMergeGrain`, `ITreeShardSplitGrain`, `IHotShardMonitorGrain`, `IAtomicWriteGrain`, `ILatticeCursorGrain`, `ITreeReshardGrain`, `ILatticeStats`) are declared `internal` and are not visible to consumer assemblies. The C# type system enforces the boundary at compile time — external code cannot name, reference, or invoke these interfaces. Internal DTOs associated with these interfaces (e.g. `SplitResult`, `KeysPage`, `EntriesPage`, `LatticeConstants`) are also `internal`.

A small number of types remain `public` because they appear directly on the `ILattice` surface or its typed extensions: `HybridLogicalClock`, `VersionedValue`, `Versioned<T>`, `RoutingInfo`, and `ShardMap` (transitively via `RoutingInfo`).

## Reserved Tree Prefixes

Lattice reserves tree-name prefixes for internal use. User code must never address a tree whose id starts with `_lattice_` — including (but not limited to) `_lattice_trees` (the registry) and `_lattice_replog_` (reserved for the forthcoming replication package's write-ahead-log trees).

The reservation is enforced as an **unbypassable** three-layer guarantee:

1. **Registry guard.** `ILatticeRegistry.RegisterAsync` / `UpdateAsync` throw `ArgumentException` for any `treeId` starting with the reserved prefix.
2. **Public-surface guard.** Every method on `ILattice` — reads, writes, scans, cursors, counts, diagnostics, and tree-lifecycle operations — throws `InvalidOperationException` with an actionable remediation message when the activation's primary key starts with the reserved prefix. Reads are as guarded as writes because namespace enumeration is as sensitive as mutation for a reserved tree.
3. **No internal bypass is visible.** The library's own code that legitimately bootstraps system trees resolves an internal grain interface that is not visible to external assemblies. The C# type system therefore makes the public guard unbypassable from user code.

The guard rejects only leading matches. A tree id such as `"my_lattice_table"` (reserved prefix appears in the middle, not at the start) is valid.
