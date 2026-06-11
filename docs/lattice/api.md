# Lattice Public API Reference

This document is the **contract** for what each public type and method
on `Orleans.Lattice` does. It states behaviour in caller-visible terms
only - signature, return value, exceptions, and observable effect. It
does not describe how the library delivers any of those guarantees.

For the consistency classification (linearizable / strongly consistent /
snapshot / eventually consistent) of every operation, see
[Consistency](consistency.md). For implementation details, follow the
topic cross-references in each section.

> **Compression** - `ILatticeCompressor`, `LatticeCompression`, `ZstdLatticeCompressor`, and `LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor` are part of the public API surface. They are documented in [`compression.md`](compression.md), which is the source of truth for registration, the tag-space partitioning, and the worked example for plugging in a custom algorithm.

## Setup

Install the NuGet package:

```shell
dotnet add package Orleans.Lattice
```

Import the namespace:

```csharp verify
using Orleans.Lattice;
```

Register Lattice on the silo, providing a grain-storage provider. The
callback receives the silo builder and the provider name that Lattice
grains will resolve against; register any Orleans grain-storage
provider under that name.

In-memory (development / tests):

```csharp verify
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));
```

Azure Table Storage (production). Requires the
`Microsoft.Orleans.Persistence.AzureStorage` and
`Orleans.Lattice.Storage.AzureTable` NuGet packages. The first
configures the grain-storage provider for tree state; the second
replaces the default in-memory WAL with a durable Azure Table-backed
provider:

```csharp verify
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Storage.AzureTable;

var connectionString = "UseDevelopmentStorage=true";

siloBuilder.AddLattice((services, storageName) =>
{
    services.AddAzureTableGrainStorage(storageName, options =>
    {
        options.TableServiceClient = new TableServiceClient(connectionString);
    });
});

siloBuilder.AddAzureTableWalStorage(o =>
{
    o.ConnectionString = connectionString;
});
```

Managed-identity deployments can either configure each extension
independently against the same storage account, or - for the canonical
"one credential, one client" shape - share a single pre-built
`TableServiceClient` between them: `AddAzureTableGrainStorage` accepts
a pre-built `TableServiceClient(serviceUri, credential)` via its
`options.TableServiceClient` slot, and `AddAzureTableWalStorage`
accepts the same instance via `options.ServiceClient`. Hosts that prefer
per-extension wiring can instead configure `AddAzureTableWalStorage`
with `ServiceUri` + `TokenCredential` (e.g.
`new DefaultAzureCredential()`) directly on its options object. See
[WAL Storage Providers](wal-storage-providers.md) for the full set of
WAL authentication modes.

> The `AddLattice` callback configures the **grain-storage** provider
> Lattice uses for its tree state. The silo also requires an
> `IWalStorageProvider` for the write-ahead log; `AddLattice`
> registers the in-memory provider by default (suitable for
> development and single-process tests), and hosts that need
> durability across silo restarts must replace it with a persistent
> provider before going to production. See
> [WAL Storage Providers](wal-storage-providers.md) for the
> `AddAzureTableWalStorage` extension shipped by the
> `Orleans.Lattice.Storage.AzureTable` package and the
> `AddWalStorage` seam for hosting custom providers.

Per-tree options are configured via `ConfigureLattice` (see
[Configuration](configuration.md) for the full options reference and
per-tree override semantics):

```csharp verify
siloBuilder.ConfigureLattice("my-tree", o =>
{
    o.CacheTtl = TimeSpan.FromMilliseconds(100);
    o.HotShardOpsPerSecondThreshold = 500;
});
```

> **Cross-cluster replication** is layered on top of the core library
> by the `Orleans.Lattice.Replication` package via
> `AddLatticeReplication`. Peer membership (who this silo ships its
> WAL to) is configured through `LatticeReplicationOptions.ReplicationPeers`
> or a custom `IReplicationTopology` registration - both are documented
> in [Replication drivers: Peer configuration](../lattice.replication/replication-drivers.md#peer-configuration-topology-vs-replicationpeers).
> The core `LatticeOptions` surface does not carry peer state.

> Structural sizing (`MaxLeafKeys`, `MaxInternalChildren`,
> `ShardCount`) is **not** configured here. Those values are pinned
> per-tree in the registry. See [Tree Sizing](tree-sizing.md) for how
> to set them on a new or existing tree.

> For a runnable end-to-end example, see [Samples](samples.md).

### Basic usage

Once Lattice is registered on the silo, resolve an `ILattice` grain
from `IGrainFactory` using the tree's logical name as the string key:

```csharp verify
// Resolve the tree (idempotent - the same name always routes to the same tree).
var tree = grainFactory.GetGrain<ILattice>("my-tree");

// Write a value.
await tree.SetAsync("user:1", "Alice"u8.ToArray());

// Read it back (returns null when absent or tombstoned).
byte[]? value = await tree.GetAsync("user:1");

// Conditional write - insert only if the key is not already present.
byte[]? existing = await tree.GetOrSetAsync("user:1", "Bob"u8.ToArray());

// Delete (tombstones the key; returns true if it was live).
bool deleted = await tree.DeleteAsync("user:1");

// Stream a key range in strict lexicographic order.
await foreach (var key in tree.ScanKeysAsync(startInclusive: "user:", endExclusive: "user;"))
{
    Console.WriteLine(key);
}
```

Keys are `string`; values are `byte[]`. For typed payloads (POCOs,
records, DTOs) use the serializer-aware overloads in
[`TypedLatticeExtensions`](#typedlatticeextensions); they accept any
`T` and default to `JsonLatticeSerializer<T>`:

```csharp verify
await tree.SetAsync("user:1", new User("Alice", 30));
var user = await tree.GetAsync<User>("user:1");
```

For the full set of runtime and maintenance operations, see
[`ILattice`](#ilattice) below.

## `ILattice`

Obtain an `ILattice` grain from the grain factory using the tree's
logical name as the string key:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
```

> The per-operation consistency contract - linearizable, strongly
> consistent, snapshot, or eventually consistent - is documented in
> [Consistency](consistency.md). Per-row notes here describe
> caller-visible behaviour and the exception surface only.

### Cancellation

Every method on `ILattice` - including scans (via the
`ScanKeysAsync` / `ScanEntriesAsync` extension wrappers), range
deletes, counts, fan-out batch operations, bulk load, stateful
cursors, and all tree-lifecycle orchestrators - accepts an optional
trailing `CancellationToken cancellationToken = default` parameter.
The signatures in the tables below omit the parameter for readability.

- A pre-cancelled token fails fast before any shard is contacted.
- Scan iterators apply `[EnumeratorCancellation]`, so
  `await foreach (...).WithCancellation(ct)` propagates correctly.
- Once a long-running coordinator (saga, resize, snapshot, reshard,
  merge) has accepted a request it drives itself to a terminal state
  and is not cooperatively cancelled.

The typed extensions in `TypedLatticeExtensions` and both streaming
`BulkLoadAsync` overloads in `LatticeExtensions` also thread the
token.

### Runtime operations

These methods are used during normal application flow to read, write,
and enumerate data. They are safe to call concurrently and do not
affect tree availability.

#### Single-key

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync` | `Task<byte[]?> GetAsync(string key)` | Returns the value for `key`, or `null` when absent or tombstoned. Staleness is bounded by `LatticeOptions.CacheTtl` (default zero - refresh on every read). |
| `GetWithVersionAsync` | `Task<VersionedValue> GetWithVersionAsync(string key)` | Returns the value paired with its `HybridLogicalClock` version; returns a default `VersionedValue` with `null` value and zero version when absent or tombstoned. Use the returned version with `SetIfVersionAsync` for optimistic concurrency. Bypasses the read cache. |
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` when `key` is live (not absent and not tombstoned). |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `SetAsync` (TTL) | `Task SetAsync(string key, byte[] value, TimeSpan ttl)` | Inserts or updates `key` with a time-to-live. The entry is treated as tombstoned on every read once `ttl` has elapsed from the server-side write instant; physical reclamation follows `LatticeOptions.TombstoneGracePeriod`. Throws `ArgumentOutOfRangeException` when `ttl` is zero or negative. A typed `SetAsync<T>(this ILattice, string, T, TimeSpan, ILatticeSerializer<T>)` overload exists in [`TypedLatticeExtensions`](#typedlatticeextensions). |
| `SetIfVersionAsync` | `Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)` | Atomic compare-and-set: writes only when the entry's current version equals `expectedVersion`. Returns `true` on success, `false` on version mismatch. Pass `HybridLogicalClock.Zero` for a key that must not exist. |
| `GetOrSetAsync` | `Task<byte[]?> GetOrSetAsync(string key, byte[] value)` | Inserts `value` only when `key` is absent or tombstoned. Returns the existing value when live, or `null` when the new value was written. No read-then-write race. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if the key was live. See [Tombstone Compaction](tombstone-compaction.md) for retention. |
| `ApplyCrdtDeltaAsync` | `Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes)` | Applies a producer-side typed CRDT delta to `key` under the declared `mode`. The owning leaf resolves the registered `CrdtShape`, folds the delta into the current state via the shape's `MergeDelta`, and appends a single WAL record carrying only the delta bytes. Returns the `HybridLogicalClock` stamped on the committed entry. CRDT merges are convergent, so this surface deliberately omits the optimistic-CAS guard `SetIfVersionAsync` carries. `LatticeMergeMode.OrMap` requires a per-tree shape registered via `ISiloBuilder.AddOrMapShape<TKey, TValue>(treeName)`; the closed-shape modes (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`) resolve through the registry's global fallback without per-tree registration. `LatticeMergeMode.LwwRegister` is rejected with `ArgumentException` - use `SetAsync` for LWW. Typed accessors (`OrSetAccessor`, `PnCounterAccessor`, `MvRegisterAccessor`, `OrMapAccessor`) wrap this surface and are the recommended caller-facing seam; see [CRDT value-surface accessors](#crdt-value-surface-accessors). |

Single-key operations transparently retry on topology-change
exceptions (`StaleShardRoutingException`, `StaleTreeRoutingException`).
Callers never see those exceptions.

#### Batch

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel. Missing or tombstoned keys are omitted from the result. A concurrent `SetManyAtomicAsync` is observed atomically tree-wide. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Writes multiple entries in parallel. **Not atomic** - partial failure leaves the batch half-applied with no rollback. Use `SetManyAtomicAsync` when all-or-nothing semantics are required. Per-leaf batches collapse their per-key WAL grain hops into a single batched dispatch (see [WAL - Batched leaf write path](wal.md#batched-leaf-write-path)). |
| `SetManyAtomicAsync` | `Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries)` | Atomically writes multiple entries: on success every key holds its new value, on any failure every key holds its pre-saga value. Concurrent readers observe the saga atomically tree-wide and across every cluster the tree replicates to. Throws `ArgumentException` on duplicate keys or null values; throws `InvalidOperationException` when compensation completes for a failed write. After completion, saga state is retained for `LatticeOptions.AtomicWriteRetention` (default 48 h). See [Atomic Writes](atomic-writes.md). |
| `SetManyAtomicAsync` (idempotency key) | `Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId)` | Caller-supplied idempotency-key overload. Re-submitting the same `operationId` re-attaches to the original saga and inherits its outcome, turning a transport-level failure into a safe client retry. The `operationId` is bound to the exact sorted key set of the first call; mismatched key sets throw `InvalidOperationException`. Reordering keys or changing values is allowed. `operationId` must be non-empty and must not contain `'/'`; otherwise throws `ArgumentException`. See [Atomic Writes - Caller-supplied idempotency keys](atomic-writes.md#caller-supplied-idempotency-keys). |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)` | Tombstones every live key in [`startInclusive`, `endExclusive`). Returns the total count tombstoned. For resumable or crash-safe range deletes, use [`OpenDeleteRangeCursorAsync`](#stateful-cursors). |
| `CountAsync` | `Task<int> CountAsync()` | Returns the exact live key count across all shards under the topology snapshot observed during the call. A concurrent `SetManyAtomicAsync` is observed atomically (included or excluded as a unit). Bounded by `LatticeOptions.MaxScanRetries` (default 3); throws `InvalidOperationException` on retry exhaustion. |
| `CountPerShardAsync` | `Task<IReadOnlyList<int>> CountPerShardAsync()` | Returns the per-shard live-key count, indexed by shard. Same consistency guarantees as `CountAsync`. Useful for diagnostics and load-balancing analysis. |

#### Enumeration

Long-running scans use the resilient extension wrappers on
`ILattice`. Each iterator survives mid-scan reconnects (silo
failover, idle expiry, cold start) and resumes deterministically -
no duplicates, no gaps, original ordering preserved.

| Method | Signature | Description |
|--------|-----------|-------------|
| `ScanKeysAsync` | `IAsyncEnumerable<string> ScanKeysAsync(this ILattice, string? startInclusive, string? endExclusive, bool reverse, bool? prefetch, int? maxAttempts)` | Streams live keys in strict lexicographic order. `prefetch=true` (or `null` with `LatticeOptions.PrefetchKeysScan = true`) overlaps the next page fetch with the current page consumption. `maxAttempts` overrides the wrapper's reconnect budget (default `LatticeExtensions.DefaultScanReconnectAttempts = 8`). A concurrent `SetManyAtomicAsync` is observed atomically across every page of a single enumeration. |
| `ScanEntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(this ILattice, string? startInclusive, string? endExclusive, bool reverse, bool? prefetch, int? maxAttempts)` | Streams live key-value entries in strict lexicographic key order. `prefetch` is gated by `LatticeOptions.PrefetchEntriesScan` (separate flag from keys because entry pages also carry `byte[]` values). Same atomic-visibility and reconnect guarantees as `ScanKeysAsync`. |

#### Scan reliability

`CountAsync`, `CountPerShardAsync`, `ScanKeysAsync`, and
`ScanEntriesAsync` use a bounded retry budget
(`LatticeOptions.MaxScanRetries`, default 3) to reconcile against
concurrent topology changes. If the topology continues to mutate
beyond the budget, the call throws `InvalidOperationException` rather
than returning a silently incomplete result. Under default settings
(`MaxConcurrentAutoSplits = 2`, `HotShardSplitCooldown = 2 min`)
exhaustion is not a realistic operational concern.

Three options for multi-minute exports in aggressively split-prone
workloads:

1. Raise `LatticeOptions.MaxScanRetries`.
2. Wrap the scan in an application-level retry that resumes from the
   last successfully yielded key using `startInclusive`.
3. Use a **stateful cursor** (below). A cursor checkpoints
   server-side after every page and survives silo failover, client
   restart, and topology changes without caller retry code.

### Stateful cursors

`ILattice` exposes a stateful cursor API for long-running scans and
resumable range deletes that survive silo failovers, client restarts,
and topology changes. Each cursor is a server-side, checkpointed
iterator identified by an opaque GUID returned at open time. See
[Durable Cursors](durable-cursors.md) for the full design and cost
model.

#### Method reference

| Method | Signature | Description |
|--------|-----------|-------------|
| `OpenKeyCursorAsync` | `Task<string> OpenKeyCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false)` | Opens a key-enumeration cursor and returns a server-assigned opaque cursor ID. `null` bounds are unbounded; `reverse=true` walks descending. `pointInTime=true` freezes the saga-decision view at open time so every page sees the same in-flight-saga view (see [Point-in-time cursors](#point-in-time-cursors)). |
| `OpenEntryCursorAsync` | `Task<string> OpenEntryCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false)` | Opens an entry-enumeration cursor (key + value pairs). Same bounds, direction, and point-in-time semantics as `OpenKeyCursorAsync`. |
| `OpenSnapshotKeyCursorAsync` | `Task<string> OpenSnapshotKeyCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false)` | Opens a **zero-observable-writes snapshot** key-enumeration cursor. Captures a tree-wide `LatticeSnapshotCoordinate` (per-shard WAL heads + tree map version + registry HLC) at open time and materialises pages by replaying WAL slices through transient per-shard `SnapshotLeafGrain`s. Subsequent foreground writes, saga commits, range deletes, and replication applies are invisible to this cursor. Open-time replay cost is gated by `LatticeOptions.MaxSnapshotReplayEntries`; failure throws `LatticeSnapshotReplayBudgetExceededException`. WAL retention is pinned through `IWalCursorRegistry` for the cursor's lifetime. See [Snapshot cursors](snapshot-cursors.md). |
| `OpenSnapshotEntryCursorAsync` | `Task<string> OpenSnapshotEntryCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false)` | Opens a zero-observable-writes snapshot entry cursor. Same isolation guarantees and open-time cost gate as `OpenSnapshotKeyCursorAsync`. |
| `OpenDeleteRangeCursorAsync` | `Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive)` | Opens a resumable range-delete cursor. Both bounds are **required** (non-null). Reverse mode is not supported. Throws `ArgumentException` on null bounds or a reverse spec. |
| `NextKeysAsync` | `Task<LatticeCursorKeysPage> NextKeysAsync(string cursorId, int pageSize)` | Returns up to `pageSize` keys and advances the cursor. `HasMore=false` signals exhaustion. Throws `ArgumentOutOfRangeException` for non-positive `pageSize`; throws `InvalidOperationException` if the cursor was opened for a different kind or has been closed. |
| `NextEntriesAsync` | `Task<LatticeCursorEntriesPage> NextEntriesAsync(string cursorId, int pageSize)` | Returns up to `pageSize` entries and advances the cursor. Same error surface as `NextKeysAsync` with expected kind `Entries`. |
| `DeleteRangeStepAsync` | `Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete)` | Deletes up to `maxToDelete` keys in a single step. `IsComplete=true` when the full range has been drained; subsequent calls return `DeletedThisStep=0`. Throws `InvalidOperationException` if the cursor is not of kind `DeleteRange`. |
| `CloseCursorAsync` | `Task CloseCursorAsync(string cursorId)` | Closes the cursor, clears its state, and deactivates the grain. Idempotent. |

#### Return types

| Type | Members | Description |
|------|---------|-------------|
| `LatticeCursorKeysPage` | `IReadOnlyList<string> Keys`, `bool HasMore` | A page returned by `NextKeysAsync`. Keys are in the cursor's scan order. |
| `LatticeCursorEntriesPage` | `IReadOnlyList<KeyValuePair<string, byte[]>> Entries`, `bool HasMore` | A page returned by `NextEntriesAsync`. |
| `LatticeCursorDeleteProgress` | `int DeletedThisStep`, `int DeletedTotal`, `bool IsComplete` | Returned by `DeleteRangeStepAsync`. `DeletedTotal` accumulates across every step. |
| `LatticeCursorKind` | `Keys`, `Entries`, `DeleteRange` | The kind of scan a cursor performs. |
| `LatticeCursorSpec` | `Kind`, `StartInclusive`, `EndExclusive`, `Reverse`, `PointInTime`, `ZeroObservableWrites` | Immutable cursor specification, frozen at open time. `ZeroObservableWrites=true` selects the WAL-replay snapshot path; `PointInTime=true` selects the registry-snapshot path; both `false` is the live path. |
| `LatticeSnapshotCoordinate` | `long TreeMapVersion`, `IReadOnlyDictionary<int, long> PerShardWalOffsets`, `HybridLogicalClock RegistrySnapshotHlc` | Tree-wide snapshot coordinate captured at `OpenSnapshot*CursorAsync` time. Pins the routing decision and every shard's WAL head so paging is deterministic across silo failovers. |
| `LatticeScopedCursor` | `string Id`, `ValueTask DisposeAsync()`, implicit conversion to `string` | `IAsyncDisposable` wrapper returned by the `LatticeExtensions.Open*CursorScopeAsync` family. Disposing the scope calls `CloseCursorAsync` exactly once; idempotent. The implicit `string` conversion lets a scope be passed directly to `NextKeysAsync` / `NextEntriesAsync` / `DeleteRangeStepAsync`. Does **not** change the durability contract of the underlying cursor; for cursors whose ID must survive a process boundary, keep using the raw `Open*CursorAsync` / `CloseCursorAsync` shape. |

#### Scoped cursor extensions

`LatticeExtensions` exposes an `IAsyncDisposable`-returning overload
for every `Open*CursorAsync` method. They are pure conveniences -
the underlying cursor grain semantics are unchanged - but they
let callers whose cursor lifetime fits in one stack frame avoid the
`try` / `finally` boilerplate.

| Method | Signature | Description |
|--------|-----------|-------------|
| `OpenKeyCursorScopeAsync` | `Task<LatticeScopedCursor> OpenKeyCursorScopeAsync(this ILattice, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default)` | Scoped variant of `OpenKeyCursorAsync`. |
| `OpenEntryCursorScopeAsync` | `Task<LatticeScopedCursor> OpenEntryCursorScopeAsync(this ILattice, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default)` | Scoped variant of `OpenEntryCursorAsync`. |
| `OpenSnapshotKeyCursorScopeAsync` | `Task<LatticeScopedCursor> OpenSnapshotKeyCursorScopeAsync(this ILattice, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default)` | Scoped variant of `OpenSnapshotKeyCursorAsync`. |
| `OpenSnapshotEntryCursorScopeAsync` | `Task<LatticeScopedCursor> OpenSnapshotEntryCursorScopeAsync(this ILattice, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default)` | Scoped variant of `OpenSnapshotEntryCursorAsync`. |
| `OpenDeleteRangeCursorScopeAsync` | `Task<LatticeScopedCursor> OpenDeleteRangeCursorScopeAsync(this ILattice, string startInclusive, string endExclusive, CancellationToken cancellationToken = default)` | Scoped variant of `OpenDeleteRangeCursorAsync`. |

```csharp verify
await using var scope = await tree.OpenEntryCursorScopeAsync();
while (true)
{
    var page = await tree.NextEntriesAsync(scope, pageSize: 500);
    foreach (var (k, v) in page.Entries)
        Console.WriteLine($"{k}={v.Length} bytes");
    if (!page.HasMore) break;
}
// scope.DisposeAsync() runs here.
```

#### Ordering and visibility

- Each step yields keys in strict lexicographic order. Once a key has
  been yielded by a cursor it is never re-yielded by the same cursor.
- In **live mode** (`pointInTime: false`, the default), values reflect
  the latest committed state at the moment each key is yielded. A
  saga that commits between two pages may have its keys split across
  the pre-commit and post-commit pages.
- In **point-in-time mode** (`pointInTime: true`), every page reads
  against the saga-decision view captured at `OpenAsync` time. A saga
  that commits between two pages is observed identically on every
  page - either every key the saga touched is visible, or none.

#### Idle TTL and self-cleanup

Cursors auto-clean if `CloseCursorAsync` is never called. Each
successful call slides an idle-TTL reminder
(`LatticeOptions.CursorIdleTtl`, default 48 h). When the reminder
fires without intervening activity the cursor grain clears its state
and deactivates.

```csharp verify
siloBuilder.ConfigureLattice(o => o.CursorIdleTtl = TimeSpan.FromHours(6));
```

Minimum effective interval is **1 minute** (Orleans reminder
granularity); smaller values are clamped. Set
`CursorIdleTtl = Timeout.InfiniteTimeSpan` to disable auto-cleanup.

#### Example - resumable export across a silo failover

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
resume on restart - the cursor grain reactivates on demand.

#### Example - bounded, resumable range delete

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
| `Open*` (point-in-time) | `pointInTime=true` passed for a `DeleteRange` cursor | `ArgumentException` |
| `Open*` (point-in-time) | Registry would exceed `LatticeOptions.MaxPinnedSagaDecisions` | `LatticeCursorRegistryPinExhaustedException` |
| `OpenSnapshot*CursorAsync` | Replay budget exceeded (any shard's `head - 0 > MaxSnapshotReplayEntries`) | `LatticeSnapshotReplayBudgetExceededException` |
| `Next*` (snapshot) | Snapshot leaf cannot be rebuilt because the WAL prefix was trimmed past the pinned offset | `LatticeSnapshotExpiredException` |
| Any `Open*` | Re-open with a different spec on the same cursor ID | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | Cursor kind mismatch | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | Cursor was closed | `InvalidOperationException` |
| `Next*` / `DeleteRangeStep*` | `pageSize` / `maxToDelete` <= 0 | `ArgumentOutOfRangeException` |
| `Next*` (point-in-time) | Pin lifetime exceeded `LatticeOptions.MaxCursorSnapshotPinTtl` | `LatticeCursorSnapshotExpiredException` |

#### Point-in-time cursors

`OpenKeyCursorAsync` and `OpenEntryCursorAsync` accept a
`pointInTime` flag. When set, every page issued by the cursor reads
against the saga-decision view captured at open time, so a
`SetManyAtomicAsync` batch that commits between two pages is
observed identically on every page (either all of its keys, or none).
The all-or-nothing guarantee that single-call reads
(`GetManyAsync`, `CountAsync`, `CountPerShardAsync`) already provide
is extended to a multi-page enumeration.

Streaming `ScanKeysAsync` / `ScanEntriesAsync` enumerations (no
cursor) provide the same all-or-nothing view automatically for the
lifetime of the `IAsyncEnumerable` - no opt-in required.

`DeleteRange` cursors cannot be opened in point-in-time mode (range
deletes are mutations, not snapshot reads).

Three independent caps bound the registry footprint of point-in-time
cursors:

| Cap | Default | Effect |
|-----|---------|--------|
| `LatticeOptions.CursorIdleTtl` | 48 h | Cursor idle-TTL reminder releases the pin on inactivity. |
| `LatticeOptions.MaxCursorSnapshotPinTtl` | 7 d | Hard cap on a single pin's lifetime. A live cursor slides this on every step; a stalled cursor surfaces `LatticeCursorSnapshotExpiredException` on its next call. |
| `LatticeOptions.MaxPinnedSagaDecisions` | 100 000 | Registry-wide footprint cap across all live pins. `Open*Async(pointInTime: true)` throws `LatticeCursorRegistryPinExhaustedException` if accepting the snapshot would breach the cap. |

```csharp verify
var cursorId = await tree.OpenEntryCursorAsync(
    startInclusive: null,
    endExclusive: null,
    reverse: false,
    pointInTime: true);
try
{
    while (true)
    {
        var page = await tree.NextEntriesAsync(cursorId, pageSize: 500);
        foreach (var (k, v) in page.Entries)
            Console.WriteLine($"{k}={v.Length} bytes");
        if (!page.HasMore) break;
    }
}
finally
{
    await tree.CloseCursorAsync(cursorId);
}
```

See [Durable Cursors - Point-in-time cursors](durable-cursors.md#point-in-time-cursors)
for the full design.

### Maintenance operations

These methods manage tree structure and lifecycle. Several of them
**take the tree offline** - reads and writes throw
`InvalidOperationException` while the operation is in progress. Plan
maintenance windows accordingly.

#### Bulk loading

| Method | Signature | Description |
|--------|-----------|-------------|
| `BulkLoadAsync` | `Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)` | One-shot bottom-up bulk load into an **empty** tree. Entries are sorted internally. Throws `InvalidOperationException` on the second and subsequent calls (every shard must still be empty). Not safe to use as a streaming-append primitive - for continuous ingestion, use `SetAsync` or the streaming `BulkLoadAsync` extension on [`LatticeExtensions`](#latticeextensions). See [Bulk Loading](bulk-loading.md). |

#### Tree lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `TreeExistsAsync` | `Task<bool> TreeExistsAsync()` | Returns `true` if this tree is registered. |
| `GetAllTreeIdsAsync` | `Task<IReadOnlyList<string>> GetAllTreeIdsAsync()` | Returns all registered tree IDs in sorted order. System trees (`_lattice_*`) are excluded. Physical trees created by `ResizeAsync` / `SnapshotAsync` are included. |
| `DeleteTreeAsync` | `Task DeleteTreeAsync()` | Soft-deletes the tree. Data is retained for `LatticeOptions.SoftDeleteDuration` before purge. Idempotent. ⚠️ **Takes the tree offline** - reads and writes throw `InvalidOperationException` until `RecoverTreeAsync`. See [Tree Deletion](tree-deletion.md). |
| `RecoverTreeAsync` | `Task RecoverTreeAsync()` | Recovers a soft-deleted tree before purge completes. |
| `PurgeTreeAsync` | `Task PurgeTreeAsync()` | Immediately purges a soft-deleted tree without waiting for the retention window. ⚠️ **Permanently destroys all data.** |

#### Resize and reshard

| Method | Signature | Description |
|--------|-----------|-------------|
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | **Online** - changes the tree's node fan-out. Reads and writes remain available throughout. Undoable within `LatticeOptions.SoftDeleteDuration`. Returns once the intent is persisted; use `IsResizeCompleteAsync` to poll for completion. Crash-safe. See [Tree Sizing](tree-sizing.md#resizing-an-existing-tree). |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize. Available before the swap (aborts cleanly) and after the swap (recovers the old tree). Only valid while the old tree is still within `LatticeOptions.SoftDeleteDuration`. |
| `ReshardAsync` | `Task ReshardAsync(int newShardCount, CancellationToken cancellationToken = default)` | **Online** - grows the tree's physical shard count to at least `newShardCount`. Grow-only: `newShardCount` must be greater than the current count and `<= LatticeConstants.DefaultVirtualShardCount` (4096). Throws `ArgumentOutOfRangeException` otherwise. Idempotent for the same target while running; throws `InvalidOperationException` when a different target is already in progress. Returns once the intent is persisted; use `IsReshardCompleteAsync` to poll. Crash-safe. Transparently absorbs up to two `ShardActivationTimeoutException`s from the coordinator's shard-root activation-readiness seed (a cold-start race during startup reshards); a third consecutive seed timeout surfaces the typed exception to the caller. See [Online Reshard](online-reshard.md). |

#### Merge

| Method | Signature | Description |
|--------|-----------|-------------|
| `MergeAsync` | `Task MergeAsync(string sourceTreeId)` | Merges every entry from `sourceTreeId` into this tree using last-writer-wins by `HybridLogicalClock` timestamp. Tombstones are preserved. The source tree is unmodified. Source and target trees may have different shard counts. See [Architecture](architecture.md). |

#### Snapshots

| Method | Signature | Description |
|--------|-----------|-------------|
| `SnapshotAsync` | `Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys, int? maxInternalChildren)` | Creates a point-in-time copy of the tree into `destinationTreeId`. In `Offline` mode the source is locked during the copy; in `Online` mode the source remains available throughout. Optional sizing overrides apply to the destination. TTL metadata and source HLC versions are preserved verbatim. ⚠️ **`Offline` mode takes the tree offline.** See [Snapshots](snapshots.md). |

#### Operation status

| Method | Signature | Description |
|--------|-----------|-------------|
| `IsMergeCompleteAsync` | `Task<bool> IsMergeCompleteAsync()` | `true` once no merge is in progress (vacuously `true` when none has ever been initiated). Monotonic: once `true` for a given operation, never returns `false` again. |
| `IsSnapshotCompleteAsync` | `Task<bool> IsSnapshotCompleteAsync()` | Same semantics for `SnapshotAsync`. |
| `IsResizeCompleteAsync` | `Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default)` | Same semantics for `ResizeAsync`. |
| `IsReshardCompleteAsync` | `Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default)` | Same semantics for `ReshardAsync`. |

#### Diagnostics

| Method | Signature | Description |
|--------|-----------|-------------|
| `DiagnoseAsync` | `Task<TreeDiagnosticReport> DiagnoseAsync(bool deep = false, CancellationToken cancellationToken = default)` | Returns a per-shard health snapshot - depth, root-is-leaf, live-key count, tombstone count (deep only), hotness counters, ops/sec, split/bulk state - plus a bounded ring buffer of recent adaptive-split events. Repeated calls within `LatticeOptions.DiagnosticsCacheTtl` (default 5 s) are served from cache; shallow and deep reports are cached independently. **Not for hot-path or correctness-critical decisions** - use the operation-specific APIs (`CountAsync`, `IsResizeCompleteAsync`, etc.) instead. See [Diagnostics](diagnostics.md). |
| `RebuildLeafProjectionAsync` | `Task RebuildLeafProjectionAsync(int shardIndex, CancellationToken cancellationToken = default)` | Operator-driven recovery: clears the projection state of every leaf in the specified physical shard and forces a WAL replay on next activation. Topology-bearing state is preserved. See [Operator tooling](#operator-tooling-projection-rebuild-and-materialiser-lag) and [Projection Rebuild](projection-rebuild.md#operator-tooling-rebuild-and-lag). |
| `GetMaterialiserLagAsync` | `Task<long> GetMaterialiserLagAsync(CancellationToken cancellationToken = default)` | Returns the maximum WAL-entry lag between any shard's WAL head and the minimum leaf-projection checkpoint offset across all leaves in the tree. `0` means fully caught up; growing values indicate the materialiser is falling behind WAL ingestion. See [Operator tooling](#operator-tooling-projection-rebuild-and-materialiser-lag). |
| `CompactShardAsync` | `Task<bool> CompactShardAsync(int shardIndex, CancellationToken cancellationToken = default)` | Operator-tooling tombstone-compaction request: schedules an out-of-cycle compaction pass scoped to a single physical shard, bypassing the per-shard cooldown gate. Returns `false` when compaction is disabled (`TombstoneGracePeriod = Timeout.InfiniteTimeSpan`) or when a pass is already in flight. Throws `ArgumentOutOfRangeException` when `shardIndex` is not a physical shard of the tree. See [Tombstone Compaction](tombstone-compaction.md#operator-api). |

```csharp verify
var report = await tree.DiagnoseAsync(deep: true, cancellationToken);
Console.WriteLine($"Tree {report.TreeId}: {report.TotalLiveKeys} live, {report.TotalTombstones} tombstones across {report.ShardCount} shards.");
foreach (var shard in report.Shards)
{
    Console.WriteLine($"  shard {shard.ShardIndex}: depth={shard.Depth}, live={shard.LiveKeys}, ops/s={shard.OpsPerSecond:F1}");
}
```

#### Storage usage

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetStorageUsageAsync` | `Task<TreeStorageUsageReport> GetStorageUsageAsync(CancellationToken cancellationToken = default)` | Returns a byte-accurate breakdown of the tree's on-disk footprint: retained WAL bytes, captured leaf-snapshot bytes, and live leaf-state bytes, plus their sum (`TotalBytes`). The aggregator fans out to every physical shard and WAL partition, then caches the assembled report for `LatticeOptions.StorageUsageCacheTtl` (default 10 s). `Partial` is `true` when the configured `IWalStorageProvider` does not support byte accounting (the in-memory and Azure Table providers both do). Diagnostic / capacity-planning use only - not a hot-path API. |

```csharp verify
var usage = await tree.GetStorageUsageAsync(cancellationToken);
Console.WriteLine($"Tree {usage.TreeId}: {usage.TotalBytes} bytes total " +
    $"(WAL={usage.WalRetainedBytes}, snapshots={usage.SnapshotBytes}, leaf-state={usage.LeafStateBytes})");
```

The `TreeStorageUsageReport` fields are:

| Field | Type | Meaning |
|-------|------|---------|
| `TreeId` | `string` | The tree the report covers. |
| `WalRetainedBytes` | `long` | Retained (un-trimmed) WAL payload bytes summed across every partition. |
| `SnapshotBytes` | `long` | Captured leaf-snapshot key + value bytes summed across every leaf. |
| `LeafStateBytes` | `long` | Live leaf-state key + value bytes summed across every leaf in every shard. |
| `TotalBytes` | `long` | `WalRetainedBytes + SnapshotBytes + LeafStateBytes`. |
| `Partial` | `bool` | `true` when WAL byte accounting was unavailable, so `WalRetainedBytes` is best-effort. |
| `SampledAt` | `DateTimeOffset` | When the underlying fan-out was sampled. |

For a cluster-wide roll-up across every registered tree, resolve the
`ILatticeAdmin` grain (see [`ILatticeAdmin`](#ilatticeadmin)).

#### Lifecycle


| Method | Signature | Description |
|--------|-----------|-------------|
| `WarmUpAsync` | `Task WarmUpAsync(CancellationToken cancellationToken = default)` | Pre-activates every physical shard root *and* each shard's current root-node grain (root leaf when the tree is flat, root internal node otherwise) for this tree using a bounded-concurrency fan-out. On a brand-new empty shard, warm-up runs the same `EnsureRootAsync` path the first traffic write would take - it materializes the deterministic root leaf at startup instead of under hot-path load. Designed to be called once at host startup, *after* the lattice has been resolved and *before* the first hot-path write lands, so the Orleans placement-directory + grain-storage first-touch cost (and root-materialization persistence cost on an empty tree) is absorbed while the silo is idle rather than against producer-driven flush concurrency. The fan-out is capped at `min(physicalShardCount, 32)` simultaneous probes. Throws `InvalidOperationException` when invoked on an internal system tree. Records the `orleans.lattice.warmup.invocations` counter and the `orleans.lattice.warmup.duration` histogram. |

```csharp verify
// Run once during host startup, after the lattice has been
// resolved but before any producer traffic is accepted.
await tree.WarmUpAsync(cancellationToken);
```

#### Events

| Method | Signature | Description |
|--------|-----------|-------------|
| `SetPublishEventsEnabledAsync` | `Task SetPublishEventsEnabledAsync(bool? enabled, CancellationToken cancellationToken = default)` | Sets or clears the per-tree override for event publication. `true` forces on, `false` forces off, `null` removes the override so the tree inherits `LatticeOptions.PublishEvents`. The override is persisted and survives silo restarts. Propagation is best-effort. See [Events - Per-tree override](events.md#per-tree-override). |

```csharp verify
// Force events on for this tree regardless of the silo default:
await tree.SetPublishEventsEnabledAsync(true, cancellationToken);

// Clear the override and inherit the silo default:
await tree.SetPublishEventsEnabledAsync(null, cancellationToken);
```

For subscribing to published events on the cluster client, see
`SubscribeToEventsAsync` under
[`LatticeExtensions`](#latticeextensions).

## `ILatticeAdmin`

`ILatticeAdmin` is the cluster-wide administrative surface. Resolve the
singleton with `grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin")`
(the admin grain uses a fixed string key).

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetTotalStorageUsageAsync` | `Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default)` | Enumerates every registered tree, fans out to each tree's `GetStorageUsageAsync` (cache-respecting), and aggregates the per-tree `TreeStorageUsageReport`s into a single cluster-wide roll-up. `Partial` is `true` when any tree's report was partial. Activates every shard root, leaf, and snapshot storage grain - **diagnostic / capacity-planning use only**; not safe to call on a polling cadence. |
| `PollWalUsageAsync` | `Task PollWalUsageAsync(CancellationToken cancellationToken = default)` | Cluster-wide WAL-only refresh. Fans out across every registered tree's WAL-only aggregator, touching only `IWalShardGrain` activations - never a leaf, internal node, snapshot storage, or shard-root grain. Drives the `storage.wal_bytes` / `storage.policy.over_threshold` observable gauges plus the byte-pressure WAL retention policy. This is the cheap path the per-silo background poller uses on its default 15 s cadence, so an idle tree is never activated by polling. |
| `RefreshStorageUsageAsync` | `Task<ClusterStorageUsageReport> RefreshStorageUsageAsync(CancellationToken cancellationToken = default)` | Operator-driven deep refresh. Same return shape as `GetTotalStorageUsageAsync`, but **bypasses every tree's `StorageUsageCacheTtl` cache** and forces a fresh fan-out. Reserved for explicit operator action (post-migration validation, manual reconciliation); the background poller never invokes it. |
| `GetWalPlacementAsync` | `Task<WalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)` | Returns the tree's durable WAL placement pin: the default catalogue key, any per-partition key overrides, and the pin's compare-and-swap `Version`. A tree that has never been moved reports the `default` key for every partition. |
| `AuditWalPlacementAsync` | `Task<WalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)` | Like `GetWalPlacementAsync` but additionally reports, **for the silo serving this call**, whether every pinned catalogue key is registered there (`AllResolvableOnThisSilo`) plus the silo's known key set. The cheapest way to catch a missing-key misconfiguration before it fails a partition closed. |
| `PlanWalMoveAsync` | `Task<WalMovePlan> PlanWalMoveAsync(string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)` | Read-only dry run. Reports what moving `partition` to `targetProviderKey` would copy (offset range, entry count), whether the partition is already at the target, and whether the target key resolves on the serving silo. Mutates nothing. |
| `PlanWalMoveAsync` (batch) | `Task<WalMoveBatchPlan> PlanWalMoveAsync(string treeId, IEnumerable<(int Partition, string TargetProviderKey)> moves, CancellationToken cancellationToken = default)` | Batch dry run: one `WalMovePlan` per requested `(partition, targetProviderKey)` pair, plus `AllTargetsResolvableOnThisSilo`. Rejects an empty batch or a repeated partition with `ArgumentException`. Mutates nothing. |
| `ExecuteWalMoveAsync` | `Task<WalMoveReceipt> ExecuteWalMoveAsync(string treeId, int partition, string targetProviderKey, WalMoveOptions? options = null, CancellationToken cancellationToken = default)` | Performs the quiesce-copy-cutover move saga: fences the partition's WAL grain, offset-preservingly copies the retained range to the target provider, re-converges on any appends that landed during the copy, flips the durable pin under compare-and-swap, then forces the WAL grain to deactivate so its next activation (any silo) binds the new provider. Non-destructive (`WalMoveReceipt.SourceRetained`); fails closed if the target key is unregistered on the serving silo. Single partition per call; see the batch overload for multi-partition moves. |
| `ExecuteWalMoveAsync` (batch) | `Task<WalMoveBatchReceipt> ExecuteWalMoveAsync(string treeId, IEnumerable<(int Partition, string TargetProviderKey)> moves, WalMoveOptions? options = null, CancellationToken cancellationToken = default)` | Moves several partitions all-or-nothing: each runs the same quiesce-copy-verify phases (bounded by `WalMoveOptions.MaxConcurrentPartitionMoves`), then the pin flips **once** under a single compare-and-swap so every partition reaches the same new placement version. Any phase failure aborts the whole batch with the pin unflipped and partial copies retained for a resumable retry; fails closed (`LatticeWalProviderMissingException`) if any target key is unresolvable. `WalMoveBatchReceipt.Moves` carries one receipt per partition in request order. |
| `ReclaimMovedWalSourceAsync` | `Task<WalMoveReceipt> ReclaimMovedWalSourceAsync(string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)` | Reclaims the now-redundant copy left on the **source** provider after a permanent move by trimming it. Refuses (throws) if the partition is still pinned to `sourceProviderKey` - you can only reclaim a placement the pin has already moved away from. |

```csharp verify
var admin = grainFactory.GetGrain<ILatticeAdmin>("_lattice_admin");
var cluster = await admin.GetTotalStorageUsageAsync(cancellationToken);
Console.WriteLine($"{cluster.TreeCount} trees, {cluster.TotalBytes} bytes total " +
    $"(WAL={cluster.WalRetainedBytes}, snapshots={cluster.SnapshotBytes}, leaf-state={cluster.LeafStateBytes})");
foreach (var t in cluster.Trees)
{
    Console.WriteLine($"  {t.TreeId}: {t.TotalBytes} bytes");
}
```

The `ClusterStorageUsageReport` fields are:

| Field | Type | Meaning |
|-------|------|---------|
| `TreeCount` | `int` | Number of trees included in the roll-up. |
| `WalRetainedBytes` | `long` | Sum of every tree's `WalRetainedBytes`. |
| `SnapshotBytes` | `long` | Sum of every tree's `SnapshotBytes`. |
| `LeafStateBytes` | `long` | Sum of every tree's `LeafStateBytes`. |
| `TotalBytes` | `long` | Sum of every tree's `TotalBytes`. |
| `Partial` | `bool` | `true` when any tree's report was partial. |
| `Trees` | `IReadOnlyList<TreeStorageUsageReport>` | The per-tree reports that were aggregated. |
| `SampledAt` | `DateTimeOffset` | When the roll-up was assembled. |

## Mutation observers


`IMutationObserver` is a grain-side extensibility hook invoked
synchronously after every durably-committed mutation, before the
grain method returns to the caller. It is the primary seam for
replication write-ahead logs, change-feed producers, and external
audit consumers.

> **Mutation observers vs. [tree events](events.md).** Both surface
> "something changed" notifications.
>
> - **`IMutationObserver` is in-process, synchronous, and carries the
>   full value bytes.** It runs on the grain's scheduler before the
>   write returns, so the caller's latency includes the observer's
>   latency. Use it when a downstream component (replication WAL,
>   outbox) must see the value at commit time and must be on the
>   write path - typically another library, not application code.
> - **Tree events are out-of-process, asynchronous, and
>   metadata-only** (key + kind + HLC - *no* value bytes). They ride
>   Orleans Streams. Use them for UI updates, cache invalidation,
>   dashboards, audit projections - anything that can tolerate
>   at-most-once delivery and is willing to call `GetAsync` itself
>   when it needs the value.
>
> A single write typically fires both: the observer first (inline,
> with value), then an event (post-commit, metadata-only).

Register one or more observers in the silo DI container. They are
resolved as `IEnumerable<IMutationObserver>`, so multiple can
coexist. When none is registered the hook is zero-cost.

```csharp verify
public sealed class MyReplicationObserver : IMutationObserver
{
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken ct)
    {
        // Inspect mutation.TreeId, mutation.Kind, mutation.Key, mutation.Value,
        // mutation.Timestamp, mutation.IsTombstone, mutation.ExpiresAtTicks,
        // mutation.OriginClusterId, mutation.VectorClock,
        // mutation.TransactionId, mutation.Category,
        // mutation.AtomicBatchSize, mutation.AtomicBatchIndex,
        // mutation.Delta, and for DeleteRange
        // also mutation.EndExclusiveKey.
        return Task.CompletedTask;
    }
}
```

```csharp verify
siloBuilder.ConfigureServices(services =>
    services.AddSingleton<IMutationObserver, MyReplicationObserver>());
```

### Emission points and shape

| Mutation | `Kind` | Shape |
|----------|--------|-------|
| `SetAsync` (all overloads) | `Set` | One event per key. `Value` holds the committed bytes; `Timestamp` is the stamped HLC; `ExpiresAtTicks` carries the TTL deadline (or `0` for no-expiry). |
| `DeleteAsync` | `Delete` | One event per tombstoned key. `IsTombstone` is `true`, `Value` is `null`. Absent-key deletes publish nothing. |
| `DeleteRangeAsync` | `DeleteRange` | One event **per shard** that received the range (not per key and not per user call), emitted **even when the shard matched zero live keys** so replication consumers propagate the range unconditionally. Consumers that need exactly-once per user call must dedup on `(TreeId, Key, EndExclusiveKey)`. `Key` carries `startInclusive`; `EndExclusiveKey` carries `endExclusive`; `Timestamp` is `HybridLogicalClock.Zero`. |

### Transaction correlation (`TransactionId`)

Every emit carries a `Guid TransactionId` that lets observers detect
when several payloads belong to the same enclosing user call or saga:

- **Single-key writes** (`SetAsync`, `SetIfVersionAsync`,
  `GetOrSetAsync`, `DeleteAsync`) get a fresh `Guid` per public call.
- **Atomic batches** (`SetManyAtomicAsync`) share a single `Guid`
  across every per-key emit - including compensation rolls and
  crash-recovery replays.
- **Per-shard fan-out** (`DeleteRangeAsync`) shares a single `Guid`
  across every per-shard emit.
- **Multi-key writes** (`SetManyAsync`) share a single `Guid` across
  every per-key emit.
- **Convergence paths** (merge, shadow-forward, snapshot restore)
  emit `Guid.Empty`.

Observers that batch by transaction group on `TransactionId`;
observers that do not care simply ignore the field.

### Pre-merge author's delta (`Delta`)

Every emit may carry an optional `byte[]? Delta` slot that lets a
producer attach the author's pre-merge delta alongside the
post-merge committed value. Lattice never opens the payload itself;
consumers decode it based on `WalRecord.Mode` (the declared
`LatticeMergeMode` of the tree the entry belongs to). The delta lets
deterministic replay - and active-active CRDT receivers - reach the
same convergence the original writer reached, which the post-merge
`Value` bytes alone cannot guarantee for non-LWW CRDTs.

Stamp the slot via the `LatticeDeltaContext` ambient helper:

```csharp verify
using (LatticeDeltaContext.With(new byte[] { 1, 2, 3 }))
{
    await tree.SetAsync("k", new byte[] { 4, 5, 6 }, cancellationToken);
}
```

The CRDT value-surface accessors (`OrSet`, `PnCounter`,
`VersionVector`, `MvRegister`, `OrMap<TKey, TValue>`) and the
atomic-write saga set the context on the caller's behalf - the
replication package's typed-delta receiver dispatch reads the
stamped slot and applies via `MergeDelta` automatically.

### Atomic-batch metadata (`AtomicBatchSize` / `AtomicBatchIndex`)

Every emit produced by an in-flight `SetManyAtomicAsync` saga
(including compensation rolls) carries `int AtomicBatchSize` (total
entry count) and `int AtomicBatchIndex` (zero-based position).
Single-key writes outside a saga emit `0` / `0`.

```csharp verify
using (LatticeAtomicBatchContext.With((5, 2)))
{
    // A forwarder for a remote saga can stamp the ambient directly;
    // the typical caller does not need to - the saga stamps it
    // on its own per-key writes.
}
```

The slots are independent of `OriginClusterId`, `VectorClock`, and
`Category`. Wire-compatible: missing slots on legacy persisted state
decode to `0`.

## WAL saturation back-pressure

Lattice publishes a per-tree, three-state saturation signal so callers driving offered load into `ILattice` can throttle their own input *before* the saturation regime's failure tail surfaces to them as a `TimeoutException` from `SetAsync` / `SetManyAsync`. The surface has three shapes - polling, await, and push - all backed by a single silo-scoped sampler that ticks at `LatticeOptions.WalSaturationSampleInterval` (default 200 ms).

| Type | Shape | Description |
|------|-------|-------------|
| `WalSaturationState` | `enum` (`Healthy`, `Throttled`, `Saturated`) | The classification a tree is in. `Healthy` = admit without waiting; `Throttled` = admission depth near cap, callers should slow down; `Saturated` = semaphore at cap with parked callers, callers should pause new appends. |
| `IWalSaturationSignal` | DI singleton (polling + await) | `GetCurrentState(treeId)` returns the cached per-tree state in one concurrent-dictionary lookup; `GetAggregateState()` returns the worst case across every observed tree; `WaitForHealthyAsync(treeId, ct)` returns a task that completes when the tree returns to `Healthy` (synchronous fast-path when already `Healthy`). |
| `IWalSaturationObserver` | DI hook (push) | `OnStateChangedAsync(change, ct)` is invoked once per transition with a `WalSaturationStateChange` payload. Registered via `services.AddSingleton<IWalSaturationObserver, MyObserver>()`. Exceptions are caught and logged; the dispatcher continues to the next observer. |
| `WalSaturationStateChange` | `readonly record struct` | `TreeId`, `PreviousState`, `NewState`, `AttributedPartition?`, `AttributedShard?`, `ObservedAt`. The attribution slots are populated when a single partition (admission-depth-driven) or shard (dispatch-timeout-driven) dominated the transition. |

The signal is driven by three underlying sources:

- **Admission-semaphore depth.** Per-partition `in_flight / WalMaxPendingBatches` ratio. `>= WalSaturationThrottledRatio` (default 0.75) raises a tree to `Throttled`; depth at cap with parked callers raises it to `Saturated`.
- **Dispatch-timeout rate.** Trips of `WalAppendDispatchTimeout` observed within a single sample window. `>= WalSaturationDispatchTimeoutThreshold` (default 1) raises a tree to `Saturated` regardless of admission depth.
- **Provider-failure rate.** Non-cancellation exceptions surfaced from a downstream `IWalShardGrain.AppendAsync` / `AppendBatchAsync` dispatch within a single sample window. `>= WalSaturationProviderFailureRateThreshold` (default 1; set to `0` to disable) raises a tree to `Saturated` regardless of admission depth and dispatch-timeout trips. Captures the regime where the provider's commit calls return quickly but terminally fail (e.g. the Azure Tables single-account 409-Conflict burst), so the silo surfaces the saturation regime instead of silently leaking entries.

The tree's state is the worst case across these signals across every partition / shard.

### Polling (canonical TCP-read-loop pattern)

```csharp verify
var signal = client.ServiceProvider.GetRequiredService<IWalSaturationSignal>();
while (!cancellationToken.IsCancellationRequested)
{
    if (signal.GetCurrentState("my-tree") == WalSaturationState.Saturated)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        continue;
    }
    // Continue reading from the producer transport (TCP, gRPC, queue, ...)
    // and dispatching SetAsync / SetManyAsync calls.
}
```

The polling getter costs one concurrent-dictionary lookup returning an `enum` - it is safe to call inside any per-message check on the producer hot path. The `SetAsync` / `SetManyAsync` hot path on `ILattice` is unchanged: the sampler runs on its own timer and never adds per-call work.

### Canonical per-call back-pressure helper (`ApplyBackPressureAsync`)

For TCP listeners, saga coordinators, and other consumers that drive offered load through a per-call decision loop, the library packages the three-state response pattern in a single call:

```csharp verify
var signal = client.ServiceProvider.GetRequiredService<IWalSaturationSignal>();
while (!cancellationToken.IsCancellationRequested)
{
    // Per-call back-pressure: no-op on Healthy, brief delay on
    // Throttled (1 ms default - tunable via the overload), full
    // park-until-Healthy on Saturated.
    await signal.ApplyBackPressureAsync("my-tree", cancellationToken);

    // Continue reading from the producer transport and dispatching
    // SetAsync / SetManyAsync calls.
}
```

The helper centralises the canonical response pattern so consumers do not roll their own (a recurring source of "the signal fires but back-pressure is too soft" when the consumer's Throttled response is `Task.Yield()` instead of an honest delay). The default Throttled delay (1 ms) slows a 10 k events/sec offered stream to ~1 k events/sec - enough to give the writer's admission gate time to drain before the regime escalates to Saturated, without producing a perceptible per-call latency penalty when the regime is transient. Pass an explicit `TimeSpan` to the four-argument overload to tune the strength of the Throttled response.

### Await (single recovery wait)

```csharp verify
var signal = client.ServiceProvider.GetRequiredService<IWalSaturationSignal>();
// Block until the tree returns to Healthy, or until the caller's CT fires.
await signal.WaitForHealthyAsync("my-tree", cancellationToken);
```

`WaitForHealthyAsync` returns `Task.CompletedTask` synchronously when the tree is already `Healthy`, so the fast-path is allocation-free. When the tree is not `Healthy`, the awaiter completes on the next sample tick that observes the tree at `Healthy` - the worst-case wake-up is one `WalSaturationSampleInterval` beyond the underlying recovery.

### Push (subscription model)

Implement the observer:

```csharp verify
public sealed class MyBackPressurePolicy : IWalSaturationObserver
{
    public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken)
    {
        // React to the transition. Keep this fast - long-running work
        // belongs on a channel drained by an IHostedService.
        return ValueTask.CompletedTask;
    }
}
```

Register it on the silo's DI container:

```csharp
siloBuilder.Services.AddSingleton<IWalSaturationObserver, MyBackPressurePolicy>();
```

Multiple observers may coexist; they are invoked in registration order. Exceptions thrown by one observer are logged as a warning and suppressed - the others continue to run. Observers do not see the per-tick state; they see only the transitions, so a tree that holds `Throttled` for an hour produces one `Healthy -> Throttled` callback at the start and one `Throttled -> Healthy` callback at the end.

### Metrics

| Instrument | Type | Tags | Description |
|------------|------|------|-------------|
| `orleans.lattice.wal.saturation.state` | observable gauge (long) | `tree`, `state` | Current per-tree state as a step function. Values: `0` = Healthy, `1` = Throttled, `2` = Saturated. |
| `orleans.lattice.wal.saturation.transitions` | counter (long) | `tree`, `state`, `previous_state`, optional `partition`, optional `shard` | Incremented once per per-tree transition. The state tag values are lowercased enum names (`healthy`, `throttled`, `saturated`). |

A flat-zero series on `transitions` is the healthy steady state. A rising rate of `state=throttled` transitions on a tree is the leading edge of the saturation regime; `state=saturated` is the regime itself. Pair with the `state` observable gauge for "what is the current regime" and with the `transitions` counter for "how often is the regime changing" - flapping between `Throttled` and `Saturated` is a different operational signal from a sustained `Saturated`.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WalSaturationSampleInterval` | `200 ms` | Sampler cadence. The worst-case observer / await transition latency is one interval beyond the underlying signal crossing the threshold. Set to `Timeout.InfiniteTimeSpan` to disable the sampler entirely (signal pins to `Healthy`). |
| `WalSaturationThrottledRatio` | `0.75` | Per-partition admission-depth ratio at or above which the signal raises a tree to `Throttled`. Range `[0.0, 1.0]`. |
| `WalSaturationDispatchTimeoutThreshold` | `1` | Minimum dispatch-timeout trips per sample window that raise a tree to `Saturated` regardless of admission depth. |
| `WalSaturationProviderFailureRateThreshold` | `1` | Minimum provider-side commit failures per sample window that raise a tree to `Saturated` regardless of admission depth and dispatch-timeout trips. Captures the regime where provider commit calls return quickly but terminally fail (e.g. Azure Tables 409-Conflict bursts) so the silo surfaces the saturation regime instead of silently leaking entries. Set to `0` to disable the trigger entirely. |
| `WalSaturationFlushLatencyThreshold` | `null` (disabled) | Per-provider-flush wall-clock latency at or above which the WAL writer increments a per-(tree, shard) flush-latency trip counter that feeds the saturation classifier. Closes the small-batch blind spot the other three inputs cannot see (slow-but-successful flushes against a saturating storage account never fill the admission semaphore, trip the dispatch deadline, or tally a provider-failure). Must be positive when set; leaving it `null` is a zero-cost no-op. Pair with `WalSaturationFlushLatencySampleWindows`. |
| `WalSaturationFlushLatencySampleWindows` | `3` | Number of consecutive sample windows that must each observe a non-zero flush-latency-threshold trip-counter delta before the classifier upgrades the tree to `Saturated`. Noise floor for the flush-latency input. Minimum 1; has no effect when `WalSaturationFlushLatencyThreshold` is left at its default `null`. |
| `WalSaturationRecoveryWindow` | `1 s` | Window after the most-recently observed `Saturated` transition during which the classifier holds a tree at or above `Throttled` even if the current sampler tick's depth observation classifies it as `Healthy`. Defends against bursty per-partition WAL drain where the per-tick `max(depth_ratio)` oscillates `~1.0 <-> ~0.0` and the classifier would otherwise flap `Healthy <-> Saturated` at the sampler cadence with `Throttled` never observed as a stable state. Set to `TimeSpan.Zero` to disable the upgrade (per-tick depth observation drives the regime directly); set to `Timeout.InfiniteTimeSpan` to hold `Throttled` forever after the first `Saturated` observation. |
| `WalAdmissionSaturationWaitBudget` | `5 s` | Wall-clock budget the WAL writer admission gate (`PartitionTracker.AcquireAsync`) spends parked on `IWalSaturationSignal.WaitForHealthyAsync` before refusing a dispatch with [`LatticeSaturatedException`](#saturation-back-pressure---latticesaturatedexception) when the per-tree saturation signal stays `Saturated` past the budget. Sized shorter than `WalAppendDispatchTimeout` (so the saturation refusal wins over the dispatch timeout) and longer than one `WalSaturationSampleInterval` (so a transient classifier flap does not surface as a refusal). Set to `TimeSpan.Zero` to disable the gate entirely (the historical pre-admission-gate behaviour). Set to `Timeout.InfiniteTimeSpan` to wait forever on recovery. |

See [WAL Saturation Signal](wal-saturation-signal.md) for the full design including the per-tree resolution contract, the multi-tree aggregate view, and the bench-side adoption pattern.

## Shutdown back-pressure - `LatticeShuttingDownException`

Public typed exception thrown by any `ILattice` operator (and by the internal saga coordinator on its caller-facing throw path) when the operation cannot complete because the owning silo's write-ahead-log writer is draining as part of host shutdown. Derives from `InvalidOperationException` so existing catch handlers continue to absorb it; the typed slot lets callers that care about the shutdown regime explicitly distinguish it from genuine `InvalidOperationException` failures (which are not back-pressure).

Surfaces from three distinct shutdown failure shapes that share the same operational meaning ("this silo is going away; the operation was refused"):

- **Lifetime-aware pre-dispatch fast-fail.** Every public write entry point on `ILattice` - `SetAsync` (both overloads), `SetIfVersionAsync`, `ApplyCrdtDeltaAsync`, `GetOrSetAsync`, `SetManyAsync`, `SetManyWherePredicateAsync`, `SetManyAtomicAsync`, `SetManyAtomicWhereAsync`, `DeleteAsync`, `DeleteRangeAsync`, and `DeleteRangeWherePredicateAsync` - checks `IHostApplicationLifetime.ApplicationStopping` first and throws this exception *before* it touches the activation directory or dispatches to the WAL writer once the host has begun shutting down. The same pre-check guards the internal shard-root write path and the operator-driven tombstone-compaction pass, so a single `catch`/`is` check covers every write path rather than only the atomic-write saga. The check is null-tolerant: non-hosted test activations (which do not register `IHostApplicationLifetime`) are unaffected.
- **Writer-side drain refusal.** `WalCommitLogWriter.DrainAsync` flips the per-instance drain flag; any new `AppendAsync` / `AppendBatchAsync` dispatch after the flip throws this exception inline.
- **Admission-semaphore drain release.** A caller already parked on the per-partition admission semaphore when the drain fired sees the release-by-drain surface as this exception (rather than the legacy `TimeoutException(WalDrainBudget)` shape).
- **Saga-coordinator short-circuit.** When the internal `AtomicWriteGrain` detects either of the above (or the Orleans `OrleansMessageRejectionException("Unable to create local activation")` shape that fires when a leaf grain has been deactivated as part of the same shutdown), the saga short-circuits the retry loop and the per-shard compensate-broadcast pass and wraps the cause in this exception so consumers can detect the regime via a single `is` check.

Caller contract: treat as back-pressure, not as a real failure. The entries the operation carried were never durably committed, but the silo refused to accept them because the host is going away rather than because the storage layer rejected them. A caller observing this exception should abandon the operation rather than retry it - every subsequent attempt against the same silo activation in this lifetime will fail with the same exception, because the writer drain is a one-way transition. Long-lived clients should either fail over to a peer silo (if the cluster is multi-node) or surface the back-pressure to upstream callers (drop the request, queue it to a side outbox, or rate-limit). Re-issuing the same operation after the host restarts is the normal recovery path; the previously failed entries are not durable, so the re-issue is a fresh attempt against a fresh silo activation.

```csharp verify
var entries = new List<KeyValuePair<string, byte[]>>
{
    new("k1", new byte[] { 0x01 }),
    new("k2", new byte[] { 0x02 }),
};

try
{
    await lattice.SetManyAsync(entries);
}
catch (LatticeShuttingDownException)
{
    // The host is shutting down. The entries did not commit.
    // Queue them to a side buffer / fail over to a peer silo /
    // surface back-pressure to the caller; do NOT retry against
    // this lattice activation.
}
```

On the `SetManyAtomicAsync` path, the saga's outcome is recorded on the `orleans.lattice.atomic_write.completed` counter as `outcome=shutdown_refused` so operators can distinguish saga failures caused by shutdown coincidence from saga failures caused by genuine commit conflicts on the same dashboard.

### Quieter shutdown logs

During the host deactivation window the Orleans runtime emits a `Warning` per in-flight grain call from two transport tear-down categories (`Orleans.Messaging` - "the silo is blocking application messages" - and `Orleans.Runtime.Placement.PlacementService` - "Unable to create local activation"). That is expected shutdown back-pressure, not a fault, but at steady-state verbosity it floods the silo log on every clean stop. `AddLattice` installs an in-library logger filter that demotes **only** those two categories' `Warning` records, and **only** while `IHostApplicationLifetime.ApplicationStopping` is signalled; on a healthy host the categories keep their `Warning` floor, and `Error`/`Critical` always survive even during shutdown so a genuine transport fault is never hidden. The filter is a Microsoft.Extensions.Logging `LoggerFilterRule` (not an `IGrainCallFilter`) because the records originate inside the Orleans runtime's own logging rather than in the grain-call pipeline. No host configuration is required; the demotion is automatic with `AddLattice`.

## Saturation back-pressure - `LatticeSaturatedException`

Public typed exception thrown by the WAL writer admission gate and the atomic-write saga coordinator when an operation cannot complete because the per-tree `IWalSaturationSignal` reported `WalSaturationState.Saturated` for longer than the caller's configured wait budget. Distinct from [`LatticeShuttingDownException`](#shutdown-back-pressure---latticeshuttingdownexception): saturation is a *recoverable* steady-state regime (offered load is exceeding the storage layer's sustained drain rate), not a one-way silo shutdown. Derives from `InvalidOperationException` so existing catch handlers continue to absorb it; the typed slot lets callers that care about the saturation regime explicitly distinguish it from genuine `InvalidOperationException` failures.

Surfaces from two distinct saturation failure shapes that share the same operational meaning ("this tree's storage layer is back-pressured; the operation was refused"):

- **Writer-side admission refusal.** `WalCommitLogWriter` consults the per-tree saturation signal before each `PartitionTracker.AcquireAsync`. On `Saturated`, the writer awaits `IWalSaturationSignal.WaitForHealthyAsync` up to `LatticeOptions.WalAdmissionSaturationWaitBudget` (default 5 seconds) and, if the regime persists, throws this exception so callers observe the back-pressure in budget time instead of parking on the admission semaphore for up to `WalAppendDispatchTimeout` (default 30 seconds).
- **Saga-coordinator quiesce refusal.** `AtomicWriteGrain.QuiesceOnSaturatedAsync` runs before each batched dispatch and parks the saga on `WaitForHealthyAsync` up to `min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)`. On budget expiry with the tree still `Saturated`, the saga's fast-path refuses with this exception rather than re-dispatching the same RowKeys into a still-throttled storage account (the canonical 409-Conflict amplification regime); the saga's persisted state stays at `Execute` with the current `NextIndex` so the caller's next retry on the same `operationId` resumes from where the refusal stopped.

Carries the originating tree id in the `TreeId` property so caller-side diagnostics can attribute the back-pressure to the specific tree without parsing the exception message. When the writer-side admission gate refused, the property names the tree whose admission semaphore observed the saturation; when the saga refused, the property names the saga's tree (or, when an underlying writer-side `LatticeSaturatedException` was wrapped through `SetManyAsync`'s leaf fan-out, the extracted tree id of the writer-side refusal).

Caller contract: treat as back-pressure and **retry after backing off**. Typical recovery is 1-10 seconds (until the underlying storage account or per-partition WAL admission gate drains). Long-lived consumers should also reduce offered load on the affected tree until the per-tree signal returns to `Healthy`. Unlike `LatticeShuttingDownException`, retries against the same silo activation can succeed once the regime clears.

```csharp verify
var entries = new List<KeyValuePair<string, byte[]>>
{
    new("k1", new byte[] { 0x01 }),
    new("k2", new byte[] { 0x02 }),
};

try
{
    await lattice.SetManyAsync(entries);
}
catch (LatticeSaturatedException ex)
{
    // The per-tree saturation signal stayed Saturated past the
    // admission budget. The entries did not commit. Back off
    // (typical 1-10s), then retry. The TreeId property attributes
    // the back-pressure to the specific tree.
    Console.WriteLine($"tree {ex.TreeId} is saturated; backing off");
    await Task.Delay(TimeSpan.FromSeconds(2));
    // retry...
}
```

The writer-side admission refusal is also recorded on the `orleans.lattice.wal.writer.append.admission_saturation_refusals` counter (tagged `tree`, `partition`) so operators can dashboard the back-pressure rate separately from the dispatch-timeout counter (`orleans.lattice.wal.writer.append.admission_timeouts`) and the drain-release counter (`orleans.lattice.wal.writer.append.drain.releases`).

## Leaf-projection digest

`LeafProjectionDigest { byte[] Hash; long EntryCount; long CheckpointOffset; int Version; }` (alias `ol.lpd`).

```csharp verify
LeafProjectionDigest digest = await tree.GetLeafProjectionDigestAsync(
    shardIndex: 0,
    cancellationToken);
```

Throws `ArgumentOutOfRangeException` when `shardIndex` is not a
physical shard of the per-tree map, `InvalidOperationException` for
activations on reserved system-tree prefixes or when
`LatticeOptions.MaintainProjectionDigest = false` (the per-tree
opt-out makes the digest API unavailable), and
`OperationCanceledException` if the token was already cancelled. See
[Projection Rebuild](projection-rebuild.md) for the determinism
contract, the cost model, and the related `ProjectionRebuildPolicy`
recovery options.

## Operator tooling: projection rebuild and materialiser lag

Two `ILattice` methods expose operator-driven projection recovery
and steady-state materialiser-lag observation. Both surfaces are
narrow: they do not freeze the tree, take no consistency lock, and
are safe to call against a live shard under load.

| Method | Description |
|--------|-------------|
| `RebuildLeafProjectionAsync(int shardIndex, CancellationToken)` | Clears the projection state of every leaf in the specified physical shard - `Entries`, `ProjectionHash`, the persisted `ProjectionCheckpointOffset`, and the in-memory pending-saga and recently-terminal dedup buffers - then deactivates each leaf so its next activation re-materialises the projection from the WAL via the standard activation-time replay path (including snapshot-then-WAL recovery when `ProjectionRebuildPolicy.SnapshotThenWal` is in effect). Topology-bearing state (`TreeId`, `ShardIndex`, key-range bounds, sibling pointers) is preserved. Throws `ArgumentOutOfRangeException` when `shardIndex` is not a physical shard of the per-tree map, `InvalidOperationException` for system-tree activations, and `OperationCanceledException` if the token is cancelled. |
| `GetMaterialiserLagAsync(CancellationToken)` | Returns the maximum lag, in WAL entries, between the shard's WAL head offset and the minimum leaf-projection checkpoint offset across all leaves in the tree. A return value of `0` indicates the materialiser is fully caught up across every shard; a steadily growing value indicates the materialiser is falling behind WAL ingestion. The result is clamped at zero. Throws `InvalidOperationException` for system-tree activations and `OperationCanceledException` if the token is cancelled. |

```csharp verify
// Operator-driven recovery: rebuild one shard's projection from
// the WAL after detecting drift via the digest surface.
await tree.RebuildLeafProjectionAsync(shardIndex: 0, cancellationToken);

// Steady-state lag observation: poll periodically and alert
// when lag crosses an SLO threshold.
long lag = await tree.GetMaterialiserLagAsync(cancellationToken);
// Emit (treeId, lag) to telemetry.
```

See [Projection Rebuild](projection-rebuild.md#operator-tooling-rebuild-and-lag)
for the rebuild semantics, the topology-preservation guarantees, and
the recommended monitoring shape for lag.

## Metrics

Orleans.Lattice publishes `System.Diagnostics.Metrics` instruments on
the static meter `orleans.lattice`, exposed via
`Orleans.Lattice.LatticeMetrics`. Instruments are grouped into five
tiers: shard-level ops, leaf-level latencies and counters, the read
cache, saga / coordinator / lifecycle outcomes, and events /
configuration. Subscribe with `.AddMeter("orleans.lattice")` on your
OpenTelemetry `MeterProviderBuilder`. See [Metrics](metrics.md) for
the full catalog and tag conventions.

## `SnapshotMode`

Controls source-tree availability during a snapshot operation.

| Value | Description |
|-------|-------------|
| `Offline` | Source tree is locked during the copy. Reads and writes throw `InvalidOperationException`. Produces a fully consistent snapshot. |
| `Online` | Source tree remains available throughout. The destination converges to a strongly-consistent view of the source at the drain's completion instant. |

## `LatticeExtensions`

| Method | Description |
|--------|-------------|
| `BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, int chunkSize)` | Streaming bulk load for large datasets. Input **must** be pre-sorted ascending by key. See [Bulk Loading](bulk-loading.md). |
| `SubscribeToEventsAsync(this ILattice, IClusterClient, Func<LatticeTreeEvent, Task>, string providerName = "Default", CancellationToken)` | Subscribes to the per-tree `LatticeTreeEvent` stream on the cluster client. Returns a `StreamSubscriptionHandle<LatticeTreeEvent>`; call `UnsubscribeAsync()` to stop. Throws `InvalidOperationException` when `providerName` is not registered on the client. See [Events](events.md). |

## `TypedLatticeExtensions`

Extension methods that serialize and deserialize via an
`ILatticeSerializer<T>`, eliminating per-caller `byte[]`
boilerplate. Each method has two overloads: one accepting an
explicit serializer and one that defaults to
`JsonLatticeSerializer<T>` (System.Text.Json with UTF-8 encoding).

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

Every method also has a parameterless overload that defaults to
`JsonLatticeSerializer<T>.Default`.

## Predicate operations

Typed overloads that take an `Expression<Func<T, bool>>` and evaluate it
**server-side** against a JSON document view of each value, so only matching
keys (or values) cross the wire. Each method has an explicit-serializer overload
and a `JsonLatticeSerializer<T>`-default overload (shown collapsed below). The
serializer must implement `ILatticePredicateSerializer` or the call throws
`NotSupportedException` before any RPC.

This table lists signatures only. For semantics, supported expressions, the
error surface, and runnable samples see
[Predicate Operations](predicated-operations.md).

| Method | Signature |
|--------|-----------|
| `GetManyAsync` | `Task<Dictionary<string, T>> GetManyAsync<T>(this ILattice, List<string> keys, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, CancellationToken = default)` |
| `SetManyAsync` | `Task<IReadOnlyList<string>> SetManyAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, CancellationToken = default)` |
| `SetManyAtomicAsync` | `Task<AtomicWriteOutcome> SetManyAtomicAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, CancellationToken = default)` |
| `SetManyAtomicAsync` (idempotent) | `Task<AtomicWriteOutcome> SetManyAtomicAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, Expression<Func<T, bool>> predicate, string operationId, ILatticeSerializer<T> serializer, CancellationToken = default)` |
| `ScanKeysAsync` | `IAsyncEnumerable<string> ScanKeysAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, int? maxAttempts = null, CancellationToken = default)` |
| `ScanEntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, T>> ScanEntriesAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, int? maxAttempts = null, CancellationToken = default)` |
| `ScanValuesAsync` | `IAsyncEnumerable<T> ScanValuesAsync<T>(this ILattice, ILatticeSerializer<T> serializer, Expression<Func<T, bool>>? predicate = null, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, int? maxAttempts = null, CancellationToken = default)` |
| `OpenKeyCursorAsync` | `Task<string> OpenKeyCursorAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken = default)` |
| `OpenEntryCursorAsync` | `Task<string> OpenEntryCursorAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken = default)` |
| `OpenSnapshotKeyCursorAsync` | `Task<string> OpenSnapshotKeyCursorAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken = default)` |
| `OpenSnapshotEntryCursorAsync` | `Task<string> OpenSnapshotEntryCursorAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken = default)` |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, string startInclusive, string endExclusive, ILatticeSerializer<T> serializer, CancellationToken = default)` |
| `OpenDeleteRangeCursorAsync` | `Task<string> OpenDeleteRangeCursorAsync<T>(this ILattice, Expression<Func<T, bool>> predicate, string startInclusive, string endExclusive, ILatticeSerializer<T> serializer, CancellationToken = default)` |

Supporting public types: `LatticePredicateTranslator`,
`LatticePredicateNode`, `LatticePredicateNodeKind`, `LatticeConstant`,
`LatticeConstantKind`, `LatticeComparisonOperator`, `LatticeBooleanOperator`,
`LatticeStringMethod`, `LatticePredicateContext`,
`ILatticePredicateSerializer`, and the `AtomicWriteOutcome` enum
(`Committed`, `PreconditionFailed`).

## Cross-tree atomic writes

`IGrainFactory` / cluster-client extension methods
(`LatticeCrossTreeAtomicWriteExtensions`) that commit a batch spanning two or
more distinct `ILattice` trees all-or-nothing, with the same atomic-visibility
guarantee `SetManyAtomicAsync` gives within a single tree. A stable
`operationId` is **required** (no auto-generated overload) because a cross-tree
saga touches multiple registries and a stable idempotency key is mandatory for
safe retry; it must be non-empty and must not contain `'/'`.

| Method | Signature |
|--------|-----------|
| `SetManyAtomicAsync` | `Task<CrossTreeAtomicWriteOutcome> SetManyAtomicAsync(this IGrainFactory factory, IReadOnlyList<LatticeTreeBatch> batches, string operationId, CancellationToken = default)` |
| `BeginAtomicWrite` | `LatticeAtomicWriteBuilder BeginAtomicWrite(this IGrainFactory factory, string operationId)` |

The fluent builder `LatticeAtomicWriteBuilder` accumulates per-tree slices and
commits them as one cross-tree saga:

| Method | Signature |
|--------|-----------|
| `ForTree` | `LatticeAtomicWriteBuilder ForTree(string treeId)` |
| `Set` | `LatticeAtomicWriteBuilder Set(string key, byte[] value)` |
| `Set<T>` | `LatticeAtomicWriteBuilder Set<T>(string key, T value, ILatticeSerializer<T> serializer)` |
| `Set<T>` (default serializer) | `LatticeAtomicWriteBuilder Set<T>(string key, T value)` |
| `SetWhere<T>` | `LatticeAtomicWriteBuilder SetWhere<T>(string key, T value, Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer)` |
| `SetWhere<T>` (default serializer) | `LatticeAtomicWriteBuilder SetWhere<T>(string key, T value, Expression<Func<T, bool>> predicate)` |
| `CommitAsync` | `Task<CrossTreeAtomicWriteOutcome> CommitAsync(CancellationToken = default)` |

`CommitAsync` (and `SetManyAtomicAsync`) returns
`CrossTreeAtomicWriteOutcome.Committed` when every tree''s optional guard passed
and all writes committed, or `CrossTreeAtomicWriteOutcome.PreconditionFailed`
when a guard failed and nothing committed on any tree. It throws
`InvalidOperationException` if a write fails (after the saga compensates) or if
the same `operationId` is re-submitted with a different tree-set or key-set.
Supporting public types: `LatticeTreeBatch` (per-tree slice: `TreeId`,
`Entries`, optional `Predicate`) and the `CrossTreeAtomicWriteOutcome` enum
(`Committed`, `PreconditionFailed`). See
[Atomic Writes - Cross-tree atomic writes](atomic-writes.md#cross-tree-multi-tree-atomic-writes).

## CRDT value-surface accessors

`ILattice.OrSet(key)`, `ILattice.PnCounter(key)`,
`ILattice.VersionVector(key)`, `ILattice.MvRegister<T>(key)`, and
`ILattice.OrMap<TKey, TValue>(key)` return lightweight,
allocation-free accessors that read and write a single key under
optimistic concurrency. Each accessor exposes the primitive's
natural mutation API - add/remove, increment/decrement, tick/merge,
set/values - instead of forcing callers to hand-roll byte arrays
and CAS retry loops. The underlying state types are CRDTs whose
`Merge` is commutative, associative, and idempotent, so concurrent
updates from multiple replicas converge without coordination.

`ILattice.Sequence<T>(key)` adds a Replicated Growable Array (RGA)
sequence accessor for collaborative ordered lists and text;
concurrent inserts under the same parent converge on a deterministic
order via the standard RGA descending `(Counter, ReplicaId)`
tie-break, and removes tombstone the targeted node so a later
re-insert against the same parent still resolves correctly.

> See [`state-primitives.md`](state-primitives.md) for the
> convergence semantics, merge rules, and example use cases of each
> primitive (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`,
> `OrMap`, `Rga`) - including when to prefer one primitive over
> another and the recursive `ICrdt<TSelf>` contract that lets `OrMap`
> nest other CRDTs as values.

```csharp verify
using Orleans.Lattice;

// Observed-remove set: concurrent adds and removes converge.
await tree.OrSet("tags:42").AddAsync("urgent"u8.ToArray(), replicaId: "siloA");
await tree.OrSet("tags:42").AddAsync("review"u8.ToArray(), replicaId: "siloA");
bool isUrgent = await tree.OrSet("tags:42").ContainsAsync("urgent"u8.ToArray());

// Positive-negative counter: concurrent increments across replicas sum.
await tree.PnCounter("hits:home").IncrementAsync("siloA");
await tree.PnCounter("hits:home").IncrementAsync("siloA", amount: 3);
long hits = await tree.PnCounter("hits:home").ValueAsync();

// Version vector: track per-replica causal history.
await tree.VersionVector("vv:order:1").TickAsync("siloA");
var vv = await tree.VersionVector("vv:order:1").GetAsync();

// Multi-value register: concurrent writes survive as conflict candidates.
await tree.MvRegister<string>("cart:42").SetAsync("siloA", "Alice's cart");
IReadOnlyList<string> candidates = await tree.MvRegister<string>("cart:42").ValuesAsync();

// Observed-remove map of CRDT-typed values: per-key values fold via
// the value CRDT's MergeFrom, so concurrent writes under the same
// map key converge into a single recursively-merged value.
var tagsByUser = tree.OrMap<string, OrSet>("tags-by-user");
var localTags = new OrSet();
localTags.Add("urgent"u8.ToArray(), replicaId: "siloA", counter: 1);
await tagsByUser.SetAsync("alice", "siloA", localTags);
OrSet? aliceTags = await tagsByUser.GetValueAsync("alice");

// Replicated Growable Array (RGA) sequence: collaborative
// ordered list / text. Concurrent inserts under the same
// parent converge on a deterministic order, removes tombstone
// nodes to keep causal stability for re-inserts.
var transcript = tree.Sequence<string>("chat:42");
await transcript.InsertAtAsync(0, "siloA", "Hello");
await transcript.InsertAtAsync(1, "siloA", "World");
IReadOnlyList<string> lines = await transcript.ToListAsync();
```

| Accessor | Method | Description |
|----------|--------|-------------|
| `OrSetAccessor` | `Task<OrSet> GetAsync()` | Reads the current set; returns an empty `OrSet` when absent or tombstoned. |
| `OrSetAccessor` | `Task AddAsync(byte[] element, string replicaId)` | Adds `element` with a fresh causal dot. Concurrent adds from other replicas survive a later remove that did not observe them. |
| `OrSetAccessor` | `Task RemoveAsync(byte[] element)` | Tombstones every dot currently observed for `element`. A no-op when the element is absent. |
| `OrSetAccessor` | `Task<bool> ContainsAsync(byte[] element)` | Returns `true` when `element` is a member of the set. |
| `OrSetAccessor` | `Task MergeAsync(OrSet other)` | Merges `other` into the stored state under CAS. |
| `PnCounterAccessor` | `Task<PnCounter> GetAsync()` | Reads the current counter state. |
| `PnCounterAccessor` | `Task<long> ValueAsync()` | Reads the current scalar value. |
| `PnCounterAccessor` | `Task IncrementAsync(string replicaId, long amount = 1)` | Advances the positive component for `replicaId`. `amount` must be non-negative. |
| `PnCounterAccessor` | `Task DecrementAsync(string replicaId, long amount = 1)` | Advances the negative component for `replicaId`. `amount` must be non-negative. |
| `PnCounterAccessor` | `Task MergeAsync(PnCounter other)` | Merges `other` into the stored state under CAS. |
| `VersionVectorAccessor` | `Task<VersionVector> GetAsync()` | Reads the current vector state. |
| `VersionVectorAccessor` | `Task TickAsync(string replicaId)` | Advances the entry for `replicaId` and persists the result. |
| `VersionVectorAccessor` | `Task MergeAsync(VersionVector other)` | Merges `other` into the stored state under CAS. |
| `MvRegisterAccessor<T>` | `Task<MvRegister> GetAsync()` | Reads the raw register state, including every dot-tagged entry. |
| `MvRegisterAccessor<T>` | `Task<IReadOnlyList<T>> ValuesAsync()` | Returns the live deserialised values. A single-valued register returns one element; a concurrently-written register returns every conflict candidate in deterministic order. |
| `MvRegisterAccessor<T>` | `Task SetAsync(string replicaId, T value)` | Writes `value` from `replicaId`. Drops every dot the writer observed and mints a fresh one - concurrent writes from other replicas survive the next merge. |
| `MvRegisterAccessor<T>` | `Task MergeAsync(MvRegister other)` | Merges `other` into the stored state under CAS. Entries observed in only one side are preserved; pointwise-max is applied to the dot context. |
| `OrMapAccessor<TKey, TValue>` | `Task<OrMap<TKey, TValue>> GetAsync()` | Reads the current map state. |
| `OrMapAccessor<TKey, TValue>` | `Task<TValue?> GetValueAsync(TKey mapKey)` | Returns the lattice-merged value at `mapKey`, or `null` when the key is absent or every observed dot has been tombstoned. |
| `OrMapAccessor<TKey, TValue>` | `Task<bool> ContainsKeyAsync(TKey mapKey)` | Returns `true` when `mapKey` has at least one live (un-tombstoned) dot. |
| `OrMapAccessor<TKey, TValue>` | `Task SetAsync(TKey mapKey, string replicaId, TValue value)` | Writes `value` at `mapKey` from `replicaId`, minting a fresh causal dot. Concurrent writes survive the next merge and are folded into a single per-key value via `ICrdt<TValue>.MergeFrom`. |
| `OrMapAccessor<TKey, TValue>` | `Task RemoveAsync(TKey mapKey)` | Tombstones every dot currently observed for `mapKey`. Concurrent writes on other replicas survive the next merge (add-wins). |
| `OrMapAccessor<TKey, TValue>` | `Task MergeAsync(OrMap<TKey, TValue> other)` | Merges `other` into the stored state under CAS. Per-key values are folded recursively through `TValue`'s `MergeFrom`. |
| `RgaAccessor<T>` | `Task<Rga> GetAsync()` | Reads the raw sequence state, including tombstoned nodes preserved for causal stability. |
| `RgaAccessor<T>` | `Task<IReadOnlyList<T>> ToListAsync()` | Returns the live values in resolved in-order projection (descending `(Counter, ReplicaId)` sibling tie-break). |
| `RgaAccessor<T>` | `Task<OrSetDot> InsertAtAsync(int index, string replicaId, T value)` | Inserts `value` at the visible position `index` in the materialised projection. Index `0` inserts at the head; an index equal to the count appends at the tail. Returns the new node's stable cursor dot. |
| `RgaAccessor<T>` | `Task<OrSetDot> InsertAfterAsync(OrSetDot parentDot, string replicaId, T value)` | Inserts as a child of `parentDot` (or `Rga.Root` for a top-level insert). Useful for tooling that captured a stable cursor identity from a previous read. |
| `RgaAccessor<T>` | `Task RemoveAtAsync(int index)` | Tombstones the live node at the visible position `index`. |
| `RgaAccessor<T>` | `Task RemoveAsync(OrSetDot dot)` | Tombstones the node identified by `dot`. A no-op when the dot is absent or already tombstoned. |
| `RgaAccessor<T>` | `Task MergeAsync(Rga other)` | Merges `other` into the stored state under CAS. |

Mutating methods retry on CAS failure up to a per-call budget
(default `OrSetAccessor.DefaultMaxAttempts` /
`PnCounterAccessor.DefaultMaxAttempts` /
`VersionVectorAccessor.DefaultMaxAttempts` /
`MvRegisterAccessor<T>.DefaultMaxAttempts` /
`OrMapAccessor<TKey, TValue>.DefaultMaxAttempts` /
`RgaAccessor<T>.DefaultMaxAttempts` = 16). When the budget is
exhausted the accessor throws `InvalidOperationException`; raise the
budget or reduce contention. Values are JSON-serialized via
`JsonLatticeSerializer<T>`, so the bytes are inspectable through
`ILattice.GetAsync`.

## `ILatticeSerializer<T>`

Implement this interface to provide a custom serialization strategy.
`JsonLatticeSerializer<T>` ships as the default.

| Member | Signature | Description |
|--------|-----------|-------------|
| `Serialize` | `byte[] Serialize(T value)` | Converts a value to bytes for storage. |
| `Deserialize` | `T Deserialize(byte[] bytes)` | Converts bytes back to a value. |

## `LatticeOptions`

See [Configuration](configuration.md) for detailed guidance, mutability
constraints, and per-tree overrides via the
[tree registry](tree-registry.md).

> **Structural sizing is pinned per-tree in the registry, not in
> `LatticeOptions`.** `MaxLeafKeys`, `MaxInternalChildren`, and
> `ShardCount` are seeded into the `TreeRegistryEntry` on first tree
> use from `LatticeConstants` (128 / 128 / 64). After seeding they
> are mutable only through `ILattice.ResizeAsync` and
> `ILattice.ReshardAsync`. Callers who want non-default sizing should
> either call `ResizeAsync` / `ReshardAsync` on a freshly-created
> tree (empty-tree fast-path) or pre-register the pin via
> `ILatticeRegistry.RegisterAsync` before first use.

> **The virtual shard space is not a runtime option.** It is a
> compile-time constant, `LatticeConstants.DefaultVirtualShardCount = 4096`.
> The pinned `ShardCount` must divide this constant evenly.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `KeysPageSize` | `int` | 512 | Keys per page in enumeration pagination. |
| `TombstoneGracePeriod` | `TimeSpan` | 24 h | Minimum age before a tombstone is eligible for compaction. `InfiniteTimeSpan` disables compaction. |
| `CompactionShardTickInterval` | `TimeSpan` | 500 ms | Gap between consecutive per-shard ticks during a compaction pass. Floor 100 ms; values below are clamped with a one-shot warning. Snapshotted at pass start. Scheduler-fairness knob, independent of leaf activation lifetime. See [Tombstone Compaction](tombstone-compaction.md#compactionshardtickinterval). |
| `CompactionLeafBatchSize` | `int` | 64 | Maximum number of leaves the coordinator visits within a single shard before yielding for one `CompactionShardTickInterval`. The leaf walk resumes on the next timer tick from a persisted in-shard cursor, so peak concurrent leaf activations during a pass are capped regardless of tree size. Floor 1; values below are clamped with a one-shot warning. Snapshotted at pass start. See [Tombstone Compaction](tombstone-compaction.md#compactionleafbatchsize). |
| `SoftDeleteDuration` | `TimeSpan` | 72 h | Retention window after soft-delete before purge. |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` | Minimum time between read-cache refreshes. Zero means refresh on every read. |
| `PrefetchKeysScan` | `bool` | `false` | When `true`, `ScanKeysAsync` overlaps the next page fetch with consumption. Overridable per call via `prefetch`. |
| `PrefetchEntriesScan` | `bool` | `false` | When `true`, `ScanEntriesAsync` overlaps the next page fetch with consumption. Gated separately from `PrefetchKeysScan` because entry pages carry values. Overridable per call. |
| `AutoSplitEnabled` | `bool` | `true` | Master switch for autonomic shard splitting. |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Ops/sec on a single shard that triggers an adaptive split. |
| `HotShardSampleInterval` | `TimeSpan` | 30 s | How often the hot-shard monitor polls hotness counters. |
| `HotShardSplitCooldown` | `TimeSpan` | 2 min | Minimum time between consecutive splits of the same shard. |
| `MaxConcurrentAutoSplits` | `int` | 2 | Maximum in-flight adaptive splits per tree. |
| `MaxConcurrentMigrations` | `int` | 4 | Maximum concurrent active-tombstone migrations per tree. |
| `MaxConcurrentDrains` | `int` | 4 | Maximum concurrent shadow-write drains per tree. |
| `SplitDrainBatchSize` | `int` | 1024 | Entries per batch during the drain phase of a split. |
| `ShardForwardTimeout` | `TimeSpan` | 15 s | Hard ceiling on a single outbound shard-to-shard write forward (the online-resize shadow forward and the adaptive-split migration forward). A forward that exceeds it is cancelled and surfaced as a `TimeoutException`, which the normal stale-routing retry envelope re-runs against refreshed routing - preventing a forward parked against a shard whose ownership is changing during a reshard swap from pinning the foreground write turn and wedging the per-shard fan-out. `InfiniteTimeSpan` restores the historical unbounded await. |
| `ActivationReadyTimeout` | `TimeSpan` | 15 s | Hard ceiling on a `ShardRootGrain`'s one-time activation-readiness seed (the first-touch cross-grain awaits a brand-new or freshly-reactivated shard runs while holding its non-reentrant activation gate: the defensive state re-read, the tree-registry registration, the deterministic root-leaf init, and the initial shard-state write). A seed that exceeds it is abandoned and surfaced as a `TimeoutException`, which the normal transient-exception retry envelope re-runs against refreshed routing once the dependency recovers - preventing a registry or leaf RPC parked against a not-yet-visible activation during a startup reshard or membership change from pinning the gate and wedging every interleaved read/write on the shard. Each seed step is idempotent on retry, so abandoning a parked seed never loses data or double-registers. `InfiniteTimeSpan` restores the historical unbounded await. |
| `DigestPublishTimeout` | `TimeSpan` | 15 s | Hard ceiling on a single internal-node upward digest publish (the `ChildDigestSnapshot` propagation a `BPlusInternalGrain` issues to its parent after folding a child's digest). The publish is held under the node's non-reentrant split gate while it recurses up the internal-node chain; a parent mid-mutation could otherwise park the await with no ceiling, pinning the gate and wedging every later mutating turn. A parked publish is abandoned and faulted as a `TimeoutException`, releasing the gate; the digest is staleness-tolerant so the next mutation's publish re-drives convergence with no count drift. `InfiniteTimeSpan` restores the historical unbounded await. |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 s | Minimum tree age before the hot-shard monitor begins sampling. |
| `MaxScanRetries` | `int` | 3 | Maximum bounded-retry passes for `CountAsync` / `ScanKeysAsync` / `ScanEntriesAsync` when topology changes mid-scan. |
| `CursorIdleTtl` | `TimeSpan` | 48 h | Sliding idle timeout for stateful cursors. `InfiniteTimeSpan` disables auto-cleanup. |
| `MaxCursorSnapshotPinTtl` | `TimeSpan` | 7 d | Hard cap on the registry-side lifetime of a point-in-time cursor's snapshot pin. `InfiniteTimeSpan` disables the cap. |
| `MaxPinnedSagaDecisions` | `int` | 100 000 | Registry-wide cap on the total saga decisions pinned across all live point-in-time cursors. |
| `AtomicWriteRetention` | `TimeSpan` | 48 h | Retention window for completed `SetManyAtomicAsync` saga state (idempotency window). `InfiniteTimeSpan` disables auto-cleanup. |
| `TxDecisionRetention` | `TimeSpan` | 60 s | Retention window for a completed saga's commit/abort decision in the per-tree registry after `ForgetAsync`. `TimeSpan.Zero` restores legacy immediate-evict semantics. See [Configuration](configuration.md#txdecisionretention). |
| `VersionVectorRetention` | `TimeSpan` | `InfiniteTimeSpan` | Retention window for version vectors of deleted keys in the read cache. `InfiniteTimeSpan` disables eviction. |
| `DiagnosticsCacheTtl` | `TimeSpan` | 5 s | Cache lifetime for `DiagnoseAsync` reports. `TimeSpan.Zero` disables caching. |
| `MaterialiserCheckpointInterval` | `TimeSpan` | 5 s | Time-based threshold for flushing a leaf-projection checkpoint. |
| `MaterialiserCheckpointEntries` | `int` | 5 000 | Entry-count threshold for flushing a leaf-projection checkpoint. |
| `MaxLeafReplayEntries` | `int` | 10 000 | Maximum WAL entries a cold leaf may replay at activation before falling back to `ProjectionRebuildPolicy`. |
| `LeafProjectionRetention` | `TimeSpan` | 7 d | Maximum age a leaf's persisted projection may have before activation forces snapshot-then-WAL recovery. `InfiniteTimeSpan` disables the age trigger. |
| `ProjectionRebuildPolicy` | enum | `SnapshotThenWal` | Recovery strategy when a leaf's fall-off-log triggers fire. See [Configuration](configuration.md#projectionrebuildpolicy). |
| `MaintainProjectionDigest` | `bool` | `true` | When `false`, `GetLeafProjectionDigestAsync` fast-fails with `InvalidOperationException`. Disabling is a **one-way operation per tree** - the first mutation under the disabled setting stamps an irreversible registry latch. System trees (`_lattice_*`) always resolve as `false`. |
| `PublishEvents` | `bool` | `false` | Opt-in publication of `LatticeTreeEvent` notifications onto an Orleans stream. See [Events](events.md). |
| `EventStreamProviderName` | `string` | `"Default"` | Stream provider Lattice publishes events onto. |
| `WalPartitions` | `int` | 8 | Number of independent WAL partitions per tree. Pinned per-tree on first WAL write. |
| `WalMaxBatchEntries` | `int` | 100 | Maximum WAL entries coalesced into a single flush. |
| `WalMaxBatchBytes` | `long` | 4 MiB | Maximum byte budget coalesced into a single flush. |
| `WalFlushTimeout` | `TimeSpan` | 15 s | Hard ceiling on a single per-shard WAL flush (the provider append plus the post-failure tail resync). A flush that exceeds it is cancelled and surfaced as a `TimeoutException` routed through the normal failure handler, preventing a hung provider call from pinning its in-flight slot and wedging the in-flight chain. `InfiniteTimeSpan` restores the historical unbounded await. |
| `WalFlushPreflightTimeout` | `TimeSpan` | 5 s | Hard ceiling on the per-shard WAL `FlushAsync` preflight region (the synchronous setup and initial scheduler yield that precede the bounded provider call). If the activation's grain scheduler never resumes the post-yield continuation within the deadline, the slot would sit in `_inFlight` with no provider-call deadline armed (`WalFlushTimeout` only covers the provider call, which has not been issued yet). The faulted preflight surfaces as a `TimeoutException` routed through the normal failure handler, the slot drains, and the `orleans.lattice.wal.flush.preflight.timeouts` counter attributes the trip per `(tree, shard)`. `InfiniteTimeSpan` restores the historical unbounded await. |
| `WalAppendDispatchTimeout` | `TimeSpan` | 30 s | Hard ceiling on a single writer-side outbound `IWalShardGrain.AppendBatchAsync` / `AppendAsync` dispatch. A dispatch that exceeds it is abandoned and surfaced as a `TimeoutException` so the request pipeline releases its slot rather than back-filling behind a wedged shard until the Orleans response deadline (default 3 minutes). Does **not** fix any wedge mechanism - the grain-side flush / activation deadlines already bound their own regions - it bounds the symptom on the writer side and makes every wedge attributable to a specific `(tree, shard)` via the `orleans.lattice.wal.append_dispatch.timeouts` counter in O(timeout) instead of O(response timeout) time. `InfiniteTimeSpan` restores the historical unbounded await. |
| `WalDrainBudget` | `TimeSpan` | 75 s | Hard ceiling on how long a per-shard WAL grain's `OnDeactivateAsync` drain may run before the remaining in-flight slots are force-faulted and the chain is released so the activation can finish tearing down. Bounds the host-level SIGTERM drain so the silo's shutdown accounting always settles within bounded time of the SIGTERM, regardless of whether the storage provider is healthy. The drain signals every in-flight flush's linked cancellation token at drain entry (so a co-operative provider gives up promptly), waits for the chain to settle naturally for up to this budget, and then force-faults any slot that has not unlinked with a typed `TimeoutException` so callers parked on `AppendAsync` / `AppendBatchAsync` are released. The matching `orleans.lattice.wal.shard.drain.budget.expirations` counter and `orleans.lattice.wal.shard.drain.budget.force_faulted_slots` histogram attribute the trip per `(tree, shard)`. `InfiniteTimeSpan` restores the historical unbounded-drain behaviour. |
| `WalRetention` | `TimeSpan?` | `null` | Optional wall-clock hard ceiling for WAL retention. `null` means retention is bounded purely by consumer cursors. |
| `WalMaxRetainedBytes` | `long?` | `null` | Optional advisory ceiling on retained WAL bytes per tree. When set, each `ILatticeWalGc.RunOnceAsync` pass samples retained bytes before and after its safe trim; if the pre-trim total exceeds the ceiling the policy schedules a byte-pressure trim (`BytePressureTriggered`), and `BytePressureOverThreshold` reports whether the tree is still over after the trim. Advisory only - the GC never trims past the safe frontier to honour it. `null` disables the policy. |
| `WalBytePressureReclaimTarget` | `double` | 0.8 | Fraction of `WalMaxRetainedBytes` a byte-pressure trim aims to reclaim toward. Ignored when `WalMaxRetainedBytes` is `null`. |
| `StorageUsageCacheTtl` | `TimeSpan` | 10 s | Cache lifetime for `ILattice.GetStorageUsageAsync` reports. `TimeSpan.Zero` disables caching. |
| `StorageUsagePollInterval` | `TimeSpan` | 15 s | Cadence at which every silo's background poller calls `ILatticeAdmin.PollWalUsageAsync` so the `storage.wal_bytes` and `storage.policy.over_threshold` gauges populate without any caller invoking the public API. The poll path is leaf-free: it activates only WAL partition grains, so idle trees stay cold. Snapshot / leaf-state / total-bytes gauges populate on demand via `ILattice.GetStorageUsageAsync` and `ILatticeAdmin.RefreshStorageUsageAsync`. Global knob read from the default (unnamed) options; per-tree overrides do not apply. `TimeSpan.Zero` or a negative value disables the poller. |
| `RetryPolicy` | `ILatticeRetryPolicy?` | `null` | Optional opt-in retry policy applied at the boundary of every public `ILattice` mutating method. Only consulted under an active `LatticeIdempotencyContext` scope. `null` preserves the throw-and-revert default. See [Retry Policy](retry-policy.md). |

## Idempotency keys and retry policy

Opt-in surface for retrying transient storage faults under a
caller-supplied identity. Default behaviour is throw-and-revert; the
library never installs a policy itself. See
[Retry Policy](retry-policy.md) for the full contract, operational
guidance, and code examples.

| Type | Member | Description |
|------|--------|-------------|
| `LatticeIdempotencyKey` | `HybridLogicalClock Timestamp { get; init; }` | Pins the logical write time. Re-stamped verbatim on every retry under the same key so LWW resolution treats retries as ties. |
| `LatticeIdempotencyKey` | `static LatticeIdempotencyKey Fresh()` | Convenience factory minting a fresh HLC tick. Consecutive calls produce distinct keys. |
| `LatticeIdempotencyContext` | `static LatticeIdempotencyKey? Current { get; set; }` | Reads or sets the ambient key on the current logical execution context. Flows across `await` points. |
| `LatticeIdempotencyContext` | `static IDisposable With(LatticeIdempotencyKey? key)` | Opens a scope that restores the previous ambient value on dispose. Idempotent on repeated dispose. |
| `LatticeIdempotencyContext` | `static IDisposable NewScope()` | Shorthand for `With(LatticeIdempotencyKey.Fresh())`. |
| `ILatticeRetryPolicy` | `Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)` | Re-invokes `operation` on transient failure under the same ambient scope; rethrows on exhaustion. |
| `ILatticeRetryPolicy` | `Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)` | Typed overload. |
| `BoundedExponentialRetryPolicy` | `ctor(int maxAttempts = 4, TimeSpan? initialDelay = null, TimeSpan? maxDelay = null, Func<Exception, bool>? retryableExceptionClassifier = null)` | Shipped default. Delay between attempts: `min(MaxDelay, InitialDelay * 2^(attempt-1))`. No jitter. |
| `BoundedExponentialRetryPolicy` | `ctor(BoundedExponentialRetryPolicyOptions options)` | Options-based constructor used by the DI extension. |
| `BoundedExponentialRetryPolicyOptions` | `int MaxAttempts { get; set; }` (default 4) | Total attempts, including the first. |
| `BoundedExponentialRetryPolicyOptions` | `TimeSpan InitialDelay { get; set; }` (default 50 ms) | First retry backoff. |
| `BoundedExponentialRetryPolicyOptions` | `TimeSpan MaxDelay { get; set; }` (default 2 s) | Per-attempt backoff cap. |
| `BoundedExponentialRetryPolicyOptions` | `Func<Exception, bool>? RetryableExceptionClassifier { get; set; }` (default `null`) | When non-null, only exceptions accepted by the classifier are retried. |
| `LatticeServiceCollectionExtensions` | `static ISiloBuilder AddLatticeRetryPolicy(this ISiloBuilder builder, Action<BoundedExponentialRetryPolicyOptions>? configure = null)` | DI helper that installs `BoundedExponentialRetryPolicy` as `LatticeOptions.RetryPolicy` for every tree. |

## Serializable types

All serializable types - and every grain interface, including the
public `ILattice` - carry stable `[Alias]` attributes (prefixed
`ol.`) to ensure wire-format and grain-manifest compatibility across
versions. Alias constants live in `TypeAliases` and must never be
renamed or removed: they are part of the public wire format.

Public types below are annotated with
`[EditorBrowsable(EditorBrowsableState.Never)]`. They remain `public`
for Orleans code generation but are hidden from IntelliSense because
they are implementation details not intended for direct use.

| Type | Alias | Visibility | Description |
|------|-------|------------|-------------|
| `HybridLogicalClock` | `ol.hlc` | public (hidden) | Hybrid logical clock for conflict-free timestamps. See [State Primitives](state-primitives.md). |
| `LwwValue<T>` | `ol.lwv` | public (hidden) | Last-writer-wins register. |
| `VersionVector` | `ol.vv` | public (hidden) | Causal version vector (pointwise-max merge). |
| `StateDelta` | `ol.sd` | public (hidden) | Delta of changed entries for replication. |
| `SplitResult` | `ol.sr` | public (hidden) | Result of a node split. |
| `KeysPage` | `ol.kp` | public (hidden) | Paginated batch of keys from a shard scan. |
| `EntriesPage` | `ol.ep` | public (hidden) | Paginated batch of key-value entries from a shard scan. |
| `TreeRegistryEntry` | `ol.tre` | public (hidden) | Per-tree metadata record. |
| `SnapshotMode` | `ol.snm` | public | Enum: `Offline`, `Online`. |
| `TreeResizeState` | `ol.trs` | internal | Persistent state tracking resize progress. |
| `ResizePhase` | `ol.rp` | internal | Enum: `Snapshot`, `Swap`, `Cleanup`. |
| `TreeSnapshotState` | `ol.tss` | internal | Persistent state tracking snapshot progress. |
| `SnapshotPhase` | `ol.snp` | internal | Enum: `Locking`, `Copying`, `Unlocking`, `Completed`. |
| `TreeDeletionState` | `ol.tds` | internal | Persistent state for soft-delete / purge tracking. |
| `TreeMergeState` | `ol.tms` | internal | Persistent state tracking merge progress. |
| `CasResult` | `ol.cas` | public (hidden) | Result of a compare-and-swap operation. |
| `VersionedValue` | `ol.vvl` | public (hidden) | A `byte[]` value paired with its `HybridLogicalClock` version. |
| `Versioned<T>` | `ol.ver` | public (hidden) | A typed value paired with its `HybridLogicalClock` version (used by typed extensions). |
| `ShardHotness` | `ol.sh` | public (hidden) | Volatile shard hotness counters. |
| `ShardMap` | `ol.sm` | public (hidden) | Per-tree mapping from virtual shard slots to physical shard indices. |
| `RoutingInfo` | `ol.ri` | public (hidden) | Per-activation routing snapshot returned by `ILattice.GetRoutingAsync()`. |
| `ShardCountResult` | `ol.scr` | internal | Per-shard count plus the set of virtual slots observed during the count. |
| `PendingMutationSnapshot` | `ol.pms` | internal | Snapshot of a single in-flight prepared mutation used during shard split. See [Shard Splitting](shard-splitting.md). |
| `LeafProjectionDigest` | `ol.lpd` | public | `readonly record struct` returned by `ILattice.GetLeafProjectionDigestAsync`. Carries the 16-byte XxHash128 hash, entry count, summed checkpoint offset, and a `Version` field stamping the contribution-function shape. Digests with different `Version` values must not be byte-compared. See [Projection Rebuild](projection-rebuild.md). |
| `ChildDigestSnapshot` | `ol.cds` | internal | `readonly record struct` propagating digest contributions up the tree. |
| `ProjectionRebuildPolicy` | - | public | Enum: `SnapshotThenWal` (default), `FullRebuildFromWal`, `Fail`. See [Configuration](configuration.md#projectionrebuildpolicy). |
| `TreeStorageUsageReport` | `ol.tsu` | public | `readonly record struct` returned by `ILattice.GetStorageUsageAsync`. Byte-accurate per-tree storage footprint. |
| `ClusterStorageUsageReport` | `ol.csu` | public | `readonly record struct` returned by `ILatticeAdmin.GetTotalStorageUsageAsync`. Cluster-wide storage roll-up. |
| `ShardStorageUsage` | `ol.ssu` | internal | `readonly record struct` per-shard leaf-state + snapshot byte roll-up. |
| `LatticeShuttingDownException` | `ol.lsd` | public | Typed `InvalidOperationException` subclass thrown by `ILattice` operators (and the internal `AtomicWriteGrain` saga coordinator) when an operation cannot complete because the owning silo's WAL writer is draining as part of host shutdown. See [Shutdown back-pressure](#shutdown-back-pressure---latticeshuttingdownexception) and [Atomic Writes](atomic-writes.md). |
| `WalPlacement` | `ol.wpl` | public | `readonly record struct` returned by `ILatticeAdmin.GetWalPlacementAsync`. A tree's durable WAL placement pin: default catalogue key, per-partition overrides, and CAS `Version`. See [WAL Storage Providers](wal-storage-providers.md#multi-account-fan-out-named-providers-and-pinned-placement). |
| `WalPartitionPlacement` | `ol.wpe` | public | `readonly record struct` - one partition's resolved catalogue key inside a `WalPlacement` / `WalPlacementAudit`. |
| `WalPlacementAudit` | `ol.wpa` | public | `readonly record struct` returned by `ILatticeAdmin.AuditWalPlacementAsync`. Placement plus per-silo resolvability of every pinned key. |
| `WalMovePlan` | `ol.wmp` | public | `readonly record struct` returned by `ILatticeAdmin.PlanWalMoveAsync`. Read-only dry run of a partition move. |
| `WalMoveBatchPlan` | `ol.wbp` | public | `readonly record struct` returned by the batch `ILatticeAdmin.PlanWalMoveAsync`. Wraps one `WalMovePlan` per partition plus `AllTargetsResolvableOnThisSilo`. |
| `WalMoveOptions` | `ol.wmo` | public | `readonly record struct` tuning a move (`QuiesceLease`, `CopyPageSize`, `VerifyAfterCopy`, `MaxConcurrentPartitionMoves`); `WalMoveOptions.Default` for the defaults. |
| `WalMoveReceipt` | `ol.wmr` | public | `readonly record struct` returned by `ILatticeAdmin.ExecuteWalMoveAsync` / `ReclaimMovedWalSourceAsync`. Records the offset range copied, the new pin version, and the move `Outcome`. |
| `WalMoveBatchReceipt` | `ol.wbr` | public | `readonly record struct` returned by the batch `ILatticeAdmin.ExecuteWalMoveAsync`. Wraps one `WalMoveReceipt` per partition plus the single placement-version transition the batch applied. |
| `WalMoveOutcome` | `ol.wmc` | public | Enum: `Moved`, `AlreadyAtTarget`, `SourceReclaimed`, `NoOp`. The terminal disposition of a move / reclaim call. |
| `LatticeSaturatedException` | `ol.lsa` | public | Typed `InvalidOperationException` subclass thrown by the WAL writer admission gate and the `AtomicWriteGrain` saga coordinator when an operation cannot complete because the per-tree saturation signal stayed `Saturated` past the caller's configured wait budget. Carries the originating `TreeId` for caller-side attribution. See [Saturation back-pressure](#saturation-back-pressure---latticesaturatedexception) and [WAL Saturation Signal](wal-saturation-signal.md). |
| `LatticeTreeBatch` | `ol.ltb` | public | `readonly record struct` - one tree's slice of a cross-tree atomic write (`TreeId`, `Entries`, optional `Predicate`). Deliberately **not** `[Immutable]` (mutable members). See [Cross-tree atomic writes](#cross-tree-atomic-writes). |
| `CrossTreeAtomicWriteOutcome` | `ol.cto` | public | Enum: `Committed`, `PreconditionFailed`. Terminal outcome of a cross-tree atomic write. |

## Public but not usable

A handful of types are necessarily `public` so that Orleans can
serialize them on the `ILattice` wire surface, but they are **not
intended as direct caller dependencies**. Their shape, members, and
return values can change in any release without notice; treat them
as wire-only.

To make this contract visible in tooling, every such type carries
`[EditorBrowsable(EditorBrowsableState.Never)]`. IDE IntelliSense
hides them from completion lists by default (the standard
IntelliSense behaviour for `EditorBrowsable(Never)`), so callers do
not surface them by accident when typing against `ILattice` or its
extensions.

The types and members in this category are:

| Symbol | Why public |
|--------|------------|
| `HybridLogicalClock` | Embedded in the wire types below; appears as the `Version` slot of `Versioned<T>` / `VersionedValue`. |
| `Versioned<T>` | Return shape of `TypedLatticeExtensions.GetWithVersionAsync<T>`. |
| `VersionedValue` | Return shape of `ILattice.GetWithVersionAsync` (the raw `byte[]` overload). |
| `RoutingInfo` | Return shape of `ILattice.GetRoutingAsync`, consumed by infrastructure helpers such as the streaming bulk loader. |
| `LatticeIdempotencyKey` | Token stamped via `LatticeIdempotencyContext.With(...)` to deduplicate retries; callers usually let the ambient retry policy mint and propagate it. |
| `RangeDeleteResult` | Return shape of an internal range-delete primitive that surfaces through DI plumbing. |
| `ILattice.KeysAsync` / `EntriesAsync` | Raw streaming overloads that omit the resume/reconnect handshake. Use `LatticeExtensions.ScanKeysAsync` / `ScanEntriesAsync` instead. |
| `ILattice.GetRoutingAsync` (both overloads) | Direct shard-addressing primitive used by infrastructure helpers. |

`ShardMap` is also `public` (it is reachable through `RoutingInfo`)
but is not marked hidden because it has no useful caller-facing
members beyond what `RoutingInfo` already exposes; treat it as
wire-only as well.

Every other grain interface in the assembly (shard root, leaf,
internal, registry, cursor, atomic-write saga, compaction, snapshot,
resize, reshard, replication apply, WAL shard, hot-shard monitor,
tree deletion / merge / split, leaf-cache, leaf-replay coordinator,
stats, and tx-registry grains) is declared `internal` and is not
visible from consumer assemblies at all. The C# type system enforces
that boundary at compile time.
