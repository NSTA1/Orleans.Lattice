# Consistency Guarantees

This document is the **single source of truth** for the consistency model
of every operation on `ILattice`. Other documents in this repository
describe *how* a given guarantee is implemented (shard-map routing,
shadow-forward splits, LWW merge, version-vector caches, saga
compensation, etc.) - but the statement of what a caller is actually
guaranteed to see lives here.

If you are looking for the algorithmic details of a specific mechanism,
follow the cross-references:

- Topology changes - [Shard Splitting](shard-splitting.md), [Online Reshard](online-reshard.md)
- Durable copies - [Snapshots](snapshots.md), [Tree Sizing](tree-sizing.md), [Tree Storage](tree-storage.md)
- Read path - [Read Caching](caching.md)
- State merge algebra - [State Primitives](state-primitives.md)
- Atomic batches - [Atomic Writes](atomic-writes.md)
- Checkpointed scans - [Durable Cursors](durable-cursors.md)
- TTL expiry - [TTL](ttl.md)

---

## Consistency levels used in this document

Lattice offers four distinct guarantees, from strongest to weakest. Every
public `ILattice` method is classified against exactly one of these.

| Level | Meaning |
|-------|---------|
| **Linearizable** | Each call appears to take effect instantaneously at a single point between its invocation and its return. Subsequent reads - from any silo, any client - observe the write once it has returned (subject to the read-cache staleness note below). |
| **Strongly consistent** | The caller observes a state that is consistent with some real-time point (usually the moment the operation started), with no entry missed, double-counted, or misattributed even when the underlying shard topology is mutating concurrently. For scans this means no phantom or missing keys; for counts it means the exact live key count. |
| **Snapshot (online)** | The operation sees a best-effort point-in-time view. No single global snapshot is taken, so a key updated between two shard visits may be observed in its pre-update state in one shard and its post-update state in the next. Equivalent to a *non-repeatable read* isolation level. |
| **Eventually consistent** | The operation may reflect a read-cache delta lag or a replication window, but converges to the authoritative state within a bounded, configurable interval. |

An additional property - **atomicity** - is called out for batch
operations: *all-or-nothing* commit. Atomicity is orthogonal to the
visibility model of concurrent readers; both are stated where they apply.

---

## Single-key operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `GetAsync` | **Linearizable** under default `CacheTtl = TimeSpan.Zero`; **eventually consistent** when `CacheTtl > 0` | Served via the per-silo `LeafCacheGrain`. Under the default `CacheTtl = TimeSpan.Zero` every read round-trips a `GetDeltaSinceAsync` against the primary leaf - same linearization point as `GetWithVersionAsync`, just with the cached bytes returned when the version-vector comparison shows no change. Setting `CacheTtl > 0` lets the cache serve reads from its local dictionary without contacting the primary; staleness is then bounded by `CacheTtl + one delta round-trip`. |
| `GetWithVersionAsync` | **Linearizable** | Bypasses the read cache and hits the primary leaf directly so the returned `HybridLogicalClock` version reflects the authoritative state. Designed for CAS loops. |
| `ExistsAsync` | **Linearizable** under default `CacheTtl = TimeSpan.Zero`; **eventually consistent** when `CacheTtl > 0` | Same path and same dual classification as `GetAsync`. |
| `SetAsync` (with or without TTL) | **Linearizable** | The write is durably persisted at the primary leaf before the call returns; the HLC tick is monotonic per-leaf. Point-consistent across shard splits because shadow-forwarding mirrors writes to the new owner during drain, and the post-swap reject phase forces stale routing to retry against the new owner. |
| `SetIfVersionAsync` | **Linearizable CAS** | Atomic compare-and-set: the HLC is checked and the new value is committed under the same leaf activation turn. |
| `GetOrSetAsync` | **Linearizable** | The leaf grain short-circuits on a live value under a single turn; no read-then-write race. |
| `DeleteAsync` | **Linearizable** | Writes a tombstone at the primary leaf. The tombstone is visible to the same guarantees as any other write; actual byte reclamation is deferred to [tombstone compaction](tombstone-compaction.md). |

Point operations transparently retry on `StaleShardRoutingException` and
`StaleTreeRoutingException` during shard-map swaps, resize alias swaps,
and snapshot drain rejects. Callers never see these exceptions.

---

## Batch operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `GetManyAsync` | **Per-key linearizable** under default `CacheTtl = TimeSpan.Zero`; **per-key eventually consistent** when `CacheTtl > 0` | Each key is fetched independently via the read-cache path and inherits `GetAsync`'s dual classification. The batch is **not** snapshot-atomic across keys: under any setting, two keys in a single call may reflect different real-time points (each is linearized at its own primary delta-check). When `CacheTtl > 0`, different keys in a single call may additionally reflect different cache refresh instants. |
| `SetManyAsync` | **Per-key linearizable, batch non-atomic** | Each key's write is linearizable; the batch as a whole is **not** atomic. A partial failure leaves the batch half-applied with no compensating rollback. |
| `SetManyAtomicAsync` | **Per-key linearizable, batch atomic, strictly isolated tree-wide** | All-or-nothing: on success every key holds its new value; on any failure every already-committed key is rolled back via LWW compensation with a fresh HLC tick. **Readers concurrent with an in-flight saga observe the saga atomically** - the per-tree `ITxRegistryGrain` is the single tree-wide linearization point, and every read path (direct leaf, read-cache, multi-shard scan) dials back through it for any key sitting in a leaf's pending-tx bucket. The post-decision / pre-terminal-fan-out window is invisible to readers. See [Atomic Writes](atomic-writes.md). |
| `DeleteRangeAsync` | **Strongly consistent** | Walks the leaf chain in every shard under the authoritative `ShardMap`, tombstoning matching entries. Robust against sparse multi-shard distributions (`RangeDeleteResult.PastRange` ensures the scan does not short-circuit on an empty leaf). For resumable, crash-safe range deletes prefer `OpenDeleteRangeCursorAsync`. |
| `CountAsync` | **Strongly consistent** | Partitions virtual slots by current owner via the authoritative `ShardMap`, asks each physical shard to count only its owned slots via `IShardRootGrain.CountForSlotsAsync`, re-reads the map version, and retries on version change up to `LatticeOptions.MaxScanRetries`. Every virtual slot is counted exactly once. Throws `InvalidOperationException` on retry exhaustion. |
| `CountPerShardAsync` | **Strongly consistent** | Same per-slot routing as `CountAsync`; per-shard counts are topology-consistent with the observed `ShardMap` snapshot. |

---

## Enumeration

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `ScanKeysAsync` | **Strongly consistent, strictly ordered** | Paginated k-way merge across shards. When a shard reports moved-away slots or the `ShardMap` version advances mid-scan, the orchestrator drains the affected slots from their current owners into a sorted in-memory cursor and injects it into the same priority queue, so output remains globally sorted end-to-end. A per-call `HashSet<string>` suppresses duplicates across pre- and post-swap views. Bounded by `LatticeOptions.MaxScanRetries`; throws `InvalidOperationException` on exhaustion. Transparently recovers from `Orleans.Runtime.EnumerationAbortedException` up to the `maxAttempts` parameter (default 8; see `LatticeExtensions.DefaultScanReconnectAttempts`). |
| `ScanEntriesAsync` | **Strongly consistent, strictly ordered** | Same reconciliation-cursor injection algorithm as `ScanKeysAsync`. Values read alongside keys reflect the authoritative leaf state at the moment the key was dequeued from the priority queue. Same enumeration-abort recovery as `ScanKeysAsync`. |
| Durable cursor steps (`NextKeysAsync`, `NextEntriesAsync`, `DeleteRangeStepAsync`) | **Per-step strongly consistent, cross-step snapshot** | Each step is routed through the stateless `ScanKeysAsync` / `ScanEntriesAsync` / `DeleteRangeAsync` path, inheriting its guarantee. Global ordering is preserved across steps by strictly excluding every previously-yielded key from the next step's range. **Values are snapshot-as-read per step, not per cursor** - a key updated between two steps is observed at its newest value when it is next visited, but once yielded by a cursor it is never re-yielded by the same cursor. **Atomic visibility is per-leaf, not tree-wide**: each step consults `ITxRegistryGrain` once per leaf RPC (no `LatticeRegistrySnapshotContext` is established), so a saga that transitions `InFlight` → `Committed` during a step's fan-out can be observed as `InFlight` on one leaf and `Committed` on another within that step; once the step yields a key, that key's visibility is final for the cursor. Use `GetManyAsync` if tree-wide atomic visibility across a known key set is required. See [Durable Cursors](durable-cursors.md). |

### Retry exhaustion

`CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` use a bounded retry budget
(`LatticeOptions.MaxScanRetries`, default 3) to reconcile against
concurrent shard splits. If the topology keeps mutating beyond the
budget, the scan throws `InvalidOperationException` rather than returning
a silently incomplete result. This is not a realistic concern under
default settings (`MaxConcurrentAutoSplits = 2`,
`HotShardSplitCooldown = 2 min`); see
[API Reference - Scan reliability](api.md#scan-reliability) for
mitigation options.

`ScanKeysAsync` / `ScanEntriesAsync` additionally recover from
`Orleans.Runtime.EnumerationAbortedException` - raised when the
orchestrator enumerator is reclaimed mid-scan (silo failover, idle
expiry, cold start). This is bounded separately by the `maxAttempts`
parameter on the wrapper (default 8; see
`LatticeExtensions.DefaultScanReconnectAttempts`); the wrapper
reopens the scan with a tightened bound based on the last yielded
key so the stream is deterministic - no duplicates, no gaps,
original ordering preserved.

---

## Maintenance operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `BulkLoadAsync` | **Linearizable on an empty tree** | Throws if any shard already has data. After return, all entries are visible with the guarantees above on subsequent operations. |
| `SnapshotAsync(Offline)` | **Linearizable point-in-time copy** | Source tree is locked (reads and writes throw `InvalidOperationException`) for the duration of the copy; the destination is an exact snapshot of the source at the lock instant, with HLC versions and TTL metadata preserved verbatim. |
| `SnapshotAsync(Online)` | **Strongly consistent** | Source tree remains available for linearizable point traffic throughout the drain. Concurrent writes are mirrored to the destination via the shadow-forward primitive with their original HLCs; LWW commutativity guarantees the destination converges to a consistent view of the source at the drain's completion instant regardless of drain/live-write interleaving. |
| `ResizeAsync` | **Linearizable (online)** | The tree continues to serve linearizable point operations and strongly-consistent scans throughout the drain. At the alias swap the stateless-worker `LatticeGrain` transparently absorbs the transient `StaleTreeRoutingException` and re-routes to the destination - callers observe at most a single retry delay. Zero data loss under concurrent load (LWW commutativity). |
| `UndoResizeAsync` | **Linearizable (online)** | See [Tree Sizing](tree-sizing.md#resizing-an-existing-tree) for the drain-window vs post-swap dual path; both paths preserve point and scan consistency. |
| `ReshardAsync` | **Linearizable (online)** | Dispatches up to `LatticeOptions.MaxConcurrentMigrations` concurrent per-shard splits, each of which atomically grows the `ShardMap` via its own shadow-write + swap phases. Reads and writes remain linearizable throughout. |
| `MergeAsync(sourceTreeId)` | **Eventually convergent (LWW)** | For each key present in both trees, the entry with the higher HLC wins. The operation completes when every source entry has been merged; the resulting tree is *strongly consistent with the LWW merge of both inputs*. The source tree is unmodified. |
| `DeleteTreeAsync` | **Linearizable (takes tree offline)** | After the call returns every subsequent `ILattice` read or write throws `InvalidOperationException` until `RecoverTreeAsync`. Data is retained for `SoftDeleteDuration` before purge. |
| `RecoverTreeAsync` | **Linearizable** | Restores full linearizable availability. |
| `PurgeTreeAsync` | **Linearizable, destructive** | Permanently removes all data. |
| `TreeExistsAsync`, `GetAllTreeIdsAsync` | **Eventually consistent (registry read)** | Registry reads reflect the latest persisted state but may briefly lag a concurrent registration/deletion observed by a different client. |
| `IsMergeCompleteAsync`, `IsSnapshotCompleteAsync`, `IsResizeCompleteAsync`, `IsReshardCompleteAsync` | **Monotonic** | Once `true` for a given operation, a subsequent query for the same operation will never return `false`. Vacuously `true` when no operation of that kind has ever been initiated. |
| `DiagnoseAsync` | **Point-in-time snapshot (non-linearizable)** | Aggregates a per-shard health sample across all physical shards; the fan-out is not atomic, so a concurrent split commit can race individual shard reports. Repeat calls within `LatticeOptions.DiagnosticsCacheTtl` return the same cached snapshot (identical `SampledAt`); the cache is invalidated on split commit so the next call after a topology change is guaranteed fresh. Per-shard RPC failures surface as empty shard entries rather than failing the whole report, so the method is safe to call during ongoing resize/reshard. **Not for hot-path or correctness-critical decisions** - use the operation-specific APIs (`GetRoutingAsync`, `CountAsync`) instead. See [Diagnostics](diagnostics.md). |

---

## Topology and durability notes

### Shard splits (autonomic and `ReshardAsync`-driven)

Every guarantee in the tables above holds **during an active shard
split**. The shadow-write primitive mirrors every mutation accepted by
the source shard to the new owner during the drain phase. At swap, the
source shard enters a reject phase that throws
`StaleShardRoutingException` for keys whose slots have moved; the
`LatticeGrain` orchestrator catches this, refreshes the cached
`ShardMap`, and retries against the new owner within the same grain
call. Scans use in-line reconciliation-cursor injection to preserve
strict order across the topology boundary. See
[Shard Splitting](shard-splitting.md) for the algorithm.

### Read-cache staleness

`GetAsync`, `ExistsAsync`, and `GetManyAsync` are the only `ILattice`
surface methods that read through `LeafCacheGrain`. Staleness is bounded
by `LatticeOptions.CacheTtl` (default `TimeSpan.Zero` - refresh on every
read, which reduces to a version-vector delta check when nothing has
changed). Raising `CacheTtl` trades freshness for fewer primary-leaf
round trips; the effective staleness bound is
`CacheTtl + one delta round-trip`. `GetWithVersionAsync` bypasses the
cache for CAS safety. For keys covered by an in-flight
`SetManyAtomicAsync` saga the cache delegates to the primary leaf
regardless of `CacheTtl` so the per-tree `ITxRegistryGrain` verdict
applies - strict per-tree atomic visibility is independent of cache
staleness.

### TTL expiry

TTL is resolved to an absolute UTC instant on the silo that accepts the
write. Expired entries are filtered on every user-facing read path and
the read cache; they are **intentionally preserved on the replication
layer** (`GetDeltaSinceAsync`, `MergeEntriesAsync`, `MergeManyAsync`) so
CRDT merge can still resolve LWW by HLC timestamp after expiry. See
[TTL](ttl.md).

### Clock skew

The guarantees above assume HLC causality, which depends on reasonably
synchronised **silo** clocks (not client clocks). Two concurrent writes
resolve by HLC: the silo with the later wall-clock tick wins. In
pathological drift scenarios the tombstone-grace window
(`LatticeOptions.TombstoneGracePeriod`, default 24 h) gives a lagging
replica time to observe and converge before physical reclamation.

### Cancellation

Every `ILattice` method accepts a `CancellationToken`. Cancellation
triggered before a mutation has committed leaves the operation as if it
had never been attempted; cancellation triggered after a long-running
coordinator (saga, resize, snapshot, reshard, merge) has accepted the
request does **not** roll back - the coordinator drives itself to a
terminal state via reminders and the corresponding `Is*CompleteAsync`
eventually returns `true`.

---

## Atomic visibility (single-tree, foreground and cross-cluster)

`SetManyAtomicAsync` delivers **strict per-tree atomic visibility**: the
per-tree `ITxRegistryGrain` is the single tree-wide linearization point
for the saga's commit/abort decision, and every read path consults it
for any key sitting in a leaf's pending-tx bucket. Concretely:

- **Direct-leaf reads** (`GetWithVersionAsync`, `SetIfVersionAsync`,
  `GetOrSetAsync`) - `BPlusLeafGrain.ResolvePendingStatusAsync` calls
  `registry.GetStatusAsync(txid)`. `Committed` surfaces the prepared
  value, `Aborted` falls through to the pre-saga value, `InFlight`
  hides the key (the strict-isolation default).
- **Cached reads** (`GetAsync`, `ExistsAsync`, `GetManyAsync`) -
  `LeafCacheGrain` tracks pending keys via
  `IBPlusLeafGrain.GetPendingKeysAsync` and round-trips any
  pending-keyed read to the primary leaf so the registry-driven verdict
  applies. Non-pending keys continue to be served from cache under the
  normal `CacheTtl` bound.
- **Tree-wide-snapshot reads** (`GetManyAsync`, `CountAsync`,
  `CountPerShardAsync`) - the entry point makes one
  `registry.SnapshotAsync()` call before fan-out, stamps it onto the
  ambient `LatticeRegistrySnapshotContext`, and every leaf in the call
  reads the same snapshot. A second snapshot taken after fan-out is
  compared against the first to detect any `InFlight` → `Committed`
  transition during the call; a transition triggers a retry under the
  fresh snapshot, closing the split-observation window between drained
  and undrained leaves. This is the path to use when atomic visibility
  across a known key set must hold tree-wide.
- **Per-leaf-snapshot reads** (`KeysAsync` / `ScanKeysAsync`,
  `EntriesAsync` / `ScanEntriesAsync`, `DeleteRangeAsync`, and the
  durable cursor steps `NextKeysAsync` / `NextEntriesAsync` /
  `DeleteRangeStepAsync`) - no tree-wide snapshot is taken. Each leaf
  consults `ITxRegistryGrain` independently per RPC and applies the
  saga verdict locally (`Committed` surfaces the prepared value,
  `InFlight` / `Aborted` fall through to the pre-saga value). Per-key
  visibility is correct, but a saga that transitions
  `InFlight` → `Committed` mid-fan-out can be observed as `InFlight`
  on one leaf and `Committed` on another within a single step. Use
  `GetManyAsync` / `CountAsync` if tree-wide atomic visibility across
  a known key set is required.

**Cross-cluster atomic visibility ships universally.** Saga prepare
writes flow through the standard per-key WAL → replication transport
carrying an additive `IsPrepared` slot and the saga's `TransactionId`;
each terminal mark (`TxCommit` / `TxAbort`) ships as a single per-shard
record exempt from the producer-side `KeyFilter` / `KeyPrefixes`
filters. On the receiver, `IReplicationApplyGrain.ApplyPreparedSetAsync`
/ `ApplyPreparedDeleteAsync` route each prepared entry into the local
leaf's per-tx pending bucket under the source HLC verbatim, and
`ApplyTxTerminalAsync` (a) marks the per-tree `ITxRegistryGrain` as
the linearization point for the saga's outcome, then (b) drives the
per-shard terminal-mark primitive that flips every receiver leaf's
pending bucket into the visible projection. Receiver-side reads
consult the same `ITxRegistryGrain` they consult locally, so a remote
reader concurrent with replication of a `SetManyAtomicAsync` observes
either zero or all of the saga's keys - never a partial view.

Once `SetManyAtomicAsync` returns without throwing, every key in the
batch holds its target value across the local tree with no
intermediate partial-visibility window observable to any reader at any
silo in that cluster. As the saga's WAL records propagate to each
peer, the same all-or-nothing window holds on every remote tree:
prepared entries are invisible until the terminal mark arrives, and
the terminal flip is atomic per-leaf and registry-coordinated
tree-wide.

---

## What Lattice does **not** guarantee

- **Global transaction ordering.** Two concurrent `SetManyAtomicAsync`
  calls touching overlapping keys resolve pairwise by LWW; there is no
  serializable global order across sagas.
- **Reader isolation during range deletes.** `DeleteRangeAsync` is
  per-key linearizable but is not registry-coordinated, so a reader
  concurrent with a long-running range delete may observe some keys
  tombstoned and others still live. For an isolated range delete,
  layer `GetWithVersionAsync` + `SetIfVersionAsync` on top, or stage
  the deletion via `OpenDeleteRangeCursorAsync` and gate visibility
  with an application-level marker key. (`SetManyAtomicAsync` does
  guarantee reader isolation tree-wide - see "Atomic visibility"
  above.)
- **Multi-tree transactions.** Operations that touch more than one tree
  (e.g. `MergeAsync`, `SnapshotAsync`) are not atomic across the tree
  boundary; they are LWW-convergent on the destination but readers of
  both trees may observe the in-flight state.
- **Cross-tree causality.** The causal+ guarantees shipped in the
  replication package are scoped to single-tree writes; a multi-tree
  operation (e.g. one saga touching two `ILattice` trees, or a merge
  fanning into a destination tree) does not establish a causal edge
  between those trees on remote peers. Each tree converges independently
  under its own per-tree vector clock.
