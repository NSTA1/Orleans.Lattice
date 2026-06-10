# Consistency Guarantees

This document is the **contract** for what a caller of `ILattice` is
guaranteed to observe. It states each guarantee in caller-visible terms
only and does not describe how Lattice delivers them.

For the implementation of any guarantee, follow the cross-references in
each section:

- Topology changes - [Shard Splitting](shard-splitting.md), [Online Reshard](online-reshard.md)
- Atomic batches - [Atomic Writes](atomic-writes.md)
- Multi-page enumerations - [Durable Cursors](durable-cursors.md), [Snapshot Cursors](snapshot-cursors.md)
- Durable copies - [Snapshots](snapshots.md), [Tree Sizing](tree-sizing.md), [Tree Storage](tree-storage.md)
- Read path - [Read Caching](caching.md)
- State merge - [State Primitives](state-primitives.md)
- TTL expiry - [TTL](ttl.md)

---

## Consistency levels

Every public `ILattice` method is classified against exactly one of the
following four levels.

| Level | What the caller observes |
|-------|--------------------------|
| **Linearizable** | The call takes effect at a single point between its invocation and its return. After the call returns, any subsequent read on any client at any silo observes the new value (subject to the cache-staleness note below). |
| **Strongly consistent** | The call observes a result consistent with some real-time point during its execution - no entry is missed, double-counted, or misattributed even when the underlying topology is changing concurrently. For scans this means no phantom or missing keys; for counts it means the exact live key count. |
| **Snapshot (online)** | The call observes a best-effort point-in-time view that is correct per-key but is not guaranteed to be a single global instant. Equivalent to a *non-repeatable read* isolation level. |
| **Eventually consistent** | The call may reflect a bounded staleness window (read-cache staleness, replication lag), but converges to the authoritative state within a configurable interval. |

An additional property - **atomicity** - is called out for batch
operations and means *all-or-nothing commit*. Atomicity is orthogonal to
the visibility model of concurrent readers, and both are stated where
they apply.

A separate property - **atomic visibility** - is called out for atomic
batches and snapshot reads, and means a concurrent reader observes
either the full effect of a saga or none of it, never a partial view.

---

## Single-key operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `GetAsync` | **Linearizable** under default `CacheTtl = TimeSpan.Zero`; **eventually consistent** when `CacheTtl > 0` | The default cache configuration refreshes on every read, so the call observes the latest committed value. Raising `CacheTtl` trades freshness for fewer round-trips; staleness is then bounded by `CacheTtl + one cache refresh`. |
| `GetWithVersionAsync` | **Linearizable** | Returns the value along with its authoritative HLC version for use in CAS loops. Bypasses the read cache. |
| `ExistsAsync` | **Linearizable** under default `CacheTtl = TimeSpan.Zero`; **eventually consistent** when `CacheTtl > 0` | Same dual classification as `GetAsync`. |
| `SetAsync` (with or without TTL) | **Linearizable** | The write is durably persisted before the call returns. Continues to hold across shard splits, resize, and reshard - callers never see topology-change exceptions. |
| `SetIfVersionAsync` | **Linearizable CAS** | Atomic compare-and-set against the HLC version returned by `GetWithVersionAsync`. |
| `GetOrSetAsync` | **Linearizable** | No read-then-write race. |
| `DeleteAsync` | **Linearizable** | The deletion is visible to subsequent reads under the same guarantee as any other write. |

Single-key operations transparently retry on any topology-change
exception. Callers never see `StaleShardRoutingException` or
`StaleTreeRoutingException`.

---

## Batch operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `GetManyAsync` | **Per-key linearizable** (default `CacheTtl`); **per-key eventually consistent** (`CacheTtl > 0`) | **Atomic visibility tree-wide**: a concurrent `SetManyAtomicAsync` is observed either entirely or not at all across the requested key set. The batch is *not* a global snapshot across non-saga keys - two unrelated keys may reflect different real-time points. |
| `SetManyAsync` | **Per-key linearizable, batch non-atomic** | Each key is written under its own linearization point. A partial failure leaves the batch half-applied with no rollback. |
| `SetManyAtomicAsync` | **Per-key linearizable, batch atomic, atomic-visible tree-wide, atomic-visible across clusters** | All-or-nothing: on success every key holds its new value; on failure every key holds its pre-saga value. **A concurrent reader observes the saga atomically** - the post-decision visibility flip is a single tree-wide point. The atomic-visibility guarantee extends across every cluster the tree replicates to. See [Atomic Writes](atomic-writes.md). |
| `DeleteRangeAsync` | **Strongly consistent** | Every key in the range is tombstoned. Robust against sparse multi-shard distributions. For resumable or crash-safe range deletes use `OpenDeleteRangeCursorAsync` instead. |
| `CountAsync` | **Strongly consistent, atomic-visible tree-wide** | Exact live key count under the topology snapshot the call observes. A concurrent `SetManyAtomicAsync` is observed atomically (included entirely or excluded entirely). Throws `InvalidOperationException` if topology changes outrun the retry budget (`LatticeOptions.MaxScanRetries`, default 3). |
| `CountPerShardAsync` | **Strongly consistent, atomic-visible tree-wide** | Per-shard counts are topology-consistent with the observed shard layout. Same atomic-visibility guarantee as `CountAsync`. |

---

## Enumeration

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `ScanKeysAsync` | **Strongly consistent, strictly ordered, atomic-visible for the lifetime of the enumeration** | Keys are yielded in lexicographic order with no duplicates and no gaps, even when shard splits or rebalances run concurrently. A concurrent `SetManyAtomicAsync` is observed identically across every page: either all of its keys appear or none. Bounded by `LatticeOptions.MaxScanRetries` (default 3); throws `InvalidOperationException` if the retry budget is exhausted. Transparently recovers from server-side enumeration aborts up to the wrapper's `maxAttempts` parameter (default 8). |
| `ScanEntriesAsync` | **Strongly consistent, strictly ordered, atomic-visible for the lifetime of the enumeration** | Same key ordering and atomic-visibility guarantees as `ScanKeysAsync`. Values reflect the authoritative state at the moment each key is yielded. |
| Durable cursor steps - **live mode** (`NextKeysAsync`, `NextEntriesAsync`, `DeleteRangeStepAsync`) | **Per-step strongly consistent and atomic-visible, cross-step snapshot** | Each step is a strongly consistent scan, atomic-visible tree-wide *within* that step. Across steps, a key updated between two pages is observed at its newest value when it is next visited, but once yielded by a cursor it is never re-yielded. A saga that commits between page *i* and page *i+1* may have its keys split across the two pages - use point-in-time mode (below) for cross-step atomicity. See [Durable Cursors](durable-cursors.md). |
| Durable cursor steps - **point-in-time mode** (opened with `pointInTime: true`) | **Strongly consistent, strictly ordered, atomic-visible for the cursor's lifetime** | Every page reads against the saga-decision view captured at `OpenAsync` time. A `SetManyAtomicAsync` that commits between two pages is observed identically on every page (either all of its keys, or none). A stalled cursor whose pin lifetime is exceeded surfaces `LatticeCursorSnapshotExpiredException` on its next call and must be reopened. Not available for `DeleteRangeStepAsync`. See [Durable Cursors - Point-in-time cursors](durable-cursors.md#point-in-time-cursors). |
| Snapshot cursor steps - **zero-observable-writes mode** (`OpenSnapshotKeyCursorAsync`, `OpenSnapshotEntryCursorAsync`) | **Snapshot-isolated, strictly ordered, atomic-visible for the cursor's lifetime** | Every page reflects the tree state captured at open time. No write committed after open - foreground `SetAsync` / `DeleteAsync`, saga `SetManyAtomicAsync`, `DeleteRangeAsync`, or replication apply - is ever visible to the cursor on any page. The captured `LatticeSnapshotCoordinate` is deterministic across silo failover. Open-time replay cost is bounded by `LatticeOptions.MaxSnapshotReplayEntries`; exceeding the budget throws `LatticeSnapshotReplayBudgetExceededException`. A cursor whose WAL retention pin is invalidated surfaces `LatticeSnapshotExpiredException` on its next call and must be reopened. See [Snapshot Cursors](snapshot-cursors.md). |

### Retry exhaustion

`CountAsync`, `CountPerShardAsync`, `ScanKeysAsync`, and
`ScanEntriesAsync` use a bounded retry budget
(`LatticeOptions.MaxScanRetries`, default 3) to reconcile against
concurrent topology changes. If the topology continues to mutate beyond
the budget the call throws `InvalidOperationException` rather than
returning a silently incomplete result. This is not a realistic concern
under default settings; see
[API Reference - Scan reliability](api.md#scan-reliability) for tuning
guidance.

The streaming scan wrappers (`ScanKeysAsync`, `ScanEntriesAsync`)
additionally recover from mid-scan enumeration aborts (silo failover,
idle expiry, cold start) up to the wrapper's `maxAttempts` parameter
(default 8). On reconnect the stream resumes with no duplicates, no
gaps, and ordering preserved.

---

## Maintenance operations

| Operation | Guarantee | Notes |
|-----------|-----------|-------|
| `BulkLoadAsync` | **Linearizable on an empty tree** | Throws if any shard already has data. After return, all entries are visible under the guarantees above. |
| `SnapshotAsync(Offline)` | **Linearizable point-in-time copy** | The source tree is locked (reads and writes throw `InvalidOperationException`) for the duration of the copy. The destination is an exact snapshot of the source at the lock instant. |
| `SnapshotAsync(Online)` | **Strongly consistent** | The source tree remains available for linearizable point traffic and strongly-consistent scans throughout. The destination converges to a consistent view of the source at the drain's completion instant regardless of how live writes interleave with the drain. |
| `ResizeAsync` / `UndoResizeAsync` | **Linearizable (online)** | Point operations and strongly-consistent scans continue throughout. Callers observe at most a single transparent retry at the alias swap. Zero data loss under concurrent load. |
| `ReshardAsync` | **Linearizable (online)** | Reads and writes remain linearizable across every concurrent shard split. |
| `MergeAsync(sourceTreeId)` | **Eventually convergent (LWW)** | For each key present in both trees, the entry with the higher HLC wins. On completion the destination is strongly consistent with the LWW merge of both inputs. The source tree is unmodified. |
| `DeleteTreeAsync` | **Linearizable (takes tree offline)** | After return, every subsequent read or write throws `InvalidOperationException` until `RecoverTreeAsync`. Data is retained for `SoftDeleteDuration` before purge. |
| `RecoverTreeAsync` | **Linearizable** | Restores full availability. |
| `PurgeTreeAsync` | **Linearizable, destructive** | Permanently removes all data. |
| `TreeExistsAsync`, `GetAllTreeIdsAsync` | **Eventually consistent (registry read)** | May briefly lag a concurrent registration or deletion observed by another client. |
| `IsMergeCompleteAsync`, `IsSnapshotCompleteAsync`, `IsResizeCompleteAsync`, `IsReshardCompleteAsync` | **Monotonic** | Once `true` for a given operation, never returns `false` again. Vacuously `true` when no operation of that kind has ever been initiated. |
| `DiagnoseAsync` | **Point-in-time snapshot (non-linearizable)** | A best-effort per-shard health sample for dashboards and post-mortems. Repeat calls within `LatticeOptions.DiagnosticsCacheTtl` return the same cached result. **Not for hot-path or correctness-critical decisions** - use the operation-specific APIs instead. See [Diagnostics](diagnostics.md). |

---

## Atomic visibility

`SetManyAtomicAsync` delivers **strict atomic visibility tree-wide and
across clusters**: no reader, on any silo, in any cluster, ever
observes a partial view of an in-flight saga. Concretely, once
`SetManyAtomicAsync` returns without throwing, every key in the batch
holds its target value across every silo in the local cluster with no
intermediate partial-visibility window observable to any reader. As
the saga's writes propagate to each peer cluster, the same
all-or-nothing window holds on every remote tree.

Atomic visibility is observed by every read path that does not stream
across multiple grain calls:

| Read path | Atomic visibility |
|-----------|-------------------|
| `GetAsync`, `ExistsAsync`, `GetWithVersionAsync`, `GetOrSetAsync`, `SetIfVersionAsync` | Per-key linearizable; an in-flight saga's keys are hidden until the saga commits, at which point all of its keys flip atomically. |
| `GetManyAsync`, `CountAsync`, `CountPerShardAsync` | Tree-wide for the call. |
| `ScanKeysAsync`, `ScanEntriesAsync` | Tree-wide for the lifetime of the `IAsyncEnumerable`. |
| Durable key/entry cursor (point-in-time mode) | Tree-wide for the lifetime of the cursor. |
| Snapshot key/entry cursor (zero-observable-writes mode) | Tree-wide for the lifetime of the cursor. Stricter than point-in-time mode: hides every concurrent write, not only sagas. |
| Durable key/entry/delete-range cursor (live mode) | Tree-wide *within* each step; not preserved across steps. |
| `DeleteRangeAsync` (one-shot) | Per-key only; a concurrent saga may be observed as committed for some keys and pending for others. Use `SetManyAtomicAsync` to layer atomic deletion semantics on top. |

See [Atomic Writes](atomic-writes.md) for the saga primitive and
[Durable Cursors](durable-cursors.md#point-in-time-cursors) for the
point-in-time cursor mode.

### Cross-tree (multi-tree) atomic visibility

`IGrainFactory.SetManyAtomicAcrossTreesAsync` (and the `BeginAtomicWrite`
builder) extend the single-tree atomic-visibility contract to a batch
spanning two or more distinct `ILattice` trees: either every targeted
key across every participating tree becomes visible, or none of them do.
A two-level saga drives this - a coordinator grain keyed by the
`operationId` writes a **single** global commit/abort decision, and each
participating tree's `ITxRegistryGrain` *delegates* the status of its
prepared txid to that coordinator until the decision lands. Before the
decision, every tree returns `InFlight` for the saga (prepared keys are
invisible, indistinguishable from pre-saga); after it, every tree
returns the same global verdict. The coordinator's single decision write
is the cross-tree linearization point.

This guarantees all-or-nothing *commit visibility*, not a simultaneous
multi-tree read transaction: a reader sampling several trees at
different instants may observe the global flip between samples, but never
a partial slice of one cross-tree saga on any single tree. See
[Atomic Writes: Cross-tree atomic writes](atomic-writes.md#cross-tree-multi-tree-atomic-writes).

---

## Topology and durability notes

### Shard splits and reshards

Every guarantee in this document holds **during an active shard split
or reshard**. Callers do not observe topology-change exceptions: point
operations transparently retry; scans use bounded reconciliation; in
the rare case of retry exhaustion, the affected call throws
`InvalidOperationException` rather than returning silently incomplete
results. See [Shard Splitting](shard-splitting.md).

### Read-cache staleness

`GetAsync`, `ExistsAsync`, and `GetManyAsync` are the only methods on
`ILattice` that may be served from the per-silo read cache. Staleness
is bounded by `LatticeOptions.CacheTtl` (default `TimeSpan.Zero` -
refresh on every read). Raising `CacheTtl` trades freshness for fewer
round-trips. `GetWithVersionAsync` bypasses the cache for CAS safety.
**Cache staleness never weakens atomic visibility**: keys covered by
an in-flight saga always observe the registry-coordinated outcome
regardless of `CacheTtl`.

### TTL expiry

Expired entries are filtered on every user-facing read path. See [TTL](ttl.md).

### Clock skew

The guarantees in this document assume reasonably synchronised **silo**
clocks (not client clocks). Two concurrent writes resolve by HLC: the
write with the later wall-clock tick wins. In pathological drift
scenarios the tombstone-grace window
(`LatticeOptions.TombstoneGracePeriod`, default 24 h) gives a lagging
replica time to converge before physical reclamation.

### Cancellation

Every `ILattice` method accepts a `CancellationToken`. Cancellation
before a mutation has committed leaves the operation as if it had never
been attempted. Cancellation after a long-running coordinator (saga,
resize, snapshot, reshard, merge) has accepted the request does **not**
roll back: the coordinator drives itself to a terminal state on its
own, and the matching `Is*CompleteAsync` eventually returns `true`.

---

## What Lattice does **not** guarantee

- **Global transaction ordering.** Two concurrent `SetManyAtomicAsync`
  calls touching overlapping keys resolve pairwise by LWW; there is no
  serializable global order across sagas.
- **Reader isolation during one-shot range deletes.** `DeleteRangeAsync`
  is per-key linearizable but is not registry-coordinated, so a reader
  concurrent with a long-running range delete may observe some keys
  tombstoned and others still live. For an isolated range delete,
  stage it via `SetManyAtomicAsync` or gate visibility with an
  application-level marker key. (`SetManyAtomicAsync` itself does
  guarantee reader isolation tree-wide.)
- **Cross-tree atomicity.** Atomic visibility is scoped to a single
  `ILattice` tree. Operations that touch more than one tree (e.g.
  `MergeAsync`, `SnapshotAsync`) are LWW-convergent on the destination
  but readers of both trees may observe the in-flight state. There is
  no cross-tree saga primitive.
- **Cross-tree causality.** The causal+ guarantees shipped in the
  replication package are scoped to single-tree writes; a multi-tree
  operation does not establish a causal edge between those trees on
  remote peers. Each tree converges independently under its own
  per-tree vector clock.
