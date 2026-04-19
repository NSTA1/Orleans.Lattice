# Orleans.Lattice Roadmap

Potential improvements and new features.

---

## ✅ Completed

### API

- [x] **F-001 — Range Delete (`DeleteRangeAsync`)**: Delete all keys within a lexicographic range (`startInclusive`, `endExclusive`) in a single call by walking the leaf chain and tombstoning matching entries in bulk.
- [x] **F-002 — `CountAsync` / `CountPerShardAsync`**: Return the number of live keys without streaming them across the wire. A per-shard variant aids diagnostics and load-balancing analysis.
- [x] **F-003 — `GetOrSetAsync` (conditional write)**: Set a key only if it does not already exist, avoiding a read-then-write roundtrip. The leaf grain short-circuits when a live value is present.
- [x] **F-004 — Typed value helpers**: A thin generic wrapper `ILattice<T>` and extension methods that accept serializer/deserializer delegates (or default to `System.Text.Json`) to eliminate per-caller `byte[]` boilerplate.
- [x] **F-005 — `EntriesAsync` (key + value scan)**: Stream `KeyValuePair<string, byte[]>` in sorted order, complementing the existing `KeysAsync`. Useful for exports, migrations, and analytics without a separate `GetAsync` per key.
- [x] **F-017 — Compare-and-swap (CAS)**: Optimistic concurrency via `SetIfVersionAsync(key, value, expectedVersion)` — the write succeeds only if the current entry's HLC matches, enabling safe read-modify-write patterns without distributed locks.
- [x] **F-026 — Operation status queries**: Expose `IsMergeCompleteAsync()`, `IsSnapshotCompleteAsync()`, and `IsResizeCompleteAsync()` on `ILattice` so callers can poll long-running maintenance operations without reaching into coordination grains. Returns `true` vacuously when no operation has ever been initiated; uses existing reminder-based crash recovery for fire-and-forget completion.

### Performance & Scalability

- [x] **F-006 — Leaf-side continuation filtering for `EntriesAsync`**: Pass the continuation token down to the leaf grain as an `afterExclusive` parameter so it filters entries at the source, avoiding unnecessary `byte[]` serialization across the grain boundary during forward pagination.
- [x] **F-007 — Leaf-side continuation filtering for `KeysAsync`**: Apply the same `afterExclusive` leaf-side filtering to `GetSortedKeysBatchAsync`, eliminating shard-level skip loops for key-only scans.
- [x] **F-008 — Reverse-scan leaf-side filtering**: Add a `beforeExclusive` parameter to leaf `GetKeysAsync` / `GetEntriesAsync` so reverse pagination also filters at the leaf, avoiding unnecessary serialization in `GetSortedKeysBatchReverseAsync` / `GetSortedEntriesBatchReverseAsync`.
- [x] **F-009 — Parallel shard pre-fetch for `KeysAsync`**: Double-buffer the k-way merge by pre-fetching the next page from each shard in parallel, hiding per-shard latency during ordered key scans. Controlled via `LatticeOptions.PrefetchKeysScan` or the per-call `prefetch` parameter.
- [x] **F-013 — Internal shard hotness counters**: Volatile in-memory counters (`reads`, `writes`, `countersSince`) on `ShardRootGrain`, incremented at zero persistence cost. Exposed via `GetHotnessAsync()` returning a `ShardHotness` struct so coordinators can poll all shards in parallel. Counters reset on grain deactivation.
- [x] **F-028 — Shard map indirection**: Replaced fixed `XxHash32 % shardCount` routing with a persistent `ShardMap` mapping virtual shard indices to physical `ShardRootGrain` identities over a large fixed virtual slot space (4 096 slots). `LatticeGrain` caches the map in memory and invalidates on topology changes.
- [x] **F-030 — Route `BulkLoadAsync` through the shard map**: The streaming `BulkLoadAsync` overload resolves the per-tree `ShardMap` up-front via `ILattice.GetRoutingAsync()` and routes each entry through `ShardMap.Resolve`. The legacy `int shardCount` overload is preserved but marked `[Obsolete]`.
- [x] **F-011 — Adaptive shard splitting** *(required F-013 ✓, F-028 ✓, F-030 ✓)*: Fully-online shadow-write split: source shard continues serving reads/writes; concurrent writes to migrating slots are mirrored via `MergeManyAsync`; after a background drain, the `ShardMap` is swapped and the source enters a reject phase emitting `StaleShardRoutingException`. `HotShardMonitorGrain` triggers splits autonomically when ops/sec exceeds `HotShardOpsPerSecondThreshold` (default 200 ops/s). Because `HotShardMonitorGrain` is keyed per-tree, `MaxConcurrentAutoSplits` (default 2) bounds concurrent splits **per tree**; in a multi-tree cluster each tree has its own independent cap. Scans use per-slot reconciliation with a `HashSet<string>` deduplicator, bounded by `LatticeOptions.MaxScanRetries` (default 3), so the complete live key set is returned with no missing or phantom entries. Strict lexicographic ordering of `KeysAsync` / `EntriesAsync` output is preserved across mid-scan splits via **F-032** (in-line reconciliation-cursor injection into the k-way merge).
- [x] **F-032 — Scan ordering preservation under topology change** *(required F-011 ✓)*: `KeysAsync` and `EntriesAsync` preserve strict lexicographic (or reverse) output ordering even when a shard split commits mid-scan. Before each priority-queue dequeue, the orchestrator checks whether any live shard cursor has reported new `MovedAwaySlots` since the last reconciliation step; if so, it drains the affected virtual slots from their current owners into an in-memory buffer, sorts it with the same comparer, and injects it as an additional cursor into the same k-way merge priority queue. A post-drain final-stability check handles splits that commit after all live cursors have finished. Reconciliation is bounded by `LatticeOptions.MaxScanRetries` (default 3); the per-call `HashSet<string>` continues to suppress duplicates across pre- and post-swap views. `CountAsync` is unaffected (it reduces rather than streams). A complete stateful-cursor design for long-running exports remains tracked by F-033.

### Reliability

- [x] **F-020 — Merge trees (`MergeAsync`)**: Merge all entries from a source tree into the current tree using LWW semantics. Useful for combining snapshots, migrating data, or rejoining forked datasets.

---

## 🔲 Outstanding

Items are ordered by estimated impact. Dependencies on completed features are noted inline; outstanding items that depend on other outstanding items are indented beneath their prerequisite.

Audit follow-up fixes (`FX-###`) are tracked in a [dedicated section below](#-audit-follow-up-fixes) and take operational precedence over feature work when they touch data-loss or liveness risks.

### 1 · F-031 — Atomic multi-key writes (saga) *(covers audit #6 — `SetMany` atomicity)*
**Fix / feature — high impact**

Write two or more keys atomically using a saga coordination grain (`IAtomicWriteGrain`) that accepts a batch of key-value pairs and applies them sequentially to their respective leaf grains. Each step of the saga persists its progress so that a reminder-driven crash-recovery path can resume or compensate after a silo failure. Compensation works by re-writing the pre-saga value of each already-written key with a fresh HLC tick, relying on LWW semantics to win over the partial write — which requires reading current values before the saga begins. Readers may observe a brief window of partial visibility between the first and last key write; this is inherent to the saga pattern and must be documented. Orleans `ITransactionalState<T>` is explicitly out of scope — it requires a separate storage mechanism incompatible with the current `IPersistentState<T>` leaf grain model. A design spike is needed to settle the compensation strategy and partial-visibility contract before implementation begins.

**Audit coverage:** the current `SetMany` surface applies writes non-transactionally, so a partial failure leaves the batch half-applied with no compensating rollback or idempotency token. F-031 is the long-form fix.

---

### 2 · F-033 — Stateful cursor / iterator grain *(covers audit #10 `DeleteRange` pagination and #15 scan fallback; completes scan liveness story started by F-032)*
**Fix / reliability — high impact**

Pagination today is stateless: the client holds a per-shard continuation token and scans are bounded by `LatticeOptions.MaxScanRetries` (throwing `InvalidOperationException` on exhaustion — see [`docs/api.md#scan-reliability`](docs/api.md#scan-reliability)). For multi-minute export pipelines, scans that must survive silo failover and client restarts, and for unbounded `DeleteRangeAsync` traversals that currently have no continuation token or server-side budget, an `ILatticeCursorGrain` that checkpoints scan progress server-side would:

- Replace the current retry-budget failure mode with a resumable cursor (client re-attaches to the same cursor grain after any transient fault).
- Persist the last-yielded key plus the set of reconciled virtual slots, so a resumption after a topology change picks up exactly where it left off rather than replaying the whole scan.
- Expose `OpenAsync(scanSpec)`, `NextAsync(pageSize)`, `CloseAsync()` on `ILattice`, keeping the stateless `KeysAsync` / `EntriesAsync` overloads for short scans.
- Provide a cursor-backed `DeleteRangeAsync` variant that bounds per-call work and is resumable across grain deactivations.

Completes the scan-liveness story that F-032 began (F-032 delivers ordering under topology change; F-033 delivers unbounded resumability). Also subsumes the audit #15 concern that the leaf-scan fallback re-traverses from the root without explicit loop/retry caps by routing long scans through a checkpointed cursor instead of the stateless retry budget.

---

### 3 · F-029 — External metrics / telemetry export *(prereq F-013 ✓ — unblocked)*
**Observability / high impact**

Publish `System.Diagnostics.Metrics` counters and histograms for OpenTelemetry-compatible dashboards. Instruments fall into two tiers:

- **Shard-level** (sourced directly from F-013's existing `ShardRootGrain` counters): per-shard read ops, write ops, split count, and ops/sec derived from `countersSince`.
- **Leaf-level** (requires new instrumentation at `BPlusLeafGrain`): read latency and write latency histograms timed around `ReadStateAsync` / `WriteStateAsync` calls, cache hit/miss ratio, tombstone count, compaction duration, and scan latency.

The leaf-level instruments cannot be inferred from F-013's shard-root counters — they require timing and counting at the leaf grain boundary where storage I/O actually occurs.

---

### 4 · F-016
**Feature / high impact**

Accept an optional `TimeSpan ttl` on `SetAsync`. Expired keys are treated as tombstoned during reads and cleaned up by existing compaction infrastructure. Requires an `ExpiresAtTicks` field on `LwwValue`. No dependencies; highly requested pattern for caching use cases.

---

### 5 · F-019 — Online (non-blocking) resize
**Reliability / high impact**

During a resize operation, redistribute shard data incrementally in the background while the tree continues to serve reads and writes, with only a brief lock for the final shard map swap. Mirrors the `SnapshotMode.Online` approach: new physical shards are seeded from the existing layout slot-by-slot, and live traffic routes through the old map until the cutover. The shard map indirection from F-028 makes the final cutover a single atomic map swap rather than a coordinated shutdown.

---

### 6 · F-025
**Feature / high impact**

Incremental, ongoing merge from one or more source trees using `VersionVector` to track a per-source high-water mark, so each cycle transfers only entries newer than the last. Requires a delta-aware leaf scan (`GetEntriesNewerThanAsync(HybridLogicalClock threshold)`) and a merge-state tracking grain to persist the vector per source tree. Should replace the current full-shard drain in `TreeMergeGrain.MergeShardAsync` with a chunked or cursor-based leaf-chain iteration.

- [ ] **F-027 — Leaf-grouped merge routing** *(supports F-025 at scale)*: `ShardRootGrain.MergeManyAsync` currently traverses the B+ tree once per entry. Group entries by target leaf before issuing grain calls, reducing tree traversals from O(n) to O(leaves) and collapsing multiple `WriteStateAsync` calls per leaf into one. Particularly impactful when F-025 drives continuous bulk traffic through `MergeManyAsync`.

---

### 7 · F-024
**Performance / medium-high impact**

Extend the F-009 double-buffering strategy to `EntriesAsync`. Because entries carry `byte[]` values, pre-fetched pages increase in-flight memory proportionally to `shardCount × pageSize × avgValueSize`. Gate behind a dedicated `PrefetchEntriesScan` option or an optional `prefetch` parameter so callers opt in only when the memory trade-off is acceptable.

---

### 8 · F-014
**Observability / medium impact**

Return per-shard health information — depth, live key count, tombstone ratio, pending splits/promotions — via an `ILatticeAdmin` interface or a method on `ILattice`.

- [ ] **F-022 — Troubleshooting guide (`docs/troubleshooting.md`)** *(follows F-014)*: Cover common issues — storage provider exceptions from oversized grains, concurrent split activity, slow scans, stale cache behavior — and how to interpret the output of `DiagnoseAsync`.

---

### 9 · F-012
**Reliability / medium impact**

Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes. No dependencies.

---

### 10 · F-015
**Feature / medium impact**

Publish tree events (key written, tree deleted, split occurred, compaction completed) via an `ILatticeObserver` interface or Orleans Streams integration for event-driven architectures.

---

### 11 · F-018
**Feature / lower impact (complex)**

Associate tags with keys and query by tag. Implementable as a secondary Lattice tree mapping `tag → Set<key>`, maintained transactionally alongside the primary write. High complexity; deferred until core feature set is stable.

---

### 12 · Documentation & Developer Experience

- [ ] **F-021 — Migration guide**: Document how to migrate data from external stores (Redis, SQL, Cosmos DB) into Lattice using the streaming bulk-load API, including key-design and value-serialization best practices.
- [ ] **F-023 — Sample applications (`samples/`)**: End-to-end examples (e.g. ASP.NET Core session store, leaderboard, distributed configuration service) showing real integration patterns beyond the Quick Start snippet.

---

## 🛠 Audit Follow-up Fixes

Reliability and hygiene fixes surfaced by the repository audit. Items tagged **Fix** here are distinct from feature work (`F-###`) — they address latent bugs or robustness gaps rather than adding new capability. The highest-severity audit items (data-loss / liveness) are already promoted into the Outstanding list above as F-031 and F-033.

PR #47 landed the first batch of audit fixes (HLC merge clock advancement, compaction short-circuit, alias / physical-shard resolution in `TombstoneCompactionGrain` and `TreeMergeGrain`, streaming per-leaf merge, and `HotShardMonitor` cooldown pruning) with 12 regression tests; the items below are what remains.

### FX-001 — Leaf split publish ordering *(audit #8 — reliability / medium)*
Split currently publishes the new sibling to the parent before the old leaf flushes its trimmed state; a crash between the two writes leaves duplicated keys in the tree. Fix: flush the trimmed source leaf first, then publish to the parent, and add a crash-injection test that asserts the post-recovery leaf chain has no duplicated keys.

### FX-002 — Reminder re-registration idempotency *(audit #9 — reliability / medium)*
Lifecycle grains (`TombstoneCompactionGrain`, `TreeMergeGrain`, `HotShardMonitorGrain`) do not defensively re-register their reminders on activation if an existing reminder is registered with a stale period. Fix: on activation, unregister-then-register when the persisted period differs from the configured option value.

### FX-003 — `VersionVector` pruning *(audit #14 — reliability / medium, memory)*
`VersionVector` grows unbounded for replicas that stop participating; no GC exists for long-absent actor IDs. Fix: add an optional `MaxAgeTicks` prune policy (configurable per tree) applied during merge, gated behind a `LatticeOptions.VersionVectorRetention` option to preserve wire compatibility.

### FX-004 — `HotShardMonitor` sampling window survives silo restart *(audit #11 — reliability / medium)*
The EMA sampling window is reset on every silo restart; short-lived silos never detect a hot shard. Fix: persist the last observed EMA and window start into a lightweight `HotShardMonitorState` so sampling resumes across restarts, or document the trade-off explicitly and raise the default window to tolerate restart jitter.

### FX-005 — `TreeMergeGrain` crash-resume retry semantics *(audit #13 — reliability / medium)*
On grain reactivation mid-merge, the shard cursor advances past the failing shard instead of retrying it. Fix: separate the "shard completed" cursor from the "shard in progress" marker in `TreeMergeState` so a crash-resume re-enters the failing shard, with a bounded retry budget before poisoning it.

### FX-006 — `CancellationToken` on `ILattice` *(audit #19 — API / breaking)*
Public `ILattice` methods don't accept `CancellationToken`; long operations (scans, merges, range deletes) can't be cooperatively cancelled. Fix: add `CancellationToken = default` optional parameters to the public surface. Marked **breaking** for binary compatibility even though source-compatible; stage behind a major-version bump.

### FX-007 — Logger category consistency *(audit #20 — hygiene / low)*
Some grains use `ILogger<TGrain>` and others use `ILogger` from the activation context. Fix: standardise on `ILogger<TGrain>` across all grain implementations so category filtering works uniformly.

### FX-008 — Metrics naming prefix *(audit #21 — hygiene / low, blocks F-029)*
Internal telemetry hooks mix `lattice.*` and `orleans.lattice.*` prefixes. Fix: standardise on `orleans.lattice.*` (aligned with the Orleans meter convention) before F-029 locks in the public instrument names.

### FX-009 — `TypeAliases` dead-entry audit *(audit #23 — hygiene / low)*
The alias table is not mechanically checked against the set of `[Alias]`-decorated types actually present in the assembly. Fix: extend `TypeAliasesTests` with a reflection sweep that asserts every `TypeAliases` constant is referenced by exactly one `[Alias(...)]` attribute in the assembly, failing the build on dead or orphan entries.

### FX-010 — Docs drift guard *(audit #24 — docs / low)*
Several `docs/*.md` files reference option names and method shapes that have since been renamed; no CI check guards this. Fix: add a docs-verify step to the build that compiles the code snippets in `docs/*.md` against the public surface (either via DocFX or a lightweight Roslyn snippet runner) so renames break the build instead of rotting silently.

**Audit items intentionally not listed here** (verified during PR planning as already implemented or inapplicable): `IValidateOptions<LatticeOptions>` is already registered (`LatticeOptionsValidator`), `SnapshotMode` default is the safe `Offline` mode, and the `LeafCacheGrain` TTL XML doc matches the implementation.

---

## 🔍 Gaps & Potential Additions

The following areas are not currently on the roadmap but represent meaningful opportunities, particularly as the library moves toward production adoption.

### G-001 — Leaf-level write batching (investigate before committing)
Sharding and adaptive splitting (F-011) already distribute write load across leaf grains, progressively undermining the need for in-grain write coalescing. The changes required for safe batching — `[Reentrant]` leaf grains, a `FlushAsync` drain contract for split/merge/snapshot coordinators, and an explicit `WriteMode` API to surface the weakened durability guarantee — touch almost every major system component. The failure modes (silent data loss on crash, stale state seeded into a new shard post-split) are hard to detect in tests and severe in production. Before pursuing, profile under realistic write-skew conditions with adaptive splitting active and confirm that leaf-level hotness persists after splits have stabilised. If poor key entropy is the root cause, key-design guidance is a lower-risk remedy. Only implement if profiling proves the leaf grain itself is the bottleneck.

### G-002 — Compaction policy controls
F-016 (TTL) assumes that "existing compaction infrastructure" handles expired-key reaping, but there is no roadmap item covering compaction itself as a configurable feature. Operators have no way to tune tombstone reaping thresholds, set compaction schedules, or observe space amplification (beyond the tombstone ratio in F-014). Compaction policy controls — minimum tombstone ratio before reaping, maximum leaf size before forced compaction, and a compaction telemetry hook for F-029 — warrant a dedicated item.

### G-003 — Stateful cursor / iterator grain
Pagination today is stateless (client holds a token per shard). For long-running scans that span many pages, or scans that need to survive client restarts, a stateful cursor grain that checkpoints scan progress server-side would significantly simplify client code and enable reliable export pipelines without external state management.

*Promoted to the outstanding list as **F-033** — see priority 2 above.*

### G-004 — Geo-replication / multi-cluster
The CRDT (LWW + HLC) nature of Lattice makes cross-cluster replication architecturally feasible: because `LwwValue.Merge` is idempotent and commutative, a replication link is just a continuous merge (F-025) operating across Orleans cluster boundaries. A dedicated item to define the multi-cluster topology, conflict resolution surface, and network transport (e.g. gRPC bridge grain) would be valuable for disaster-recovery and read-local query patterns.

### G-005 — Per-key change subscriptions
F-015 targets tree-level events. Reactive patterns (e.g. cache invalidation, real-time leaderboard updates) need per-key or prefix-scoped subscriptions — closer to Redis `SUBSCRIBE` or Cosmos DB change feed. This could be layered on top of F-015 with a filter registry grain, but the routing and fan-out semantics need explicit design.

### G-006 — Value compression / encryption
There is no transparent per-tree compression or encryption of stored `byte[]` values. For storage-cost-sensitive workloads (large values, many tombstones) or compliance-sensitive data, a pluggable `IValueTransformer` pipeline applied before `WriteStateAsync` and after read would address both concerns without changing the public API surface.

### G-007 — Quota / admission control per tree
Multi-tenant deployments need a way to cap a single tree's live key count or total estimated storage size and reject writes once the cap is breached. Without this, a misbehaving tenant can starve storage for all other trees in the cluster. A `MaxLiveKeys` or `MaxEstimatedBytes` option on `LatticeOptions`, enforced at the shard level via the hotness counters already present in F-013, would be low-friction to implement.

### G-008 — Admin CLI / `dotnet` tool
All management operations (run diagnostics, inspect shard map, trigger snapshot) require in-process code or test harnesses. A `dotnet tool` wrapping the `ILatticeAdmin` surface (once F-014 lands) would lower the operational bar significantly — particularly for diagnosing concurrent split activity or stale cache behaviour described in F-022. Shard splits remain an autonomic concern — `ITreeShardSplitGrain` is guarded and not part of any planned public surface.

### G-009 — Shard-affine grain placement
Each `ShardRootGrain` is currently placed by Orleans' default random placement director. Because `LatticeGrain` is a `[StatelessWorker]` that activates on every silo, every shard call that lands on a silo that does not host the target shard's activation pays a cross-silo round-trip. A custom `IPlacementDirector` that deterministically maps each `ShardRootGrain` (by shard index) to a specific silo — stable hash of shard index across the current membership ring — would co-locate the router and the shard on the same silo for the majority of requests. Realising the full benefit also requires `LatticeGrain` to forward each fan-out leg to the shard's home silo rather than executing the grain call from the local activation. The benefit is **limited to point operations** (`GetAsync`, `SetAsync`, `DeleteAsync`, `SetIfVersionAsync`, etc.) — scans fan out to all shards by definition and pay cross-silo hops regardless of placement. The magnitude of the gain is storage-latency-dependent: against a slow backend (Azure Table Storage, Cosmos DB) the cross-silo hop is noise; against a fast local backend it can be a meaningful fraction of total round-trip time. F-029 telemetry should land first so profiling can confirm whether the hop is actually showing up before committing to the placement director design. The main implementation complication is adaptive splitting (F-011): a `ShardMap` swap changes which shard indices exist, so the placement director must either re-evaluate home-silo assignments after each swap or accept degraded affinity until shard grains naturally migrate. `[PreferLocal]` is not sufficient — it only biases initial activation and has no effect once a grain is already activated on another silo.

### G-010 — Cluster-wide split concurrency control
`MaxConcurrentAutoSplits` is enforced per-tree: because `HotShardMonitorGrain` is keyed by tree ID, each tree independently counts its own in-flight splits. In a cluster hosting many trees simultaneously, aggregate storage I/O from drain phases across all trees is unbounded — a large number of concurrent trees each with 2 in-flight splits can saturate the storage provider or silo thread pool just as effectively as a single tree with a high limit. A cluster-level admission gate (e.g. a singleton `IClusterSplitConcurrencyGrain` tracking cluster-wide in-flight count, checked by each `HotShardMonitorGrain` before triggering) would cap aggregate drain I/O independently of tree count. Low priority in single-tree or low-tree-count deployments; becomes relevant in multi-tenant or many-tree scenarios.

---

## Dependencies

| Feature | Depends on | Status |
|---|---|---|
| F-011 (Adaptive shard splitting) | F-013 (Shard hotness counters) | ✅ Both complete |
| F-011 (Adaptive shard splitting) | F-028 (Shard map indirection) | ✅ Both complete |
| F-011 (Adaptive shard splitting) | F-030 (Route `BulkLoadAsync` through shard map) | ✅ Both complete |
| F-029 (External metrics / telemetry) | F-013 (Shard hotness counters) | ✅ Prereq complete — F-029 unblocked |
| F-025 (Continuous merge) | F-020 (Merge trees) | ✅ Prereq complete — F-025 unblocked |
| F-027 (Leaf-grouped merge routing) | F-025 (Continuous merge) | 🔲 Supports F-025 at scale |
| F-019 (Online resize) | F-028 (Shard map indirection) | ✅ Prereq complete — F-019 unblocked |
| F-024 (Entry pre-fetch) | F-009 (Key pre-fetch) | ✅ Prereq complete — F-024 unblocked |
| F-022 (Troubleshooting guide) | F-014 (Tree diagnostics) | 🔲 Guide should follow diagnostics implementation |
| F-031 (Atomic multi-key writes) | F-017 (CAS) | 🔲 Single-key CAS is the building block; F-031 extends atomicity across keys via saga |
| F-032 (Scan ordering under topology change) | F-011 (Adaptive shard splitting) | ✅ Prereq complete — F-032 complete |
| F-033 (Stateful cursor grain) | F-032 (Scan ordering under topology change) | ✅ Prereq complete — F-033 completes the scan-liveness story |
