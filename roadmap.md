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
- [x] **F-016 — TTL on `SetAsync`**: New `ILattice.SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken)` overload (with matching typed `SetAsync<T>` overloads on `TypedLatticeExtensions`) writes an entry that expires after `ttl`. `LwwValue<T>` gains an `[Id(3)] ExpiresAtTicks` field (defaults to `0` — no expiry; wire-compatible with existing persisted state) and an `IsExpired(long nowUtcTicks)` helper. The TTL is resolved to an absolute UTC `DateTimeOffset.UtcNow.Add(ttl).UtcTicks` on the silo handling the call, so per-entry lifetimes are not shifted by client-clock skew. Expired entries are hidden from every read path — `GetAsync`, `GetWithVersionAsync`, `ExistsAsync`, `GetManyAsync`, `GetOrSetAsync`, `SetIfVersionAsync`, `CountAsync`, `KeysAsync`, `EntriesAsync`, `GetLiveEntriesAsync`, and `DeleteRangeAsync` — and reaped by the existing `CompactTombstonesAsync` alongside tombstones past the configured `LatticeOptions.TombstoneGracePeriod`. The grace retention protects against a stale-clock peer resurrecting an expired entry via CRDT merge: replication paths (`GetDeltaSinceAsync`, `MergeEntriesAsync`, `MergeManyAsync`) deliberately keep expired entries visible so they propagate correctly and win LWW by timestamp. `LeafCacheGrain` also filters expired on read. `SetAsync` throws `ArgumentOutOfRangeException` when `ttl <= TimeSpan.Zero`. **TTL metadata is preserved end-to-end across every lifecycle path**: shard-split shadow-forwards (`ForwardLocalWriteToShadowIfNeededAsync` reads the raw `LwwValue` including `ExpiresAtTicks` before merging into the target shard); saga compensation rollback (`AtomicWriteGrain.PrepareAsync` resolves routing via the public-internal `ILattice.GetRoutingAsync` hook and then captures the pre-saga `LwwValue` by calling `IShardRootGrain.GetRawEntryAsync` directly on the guarded internal interface — the raw `LwwEntry` DTO never crosses the public `ILattice` surface — and `CompensatePhaseAsync` restores TTL'd entries through the TTL-aware `SetAsync(key, value, TimeSpan)` overload with `ttl = expiresAtTicks - now`; past-due entries are equivalent to "absent" and are tombstoned); and tree snapshot / restore (`TreeSnapshotGrain.CopyShardAsync` drains via a new raw `IBPlusLeafGrain.GetLiveRawEntriesAsync` and bulk-loads via `IShardRootGrain.BulkLoadRawAsync`, both of which carry the source HLC and `ExpiresAtTicks` through verbatim). The raw / bulk-load paths exchange a new `LwwEntry` DTO (alias `ol.lwe`, `[EditorBrowsable(Never)]`) that lives exclusively on guarded internal grain interfaces (`IBPlusLeafGrain`, `IShardRootGrain`) — it is never exposed on `ILattice`. Its fields are stored flat rather than embedding `LwwValue<byte[]>` as a property, because the Orleans type-alias encoder has a codec-generation race with `Task<LwwValue<byte[]>?>` grain signatures that produces malformed alias strings under concurrent cold activation.
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
- [x] **F-027 — Leaf-grouped merge routing**: `ShardRootGrain.MergeManyAsync` groups entries by target leaf before issuing grain calls, reducing tree traversals from O(entries) to O(leaves) and collapsing multiple `WriteStateAsync` calls per leaf into one. Root-is-leaf trees fast-path the whole batch through a single grain call. Per-group re-traversal preserves correctness under mid-batch splits — distinct groups target distinct leaves, so a split produced by one group cannot re-route another group's keys (internal splits create sibling internals but preserve child `GrainId`s; leaf splits only affect the one leaf being written to). Per-group retry on transient `OrleansException` / `TimeoutException` / `IOException` is retained; the leaf's `MergeManyAsync` is LWW-idempotent so replay is safe. No interface or wire-format changes — every caller (`TreeMergeGrain`, `TreeShardSplitGrain` shadow-forward and drain, continuous merge) benefits transparently. Regression tests: `ShardRootGrainMergeManyTests` covering empty batch, root-is-leaf fast path, multi-leaf grouping, single-leaf fan-in, transient-exception retry, and non-transient propagation.

### Reliability

- [x] **F-020 — Merge trees (`MergeAsync`)**: Merge all entries from a source tree into the current tree using LWW semantics. Useful for combining snapshots, migrating data, or rejoining forked datasets.
- [x] **F-031 — Atomic multi-key writes (saga)** *(covers audit #6 — `SetMany` atomicity)*: `ILattice.SetManyAtomicAsync` routes a batch of key-value pairs through a dedicated `IAtomicWriteGrain` saga coordinator (keyed per `{treeId}/{operationId}`). The saga reads pre-saga values, applies writes sequentially, and — on any failure — compensates already-committed entries by rewriting their pre-saga values (or tombstoning previously-absent keys) with fresh HLC ticks, relying on LWW to win over the partial write. A keepalive reminder registered at saga start drives reminder-based crash recovery: on reactivation the grain consults its persisted phase (`Prepare` / `Execute` / `Compensate` / `Completed`) and resumes. Duplicate keys and null values fail fast with `ArgumentException`; post-compensation failures surface as `InvalidOperationException` with the original failure's message preserved. `SetManyAsync` (the non-atomic, parallel fan-out path) is retained and now documents its non-atomicity. Readers may observe a partial-visibility window between the first and last committed write — inherent to the saga pattern and documented in `docs/api.md`.
- [x] **F-033 — Stateful cursor / iterator grain** *(covers audit #10 `DeleteRange` pagination and #15 scan fallback; completes scan liveness story started by F-032)*: `ILatticeCursorGrain` checkpoints scan progress server-side after every page so multi-minute exports and resumable range deletes survive silo failover, client restarts, and topology changes. Exposed via `ILattice.OpenKeyCursorAsync` / `OpenEntryCursorAsync` / `OpenDeleteRangeCursorAsync` / `NextKeysAsync` / `NextEntriesAsync` / `DeleteRangeStepAsync` / `CloseCursorAsync`; the cursor grain is keyed `{treeId}/{cursorId}` and persists `LastYieldedKey` plus phase so a new activation resumes exactly where it left off. Each step is routed through the normal `ILattice.KeysAsync` / `EntriesAsync` / `DeleteRangeAsync` path, inheriting F-032 ordering preservation under concurrent shard splits. **Self-cleaning:** the grain registers a sliding idle-TTL reminder (`cursor-ttl`) refreshed on every call; if it fires without activity, the grain clears state, unregisters the reminder, and deactivates. Interval is configured by `LatticeOptions.CursorIdleTtl` (default 48h, clamped to a 1-minute floor, disabled with `Timeout.InfiniteTimeSpan`). The same retention-reminder pattern was layered onto `AtomicWriteGrain` (F-031) via `LatticeOptions.AtomicWriteRetention` (default 48h) so completed saga state does not leak indefinitely. Public DTOs (`LatticeCursorSpec`, `LatticeCursorKeysPage`, `LatticeCursorEntriesPage`, `LatticeCursorDeleteProgress`, `LatticeCursorKind`) are visible to IntelliSense as first-class public API; only the internal `ILatticeCursorGrain` is marked `[EditorBrowsable(Never)]`. Stateless `KeysAsync` / `EntriesAsync` / `DeleteRangeAsync` overloads are retained for short scans.

---

## 🔲 Outstanding

Items are ordered by estimated impact. Dependencies on completed features are noted inline; outstanding items that depend on other outstanding items are indented beneath their prerequisite.

Audit follow-up fixes (`FX-###`) are tracked in a [dedicated section below](#-audit-follow-up-fixes) and take operational precedence over feature work when they touch data-loss or liveness risks.

### 1 · F-029 — External metrics / telemetry export *(prereq F-013 ✓ — unblocked)*
**Observability / high impact**

Publish `System.Diagnostics.Metrics` counters and histograms for OpenTelemetry-compatible dashboards. Instruments fall into two tiers:

- **Shard-level** (sourced directly from F-013's existing `ShardRootGrain` counters): per-shard read ops, write ops, split count, and ops/sec derived from `countersSince`.
- **Leaf-level** (requires new instrumentation at `BPlusLeafGrain`): read latency and write latency histograms timed around `ReadStateAsync` / `WriteStateAsync` calls, cache hit/miss ratio, tombstone count, compaction duration, and scan latency.

The leaf-level instruments cannot be inferred from F-013's shard-root counters — they require timing and counting at the leaf grain boundary where storage I/O actually occurs.

---

### 2 · F-019 — Online (non-blocking) resize
**Reliability / high impact**

During a resize operation, redistribute shard data incrementally in the background while the tree continues to serve reads and writes, with only a brief lock for the final shard map swap. Mirrors the `SnapshotMode.Online` approach: new physical shards are seeded from the existing layout slot-by-slot, and live traffic routes through the old map until the cutover. The shard map indirection from F-028 makes the final cutover a single atomic map swap rather than a coordinated shutdown.

**Status: ✅ delivered.** Both halves of F-019 are live:

- **Online `ReshardAsync`** (shipped previously) — `ILattice.ReshardAsync(int newShardCount, CancellationToken)` grows a tree's physical shard count online via `ITreeReshardGrain`, dispatching up to `LatticeOptions.MaxConcurrentMigrations` concurrent per-shard splits (each using its own shadow-write + swap + reject phases). Grow-only; idempotent on same target; crash-safe via reminder-anchored coordinator. Progress via `IsReshardCompleteAsync`. Interlocked against `HotShardMonitorGrain`.

- **Online `ResizeAsync`** (shipped, phases 1–4 complete) — `ILattice.ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` now runs online. A new `ShardRootGrain` shadow-forwarding primitive (`BeginShadowForwardAsync` / `MarkDrainedAsync` / `EnterRejectingAsync` / `ClearShadowForwardAsync`, LWW-commutative by construction) lets `TreeSnapshotGrain` run a strictly-consistent `SnapshotMode.Online` drain while live traffic is mirrored to the destination physical tree. The drain itself merges via `IShardRootGrain.MergeManyAsync` on the destination so concurrent shadow-forward writes converge with the drain batch under LWW. `TreeResizeGrain` adds a `ResizePhase.Reject` transition, a phase-aware dual-window `UndoResizeAsync` (drain-window gated on `Phase == Snapshot`; all later phases route through after-swap recovery), and threads a `logicalTreeId` through `SnapshotWithOperationIdAsync` so the transient per-shard `StaleTreeRoutingException` at swap carries the caller's logical name. `HotShardMonitorGrain` polls `ITreeResizeGrain.IsCompleteAsync`; `ReshardAsync` rejects while a resize is running. Verified end-to-end by `ChaosResizeIntegrationTests.Chaos_resize_under_concurrent_load_preserves_all_data`. See [Tree Sizing](docs/tree-sizing.md#resizing-an-existing-tree).

**Follow-ups.** A future `MergeAsync`-style shrink primitive and cross-cluster replication over the shadow-forward primitive are separate features (see §11 of the design doc).

---

### 3 · F-025
**Feature / high impact**

Incremental, ongoing merge from one or more source trees using `VersionVector` to track a per-source high-water mark, so each cycle transfers only entries newer than the last. Requires a delta-aware leaf scan (`GetEntriesNewerThanAsync(HybridLogicalClock threshold)`) and a merge-state tracking grain to persist the vector per source tree. Should replace the current full-shard drain in `TreeMergeGrain.MergeShardAsync` with a chunked or cursor-based leaf-chain iteration.

---

### 4 · F-024
**Performance / medium-high impact**

Extend the F-009 double-buffering strategy to `EntriesAsync`. Because entries carry `byte[]` values, pre-fetched pages increase in-flight memory proportionally to `shardCount × pageSize × avgValueSize`. Gate behind a dedicated `PrefetchEntriesScan` option or an optional `prefetch` parameter so callers opt in only when the memory trade-off is acceptable.

---

### 5 · F-014
**Observability / medium impact**

Return per-shard health information — depth, live key count, tombstone ratio, pending splits/promotions — via an `ILatticeAdmin` interface or a method on `ILattice`.

- [ ] **F-022 — Troubleshooting guide (`docs/troubleshooting.md`)** *(follows F-014)*: Cover common issues — storage provider exceptions from oversized grains, concurrent split activity, slow scans, stale cache behavior — and how to interpret the output of `DiagnoseAsync`.

---

### 6 · F-012
**Reliability / medium impact**

Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes. No dependencies.

---

### 7 · F-015
**Feature / medium impact**

Publish tree events (key written, tree deleted, split occurred, compaction completed) via an `ILatticeObserver` interface or Orleans Streams integration for event-driven architectures.

---

### 8 · F-018
**Feature / lower impact (complex)**

Associate tags with keys and query by tag. Implementable as a secondary Lattice tree mapping `tag → Set<key>`, maintained transactionally alongside the primary write. High complexity; deferred until core feature set is stable.

---

### 9 · Documentation & Developer Experience

- [ ] **F-021 — Migration guide**: Document how to migrate data from external stores (Redis, SQL, Cosmos DB) into Lattice using the streaming bulk-load API, including key-design and value-serialization best practices.
- [ ] **F-023 — Sample applications (`samples/`)**: End-to-end examples (e.g. ASP.NET Core session store, leaderboard, distributed configuration service) showing real integration patterns beyond the Quick Start snippet.

---

## 🛠 Audit Follow-up Fixes

Reliability and hygiene fixes surfaced by the repository audit. Items tagged **Fix** here are distinct from feature work (`F-###`) — they address latent bugs or robustness gaps rather than adding new capability. The highest-severity audit items (data-loss / liveness) are already addressed: F-031 (atomic multi-key writes saga) and F-033 (stateful cursor grain) are both complete.
PR #47 landed the first batch of audit fixes (HLC merge clock advancement, compaction short-circuit, alias / physical-shard resolution in `TombstoneCompactionGrain` and `TreeMergeGrain`, streaming per-leaf merge, and `HotShardMonitor` cooldown pruning) with 12 regression tests. A second batch closed the remaining reliability and hygiene items below (FX-001, FX-002, FX-003, FX-004, FX-005, FX-007, FX-008, FX-009, FX-011, FX-012). **v2.0.0** delivered the final two breaking-change items bundled together: grain-manifest identities are now anchored to stable `[Alias]` constants on every grain interface, and FX-006 adds `CancellationToken` to every public `ILattice` method. Only FX-010 (CI infra — requires DocFX/Roslyn snippet runner) remains outstanding.

### ✅ FX-001 — Leaf split publish ordering *(audit #8 — reliability / medium)*
Split now flushes the trimmed source leaf **before** publishing the new sibling to the parent, so a crash between the two writes can never leave duplicated keys in the tree. Regression test: `BPlusLeafGrainTests.Split_flushes_source_state_before_returning_SplitResult` asserts the source's persisted `Entries` contain no key ≥ the promoted split key and that `SplitState == SplitComplete` / `NextSibling == result.NewSiblingId` before `SplitResult` is returned.

### ✅ FX-002 — Reminder re-registration idempotency *(audit #9 — reliability / medium)*
`TombstoneCompactionGrain`, `TreeMergeGrain`, and `HotShardMonitorGrain` now compare `TickStatus.Period` to the configured option on every reminder firing and call `RegisterOrUpdateReminder` when they drift. Regression tests: `TombstoneCompactionGrainTests.ReceiveReminder_reregisters_when_period_drifts_from_options` and `ReceiveReminder_does_not_reregister_when_period_matches`.

### ✅ FX-003 — `VersionVector` pruning *(audit #14 — reliability / medium, memory)*
`VersionVector.PruneOlderThan(long minRetainedUtcTicks)` removes entries whose HLC wall-clock ticks fall below the cutoff. Gated behind `LatticeOptions.VersionVectorRetention` (default `Timeout.InfiniteTimeSpan` — opt-in, wire-compatible). Regression tests: `VersionVectorTests.PruneOlderThan_removes_entries_below_cutoff`, `_returns_zero_on_empty_vector`, `_keeps_entries_at_cutoff`, `_drops_all_when_cutoff_high`.

### ✅ FX-004 — `HotShardMonitor` sampling window survives silo restart *(audit #11 — reliability / medium)*
`HotShardMonitorGrain` now persists `HotShardMonitorState.ActivationUtc` (alias `ol.hms`) via `IPersistentState<HotShardMonitorState>` and only writes it on first activation; the grace window is computed from the persisted time so it survives silo restart. Regression test: `HotShardMonitorGrainTests.ActivationUtc_persists_and_survives_reactivation`.

### ✅ FX-005 — `TreeMergeGrain` crash-resume retry semantics *(audit #13 — reliability / medium)*
`ProcessCurrentShardAsync` now pre-increments `TreeMergeState.ShardRetries` and persists the increment **before** attempting the merge, so a non-throwing crash still burns retry budget on reactivation. When `ShardRetries` reaches `MaxRetriesPerShard` (2) the shard is poisoned (skipped, cursor advances, retry counter resets) with a warning log. Regression tests: `TreeMergeGrainTests.ProcessNextShardAsync_poisons_shard_after_retry_budget_exhausted` and `ProcessNextShardAsync_preincrements_retry_counter_before_attempt`.

### ✅ FX-006 — `CancellationToken` on `ILattice` *(audit #19 — API / breaking — delivered in v2.0.0)*
Every public `ILattice` method now accepts an optional `CancellationToken cancellationToken = default`, including scans (`KeysAsync`, `EntriesAsync`), range deletes, counts, fan-outs, cursors, bulk load, and all tree-lifecycle operations. `LatticeGrain` calls `cancellationToken.ThrowIfCancellationRequested()` at method entry, inside retry `when` filters, and at fan-out / pagination checkpoints, so a pre-cancelled token fails fast without contacting shards. The scan iterators apply `[EnumeratorCancellation]` at the implementation so `await foreach (...).WithCancellation(ct)` propagates into the orchestrator. Cooperative cancellation is scoped to the `LatticeGrain` orchestrator — downstream `IShardRootGrain` / `IBPlusLeafGrain` / coordinator grain interfaces are deliberately unchanged (their work is short-lived; propagation would balloon the breaking surface without observable benefit). The typed `ILattice<T>` extension overloads and both streaming `BulkLoadAsync` overloads in `LatticeExtensions` / `TypedLatticeExtensions` also thread the token. Binary-breaking (signatures changed) even though source-compatible for default-token callers; shipped in **v2.0.0**. Regression tests: `CancellationTokenIntegrationTests` (16 tests) — pre-cancellation fail-fast for every public method plus a mid-stream `KeysAsync_stops_mid_stream_when_token_cancelled` scenario.

### ✅ FX-007 — Logger category consistency *(audit #20 — hygiene / low)*
`TtlGrain` was generalised to `TtlGrain<TSelf>` exposing a `protected ILogger<TSelf> Logger`; `AtomicWriteGrain` and `LatticeCursorGrain` now share the typed-category pattern used by every other grain. Regression test: `AuditHygieneRegressionTests.Every_grain_uses_generic_ILogger_category` reflection sweep fails the build if any grain re-introduces a non-generic `ILogger` ctor parameter or field.

### ✅ FX-008 — Metrics naming prefix *(audit #21 — hygiene / low, blocks F-029)*
`LatticeMetrics.MeterName = "orleans.lattice"` pins the meter namespace ahead of F-029 locking public instrument names. Regression test: `AuditHygieneRegressionTests.LatticeMetrics_meter_name_is_orleans_lattice`.

### ✅ FX-009 — `TypeAliases` dead-entry audit *(audit #23 — hygiene / low)*
`TypeAliasesTests.Every_alias_constant_is_referenced_by_exactly_one_type` is a reflection sweep that fails the build on dead or orphan alias constants. Runs on every PR.

### FX-010 — Docs drift guard *(audit #24 — docs / low — deferred)*
Still outstanding. Several `docs/*.md` files reference option names and method shapes that have since been renamed; no CI check guards this. Fix: add a docs-verify step to the build that compiles the code snippets in `docs/*.md` against the public surface (either via DocFX or a lightweight Roslyn snippet runner) so renames break the build instead of rotting silently.

### ✅ FX-011 — `ShardRootGrain.DeleteRangeAsync` under-deletion on sparse multi-shard trees *(discovered during F-033 split-test authoring — reliability / high, data-loss)*
`IBPlusLeafGrain.DeleteRangeAsync` now returns `RangeDeleteResult { Deleted, PastRange }` (alias `ol.rdr`); the leaf sets `PastRange = true` as soon as it encounters a key ≥ `endExclusive`. `ShardRootGrain.DeleteRangeAsync` terminates on `result.PastRange` instead of the old `deleted == 0` short-circuit, so multi-shard trees with sparse key distribution delete every matching key. Regression tests: `MultiShardDeleteRangeRegressionTests.DeleteRange_walks_past_empty_leaves_on_multi_shard_tree` (200 keys / 4 shards / `MaxLeafKeys=4`) and `RangeDeleteResult_reports_past_range_flag`.

### ✅ FX-012 — `CountAsync` over-counts during concurrent shard splits *(discovered by chaos test — reliability / high, correctness)*
Fixed in two passes. **v1** added a shard-map version stability check after the `CountWithMovedAwayAsync` fan-out but still relied on each source shard filtering moved-slot keys via its persisted `SplitInProgress.Phase`. That left a race window: the split coordinator publishes the new `ShardMap` (including the target shard) before advancing the source's phase to `Reject`, so a count arriving in the window saw slot-S keys on both the source (not yet filtering) and the target (already drained) while the map version was already at its new value — the stability check passed and the over-count was returned. **v2** (this fix) routes counts through the authoritative `ShardMap` directly: `LatticeGrain.CountAsyncCore` and `CountPerShardAsyncCore` partition virtual slots by current owner via `BuildOwnedSlotMap(map)` and ask each physical shard to count only its owned slots via `IShardRootGrain.CountForSlotsAsync(sortedSlots, virtualShardCount)`. Every virtual slot is counted exactly once, against whichever shard the map identifies as its owner, independent of the source shard's per-split phase. The version stability check is retained to guard against splits committing mid-fan-out and is bounded by `LatticeOptions.MaxScanRetries`. Regression tests: `CountAsyncSplitRegressionTests.CountAsync_matches_true_live_count_after_split`, `CountPerShardAsync_matches_true_live_count_after_split`, `CountAsync_never_overcounts_during_concurrent_split`, `CountPerShardAsync_never_overcounts_during_concurrent_split` (the last two spin the public counter while driving `SplitAsync` + `RunSplitPassAsync` so every observation inside the phase-machine window is checked), plus the `Chaos_concurrent_reads_writes_scans_counts_and_splits_preserve_invariants` and `CountAsync_returns_exact_count_during_active_split` integration tests that caught the v1 gap in CI.

### 🔲 FX-013 — Pin structural sizing options to the tree registry *(reliability / high, data-loss — high priority)*
**Problem.** `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` control where splits fire and how keys are routed; they cannot be changed once a tree has data without re-paginating the whole tree. Today `BPlusLeafGrain.Options` reads these values directly from `IOptionsMonitor<LatticeOptions>.Get(treeId)` without consulting the registry, so a caller who changes the configured value on an already-populated tree (e.g. raising `MaxLeafKeys` from 128 to 512 in silo startup code) will silently cause new writes to use the new threshold while the existing internal structure was built at the old one. This corrupts the node-size invariants the tree relies on and can load data past provider limits, with no error surface. `ShardRootGrain` already resolves options registry-first (`GetOptionsAsync` at `ShardRootGrain.cs:35`) and `TreeRegistryEntry` already has the three `int?` slots; the gap is that `BPlusLeafGrain` bypasses them, and the entry is not seeded with the configured values on first tree creation, so even a freshly-created tree is vulnerable until the first `ResizeAsync`.

**Fix.**
1. **Promote the registry-first resolver to a shared helper** (e.g. `LatticeOptionsResolver` / `IOptionsMonitor<LatticeOptions>` extension). `BPlusLeafGrain`, `ShardRootGrain`, and every other grain that reads `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount` goes through it. Non-structural options (timeouts, cache TTLs, split thresholds) continue to read directly from the monitor so they remain tunable at runtime.
2. **Seed the registry entry on first tree creation** with the configured `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount` (not `null`). The registry becomes the authoritative source of these three fields from day one.
3. **Reject drift at startup.** When a tree is touched and the entry's pinned values differ from the current `IOptionsMonitor` configuration, throw `InvalidOperationException` with a message directing the caller to `ResizeAsync` (for fan-out) or documenting that `ShardCount` is immutable (for reshard, use `ReshardAsync`). Fail fast rather than silently ignore — a mismatch is almost always a latent data-corruption bug.

**Scope.** ~300 LOC source, ~15 tests, one PR. No public API surface change. Docs: soften the warning at the top of `tree-sizing.md` once the guard is in place, document registry precedence in `configuration.md`, document auto-seeding in `tree-registry.md`. Regression tests must cover: (a) fresh tree seeded with configured values; (b) mid-life config change throws with an actionable message; (c) `ResizeAsync` path updates the pinned values and subsequent activations succeed; (d) non-structural option changes (e.g. `CacheTtl`) still take effect without error; (e) system trees (`SystemTreePrefix`) bypass the guard as they do today.

**Why high priority.** A user raising `MaxLeafKeys` in silo config — a plausible scaling adjustment — can silently corrupt an existing tree's size invariants and push leaf state past the provider hard limit. There is no error surface today; the corruption only manifests later as `WriteStateAsync` failures or unexpected split behaviour. The guard is the difference between a self-inflicted outage and a fast-fail at startup.

### 🔲 FX-014 — Observe shadow-forward tasks in `ShardRootGrain` mutation paths *(reliability / low — follow-up from F-019 review)*
**Problem.** Every mutation path in `ShardRootGrain` (`SetAsync`, `SetAsync(ttl)`, `GetOrSetAsync`, `SetIfVersionAsync`, `SetManyAsync`, `DeleteAsync`, `DeleteRangeAsync`, `MergeManyAsync`) dispatches a parallel `ForwardShadowAsync` task when shadow forwarding is active. Two gaps surfaced in the F-019 branch review:

1. **Orphaned task on transient-error retry.** The single-key mutation methods wrap the local write in a `while` loop that retries on transient Orleans exceptions. Each iteration allocates a fresh `forwardTask`; the previous iteration's task is abandoned — neither awaited nor cancelled. LWW guarantees the destination still converges, but if an abandoned forward faults, it surfaces only via `TaskScheduler.UnobservedTaskException`, bypassing the grain's normal error-handling path.
2. **Exception masking in `SetManyAsync`.** The `finally { await forwardTask; }` at the end of `SetManyAsync` will replace a local-loop exception with a forward-path exception if both fail in the same call. Callers lose visibility of the original (root-cause) error.

**Fix.** Track every in-flight shadow-forward task in an activation-scoped list, observe all prior tasks (await-and-discard-exception or attach a faulted-task logger) before dispatching the next one, and switch the `SetManyAsync` finally block to `Task.WhenAll` with aggregated exception preservation so the local failure is the primary.

**Scope.** ~80 LOC source, ~6 tests, one PR. No public API change. Tests must cover: (a) retry loop does not leak unobserved faulted forward tasks; (b) `SetManyAsync` surfaces the local exception when both paths fail; (c) happy-path forward still completes before the grain deactivates.

**Why low priority.** Neither gap is observable in healthy operation — LWW absorbs the data-plane impact, and double-failure masking only fires when the cluster is already degraded. The fix is a hygiene item to keep the F-019 primitive robust under long-tail failure modes.

---

## 🔍 Gaps & Potential Additions

The following areas are not currently on the roadmap but represent meaningful opportunities, particularly as the library moves toward production adoption.

### G-001 — Leaf-level write batching (investigate before committing)
Sharding and adaptive splitting (F-011) already distribute write load across leaf grains, progressively undermining the need for in-grain write coalescing. The changes required for safe batching — `[Reentrant]` leaf grains, a `FlushAsync` drain contract for split/merge/snapshot coordinators, and an explicit `WriteMode` API to surface the weakened durability guarantee — touch almost every major system component. The failure modes (silent data loss on crash, stale state seeded into a new shard post-split) are hard to detect in tests and severe in production. Before pursuing, profile under realistic write-skew conditions with adaptive splitting active and confirm that leaf-level hotness persists after splits have stabilized. If poor key entropy is the root cause, key-design guidance is a lower-risk remedy. Only implement if profiling proves the leaf grain itself is the bottleneck.

### G-002 — Compaction policy controls
F-016 (TTL) assumes that "existing compaction infrastructure" handles expired-key reaping, but there is no roadmap item covering compaction itself as a configurable feature. Operators have no way to tune tombstone reaping thresholds, set compaction schedules, or observe space amplification (beyond the tombstone ratio in F-014). Compaction policy controls — minimum tombstone ratio before reaping, maximum leaf size before forced compaction, and a compaction telemetry hook for F-029 — warrant a dedicated item.

### G-003 — Stateful cursor / iterator grain
Pagination today is stateless (client holds a token per shard). For long-running scans that span many pages, or scans that need to survive client restarts, a stateful cursor grain that checkpoints scan progress server-side would significantly simplify client code and enable reliable export pipelines without external state management.

*✅ Delivered as **F-033** — see Completed / Reliability above.*

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

### G-011 — Caller-supplied idempotency key for `SetManyAtomicAsync`
`SetManyAtomicAsync` generates a fresh `Guid` per call as the saga's `operationId`. On a transport failure (timeout, silo crash during the call) the client has no way to re-attach to the in-flight saga: it must treat the call as failed and issue a new saga, accepting that the original may still commit server-side. A caller-supplied stable `operationId` — modelled on the Stripe / Azure Service Bus idempotency-key pattern — would turn this into a genuine exactly-once write from the client's perspective. The saga grain is already keyed by `{treeId}/{operationId}` and already idempotent on re-entry, so the implementation cost is low: one additional overload on `ILattice` and corresponding typed extension. The main design question is what to do when a caller reuses an `operationId` with a different `entries` list — the grain would silently apply the original persisted entries. The overload must document this clearly and consider whether to detect the mismatch by comparing entry counts or key sets before resuming.

### G-012 — `CoordinatorGrain<TSelf, TState>` base class for long-running coordinators
`TreeSnapshotGrain`, `TreeResizeGrain`, `TreeShardSplitGrain`, `TreeReshardGrain`, and `TombstoneCompactionGrain` all implement the same reminder-anchored work-pump pattern: a 1-minute `{name}-keepalive` reminder re-arms a 2-second grain timer on silo restart, the timer calls into a `ProcessNextPhaseAsync` state-machine hook, and a terminal `Complete*Async` helper disposes the timer + unregisters the reminder + `DeactivateOnIdle`. Each grain duplicates ~80 lines of `KeepaliveReminderName` const / `_xxxTimer` field / `StartXxxAsync` / `OnXxxTimerTick` / `UnregisterKeepaliveAsync` boilerplate. This is semantically distinct from the existing `TtlGrain<TSelf>` (which models inactivity-driven self-destruct, not work-driven keepalive) and should be a sibling abstraction rather than an extension of `TtlGrain`. Proposed shape: `internal abstract class CoordinatorGrain<TSelf, TState> : IRemindable, IGrainBase` with `protected abstract string KeepaliveReminderName`, `protected abstract bool InProgress`, `protected abstract Task ProcessNextPhaseAsync()`, and non-virtual helpers for timer lifecycle + reminder register/unregister + completion. Pure refactor — zero behaviour change, zero public API change. Blocked by nothing; estimated ~5 files rewritten, ~400 lines deleted. Orthogonal to F-019 and should ship as its own PR after F-019 lands.

Two smaller follow-ups identified during F-019 phase-3 review that belong in the same refactor (they disappear when the coordinator base class lands):

- **Narrow the `InvalidOperationException` catch in `TreeResizeGrain.InitiateResizeAsync`.** The idempotent re-entry path currently catches `InvalidOperationException` unconditionally to detect "snapshot already in progress" — it should match on the `SnapshotAlreadyInProgress` specific exception (or a sentinel) so unrelated invalid-state bugs surface rather than being swallowed. The `CoordinatorGrain` base should expose a typed `AlreadyInProgress` signal that collapses every coordinator's custom string-matching into one shared helper.
- **Rename `IsCompleteAsync` on `ITreeSnapshotGrain` / `ITreeResizeGrain` / `ITreeShardSplitGrain`.** Today it means "the coordinator is not currently running" (true before any operation starts, true after one finishes, false while one is in flight). The name suggests per-operation completion. Better: `IsIdleAsync` — semantically precise and preserves the polling idiom in `HotShardMonitorGrain`. Breaking change on three internal interfaces; trivial source update across docs and tests. Bundle with G-012 so the rename and base-class migration ship atomically.

---

## Dependencies

| Feature | Depends on | Status |
|---|---|---|
| F-011 (Adaptive shard splitting) | F-013 (Shard hotness counters) | ✅ Both complete |
| F-011 (Adaptive shard splitting) | F-028 (Shard map indirection) | ✅ Both complete |
| F-011 (Adaptive shard splitting) | F-030 (Route `BulkLoadAsync` through shard map) | ✅ Both complete |
| F-029 (External metrics / telemetry) | F-013 (Shard hotness counters) | ✅ Prereq complete — F-029 unblocked |
| F-025 (Continuous merge) | F-020 (Merge trees) | ✅ Prereq complete — F-025 unblocked |
| F-027 (Leaf-grouped merge routing) | — | ✅ Complete (amortises batched merge at scale for F-025) |
| F-019 (Online resize) | F-028 (Shard map indirection) | ✅ Prereq complete — F-019 unblocked |
| F-024 (Entry pre-fetch) | F-009 (Key pre-fetch) | ✅ Prereq complete — F-024 unblocked |
| F-022 (Troubleshooting guide) | F-014 (Tree diagnostics) | 🔲 Guide should follow diagnostics implementation |
| F-031 (Atomic multi-key writes) | F-017 (CAS) | ✅ Both complete |
| F-032 (Scan ordering under topology change) | F-011 (Adaptive shard splitting) | ✅ Both complete |
| F-033 (Stateful cursor grain) | F-032 (Scan ordering under topology change) | ✅ Both complete |
