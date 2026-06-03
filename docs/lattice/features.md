# Orleans.Lattice Feature Index

Feature planning for the core `Orleans.Lattice` package is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice), not in roadmap files. This page is a grouped, human-readable index that links each tracked item to its issue. Keep it in sync whenever an issue is opened, closed, or retitled (see the agent instructions in `.github/copilot-instructions.md`).

- **Browse all core issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice
- **Open core issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=is%3Aopen+label%3Alattice

## Features

### Planned / open

- [F-012](https://github.com/NSTA1/Orleans.Lattice/issues/332) - Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes
- [F-018](https://github.com/NSTA1/Orleans.Lattice/issues/338) - Associate tags with keys and query by tag
- [F-021](https://github.com/NSTA1/Orleans.Lattice/issues/341) - Migration guide
- [F-022](https://github.com/NSTA1/Orleans.Lattice/issues/342) - Troubleshooting guide (`docs/troubleshooting.md`)
- [F-023](https://github.com/NSTA1/Orleans.Lattice/issues/343) - Sample applications (`samples/`)
- [F-025](https://github.com/NSTA1/Orleans.Lattice/issues/345) - Incremental, ongoing merge from one or more source trees using `VersionVector` to track a per-source high-water mark, so each cycle transfers only entries newer than the last
- [F-042](https://github.com/NSTA1/Orleans.Lattice/issues/362) - Cluster-internal queue abstraction (`ILatticeQueue<T>`)
- [F-066](https://github.com/NSTA1/Orleans.Lattice/issues/386) - Caching call-path co-location candidates
- [F-067](https://github.com/NSTA1/Orleans.Lattice/issues/387) - Bound `LeafCacheGrain._cache` size without violating Orleans semantics
- [F-075](https://github.com/NSTA1/Orleans.Lattice/issues/395) - Per-row WAL payload compression on the Azure Table provider
- [F-076](https://github.com/NSTA1/Orleans.Lattice/issues/396) - Clean silo shutdown for benchmark / production hosts
- [F-078](https://github.com/NSTA1/Orleans.Lattice/issues/398) - Promote public Primitives types into `Orleans.Lattice` per the namespace convention

### Shipped

- [F-001](https://github.com/NSTA1/Orleans.Lattice/issues/322) - Range Delete (`DeleteRangeAsync`)
- [F-002](https://github.com/NSTA1/Orleans.Lattice/issues/323) - `CountAsync` / `CountPerShardAsync`
- [F-003](https://github.com/NSTA1/Orleans.Lattice/issues/324) - `GetOrSetAsync` (conditional write)
- [F-004](https://github.com/NSTA1/Orleans.Lattice/issues/325) - Typed value helpers
- [F-005](https://github.com/NSTA1/Orleans.Lattice/issues/326) - `EntriesAsync` (key + value scan)
- [F-006](https://github.com/NSTA1/Orleans.Lattice/issues/327) - Leaf-side continuation filtering for `EntriesAsync`
- [F-007](https://github.com/NSTA1/Orleans.Lattice/issues/328) - Leaf-side continuation filtering for `KeysAsync`
- [F-008](https://github.com/NSTA1/Orleans.Lattice/issues/329) - Reverse-scan leaf-side filtering
- [F-009](https://github.com/NSTA1/Orleans.Lattice/issues/330) - Parallel shard pre-fetch for `KeysAsync`
- [F-011](https://github.com/NSTA1/Orleans.Lattice/issues/331) - Adaptive shard splitting
- [F-013](https://github.com/NSTA1/Orleans.Lattice/issues/333) - Internal shard hotness counters
- [F-014](https://github.com/NSTA1/Orleans.Lattice/issues/334) - Per-shard health / diagnostics
- [F-015](https://github.com/NSTA1/Orleans.Lattice/issues/335) - Tree events (Orleans Streams)
- [F-016](https://github.com/NSTA1/Orleans.Lattice/issues/336) - TTL on `SetAsync`
- [F-017](https://github.com/NSTA1/Orleans.Lattice/issues/337) - Compare-and-swap (CAS)
- [F-019](https://github.com/NSTA1/Orleans.Lattice/issues/339) - Online (non-blocking) resize
- [F-020](https://github.com/NSTA1/Orleans.Lattice/issues/340) - Merge trees (`MergeAsync`)
- [F-024](https://github.com/NSTA1/Orleans.Lattice/issues/344) - Parallel shard pre-fetch for `EntriesAsync`
- [F-026](https://github.com/NSTA1/Orleans.Lattice/issues/346) - Operation status queries
- [F-027](https://github.com/NSTA1/Orleans.Lattice/issues/347) - Leaf-grouped merge routing
- [F-028](https://github.com/NSTA1/Orleans.Lattice/issues/348) - Shard map indirection
- [F-029](https://github.com/NSTA1/Orleans.Lattice/issues/349) - External metrics / telemetry export
- [F-030](https://github.com/NSTA1/Orleans.Lattice/issues/350) - Route `BulkLoadAsync` through the shard map
- [F-031](https://github.com/NSTA1/Orleans.Lattice/issues/351) - Atomic multi-key writes (saga)
- [F-032](https://github.com/NSTA1/Orleans.Lattice/issues/352) - Scan ordering preservation under topology change
- [F-033](https://github.com/NSTA1/Orleans.Lattice/issues/353) - Stateful cursor / iterator grain
- [F-034](https://github.com/NSTA1/Orleans.Lattice/issues/354) - Resilient client-side scan iterators
- [F-035](https://github.com/NSTA1/Orleans.Lattice/issues/355) - Grain-side mutation observer hook
- [F-036](https://github.com/NSTA1/Orleans.Lattice/issues/356) - `OriginClusterId` on `LwwValue` / `LwwEntry`
- [F-037](https://github.com/NSTA1/Orleans.Lattice/issues/357) - Replication write-ahead-log prefix reservation as a hard guarantee
- [F-038](https://github.com/NSTA1/Orleans.Lattice/issues/358) - Typed CRDT primitive value surface
- [F-039](https://github.com/NSTA1/Orleans.Lattice/issues/359) - MV-Register primitive + accessor
- [F-040](https://github.com/NSTA1/Orleans.Lattice/issues/360) - OR-Map primitive + accessor
- [F-041](https://github.com/NSTA1/Orleans.Lattice/issues/361) - RGA sequence primitive + accessor
- [F-043](https://github.com/NSTA1/Orleans.Lattice/issues/363) - `VectorClock` slot on `LwwValue` / `LwwEntry`
- [F-044](https://github.com/NSTA1/Orleans.Lattice/issues/364) - Atomic-transaction boundary on the mutation observer
- [F-045](https://github.com/NSTA1/Orleans.Lattice/issues/365) - `MutationCategory` classification on `LatticeMutation`
- [F-046](https://github.com/NSTA1/Orleans.Lattice/issues/366) - VC + origin context preservation through structural ops
- [F-047](https://github.com/NSTA1/Orleans.Lattice/issues/367) - Pre-merge delta capture on the observer payload
- [F-048](https://github.com/NSTA1/Orleans.Lattice/issues/368) - Leaf-grain projection rebuild seam
- [F-049](https://github.com/NSTA1/Orleans.Lattice/issues/369) - WAL-as-sole-commit-point promotion
- [F-050](https://github.com/NSTA1/Orleans.Lattice/issues/370) - Leaf-grain cursor-registry integration
- [F-051](https://github.com/NSTA1/Orleans.Lattice/issues/371) - Materialiser-side HWM (offset-keyed)
- [F-052](https://github.com/NSTA1/Orleans.Lattice/issues/372) - Operator tooling for projection inspection and rebuild
- [F-053](https://github.com/NSTA1/Orleans.Lattice/issues/373) - Chain leaf projection digests through internal grains
- [F-054](https://github.com/NSTA1/Orleans.Lattice/issues/374) - `ApplyManyAtomicAsync` source-HLC-preserving atomic apply seam
- [F-055](https://github.com/NSTA1/Orleans.Lattice/issues/375) - Reader isolation during in-flight sagas
- [F-056](https://github.com/NSTA1/Orleans.Lattice/issues/376) - Promote WAL cursor registry and GC to the core library
- [F-057](https://github.com/NSTA1/Orleans.Lattice/issues/377) - Hardening of the cross-migration LWW backstop branch on `BPlusLeafGrain.ApplyTxTerminalAsync` introduced by F-055
- [F-058](https://github.com/NSTA1/Orleans.Lattice/issues/378) - Route the cross-migration LWW backstop write through the per-shard WAL instead of through `state.WriteStateAsync()`
- [F-059](https://github.com/NSTA1/Orleans.Lattice/issues/379) - Retroactive shadow-forward of in-flight prepared mutations when a shard split begins
- [F-060](https://github.com/NSTA1/Orleans.Lattice/issues/380) - Route the remaining foreground leaf-write sites in `BPlusLeafGrain.MergeEntriesAsync`, `BPlusLeafGrain.MergeManyAsync`, and `BPlusLeafGrain.CompactTombstonesAsync` through `ICommitLogWriter` instead of through standalone `state.W
- [F-061](https://github.com/NSTA1/Orleans.Lattice/issues/381) - Today every public mutating method on `ILattice` (`SetAsync`, `RemoveAsync`, `SetManyAsync`, `SetManyAtomicAsync`, `IncrementAsync`, `DeleteRangeAsync`, `MergeAsync`, ...) follows the contract "on transient storage failure, throw
- [F-062](https://github.com/NSTA1/Orleans.Lattice/issues/382) - Two independent receiver-side correctness gaps in the universal-saga design caused a continuous remote reader, polling at 10 ms cadence during cross-cluster replication of a multi-shard `SetManyAtomicAsync`, to intermittently obs
- [F-063](https://github.com/NSTA1/Orleans.Lattice/issues/383) - The leaf's `BPlusLeafGrain.CommitSetAsync` / `CommitDeleteAsync` / `DeleteRangeAsync` foreground write paths each end with an awaited cross-grain RPC to the parent internal node (`IBPlusInternalGrain.OnChildDigestPublishedAsync`)
- [F-064](https://github.com/NSTA1/Orleans.Lattice/issues/384) - Point-in-time read views over multi-page enumerations
- [F-065](https://github.com/NSTA1/Orleans.Lattice/issues/385) - Strict snapshot-isolation reads:
- [F-068](https://github.com/NSTA1/Orleans.Lattice/issues/388) - Order-independent WAL provider registration
- [F-069](https://github.com/NSTA1/Orleans.Lattice/issues/389) - Batched WAL append on the leaf write path
- [F-070](https://github.com/NSTA1/Orleans.Lattice/issues/390) - Pipelined phase-2 commit on the Azure Table WAL provider
- [F-071](https://github.com/NSTA1/Orleans.Lattice/issues/391) - Compaction policy controls and telemetry
- [F-072](https://github.com/NSTA1/Orleans.Lattice/issues/392) - Configurable compaction tick cadence
- [F-073](https://github.com/NSTA1/Orleans.Lattice/issues/393) - Intra-shard leaf-walk batching for tombstone compaction
- [F-074](https://github.com/NSTA1/Orleans.Lattice/issues/394) - Shard-root dirty-leaf tracking to skip idle leaves on compaction
- [F-077](https://github.com/NSTA1/Orleans.Lattice/issues/397) - Multi-partition WAL replay on leaf activation
- [F-079](https://github.com/NSTA1/Orleans.Lattice/issues/399) - Multi-silo restart chaos test
- [F-080](https://github.com/NSTA1/Orleans.Lattice/issues/400) - Drop the per-append registry RPC from the WAL hot path (F-077 throughput regression)
- [F-081](https://github.com/NSTA1/Orleans.Lattice/issues/535) - Byte-accurate storage-usage visibility + advisory byte-pressure WAL retention policy

## Follow-up fixes

### Planned / open

### Shipped

- [FX-001](https://github.com/NSTA1/Orleans.Lattice/issues/401) - Leaf split publish ordering
- [FX-002](https://github.com/NSTA1/Orleans.Lattice/issues/402) - Reminder re-registration idempotency
- [FX-003](https://github.com/NSTA1/Orleans.Lattice/issues/403) - `VersionVector` pruning
- [FX-004](https://github.com/NSTA1/Orleans.Lattice/issues/404) - `HotShardMonitor` sampling window survives silo restart
- [FX-005](https://github.com/NSTA1/Orleans.Lattice/issues/405) - `TreeMergeGrain` crash-resume retry semantics
- [FX-006](https://github.com/NSTA1/Orleans.Lattice/issues/406) - `CancellationToken` on `ILattice`
- [FX-007](https://github.com/NSTA1/Orleans.Lattice/issues/407) - Logger category consistency
- [FX-008](https://github.com/NSTA1/Orleans.Lattice/issues/408) - Metrics naming prefix
- [FX-009](https://github.com/NSTA1/Orleans.Lattice/issues/409) - `TypeAliases` dead-entry audit
- [FX-010](https://github.com/NSTA1/Orleans.Lattice/issues/410) - Docs drift guard
- [FX-011](https://github.com/NSTA1/Orleans.Lattice/issues/411) - `ShardRootGrain.DeleteRangeAsync` under-deletion on sparse multi-shard trees
- [FX-012](https://github.com/NSTA1/Orleans.Lattice/issues/412) - `CountAsync` over-counts during concurrent shard splits
- [FX-013](https://github.com/NSTA1/Orleans.Lattice/issues/413) - Observe shadow-forward tasks in `ShardRootGrain` mutation paths
- [FX-014](https://github.com/NSTA1/Orleans.Lattice/issues/414) - Rejecting precedence over IsDeleted on shard preamble
- [FX-017](https://github.com/NSTA1/Orleans.Lattice/issues/415) - Bound saga-terminal RPC chain depth under cascading mid-saga splits
- [FX-018](https://github.com/NSTA1/Orleans.Lattice/issues/416) - Close residual orphan-pending-tx visibility-race on `ReshardTopologyTests` chaos surface
- [FX-019](https://github.com/NSTA1/Orleans.Lattice/issues/417) - Shadow-forward race during ShardSplitPhase Reject and post-Complete transitions
- [FX-020](https://github.com/NSTA1/Orleans.Lattice/issues/538) - `EnsureRootAsync` clobbers live topology on concurrent shard-root reactivation after a secondary-silo restart
- [FX-021](https://github.com/NSTA1/Orleans.Lattice/issues/544) - Leaf-level `CountAsync` / `GetStatsAsync` over-count during an in-progress (or restart-interrupted) leaf split
- [FX-022](https://github.com/NSTA1/Orleans.Lattice/issues/564) - Internal-node split leaves stale per-child digest rows, double-counting subtree entry totals
- [FX-023](https://github.com/NSTA1/Orleans.Lattice/issues/570) - Reshard to equal shard count throws instead of idempotent no-op (crashes host)
- [FX-024](https://github.com/NSTA1/Orleans.Lattice/issues/573) - Scrub ConfigureAwait(false) from WalCommitLogWriter except the deliberate G-023 catch-to-threadpool sites

## Gaps & potential additions

### Planned / open

- [G-001](https://github.com/NSTA1/Orleans.Lattice/issues/418) - Leaf-level write batching (investigate before committing)
- [G-003](https://github.com/NSTA1/Orleans.Lattice/issues/420) - Per-key change subscriptions
- [G-004](https://github.com/NSTA1/Orleans.Lattice/issues/421) - Value compression / encryption
- [G-005](https://github.com/NSTA1/Orleans.Lattice/issues/422) - Quota / admission control per tree
- [G-006](https://github.com/NSTA1/Orleans.Lattice/issues/423) - Admin CLI / `dotnet` tool
- [G-007](https://github.com/NSTA1/Orleans.Lattice/issues/424) - Shard-affine grain placement
- [G-008](https://github.com/NSTA1/Orleans.Lattice/issues/425) - Cluster-wide split concurrency control
- [G-009](https://github.com/NSTA1/Orleans.Lattice/issues/426) - `AtomicWriteGrain` generalization to non-tree mutations
- [G-010](https://github.com/NSTA1/Orleans.Lattice/issues/427) - Repository of point-in-time tree snapshots
- [G-013](https://github.com/NSTA1/Orleans.Lattice/issues/430) - Observer-latency telemetry
- [G-015](https://github.com/NSTA1/Orleans.Lattice/issues/432) - Structural ops as WAL-replayable deterministic log entries
- [G-017](https://github.com/NSTA1/Orleans.Lattice/issues/434) - Snapshot blob size cap and oversized-row policy (investigative)
- [G-018](https://github.com/NSTA1/Orleans.Lattice/issues/435) - Periodic recheck classifier-input cache (investigative)
- [G-022](https://github.com/NSTA1/Orleans.Lattice/issues/567) - Bound internal-node digest publish held under the split gate to prevent a recursive publish-chain wedge

### Shipped

- [G-002](https://github.com/NSTA1/Orleans.Lattice/issues/419) - Compaction policy controls
- [G-011](https://github.com/NSTA1/Orleans.Lattice/issues/428) - Caller-supplied idempotency key for `SetManyAtomicAsync`
- [G-012](https://github.com/NSTA1/Orleans.Lattice/issues/429) - `CoordinatorGrain<TSelf>` base class + `IsIdleAsync` rename
- [G-014](https://github.com/NSTA1/Orleans.Lattice/issues/431) - WAL-as-sole-commit-point promotion (substantially shipped via F-047 ÔåÆ F-052)
- [G-016](https://github.com/NSTA1/Orleans.Lattice/issues/433) - Operator dashboards package (`Orleans.Lattice.Dashboards`)
- [G-019](https://github.com/NSTA1/Orleans.Lattice/issues/546) - Bimodal phase-1/activation wedge on the Azure-Tables WAL hot path (investigative)
- [G-020](https://github.com/NSTA1/Orleans.Lattice/issues/551) - Non-atomic `GetManyAsync` snapshot across mid-saga reshard (migration Swap-phase ordering)
- [G-021](https://github.com/NSTA1/Orleans.Lattice/issues/552) - Reshard swap-phase write-path wedge: bounded outbound shard-forward deadline
- [G-023](https://github.com/NSTA1/Orleans.Lattice/issues/572) - Bound and attribute the residual phase-1/activation WAL wedge (post-#568 diagnostic pack)
- [G-024](https://github.com/NSTA1/Orleans.Lattice/issues/574) - Per-shard FlushAsync lifecycle / StartFlush / reshard diagnostics to attribute the residual phase-1/activation WAL wedge
- [G-025](https://github.com/NSTA1/Orleans.Lattice/issues/575) - Writer-layer pending-append dispatch lifecycle diagnostics to attribute the Mode B WAL wedge upstream of the shard grain

