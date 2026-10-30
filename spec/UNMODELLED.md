# Unmodelled production grains

This register lists every production grain that no refinement note names, with
the reason no specification models it. It is the converse of the refinement
gates: they check that what a note names still exists, and
`RefinementSurfaceCensusTests` (in `test/lattice/Formal/`) checks that every
grain implementation class under `src/` is either named by a refinement note
or listed here.

A grain counts as named when a refinement note mentions its class name, or its
own contract interface (`I` followed by the class name, when the grain
implements it), anywhere: a mapping row, an abstraction-gap bullet or prose.
Grains are found syntactically: classes deriving from Orleans' `Grain` or
`Grain<T>`, implementing `IGrainBase`, or deriving from another grain found that
way.

The gate also fails on a stale row: a grain that no longer exists, that moved
package, or that a note now names. To retire a row, name the grain in the note
of the specification that models it, or explain in that note's abstraction gaps
why the model leaves it out, and delete the row in the same change.

Each reason opens with a classification:

- **Unmodelled protocol** - the grain carries a protocol no specification
  models. Each is a candidate for a model of its own.
- **Abstracted, not named** - a specification models the behaviour without
  naming this grain. Naming it in that note retires the row.
- A classification naming a **gap** - the grain itself is policy, but a rule it
  drives is unmodelled.
- Otherwise the grain is policy, bookkeeping, storage, a derived index, a
  diagnostic or a base class: it decides when work runs, records state some
  modelled grain decides, or touches no tree data.

Rows are sorted by package, then grain, ordinally. The package is the directory
under `src/`.

## Register

| Grain | Package | Reason |
| --- | --- | --- |
| `AtomicActionGrain` | `lattice` | **Unmodelled protocol.** The generic saga / TCC coordinator: ordered forward and compensating steps, persisted after every transition and resumed by a keepalive reminder. Its step rule lives in `AtomicActionPlanCore`, which a Coyote model checks, and its tree-write steps delegate to `AtomicWriteGrain`, which atomic-commit models; no TLA+ module models the compensation protocol itself. |
| `ClusterSplitConcurrencyGrain` | `lattice` | **Admission policy.** Caps the shard migrations in flight cluster-wide (off by default), with time-to-live footprints so a crashed silo's share is reclaimed. Granting or denying a slot changes when a split runs, never what it does; the split is modelled by shard-ownership. |
| `HotShardMonitorGrain` | `lattice` | **Scheduling policy.** Per-tree monitor that picks hot shards to split, under suppression rules (resize, merge, snapshot, pending graft, cooldown). It decides when and where, not how; the split it triggers is modelled by shard-ownership. |
| `LatticeCursorGrain` | `lattice` | **No protocol of its own.** A stateful scan cursor: each step is an ordinary bounded scan through the public API with an exclusive continuation, after which the advanced position is persisted. Its ordering rests on the read path's scan semantics, and its idle-TTL self-cleanup deletes only the cursor's own state. |
| `LatticeLockGrain` | `lattice` | **Unmodelled protocol.** FIFO-fair lock and lease with fencing tokens. Its admission rule (token monotonicity, mutual exclusion, lease reclamation) lives in `LockAdmissionCore`, which a Coyote model checks; no TLA+ module models the lease protocol. |
| `LatticeOperationGrain` | `lattice` | **Bookkeeping.** The durable status record of one long-running operation: status, a cancellation flag, and a runner-liveness check that records a lost operation as failed instead of resuming it. The operation's own work runs elsewhere and is judged there. |
| `LatticeOperationIndexGrain` | `lattice` | **Bookkeeping.** A tenant's bounded, newest-first list of operations with retention-based eviction; a listing aid with no effect on any operation. |
| `LatticeQueueGrain` | `lattice` | **Unmodelled protocol.** FIFO queue over a reserved system tree: monotonic id assignment, a persisted head cursor and FIFO eviction, decided inside one activation. No module models enqueue and dequeue across a crash between the row writes and the head-cursor write. |
| `LeafCacheGrain` | `lattice` | **Not protocol-bearing.** Stateless-worker read-through cache of a leaf's rows, merged with the commutative, idempotent LWW merge, so a stale entry is overwritten without an invalidation protocol; with `CacheTtl` set it serves bounded-stale reads by design. It holds no durable state. |
| `LeafSnapshotSegmentGrain` | `lattice` | **Storage holder.** Persists one leaf snapshot segment row: one read, one write, one clear, no decisions. What a snapshot covers is decided by `BPlusLeafGrain` and recorded through `LeafSnapshotStorageGrain`, which wal models (`Capture`). |
| `ShardHealingOrchestratorGrain` | `lattice` | **Scheduling policy.** Decides which over-split shard pairs to fold and when, admitting at most one new fold per sweep. A fold's correctness is `TreeShardConsolidationGrain`'s, which shard-ownership models (`FoldBegin` and the split steps it enters). |
| `SnapshotBaselineStorageGrain` | `lattice` | **Storage holder.** Persists one materialised snapshot-cursor baseline per (cursor, shard): one read, one write, one clear, plus a sliding self-clear reminder that reclaims a baseline an abandoned scan left behind. The capture fold that produces the baseline lives elsewhere. |
| `SnapshotLeafGrain` | `lattice` | **Not protocol-bearing.** A transient, in-memory, read-only view of one shard, rebuilt by replaying the WAL up to a captured offset. It writes nothing and is rebuilt on the next access after eviction; the WAL prefix it reads is kept by the snapshot's cursor pin, which the WAL GC honours. |
| `TreeMergeGrain` | `lattice` | **Unmodelled protocol.** Merges a source tree into a target shard by shard under LWW, preserving timestamps, with per-shard progress persisted so a restart resumes mid-merge. No module models the merge, its resume, or writes to either tree while it runs. |
| `TtlGrain` | `lattice` | **Abstract base, no protocol.** Reminder plumbing (register, slide, unregister, dispatch) for grains that clean themselves up after a TTL; each subclass is judged in its own right. |
| `TxRegistryHighWaterGrain` | `lattice` | **Abstracted, not named.** The durable monotone mark that makes a tree-wide decision-registry read cover every registry shard that may hold a decision (issue #3501). atomic-commit's `decision[t]` row takes a sharded registry's tree-wide snapshot as the union of every shard's; this grain is what keeps that union complete when the shard count changes. A candidate to name there. |
| `ViewRegistryGrain` | `lattice` | **Configuration store.** Persists runtime-created view definitions so they are re-registered after a restart, with startup declarations winning on a conflict. The view maintenance protocols are modelled by the materialised-views and view-* modules, which take the set of views as given. |
| `WalOrphanPinSweepGrain` | `lattice` | **Scheduler over a gap.** Rate-limits the WAL GC's orphan-pin sweep to once per interval per tree across the cluster, with in-memory demand only: it decides when the sweep runs, not what it removes. The sweep's pin-removal rule (`IWalOrphanPinSweepRunner`) is named by no wal note either, so the safety of removing a pin it judges orphaned is unmodelled. |
| `WalTrimWatermarkSupportGrain` | `lattice` | **Capability marker.** Holds no state and is never activated: a silo hosts it to advertise that its WAL providers persist the trim watermark (issue #4621). The watermark itself is modelled by wal (`SettleHole`). |
| `RepoContextAnnIndexBuildGrain` | `lattice.api.mcp.repocontext` | **Derived-index maintenance.** Reminder-anchored builder of one repository's approximate vector index, which also retires abandoned embedding spaces. Its output is a rebuildable search index outside the storage core: a failed or lost build costs search quality or speed until the next build, never tree data. |
| `RepoContextSelfIndexGrain` | `lattice.api.mcp.repocontext` | **Derived-index maintenance.** Per-repository background scan that re-drives the idempotent indexing pass when it finds an unembedded file or a failed run. A missed pass leaves the index stale until the next scan. |
| `RepoIndexJobGrain` | `lattice.api.mcp.repocontext` | **Derived-index maintenance.** The durable record and resume reminder of one indexing job. The indexing pass it re-enqueues is idempotent, so a resumed or duplicate run redoes nothing that already committed. |
| `TenantRegionBackfillCoordinatorGrain` | `lattice.api.tenantadmin` | **Retry pump.** Holds a tenant's durable backfill intent and pinned source across restarts and delegates each attempt to the idempotent replication bootstrap (`LatticeBootstrapCoordinatorGrain`), which atomic-commit's cross-cluster note names. |
| `AppActivationGrain` | `lattice.apps` | **Security orchestration, not distributed state.** Serializes one tenant app's activation pipeline across the cluster and enforces its authorization; it coordinates no replicated or sharded state. |
| `AuthPolicyEpochGrain` | `lattice.auth` | **Cache-invalidation signal.** One in-memory epoch per cluster; every reactivation mints a fresh epoch, so losing it reads as a policy change, never as no change. It coordinates no durable state. |
| `BackupHealthMonitorGrain` | `lattice.backup` | **Diagnostic.** Periodically re-verifies enrolled backups and writes only health reports; it changes no backup and no tree. |
| `BackupSchedulerGrain` | `lattice.backup` | **Scheduling, and a retention gap.** Triggers captures (the capture is modelled by the backup modules) and prunes the scope's chain under its retention policy, keeping the base closure of every retained link. No backup module models retention pruning. |
| `GrainIndexBackfillGrain` | `lattice.grainindex` | **Crawler.** Activates not-yet-indexed grains so they enrol themselves and advances one durable checkpoint, rate-limited per tick; enrolment is idempotent, so a repeated or lost pass only delays coverage. The grain-index package as a whole has no specification. |
| `IndexedGrain` | `lattice.grainindex` | **Abstract convenience base, no protocol.** Holds the state object the `[Indexed]` attribute produced and forwards to it; it contains no enrolment logic and is never activated itself. |
| `CrossTreeDecisionSequenceGrain` | `lattice.replication` | **Abstracted, not named.** A tree's monotone cross-tree decision counter (issue #4733). atomic-commit's cross-cluster note models the decision sequence a barrier tombstone waits for (`TombstoneDrop`) without naming the grain that issues it. A candidate to name there. |
| `CrossTreeHoldTrackerGrain` | `lattice.replication` | **Abstracted, not named.** The origin-side record of the cross-tree decision purge hold (issue #4684). atomic-commit's cross-cluster note models the hold (`OriginPurge`, through `ReplicationCrossTreeDecisionHold.MayPurgeAsync` and `ICrossTreePeerEnrolmentGrain`) without naming this store. A candidate to name there. |
| `CrossTreePurgeFrontierSourceGrain` | `lattice.replication` | **Abstracted, not named.** The origin's cross-tree purge frontier (issue #4733). atomic-commit's cross-cluster note models the frontier and names the receiver's `CrossTreePurgeFrontierGrain` (`TombstoneDrop`), but not this origin-side source. A candidate to name there. |
| `ReplicationDecommissionedPeerRegistryGrain` | `lattice.replication` | **Abstracted, not named.** The durable record of decommissioned peers, which refuses to re-enrol one until a fresh bootstrap. atomic-commit's cross-cluster note models decommission and a fresh re-add (`Decommission`, `decom`, `afresh`) without naming this registry. A candidate to name there. |
| `ReplicationExportEpochGrain` | `lattice.replication` | **Abstracted, not named.** The per-tree snapshot export epoch (issue #4534) that a re-seeded peer echoes back. The replication note (`requested`) and atomic-commit's cross-cluster note (`uimp`, `Readd`) model the epoch without naming the grain that advances it. A candidate to name there. |
| `LatticeSchemaRemediationGrain` | `lattice.schema` | **Unmodelled protocol.** Per-tree schema remediation: a read-only dry-run gate, a transformed copy into a fresh physical tree laid out like the source, then a cutover that installs the target policy and repoints the logical tree. No module models the copy-and-cutover sequence. |
