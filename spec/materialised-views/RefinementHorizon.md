# Multi-partition prepare-horizon refinement

This is a documented abstraction, not a machine-checked refinement proof.
The instance deliberately separates key prepare partitions from terminal
partitions. Reading both shard terminals is not proof that every prepare
partition was consumed. Source installation can precede terminal append.
The detector rows are proposed mappings pending production acceptance evidence;
they deliberately fail the unqualified-Yes acceptance gate until integrated.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `length`, `installed` | Physical WAL heads and separately installed source values | `ICommitLogReader.GetHeadOffsetAsync`, `ILattice.GetAsync`. |
| `wal`, `horizonWal` | The physical identity addressed by heads and consumed offsets | `ViewCheckpointState.BoundPhysicalTreeId`. |
| `read`, `ckpt` | Per-partition consumed position and durable held checkpoint | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewCheckpointState.AppliedOffsets`. |
| `prepares`, `terminals`, `staged` | Prepared indices, distinct shard terminal tally and staging membership | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. |
| `historyPrepares`, `historyTerminals`, `historyCursor`, `historyBudgetUsed` | Same-physical retained-history evidence, resumable scan position, and per-drain examined-entry budget | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. |
| `horizon` | Heads captured after observing a terminal with missing prepares | `ViewMaintainerGrain.DrainAsync`. |
| `abandoned` | Durable identity whose later terminals must trigger recovery | `ViewMaintainerGrain.DrainAsync`. |
| `heads`, `floor`, `planned`, `shadow`, `value` | Rebuild head vector, planned replay floor, shadow and published generation | `ViewMaintainerGrain.CaptureResumeFloorsAsync`, `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.SwapToShadowAsync`. |
| `phase`, `requested`, `recovery`, `crashes`, `rebuilds`, `backstops` | Abstract turn boundaries and finite environment budgets | Modelling devices; no additional persisted production state. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AppendPrepare` | Append the two prepared keys before any terminal | `ViewMaintainerGrain.HandleStagingEntry`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `InstallSource` | Source values become readable independently of terminal append | `ILattice.GetAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `AppendCommit` | Append one distinct shard terminal on each of two partitions | `ViewMaintainerGrain.HandleStagingEntry`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Consume` | Stage per-partition entries without advancing the durable checkpoint over them | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `HistorySearch` | Examine one bounded retained-history entry at or below the durable checkpoint, resume from the prior cursor, and recover its prepare or terminal witness | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`. |
| `ResetHistoryBudget` | Start another drain pass with a fresh examination budget without rewinding the retained-history cursor | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_historical_orphan_search_shares_one_budget_across_batches`. |
| `CaptureHorizon` | Sample every physical partition head after a terminal is observed | `ViewMaintainerGrain.DrainAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Flush` | Flush only after both prepares and every distinct shard terminal arrived | `ViewMaintainerGrain.IsComplete`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Recover` | Missing prepares after the entire captured horizon cause source rebuild | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Backstop` | Discard bounded staging while retaining identity for later terminals | `ViewMaintainerGrain.StagingBackstopTripped`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`. |
| `RequestRebuild` | Operator rebuild participates in the same replay-floor protocol | `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `Capture` | Capture the complete per-partition head vector | `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Plan` | Lower discovered undecided-entry floors; a bounded scan may miss another partition | `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Scan` | Scan current source values into the shadow without exposing partial work | `ViewMaintainerGrain.BuildShadowAsync`. | Pending: proposed detector `MaterialisedViewShadowSwapTests.Rebuild_never_exposes_empty_or_partial_view_to_a_concurrent_reader`. |
| `Swap` | Publish the scanned generation and floor; retire abandoned identity only after all shard terminals were captured | `ViewMaintainerGrain.SwapToShadowAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `Crash` | Lose volatile staging/horizon and replay from held-back offsets; retry interrupted rebuild | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`, `MaterialisedViewShadowSwapTests.Rebuild_converges_after_a_simulated_crash_before_swap`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `HorizonAfterTerminal` | Missing-prepare detection samples heads only after a terminal | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `HorizonRequiresHistoryEntitlement` | A terminal, sampled head, or incomplete retained-history scan cannot stand in for prepare cardinality and the complete distinct terminal set | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare`. |
| `HistorySearchProgress` | A bounded retained-history pass advances rather than repeating the same record | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`. |
| `CheckpointHeld` | An unresolved entry remains above its durable resume offset | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `QuiescentExact` | At a committed, fully consumed source head the view contains both source keys | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `NoPrematureFlush` | One shard commit cannot flush a multi-shard batch | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `NoPrematureRetirement` | Rebuild/backstop preserve abandoned identity until every shard's terminal lies below the captured heads | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `RecoveryEntitled` | Orphan recovery consumes every partition below the post-terminal horizon, not just terminal-bearing partitions | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `RebuildReplayEntitled` | Undecided entries discovered below the heads remain in the resumed replay window | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `Converges` | A straddling or abandoned committed batch eventually reaches the view and releases its checkpoint; fair bounded retained-history passes resume after each examined-entry budget reset | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`, `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain constraint, not a production behaviour. |

## Evidence and limits

The fixed topology has two source keys, two source shards and three physical
WAL partitions. Prepares land on p0/p2; terminals land on p0/p1. The head and
checkpoint representation is consumed-prefix length, consistently translated
from production's zero-based last-offset/head-minus-one convention. HLC values
20/21 are separate from offsets. Every prepare exists before any terminal;
installation and terminal append are separately interleaved.

The bounded retained-history tape is fixed to one selected physical WAL. Its
prepare-cardinality witness and distinct terminal-partition evidence are
recovered incrementally from entries at or below each partition's durable
checkpoint. A new history pass gets the configured examined-entry budget but
retains its cursor; multiple unresolved batches share that pass budget. A
terminal-only fresh observation is not sufficient evidence of its own
prepares, and a prepare witness without every distinct terminal is not enough
to capture a horizon or retire an orphan. This represents the production
`ReadFilteredAsync` call bounded by the durable checkpoint and remaining shared
budget, not an unbounded enumeration.

An orphan is recoverable after any terminal and complete horizon consumption,
not only after all terminals reappear above the checkpoint: a preceding rebuild
may have skipped a terminal already below its floor. The rebuild retains the
abandoned identity until all shard terminals are in its captured head vector.
The source scan may therefore reflect one installed shard before the other;
this model does not promise source snapshot isolation or atomicity of rebuild
reads. It promises eventual repair and does not confuse a single terminal
with a full batch decision.

`Plan` abstracts a clamped WAL scan by a deterministic partition cut. It checks
both discovered undecided entries and omitted prepares, not every byte-budget
distribution or every retention gap. The retained-history tape has no retention
gap and no physical-log rebind; if an entry has expired before this same-log
bounded read, the model makes no recovery claim. `Scan` abstracts a completed
source scan as one action; generation visibility and trim/rebind are the tail
model's. There is one transaction, no abort, no ordinary competing write, no
aggregation fold, and no cross-tree coordinator. Those interactions belong to
companion models. Fair source completion and fair maintenance follow finite
faults; unbounded crashes or a permanently unavailable source are not
convergence claims. The production detector rows name seam tests, not an
automatic refinement proof or crash injection at every await.
