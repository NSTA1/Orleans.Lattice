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
| `length`, `installed` | Physical WAL heads and prepared source-row application, distinct from logical decision visibility | `ICommitLogReader.GetHeadOffsetAsync`, source leaf apply path. |
| `wal`, `horizonWal` | The physical identity addressed by heads and consumed offsets | `ViewCheckpointState.BoundPhysicalTreeId`. |
| `read`, `ckpt` | Per-partition consumed position and durable held checkpoint | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewCheckpointState.AppliedOffsets`. |
| `prepares`, `terminals`, `staged`, `cardinalityKnown` | Observed prepare payload indices, distinct shard terminal tally, staging membership, and independent positive batch-size witness | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`, `RecoverOrphanPrepareCardinalityAsync`. |
| `historyRetained`, `historyPrepares`, `historyTerminals`, `historyCursor`, `historyBudgetUsed` | Fixed retention-availability mask, same-physical evidence, resumable scan position, and per-drain examined-entry budget | Bounded `ICommitLogReader.ReadFilteredAsync` through the durable checkpoint and remaining shared budget. |
| `horizon` | Heads captured after observing a terminal with missing prepares | `ViewMaintainerGrain.DrainAsync`. |
| `abandoned` | Durable identity whose each newly observed commit terminal can trigger another rebuild | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.DrainAsync`. |
| `sourceDecisionCommitted` | External transaction decision/read-visibility seam; never inferred from one terminal or sampled heads | `TxDecisionView`, `SnapshotPendingForReadAsync`, `GetVisibleRawEntriesAsync`. |
| `heads`, `floor`, `planned`, `shadow`, `value` | Rebuild head vector, planned replay floor, shadow and published generation | `ViewMaintainerGrain.CaptureResumeFloorsAsync`, `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.SwapToShadowAsync`. |
| `phase`, `requested`, `recovery`, `crashes`, `rebuilds`, `backstops` | Abstract turn boundaries and finite environment budgets | Modelling devices; no additional persisted production state. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AppendPrepare` | Append the two prepared keys before any terminal | `ViewMaintainerGrain.HandleStagingEntry`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `InstallSource` | Prepared source rows are installed independently of shard-terminal append, but remain hidden from rebuild reads until the source decision is confirmed | Source leaf apply path, visible-row read seam. | Pending: proposed detector `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`. |
| `ConfirmSourceDecision` | Confirm the complete prepared source batch separately from WAL terminals; visibility is not inferred from terminal arrival | `SnapshotPendingForReadAsync`, `TxDecisionView`, `GetVisibleRawEntriesAsync`. | Pending: proposed detector `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`. |
| `AppendCommit` | Append one distinct shard terminal on each of two partitions | `ViewMaintainerGrain.HandleStagingEntry`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Consume` | Stage per-partition entries without advancing the durable checkpoint over them | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `HistorySearch` | Examine one bounded retained-history entry at or below the durable checkpoint, resume from the prior cursor, and recover its prepare or terminal witness | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`. |
| `ResetHistoryBudget` | Start another drain pass with a fresh examination budget without rewinding the retained-history cursor | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_historical_orphan_search_shares_one_budget_across_batches`. |
| `CaptureHorizon` | Sample every physical partition head after a terminal is observed | `ViewMaintainerGrain.DrainAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Flush` | Flush only after both prepares and every distinct shard terminal arrived | `ViewMaintainerGrain.IsComplete`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Recover` | A regular staged orphan may rebuild only after a retained positive size witness, every distinct terminal, and consumption through the captured physical horizon | `OrphanedCommittedBatchAsync`, `RecoverOrphanPrepareCardinalityAsync`, `ViewMaintainerGrain.RebuildAsync`. | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare`. |
| `RecoverAbandoned` | An observed commit terminal on an abandoned batch triggers rebuild without regular orphan history entitlement; a later shard terminal may trigger another rebuild | `AbandonedBatchCommitted`, `ViewMaintainerGrain.RebuildAsync`. | Pending: `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `Backstop` | Discard bounded staging while retaining identity for later terminals | `ViewMaintainerGrain.StagingBackstopTripped`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`. |
| `RequestRebuild` | Operator rebuild participates in the same replay-floor protocol | `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `Capture` | Capture the complete per-partition head vector | `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Plan` | Lower discovered undecided-entry floors; a bounded scan may miss another partition | `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Scan` | Read only source rows visible through the transaction-decision seam; individually installed but undecided prepared rows are not a valid rebuild image | `ViewMaintainerGrain.BuildShadowAsync`, `GetVisibleRawEntriesAsync`. | Pending: proposed detector `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `Swap` | Publish the scanned generation and floor; retire abandoned identity only after all shard terminals were captured | `ViewMaintainerGrain.SwapToShadowAsync`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `Crash` | Lose volatile staging/horizon and replay from held-back offsets; retry interrupted rebuild | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.RebuildAsync`. | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`, `MaterialisedViewShadowSwapTests.Rebuild_converges_after_a_simulated_crash_before_swap`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `HorizonAfterTerminal` | Missing-prepare detection samples heads only after a terminal | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `HorizonRequiresHistoryEntitlement` | One positive size witness from an actual retained prepare and every distinct terminal are both required; a terminal, head, or retention gap cannot establish cardinality | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare`. |
| `HistorySearchProgress` | A bounded retained-history pass advances rather than repeating the same record | Pending: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`. |
| `CheckpointHeld` | An unresolved entry remains above its durable resume offset | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `QuiescentExact` | At a committed, fully consumed source head the view contains both source keys | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `PublishedBatchAtomic` | A rebuild never publishes a partial prepared batch when raw rows exist before the source decision is visible | Pending: proposed detectors `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`, `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `SourceDecisionPrerequisite` | The source-decision abstraction requires both prepare records and both source rows; one shard terminal is not decision evidence | Pending: proposed detector `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`. |
| `NoPrematureFlush` | One shard commit cannot flush a multi-shard batch | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `NoPrematureRetirement` | Rebuild/backstop preserve abandoned identity until every shard's terminal lies below the captured heads | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `RecoveryEntitled` | Orphan recovery consumes every partition below the post-terminal horizon, not just terminal-bearing partitions | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `RebuildReplayEntitled` | Undecided entries discovered below the heads remain in the resumed replay window | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `Converges` | Eligible staged or abandoned committed batches eventually reach the view; abandoned rebuilds need no retained-history witness, while a complete retained-history cardinality gap is explicitly excluded | Pending: proposed detectors `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`, `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain constraint, not a production behaviour. |

## Evidence and limits

The model's `installed` set denotes prepared source-row installation; it is
independent of WAL terminal append. `AppendCommit` permits the first distinct
terminal before source-decision confirmation but requires confirmation before
the final distinct terminal. This is a deliberately relaxed ordering for the
two-key visibility abstraction, not evidence that production permits a
pre-decision terminal: the producer's stronger decision-before-any-terminal
ordering is outside the model's validated guarantee.

The fixed topology has two source keys, two source shards and three physical
WAL partitions. Prepares land on p0/p2; terminals land on p0/p1. The head and
checkpoint representation is consumed-prefix length, consistently translated
from production's zero-based last-offset/head-minus-one convention. HLC values
20/21 are separate from offsets. Every prepare exists before any terminal;
installation and terminal append are separately interleaved.

The bounded retained-history tape is fixed to one selected physical WAL. The
retention mask is independent of the tape's original entries: the positive
orphan case trims the p2 prepare row while retaining one actual p0 prepare with
its positive `AtomicBatchSize` witness and a p1 terminal below checkpoint. The
model records `cardinalityKnown` separately from recovered prepare payload
membership, so that one witness does not pretend the missing p2 payload was
read. Every distinct terminal partition is still required before capturing a
horizon or retiring a regular orphan. A fresh terminal-only log and a retained
history with all prepare witnesses trimmed are explicit negative controls; the
latter is a retention gap for which convergence is not claimed. Each pass has
the configured examined-entry budget, the cursor resumes, and a skipped or
trimmed entry does not consume budget. This abstracts the production bounded
`ReadFilteredAsync` call through the durable checkpoint and remaining shared
budget, not an unbounded enumeration.

Regular orphan recovery and abandoned-batch rebuild are separate paths. A regular
staged orphan needs the retained cardinality witness, all distinct terminal
partitions, and consumption through the post-terminal horizon; this recovers a
terminal already below a preceding rebuild's floor without treating history as
read progress. An abandoned batch instead requests a rebuild for each observed
commit terminal and keeps its identity until all terminals lie below captured
heads. Its source read is gated by a separate transaction-decision abstraction:
individually installed prepared rows remain invisible until the entire two-key
batch is confirmed. This models the narrow `TxDecisionView` / visible-row seam,
not whole-tree source snapshot isolation, arbitrary row/version metadata, abort,
cross-tree decision routing, or the complete distributed transaction protocol.

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
