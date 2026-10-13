# Multi-partition prepare-horizon refinement

This is a documented abstraction, not a machine-checked refinement proof.
The instance deliberately separates key prepare partitions from terminal
partitions. Reading both shard terminals is not proof that every prepare
partition was consumed. Source installation can precede terminal append.
The detector rows are bounded seam mappings supported by the paired production
controls below. They do not form a machine-checked implementation refinement
or execute the whole finite model in one fixture.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `length`, `installed` | Physical WAL heads and prepared source-row application, distinct from logical decision visibility | `ICommitLogReader.GetHeadOffsetAsync`, source leaf apply path. |
| `wal`, `horizonWal` | The physical identity addressed by heads and consumed offsets | `ViewCheckpointState.BoundPhysicalTreeId`. |
| `read`, `ckpt` | Per-partition consumed position and durable held checkpoint | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewCheckpointState.AppliedOffsets`. |
| `prepares`, `terminals`, `staged`, `cardinalityKnown` | Observed prepare payload indices, distinct shard terminal tally, staging membership, and independent positive batch-size witness | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`, `RecoverOrphanPrepareCardinalityAsync`. |
| `historyRetained`, `historyPrepares`, `historyTerminals`, `historyCursor`, `historyBudgetUsed` | Fixed retention-availability mask, same-physical evidence, resumable scan position, and per-drain examined-entry budget | Bounded `ICommitLogReader.ReadFilteredAsync` through the durable checkpoint and remaining shared budget. |
| `horizon` | Heads captured after a positive size witness and complete distinct terminals, with missing prepares | `ViewMaintainerGrain.DrainAsync`. |
| `abandoned` | Durable identity whose each newly observed commit terminal can trigger another rebuild | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.DrainAsync`. |
| `sourceDecisionCommitted` | External transaction decision/read-visibility seam; never inferred from one terminal or sampled heads | `TxDecisionView`, `SnapshotPendingForReadAsync`, `GetVisibleRawEntriesAsync`. |
| `heads`, `floor`, `planned`, `shadow`, `value` | Rebuild head vector, planned replay floor, shadow and published generation | `ViewMaintainerGrain.CaptureResumeFloorsAsync`, `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.SwapToShadowAsync`. |
| `phase`, `requested`, `recovery`, `crashes`, `rebuilds`, `backstops` | Abstract turn boundaries and finite environment budgets | Modelling devices; no additional persisted production state. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AppendPrepare` | Append the two prepared keys before any terminal | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush`. |
| `InstallSource` | Prepared source rows are installed independently of shard-terminal append, but remain hidden from rebuild reads until the source decision is confirmed | Source leaf apply path, visible-row read seam. | Yes: `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`, `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `ConfirmSourceDecision` | Confirm the complete prepared source batch separately from WAL terminals; visibility is not inferred from terminal arrival | `SnapshotPendingForReadAsync`, `TxDecisionView`, `GetVisibleRawEntriesAsync`. | Yes: `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`, `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `AppendCommit` | Append one distinct shard terminal on each of two partitions | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush`. |
| `Consume` | Stage per-partition entries without advancing the durable checkpoint over them | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint`, `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals`. |
| `HistorySearch` | Examine one bounded retained-history entry at or below the durable checkpoint, resume from the prior cursor, and recover its prepare or terminal witness | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_one_retained_size_witness_recovers_a_batch_with_another_prepare_trimmed`. |
| `ResetHistoryBudget` | Start another drain pass with a fresh examination budget without rewinding the retained-history cursor | `ViewMaintainerGrain.DrainAsync`, `ICommitLogReader.ReadFilteredAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `ViewMaintainerPhysicalStagingTests.Drain_historical_orphan_search_shares_one_budget_across_batches`. |
| `CaptureHorizon` | Sample every physical partition head after a terminal is observed | `ViewMaintainerGrain.DrainAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_incomplete_prepare_without_a_terminal_does_not_capture_recovery_entitlement`, `ViewMaintainerPhysicalStagingTests.Drain_orphan_recovery_waits_for_the_nonterminal_partition_horizon`. |
| `Flush` | Flush only after both prepares and every distinct shard terminal arrived | `ViewMaintainerGrain.IsComplete`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush`. |
| `Recover` | A regular staged orphan may rebuild only after a retained positive size witness, every distinct terminal, and consumption through the captured physical horizon | `OrphanedCommittedBatchAsync`, `RecoverOrphanPrepareCardinalityAsync`, `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_one_retained_size_witness_recovers_a_batch_with_another_prepare_trimmed`, `ViewMaintainerPhysicalStagingTests.Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare`, `ViewMaintainerPhysicalStagingTests.Drain_orphan_recovery_waits_for_the_nonterminal_partition_horizon`. |
| `RecoverAbandoned` | An observed commit terminal on an abandoned batch triggers rebuild without regular orphan history entitlement; a later shard terminal may trigger another rebuild | `AbandonedBatchCommitted`, `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_backstop_identity_survives_one_terminal_and_rebuilds_for_the_later_distinct_shard`, `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |
| `Backstop` | Discard bounded staging while retaining identity for later terminals | `ViewMaintainerGrain.StagingBackstopTripped`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_backstop_identity_survives_one_terminal_and_rebuilds_for_the_later_distinct_shard`, `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`. |
| `RequestRebuild` | Operator rebuild participates in the same replay-floor protocol | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Rebuild_unread_prepares_lower_each_physical_partition_resume_floor`, `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`. |
| `Capture` | Capture the complete per-partition head vector | `ViewMaintainerGrain.CaptureResumeFloorsAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Rebuild_unread_prepares_lower_each_physical_partition_resume_floor`, `ViewMaintainerPhysicalStagingTests.Drain_orphan_recovery_waits_for_the_nonterminal_partition_horizon`. |
| `Plan` | Lower discovered undecided-entry floors; a bounded scan may miss another partition | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Rebuild_unread_prepares_lower_each_physical_partition_resume_floor`, `MaterialisedViewRebuildSplitSagaTests.A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit`. |
| `Scan` | Read only source rows visible through the transaction-decision seam; individually installed but undecided prepared rows are not a valid rebuild image | `ViewMaintainerGrain.BuildShadowAsync`, `GetVisibleRawEntriesAsync`. | Yes: `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`, `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`. |
| `Swap` | Publish the scanned generation and floor; retire abandoned identity only after all shard terminals were captured | `ViewMaintainerGrain.SwapToShadowAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_backstop_identity_survives_one_terminal_and_rebuilds_for_the_later_distinct_shard`, `MaterialisedViewShadowSwapTests.Rebuild_converges_after_a_simulated_crash_before_swap`. |
| `Crash` | Lose volatile staging/horizon and replay from held-back offsets; retry interrupted rebuild | `ViewMaintainerGrain.DrainAsync`, `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals`, `MaterialisedViewShadowSwapTests.Rebuild_converges_after_a_simulated_crash_before_swap`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `HorizonAfterTerminal` | Missing-prepare detection samples heads only after a terminal | Yes: `ViewMaintainerPhysicalStagingTests.Drain_incomplete_prepare_without_a_terminal_does_not_capture_recovery_entitlement`. |
| `HorizonRequiresHistoryEntitlement` | One positive size witness from an actual retained prepare and every distinct terminal are both required; a terminal, head, or retention gap cannot establish cardinality | Yes: `ViewMaintainerPhysicalStagingTests.Drain_one_retained_size_witness_recovers_a_batch_with_another_prepare_trimmed`, `ViewMaintainerPhysicalStagingTests.Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare`, `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_history_without_a_prepare_witness_keeps_waiting`. |
| `HistorySearchProgress` | A bounded retained-history pass advances rather than repeating the same record | Yes: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`. |
| `CheckpointHeld` | An unresolved entry remains above its durable resume offset | Yes: `ViewMaintainerPhysicalStagingTests.Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint`, `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals`. |
| `QuiescentExact` | At a committed, fully consumed source head the view contains both source keys | Yes: `MaterialisedViewRebuildSplitSagaTests.A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit`, `ViewMaintainerPhysicalStagingTests.Drain_one_retained_size_witness_recovers_a_batch_with_another_prepare_trimmed`. |
| `PublishedBatchAtomic` | In the bounded two-key case, decision-visible source rows and a complete staged batch publish the confirmed slice, not individually undecided prepared rows | Yes: `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`, `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush`. |
| `SourceDecisionPrerequisite` | Prepared-row visibility follows the recorded decision rather than a leaf terminal; complete installation in the model is an explicit producer rely, not terminal-derived decision validation | Yes: `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`, `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `NoPrematureFlush` | One shard commit cannot flush a multi-shard batch | Yes: `ViewMaintainerPhysicalStagingTests.Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush`. |
| `NoPrematureRetirement` | Rebuild/backstop preserve abandoned identity until every shard's terminal lies below the captured heads | Yes: `ViewMaintainerPhysicalStagingTests.Drain_backstop_identity_survives_one_terminal_and_rebuilds_for_the_later_distinct_shard`. |
| `RecoveryEntitled` | Orphan recovery consumes every partition below the post-terminal horizon, not just terminal-bearing partitions | Yes: `ViewMaintainerPhysicalStagingTests.Drain_orphan_recovery_waits_for_the_nonterminal_partition_horizon`. |
| `RebuildReplayEntitled` | Undecided entries discovered below the heads remain in the resumed replay window | Yes: `ViewMaintainerPhysicalStagingTests.Rebuild_unread_prepares_lower_each_physical_partition_resume_floor`, `ViewMaintainerPhysicalStagingTests.Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals`. |
| `Converges` | Eligible staged or abandoned committed batches eventually reach the view; abandoned rebuilds need no retained-history witness, while a complete retained-history cardinality gap is explicitly excluded | Yes: `ViewMaintainerPhysicalStagingTests.Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes`, `MaterialisedViewRebuildSplitSagaTests.A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits`, `MaterialisedViewRebuildSplitSagaTests.An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain constraint, not a production behaviour. |

## Evidence and limits

### Production control census

The owned detector exports are `0a4bbd830` (retained history), `11e9344d9`
(exact interrupted shadow, #4906) and `c8c789a7e` (Horizon lifecycle seams).
Production mutations were temporary edits to the actual callers or guards;
all were restored, with no runtime change in these detector exports.
Artifacts are TRX files under this session's `files/horizon-acceptance/`.
The restored exact core filter combines `ViewMaintainerPhysicalStagingTests`
and `BPlusLeafGrainTests.GetVisibleRawEntries`: 45 executed/passed (33 physical,
12 logical-row cases), zero skipped/inconclusive/not-executed. The exact live
filter executes five split-saga cases, held-checkpoint replay, interrupted shadow
retry, and three held-leaf-terminal decision cases: 10 executed/passed.

| Artifact | Actual source perturbation | Executed outcome and discriminating assertion |
|----------|----------------------------|----------------------------------------------|
| `disabled-orphan-caller.trx` | Disable the actual drain orphan-recovery caller | 6 failed: no bounded history search or no recovered generation. |
| `terminal-infers-size.trx` | Infer positive size from terminals/head | 1 failed: fresh terminal-only log rebuilds before its own late prepare. |
| `incomplete-terminal-guard-removed.trx` | Remove complete distinct terminal membership | 2 failed: incomplete/duplicate shard evidence retires the orphan. |
| `unread-partition-recovered.trx` | Ignore nonterminal partition consumption | 1 failed: third partition below its captured horizon still rebuilds. |
| `history-cursor-rewinds.trx` | Restart history at -1 every pass | 4 failed: budgets 1/2 cannot reach the retained cardinality witness. |
| `per-batch-budget-reset.trx` | Reset the shared examined-entry budget for each batch | 1 failed: the pass examines four records instead of one. |
| `preterminal-horizon-recovery.trx` | Remove the actual terminal-seen orphan guard | 1 failed: missing prepare without any terminal advances generation. |
| `disabled-discovered-floor-direct.trx` | Disable discovered-entry replay-floor lowering | 1 failed: published p0/p2 floors skip their unread prepares. |
| `disabled-checkpoint-holdback-reactivation.trx` | Remove drain's durable holdback caller | 2 failed: durable offsets and fresh-activation reads pass staged prepares. |
| `one-terminal-flush.trx` | Treat first shard terminal as complete | 1 failed: two-key atomic write occurs before the second distinct terminal. |
| `one-terminal-retirement-direct.trx` | Treat first shard terminal as decided during rebuild planning | 1 failed: abandoned identity is removed before the second distinct terminal. |
| `disabled-abandoned-caller.trx` | Disable the actual abandoned-commit rebuild caller | 2 failed: the real late committed rows remain absent. |
| `logical-dbt-raw-metadata.trx` | Bypass prepared rows in the logical metadata API | 3 failed: materialised HLC50/100 replaces committed pending HLC200, including deletion metadata. |
| `logical-dbt-raw-caller.trx` | Use raw rather than logical rows at actual source-confirmation caller | 3 failed: source logical assertions pass but the view does not catch up in its bounded drain loop. |
| `interrupted-shadow-not-cleared-exact.trx` | Omit actual shadow clearing before retry | 1 failed: exact reused generation publishes orphan and count3 instead of count2. |

Every listed red outcome is an assertion failure with error/timeout/inconclusive/
not-executed counters zero, not a compilation failure or a runner timeout. The
source-confirmation caller control demonstrates bounded convergence failure,
not a sampled partial reader snapshot; the separate metadata control fails
before maintainer catch-up. The crash test seeds an interrupted shadow rather
than killing a process at an await, and now asserts the retry targets that exact
storage-safe generation. Its old `#g1` target was an unrelated legacy tree:
`interrupted-shadow-not-cleared.trx` survived 1/1 with clearing omitted and is
not accepted evidence. Likewise, the three split-saga output controls in
`disabled-discovered-floor.trx` and the one in `one-terminal-retirement.trx`
survived because orphan recovery healed their outputs. The new direct state
assertions discriminate these narrower protocol properties instead.

The first four historical baseline failures were a mock setup gap after the
physical-root source-read path changed; they are not runtime fault evidence.
Only the corrected physical-root harness, genuine red pairs and restored
results are admitted. Positive and negative controls cover one retained size
witness with another prepare absent, history below checkpoint on another
partition, a fresh terminal-only physical log, duplicate shard observations,
all-partition consumed horizons, bounded resumable cursors and global budgets.
Abandoned-identity preservation is a no-premature-retirement claim: if an older
terminal is already below the planning window, identity may remain until a later
complete captured terminal set, eviction or physical scope change. No immediate
retirement after arbitrary split deliveries is claimed.

The exact structural/acceptance filter is
`FullyQualifiedName~Formal&FullyQualifiedName!~TlcModelCheckTests&Name~MaterialisedViewHorizon`:
53 executed/passed. The exact TLC filter is
`FullyQualifiedName~TlcModelCheckTests&Name~MaterialisedViewHorizon`:
25 executed/passed, 113.521 seconds actual harness wall time with one JVM,
2 GiB heap ceiling and two visible CPUs. All census counters for failed,
skipped/inconclusive/not-executed are zero in these restored runs. The model and
case count are unchanged; the integration owner retains shard-weight ownership.
The earlier independent raw base/FullScan measurements remain 16,792/29,932
states and 7.180/8.645 seconds respectively, not this run's harness duration.

The stronger producer decision-before-any-terminal rely has separate parent
controls: `AtomicWriteGrainTests.Commit_terminal_waits_for_the_durable_decision_response`
holds the real `MarkCommittedAsync` reply on normal Execute and cross-tree
Finalize. Removing the await independently kills the respective case; the
restored exact parent scope executes five green tests. This records only
ordering of durable confirmation and terminal append, not a proof of the
complete distributed producer. The Horizon model still deliberately permits
its relaxed first-terminal ordering. Complete two-key installation is an
explicit source-environment condition, not a validation inferred from any
terminal or sampled head. Published-batch evidence composes bounded logical-row
selection, whole atomic flush and the held-terminal reader snapshots; it does
not establish transactional sampling across an arbitrary whole-tree scan.

### Bounded abstraction

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
distribution or every retention gap. The fixed retained-history mask exercises
one missing prepare and a complete cardinality-witness gap, but not arbitrary
retention changes during the bounded read or physical-log rebind. Recovery is
not claimed when every positive cardinality witness is gone. `Scan` abstracts a completed
source scan as one action; generation visibility and trim/rebind are the tail
model's. There is one transaction, no abort, no ordinary competing write, no
aggregation fold, and no cross-tree coordinator. Those interactions belong to
companion models. Fair source completion and fair maintenance follow finite
faults; unbounded crashes or a permanently unavailable source are not
convergence claims. The production detector rows name seam tests, not an
automatic refinement proof or crash injection at every await.
