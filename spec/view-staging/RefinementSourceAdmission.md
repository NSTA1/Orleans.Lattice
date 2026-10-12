# Coherent row/version admission refinement

This finite model documents required admission semantics. It is not proof
that the pending source-confirmation or joint-publication candidates implement
them. Maintainer admission verdicts remain explicitly Pending. The separately
accepted decision-before-terminal producer rows are a rely-guarantee, not a
claim that the maintainer validates fabricated terminal records.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `decision`, `installed`, `appended` | Durable decision, readable source installation and independently arriving terminals | `ViewMaintainerGrain.HandleStagingEntry`; the source protocol steps are an explicit environment, not the same event. |
| `read`, `prepares`, `terminals`, `ckpt` | Subscription completeness and held durable progress | `ViewMaintainerGrain.IsComplete`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`, `ViewCheckpointState.AppliedOffsets`. |
| `source`, `sampled`, `classes`, `horizon` | Materialised versus observed logical row identity, admission classification and post-observation head | Candidate `GetVisibleRawEntriesAsync`; timestamp/kind/value come from one observed row, not independent reads. |
| `applied`, `dropped`, `value` | Actual replacement eligibility, skipped upstream event and projected row | `ViewMaintainerGrain.IsSupersededByOrdinary`, `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate dropped-superseder gate still pending. |
| `pending`, `pendingCommitted`, `samplePending`, `sampleDecision` | Undrained B prepare, commit decision and captured observation witness | Candidate `SnapshotPendingForReadAsync` / `TxDecisionView` followed by per-row visibility resolution; not a source-wide snapshot. |
| `registered` | Complete expected-participant readiness evidence | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`; peer-drain/certificate internals are abstracted. |
| `done`, `history` | Durable idempotent effect or committed history revisions | `ViewMaintainerGrain.MarkResolved`, `HistoryLatticeViewProjection`. |
| `phase`, `accumulative`, `crashes` | Maintainer turn, registration branch and finite failures | `ViewRegistration.Accumulative`; turn and fault counters are modelling devices. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Decide` | Commit before announcing any terminal | `AtomicWriteGrain.RecordTerminalDecisionAsync`; source-producer rely-guarantee, not maintainer terminal presence. | Yes: `AtomicWriteGrainTests.Commit_terminal_waits_for_the_durable_decision_response` holds the decision response at both normal and cross-tree producer entrypoints. |
| `SupersedeTwin` | Fair later source write corrects a contradictory same-clock tombstone tuple | Source environment step, not a claim of normal revision reuse. | Yes: `ViewMaintainerReplayFlushTests.Source_confirmation_defers_same_HLC_contradictory_tombstone_kind_as_a_whole_batch` first defers both contradictory deletion-kind cases, then installs the matching tuple and observes complete publication. |
| `Install` | Source becomes readable independently of terminal delivery | Candidate source-confirmation path. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `AppendTerminal` | The producer may announce a terminal only after its decision completes; shard delivery remains independently scheduled | `AtomicWriteGrain.BroadcastTerminalsAsync`, `ShardRootGrain.AppendTxTerminalAsync`. | Yes: `AtomicWriteGrainTests.Commit_terminal_waits_for_the_durable_decision_response`, `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals` and `AtomicWriteGrainTests.FinalizeAsync_records_the_decision_before_broadcasting_terminals`. |
| `Read` | Stage each partition; dropped ordinary progress is not an applied replacement | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsSupersededByOrdinary`. | Yes: `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write` consumes a superseded ordinary write while its actual newer replacement remains beyond the consumed window. Disabling the production dropped-superseder wait publishes the other batch key alone and fails the snapshot assertion. |
| `Confirm` | Compare one coherent logical tuple and captured pending-decision witness against the prepare | Candidate `GetVisibleRawEntriesAsync`; committed undrained metadata can supersede an older coherent materialised row, without a synthetic projection mutation. | Pending: existing regression detectors `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_value_and_version_come_from_one_row` and `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`. |
| `Retry` | Defer the whole batch and continue reading/rechecking | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate gates retain whole batch identity. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |
| `Repair` | Publish the real replacement before using it to discharge omission | `ViewMaintainerGrain.GateBatchesOnUnappliedReplacements`, not projecting an unseen source sample. | Yes: `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write` holds publication until the actual newer ordinary WAL event is consumed and applied, then verifies both the replacement and the previously deferred batch key. The actual wait-disable mutation fails; the restored detector passes. |
| `Register` | Register only an eligible local slice or a ready expected peer | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Pending: existing coordinator tests are not accepted proof of the pending joint-visibility repair. |
| `Publish` | Whole-batch admission, all expected participants, one atomic local effect | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`, `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`; cross-tree visibility evidence remains unresolved. |
| `Checkpoint` | No durable advancement over a deferred batch | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Pending: existing regression detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Crash` | Retain backing effects/history while discarding volatile admission | `ViewMaintainerGrain.ArmReplayFence`, `HistoryLatticeViewProjection`. | Pending: existing regression detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `CommitPending` | B becomes logically committed while its materialised row can still lag | Captured pending metadata plus resolved `TxDecisionView`; source environment decision, not terminal observation. | Pending: existing regression detector `BPlusLeafGrainTests.GetVisibleRawEntries_pending_decision_controls_the_entire_row`. |
| `MaterialisePending` | Install committed B before retiring its pending metadata | Source pending-prepare materialisation; logical visibility must survive retirement. | Yes: `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_value_survives_terminal_materialisation` covers equal/different payloads and `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_tombstone_survives_terminal_materialisation` covers deletion. Replacing the actual commit terminal's `ApplyTxCommit` installation with `ApplyTxAbort` retirement makes all three positives fail; restoration and rebuilding pass. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CheckpointHeld` | A wholly deferred batch remains replayable | Pending: existing regression detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `TerminalSound` | A terminal is not a substitute for its prior committed decision; the source producer awaits that decision before broadcasting | Yes: `AtomicWriteGrainTests.Commit_terminal_waits_for_the_durable_decision_response`; removing the decision await independently at either commit producer fails its held-response case. This is not maintainer rejection of a fabricated terminal. |
| `HistoryRetained` | Accumulative registrations retain every revision, bypassing current-state U/T filtering | Yes: both `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` cases retain ordinary and overlapping committed revisions across lost-response replay rather than only current winners. The captured-floor public-history scenario `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` verifies later ordinary and committed revisions survive source confirmation, local atomic publication, and maintainer reactivation. A temporary production mutation that skipped unprepared mutations for accumulative registrations made the positive public-history detector fail on its missing post-floor ordinary revision; after restoring production and rebuilding, it passed. The contract is limited to revisions still available after the captured rebuild floor. |
| `VersionIdentity` | Identity compares clock, bytes and tombstone kind, not byte equality | Yes: `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot` distinguishes equal payloads at HLC20/50, with and without a lost publication response. Removing the actual source-tuple timestamp comparison makes both cases publish HLC20 and fail. `ViewMaintainerReplayFlushTests.Source_confirmation_defers_same_HLC_contradictory_tombstone_kind_as_a_whole_batch` covers both contradictory deletion-kind cases; accepting the unconfirmed branch makes both fail. |
| `RegistrationSound` | An expected participant cannot register before its local eligibility evidence | Pending: cross-tree candidate source admission remains unresolved. |
| `SampleCoherent` | Logical payload/version and pending metadata/decision witness come from the same bounded capture | Pending: existing regression detectors `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_value_and_version_come_from_one_row` and `BPlusLeafGrainTests.GetVisibleRawEntries_root_preserves_pending_row_alignment_and_logical_versions`; the accepted raw-row prerequisite alone is insufficient. |
| `ReplacementEntitled` | Consumed/dropped ordinary progress never falsely proves an applied omission replacement | Yes: `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write` rejects every partial publication before the real replacement event arrives. Making the actual `WaitsOnDroppedSuperseder` branch always return false exposes a j-only snapshot and fails; restoration and rebuilding pass. Staged replacement dependencies retain their separate ready-closure obligation. |
| `AdmissionComplete` | Absent, older or same-version nonmatching rows defer the entire batch even after terminal heads are consumed | Yes: `ViewMaintainerReplayFlushTests.Source_confirmation_waits_for_absent_or_older_rows_even_after_the_terminal_horizon_is_consumed` and `ViewMaintainerReplayFlushTests.Source_confirmation_defers_same_HLC_contradictory_tombstone_kind_as_a_whole_batch` hold the whole batch and checkpoint until the exact source tuple is installed. Returning true for the actual unconfirmed admission branch makes all four cases fail. |
| `ParticipantsClosed` | Local completion never substitutes for all expected participant registrations | Pending: coordinator memoization and per-view replay are not proof of joint visibility. |
| `ProjectionMonotone` | Timestamp-sensitive projection cannot regress to an equal-byte older prepared version | Yes: both `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot` cases inspect every captured publication. Removing the production timestamp identity comparison exposes HLC20 after HLC50 and fails both normal and lost-response cases; restoring it and rebuilding passes. |
| `DecisionEntitled` | A captured committed pending winner cannot be ignored in favour of an older coherent materialised prepare | Pending: existing regression detector `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision`; raw-only confirmation fails this property. |
| `LogicalVisibilityMonotone` | Retiring committed pending metadata cannot reveal an older materialised revision | Yes: `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_value_survives_terminal_materialisation` and `BPlusLeafGrainTests.GetVisibleRawEntries_committed_pending_tombstone_survives_terminal_materialisation` preserve payload, HLC and deletion kind across the real terminal path. The actual install-to-abort mutation fails all three positives; the restored positives plus `BPlusLeafGrainTests.GetVisibleRawEntries_detector_rejects_terminal_retirement_without_installation` pass. This is the per-leaf installation/retirement contract, not a source-wide snapshot. |
| `Converges` | Fair pending commit/materialisation, real replacement repair and rechecking complete after finitely many crashes | Pending: existing regression detectors `MaterialisedViewDecisionBeforeTerminalTests.Committed_batch_logged_before_its_leaves_drain_confirms_against_the_decision` and `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain check, not a production behaviour. |

## Evidence and limits

The accepted coherent-row prerequisite is source child 82b6e8f (parent 4aee14196):
IShardRootGrain.GetRawEntriesIncludingTombstonesAsync returns aligned raw
timestamp/kind/value tuples and GetRawVersionsAsync retains tombstone versions.
This establishes single-row materialised coherence only. Sequential multi-leaf
traversal is not transaction-atomic sampling, participant closure or admission
acceptance. The logical-row API is a distinct seam: one bounded leaf
metadata capture carries pending keys and decision outcomes together; the
resolved committed prepare may be newer than the coherent materialised row.
The logical-row API implementation reference is source child
466494d1da5fbd3a3c965322982eba1a03c55b29 (parent 92008013b). The parent accepted
its narrow logical/raw/bootstrap scope, not completed U/T or held-terminal
composition. That prerequisite is not an unqualified admission verdict.

The source producer's decision-before-terminal rely-guarantee is separately
accepted. The normal saga and cross-tree finalizer are each held at the actual
registry decision response; neither may broadcast a terminal or finish while
that response is unresolved. Removing the decision await at either call site
independently fails its held-response case while the other case passes, and
the restored two cases plus three established ordering controls pass. This
does not establish maintainer validation of fabricated terminal records.

The base includes current/history registrations, source application lag and an
undrained committed/in-flight B prepare. Original A is committed and installed
in that latter prefix, but its WAL terminals still arrive independently. B's
commit decision and materialisation are separate actions; only a captured
committed witness requires B's newer logical row at that observation. B can
commit after an earlier in-flight sample: no freshness or source-wide snapshot
promise is inferred. The only contended overlay row is key1; key2 is stable.
The materialised alternatives distinguish equal bytes at stamps20/50 and an
empty live revision from a tombstone. A same-clock contradictory kind is an
adversarial read result, not a claim that legitimate writes reuse identity.

The overlay abstracts only in-flight and committed decisions. Aborted,
indeterminate, unavailable-resolution, expiry and already-terminal/orphan
visibility rules are not expanded. In particular, the production contract must
hide indeterminate rows, propagate unavailable resolution failures and retain
committed tombstone/expired metadata; those are not certified by this model.
The named tombstone/expiry unit detector is supplementary source-seam evidence,
not a modeled overlay branch. History checks original A's revision retention
despite B supersession, not the complete processing of B's history revisions.

Source installation is not derived from terminal observation. A newer logical
row authorizes whole-A deferral, never substitution or invented projection.
Omission requires an actual durable replacement, all captured heads consumed
and the expected participant set closed. `Read`/`Repair` abstract delivery and
actual publication of a known replacement event, not copying `VisibleSource`:
when B is atomic, its own decision, participant readiness and local publication
closure are supplied by ReadyClosure's rely-guarantee contract. Dropped progress
alone cannot establish that effect. The external peer is a readiness certificate;
its joint backend write and every view's concurrent public read are not expanded,
so simultaneous cross-view visibility is outside the claim.

The maintainer excludes interleaving with its own sampled publication turn;
source installation/correction and B's decision/materialisation can still
progress. Strong fairness on read/repair rules out starvation by repeated
confirmation turns. History bypass is intentional. ReadyClosure supplies
connected per-key folding and single-slice publication; Rebind, Fence, Horizon
and ResolvedIdentity supply separate physical-identity and recovery obligations.
