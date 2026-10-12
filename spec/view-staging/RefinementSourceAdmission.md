# Coherent row/version admission refinement

This finite model documents required admission semantics. It is not proof
that the pending source-confirmation or joint-publication candidates implement
them. All detector verdicts remain explicitly Pending.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `decision`, `installed`, `appended` | Durable decision, readable source installation and independently arriving terminals | `ViewMaintainerGrain.HandleStagingEntry`; the source protocol steps are an explicit environment, not the same event. |
| `read`, `prepares`, `terminals`, `ckpt` | Subscription completeness and held durable progress | `ViewMaintainerGrain.IsComplete`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`, `ViewCheckpointState.AppliedOffsets`. |
| `source`, `sampled`, `classes`, `horizon` | Whole raw row identity, admission classification and post-observation head | Candidate coherent source-confirmation path; timestamp/kind/value come from one observed row, not independent reads. |
| `applied`, `dropped`, `value` | Actual replacement eligibility, skipped upstream event and projected row | `ViewMaintainerGrain.IsSupersededByOrdinary`, `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate dropped-superseder gate still pending. |
| `registered` | Complete expected-participant readiness evidence | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`; peer-drain/certificate internals are abstracted. |
| `done`, `history` | Durable idempotent effect or committed history revisions | `ViewMaintainerGrain.MarkResolved`, `HistoryLatticeViewProjection`. |
| `phase`, `accumulative`, `crashes` | Maintainer turn, registration branch and finite failures | `ViewRegistration.Accumulative`; turn and fault counters are modelling devices. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Decide` | Commit before announcing any terminal | Source atomic decision, not maintainer terminal presence. | Pending: no accepted detector for the model's source decision/append interleaving. |
| `SupersedeTwin` | Fair later source write corrects a contradictory same-clock tombstone tuple | Source environment step, not a claim of normal revision reuse. | Pending: no accepted detector for this adversarial tuple correction. |
| `Install` | Source becomes readable independently of terminal delivery | Candidate source-confirmation path. | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `AppendTerminal` | Deliver each distinct shard terminal separately | `ViewMaintainerGrain.HandleStagingEntry`. | Pending: proposed detector `MaterialisedViewRebuildSplitSagaTests.A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard`. |
| `Read` | Stage each partition; dropped ordinary progress is not an applied replacement | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsSupersededByOrdinary`. | Pending: proposed detector `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write`. |
| `Confirm` | Compare one coherent timestamp/kind/value tuple against the prepare | Candidate coherent raw-row source read; no synthetic projection mutation. | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `Retry` | Defer the whole batch and continue reading/rechecking | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; candidate gates retain whole batch identity. | Pending: proposed detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |
| `Repair` | Publish the real replacement before using it to discharge omission | Candidate dropped-superseder admission, not projecting an unseen source sample. | Pending: proposed detector `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write`. |
| `Register` | Register only an eligible local slice or a ready expected peer | `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Pending: existing coordinator tests are not accepted proof of the pending joint-visibility repair. |
| `Publish` | Whole-batch admission, all expected participants, one atomic local effect | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`, `ViewCrossTreeCoordinatorGrain.RegisterReadyAsync`. | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`; cross-tree visibility evidence remains unresolved. |
| `Checkpoint` | No durable advancement over a deferred batch | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Crash` | Retain backing effects/history while discarding volatile admission | `ViewMaintainerGrain.ArmReplayFence`, `HistoryLatticeViewProjection`. | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CheckpointHeld` | A wholly deferred batch remains replayable | Pending: proposed detector `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `TerminalSound` | A terminal is not a substitute for its prior committed decision | Pending: source decision/terminal discrimination is not accepted here. |
| `HistoryRetained` | Accumulative registrations retain every revision, bypassing current-state U/T filtering | Pending: accepted history bypass evidence exists, but this compound admission/history mapping remains unaccepted. |
| `VersionIdentity` | Identity compares clock, bytes and tombstone kind, not byte equality | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`; same-clock tombstone-kind control is formal-only. |
| `RegistrationSound` | An expected participant cannot register before its local eligibility evidence | Pending: cross-tree candidate source admission remains unresolved. |
| `SampleCoherent` | The payload and version belong to the same observed raw row | Pending: coherent-row prerequisite has isolated evidence, not accepted completed-admission refinement. |
| `ReplacementEntitled` | Consumed/dropped ordinary progress never falsely proves an applied omission replacement | Pending: proposed detector `ViewMaintainerReplayFlushTests.A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write`. |
| `AdmissionComplete` | Absent, older or same-version nonmatching rows defer the entire batch even after terminal heads are consumed | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_waits_for_absent_or_older_rows_even_after_the_terminal_horizon_is_consumed`. |
| `ParticipantsClosed` | Local completion never substitutes for all expected participant registrations | Pending: coordinator memoization and per-view replay are not proof of joint visibility. |
| `ProjectionMonotone` | Timestamp-sensitive projection cannot regress to an equal-byte older prepared version | Pending: proposed detector `ViewMaintainerReplayFlushTests.Source_confirmation_distinguishes_equal_bytes_at_different_versions_in_every_reader_snapshot`. |
| `Converges` | Fair repair/source correction and rechecking complete after finitely many crashes | Pending: proposed detector `ViewMaintainerReplayFlushTests.A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain check, not a production behaviour. |

## Evidence and limits

The accepted coherent-row prerequisite is source child 82b6e8f (parent 4aee14196):
IShardRootGrain.GetRawEntriesIncludingTombstonesAsync returns aligned raw
timestamp/kind/value tuples and GetRawVersionsAsync retains tombstone versions.
This establishes single-row coherence only. Sequential multi-leaf traversal is
not transaction-atomic sampling, participant closure, or admission acceptance.
`SampleCoherent` remains Pending because its finite vector observation is stronger
than the accepted seam alone; completing its refinement requires the constrained
source evolution and admission detectors, not merely the raw-row tests.

The model includes current/history registration branches at base bounds.
The coherent row domain distinguishes equal bytes at stamps20/50 and an empty
live revision from a tombstone. A same-clock contradictory kind is a deliberately
adversarial read result; it must not be mistaken for the prepare and must later
be corrected for convergence. No claim is made that legitimate writes reuse
the same full version identity.

Source installation is not derived from terminal observation. A sample that
proves a newer source revision authorizes whole-batch deferral, not inventing
that revision's projection. Omission requires an actual durable replacement,
all captured heads consumed, and the expected participant set closed. The
external peer is a readiness certificate; the joint backend write and every
view's concurrent public read are not expanded, so simultaneous cross-view
visibility is outside the model's claim.

The maintainer read/repair excludes interleaving with its own sampled publication
turn; source installation/correction can still progress. Strong fairness on
read/repair rules out starvation by infinitely retried confirmation turns.
History bypass is intentional and does not weaken the current-state branch.
ReadyClosure supplies connected per-key folding and single-slice publication;
Rebind/Fence/Horizon supply separate physical-identity and recovery obligations.
