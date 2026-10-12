# MaterialisedViewAggregation refinement

This is a documented abstraction, not a machine-checked refinement proof.
It composes the numeric membership engine with the staging/source-admission
contract and a separate accumulative revision set. HLC is chosen independently
of WAL order: the instance checks every permutation of both.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `stamps`, `log`, `outcome`, `source` | Independent source versions, WAL arrivals and source decision | `LatticeMutation.Timestamp`, `ViewMaintainerGrain.HandleStagingEntry`. |
| `read`, `checkpoint`, `staged` | Volatile staging and durable resume floor | `ViewMaintainerGrain.ApplyCheckpointHoldBack`, `ViewCheckpointState.AppliedOffsets`. |
| `members`, `counts`, `sums` | Numeric membership plus count/sum slots | `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.RetractNumericAsync`. |
| `materialised`, `dirty` | Published group values and the set of unfinished group writes | `AggregationApplier.MaterialiseAccumulatorAsync`, `AggregationRowCodec.NumericMaterialisationKey`. |
| `history`, `seen`, `seed` | Revision-keyed accumulative projection and rebuild seed | `HistoryLatticeViewProjection.Project`, `ViewMaintainerGrain.BuildShadowAsync`. |
| `rebuilding`, `shadow`, `floor`, `rebuilt` | A bounded shadow scan and resume capture | `ViewMaintainerGrain.CaptureResumeFloorsAsync`, `ViewMaintainerGrain.SwapToShadowAsync`. |
| `crashes`, `omissionSafe` | Failure budget and omission visibility witness | Modelling devices, not production fields. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AppendOrdinary` | Source ordinary write with independent HLC | `ILattice.SetAsync`. | Yes: `MaterialisedViewIntegrationTests.View_update_wins_by_hlc`. |
| `AppendPrepare` | Buffer an invisible source prepare | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`. |
| `Decide` | Source commits or aborts the batch | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `ReadOrdinary` | Retract prior membership and add replacement | `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.RetractNumericAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`. |
| `Stage` | Read prepares without folding them | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `MaterialisedViewAtomicVisibilityTests.Prepared_but_uncommitted_batch_is_not_visible_in_the_view`. |
| `Flush` | Fold the completed numeric batch; accumulative revisions are not source-latest filtered | `ViewMaintainerGrain.FlushCompletedAggregationBatchesAsync`. | Yes: `MaterialisedViewAggregationStagingOverlapOrderTests.Omitted_batch_member_flushes_only_after_its_newer_ordinary_replacement_is_visible` observes the replacement's actual regroup before source terminals complete and checks the surviving numeric closure through public `GetManyAsync`; `MaterialisedViewAggregationStagingOverlapOrderTests.Public_snapshot_detector_rejects_split_omitted_member_numeric_publication` deliberately splits the actual outgoing publication and rejects the mixed result. The two `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` cases cover the separate accumulative branch. |
| `Materialise` | Publish each touched group from its actual numeric slots before retiring the durable intent | `AggregationApplier.CompletePendingNumericMaterialisationAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`. |
| `Checkpoint` | Bank only resolved, materialised work | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Yes: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `Crash` | Lose staging but retain affected-group obligations and replay the durable floor | `AggregationApplier.CompletePendingNumericMaterialisationAsync`, `ViewMaintainerGrain.DrainAggregationAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `AggregationMembershipAtomicityTests.ApplyAsync_absent_member_retry_completes_a_failed_retraction_materialisation`. |
| `StartRebuild` | Capture before scanning and before discarding staging | `ViewMaintainerGrain.CaptureResumeFloorsAsync`, `ViewMaintainerGrain.BuildShadowAsync`. | Yes: `MaterialisedViewAtomicVisibilityTests.Bounded_staging_buffer_forces_a_rebuild_and_the_view_still_converges`. |
| `InstallRebuild` | Publish shadow membership, slots, group rows and resume floor together | `ViewMaintainerGrain.SwapToShadowAsync`. | Yes: `MaterialisedViewShadowSwapTests.Aggregation_rebuild_and_reconcile_preserve_and_repair_group_values`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `MembershipAccumulatorAtomic` | Every observed internal numeric slot agrees with membership, including regroup, retract and lost responses | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`. |
| `SourceMatchesDecision` | Source LWW winner depends on prepared/ordinary HLC rather than arrival order | Yes: `MaterialisedViewAggregationStagingOverlapOrderTests.An_older_batch_completing_after_a_newer_batch_does_not_regroup_the_source_key`. |
| `NoNegativeCounts` | Replay cannot double-retract a numeric member | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`. |
| `HistoryKeepsSuperseded` | Revision projection retains superseded Set/Delete revisions within its generation | Yes: `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` exercises real maintainer admission with normal and lost-response replay; `HistoryLatticeViewProjectionTests.Project_delete_emits_delete_revision_not_view_delete` covers Delete row identity. |
| `NoAbortedContribution` | Aborted prepared contributions never enter membership or revisions | Yes: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `CheckpointRetainsStage` | Resume never skips an unresolved prepared contribution | Yes: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `MaterialisedWhenClean` | Once every touched group is repaired, public totals match internal slots; intent deletion follows those writes | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`. |
| `OmittedReplacementAlreadyVisible` | An omitted prepared member's replacing contribution is already applied before the remaining batch can flush | Yes: `MaterialisedViewAggregationStagingOverlapOrderTests.Omitted_batch_member_flushes_only_after_its_newer_ordinary_replacement_is_visible` stages a three-member Sum batch, consumes its newer ordinary replacement on the same key partition while terminals are absent, and observes blue7 with the old red group absent before either surviving green/yellow member can publish. Its paired public split-publication control rejects a partial surviving closure. The corrected positive, control and existing regroup case pass together; exact WAL ranges distinguish source HLC from partition progress. |
| `AggregateConverges` | Quiescent same-source numeric replay and rebuild repair every affected group, including the forgotten old group | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `MaterialisedViewShadowSwapTests.Aggregation_rebuild_and_reconcile_preserve_and_repair_group_values`. |
| `HistoryConverges` | Fair draining retains every available ordinary and committed-batch revision after the captured rebuild floor, including superseded revisions | Yes: `MaterialisedViewHistoryRebuildCompositionTests.Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation` rebuilds at an explicit floor, appends ordinary and overlapping committed revisions, loses the response after the atomic history rows commit, reactivates the maintainer, and checks public history plus the current source winner. The two `ViewMaintainerHistoryAdmissionTests.Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay` cases cover independent clocks and replay. A temporary production mutation that skipped unprepared mutations for accumulative registrations made this positive detector fail with the post-floor ordinary HLC/value missing; restoring production and rebuilding made it pass. This proves the available post-floor suffix only; pre-floor and already-trimmed revisions are not recoverable. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Composition contracts and limits

Numeric admission is a source-confirmed replacement, not an append-only fold.
The main staging/visibility model owns source raw-version confirmation, terminal
horizons, ordinary/range supersession and joint cross-tree readiness. The
`ReplacementVisible` guard is indispensable: dropping a prepared member because
the source holds a newer value does not mean that value is visible in the view.
`omissionSafe` records the pre-flush observation, so its mutation detects the
intermediate failure rather than merely checking terminal convergence.

Count and sum update by retract/add deltas. Their invariant is checked at every
state, not just when materialisation or draining finishes. `dirty` is the set of
unfinished group writes; each group publishes separately. Issue #4890 exposed
production losing the old group after a committed regroup or retract with a lost
response. The correction persists a bounded one/two-group intent atomically with
membership and slots, completes prior intent before the next same-source numeric
apply or retract (including a rejected regroup), and retires it only after all
public writes succeed. Failed retirement preserves the intent; a lost committed
delete response is safe because publication already finished. Intent retirement
itself is a stutter here once all group writes finish. The protocol does not
retroactively reconstruct identities already forgotten before this correction.
Maintainer composition is now independently covered: a bounded pass-local
source-key set invokes `CompletePendingNumericSourceAsync` before checkpoint
retirement even when coherent source admission suppresses an interrupted event
and its newer source event projects no contribution. The Count/Sum
`MaterialisedViewNumericIntentCompositionTests` runtime cases lose the response
only after the real membership/intent atomic flip commits. Their single public
`GetManyAsync` observes repaired old/new groups and the absent intent, and a
follow-up drain checks no duplicate effect. Both were red before the caller
correction and green afterward; parent composition at `6dc13090c` passed
196 core and 45 real runtime cases with zero skipped/inconclusive. The direct
applier evidence alone did not certify this wiring. These cases establish
same-source repair before checkpoint retirement, not ordinary group snapshot
atomicity or cross-tree joint repair.

Ordinary group materialisation is separate from the numeric internal flip. The
model does not claim the public group row and all internal rows are one atomic
operation for an ordinary write. Source atomic batches use one buffered slice;
the main visibility companion owns the public multi-group read guarantee.

History is a separate revision set, not latest-value membership. A delete adds
a revision and a superseded write still contributes its own revision. Explicit
rebuild installs only the scan seed and resumes its captured floor: it may
collapse older history, matching the documented fall-off behaviour. This is not
a promise to recover revisions absent from both source state and retained WAL.
The progress property checks every ordinary and committed-batch revision, not
just one ordinary revision. Its only prefix exception is the fixed captured
rebuild floor, not a subsequently advanced checkpoint: banking an offset cannot
excuse silently dropping a revision that was available after the scan capture.
This bounded instance has one batch overlapping ordinary work and rebuild;
two-batch closure cycles are the main staging/visibility companion's obligation.

General custom folds, min/max/set-union inverse maps, approximate eviction,
floating-point rounding, TTL and retention shaping are not modelled here.
