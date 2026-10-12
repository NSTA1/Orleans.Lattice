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
| `Flush` | Fold the completed numeric batch; accumulative revisions are not source-latest filtered | `ViewMaintainerGrain.FlushCompletedAggregationBatchesAsync`. | Partial: `MaterialisedViewAggregationStagingOverlapOrderTests.An_older_batch_completing_after_a_newer_batch_does_not_regroup_the_source_key` detects regroup ordering; the replacement-visible admission contract requires the reviewed omitted-key source detector. #4885/#4886 retains the pending composition contract. |
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
| `HistoryKeepsSuperseded` | Revision projection retains superseded Set/Delete revisions within its generation | Partial: `HistoryLatticeViewProjectionTests.Project_revisions_of_same_key_sort_chronologically_by_view_key`, `HistoryLatticeViewProjectionTests.Project_delete_emits_delete_revision_not_view_delete` prove projection row identity only, not maintainer admission or WAL replay. The accumulative drain regression must be integrated and accepted. |
| `NoAbortedContribution` | Aborted prepared contributions never enter membership or revisions | Yes: `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `CheckpointRetainsStage` | Resume never skips an unresolved prepared contribution | Yes: `MaterialisedViewAtomicVisibilityTests.Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch`. |
| `MaterialisedWhenClean` | Once every touched group is repaired, public totals match internal slots; intent deletion follows those writes | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`. |
| `OmittedReplacementAlreadyVisible` | A omitted prepared member's replacing contribution is already applied before the remaining batch can flush | Partial: `MaterialisedViewAggregationStagingOverlapOrderTests.An_older_batch_completing_after_a_newer_batch_does_not_regroup_the_source_key` covers final regroup ordering, not snapshot-level omission visibility. #4885/#4886 retains the pending composition contract. |
| `AggregateConverges` | Quiescent same-source numeric replay and rebuild repair every affected group, including the forgotten old group | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `MaterialisedViewShadowSwapTests.Aggregation_rebuild_and_reconcile_preserve_and_repair_group_values`. |
| `HistoryConverges` | Fair draining retains every available ordinary and committed-batch revision after the captured rebuild floor, including superseded revisions | Partial: `HistoryLatticeViewProjectionTests.Project_revisions_of_same_key_sort_chronologically_by_view_key` proves revision identity, not batch-drain completeness; requires the source child accumulative batch detector. #4885/#4886 retains the pending composition contract. |

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
Source admission must still deliver a same-source apply/retract or retry the
interrupted record; this engine detector cannot certify pre-applier suppression.

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
