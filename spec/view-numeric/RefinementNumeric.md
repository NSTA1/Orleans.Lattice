# Numeric projection companion refinement

This is a documented abstraction, not a machine-checked refinement proof. It
checks a Count/Sum aggregation view on its own, with no schema component
installed, over a fixed two-key source script: create, in-place update,
regroup, a reserved group key, a filter exit and re-entry, a zero-valued Sum
member and delete, through ordinary writes and two overlapping two-key atomic
batches. The environment decides each batch, changes the projection's filter
definition (or swaps the source's physical copy), loses a flip or write
response or crashes the maintainer, and asks for a reconcile rebuild.

## Model scope in one paragraph

Each source event is extracted from its own kind and bytes into a contribution
(`Extract`): a delete, a reserved group key or a value the filter rejects
contributes nothing and so retracts the key's prior member. An ordinary
contribution is applied by the per-source-key read-before-write flip that moves
membership, both accumulator slots and a durable affected-group intent together
under an operation id of generation, source key and source HLC; the groups are
then republished one at a time and the intent retired. An atomic batch flips
its whole captured net slice, public groups included, in one write keyed by the
batch. A changed projection version or a swapped physical copy rebinds by
rebuilding from current source under a fresh generation; a reconcile does the
same for a live view. Source-superseded writes are dropped on replay. The
checked contract is that whenever a pass has caught up, membership is the
extraction of every key's source winner and every group is published as its
Count or Sum, absent exactly when it has no member.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `appended`, `logs`, `phys`, `decision`, `src` | Source writes, per-copy WAL, current physical copy, batch decisions and last-writer winners | `ILattice.SetAsync`, `ILattice.SetManyAtomicAsync`, `ViewMaintainerGrain.ResolveSourcePhysicalAsync`. |
| `defn`, `viewDefn` | Registered projection definition and the version the view was built under | `AggregationLatticeViewProjection.ProjectionVersion`, `ViewCheckpointState.ProjectionVersion`. |
| `bound`, `epoch` | Bound physical copy and rebuild generation that freshens operation ids | `ViewCheckpointState.BoundPhysicalTreeId`, `ViewCheckpointState.RebuildGeneration`. |
| `read`, `checkpoint`, `phase` | Volatile pass position, durable resume floor and the in-flight ordinary unit | `ViewMaintainerGrain.DrainAggregationAsync`, `ViewCheckpointState.AppliedOffsets`. |
| `mem`, `slots` | Per-source-key membership row and per-group count/sum accumulator | `AggregationRowCodec.MembershipKey`, `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.RetractNumericAsync`. |
| `pub` | Public group value under the bare group key | `AggregationApplier.MaterialiseAccumulatorAsync`, `LatticeAggregationValue.EncodeInt64`, `LatticeAggregationValue.EncodeDouble`. |
| `intent` | Durable affected-group rematerialisation obligation | `AggregationRowCodec.NumericMaterialisationKey`, `AggregationApplier.CompletePendingNumericMaterialisationAsync`. |
| `registry`, `conflict` | Retained saga ids with their payloads, and a witness of an id resubmitted with another payload | `IAggregationViewStore.SetManyAtomicAsync`, `LatticeIdempotencyKeyMismatchException`; `conflict` is a modelling device. |
| `faults`, `reconciles` | Failure and reconcile budgets | Modelling devices, not production fields. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | Source ordinary write or decided two-key atomic batch | `ILattice.SetAsync`, `ILattice.SetManyAtomicAsync`. | Yes: `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`, `MaterialisedViewAggregationIntegrationTests.Sum_view_converges_across_inserts_overwrites_and_deletes`. |
| `ChangeFilter` | The registered projection's filter, and so its version, changes | `AggregationLatticeViewProjection.ProjectionVersion`, `ViewMaintainerGrain.EnsureActiveAsync`. | Yes: `ViewMaintainerAggregationDrainTests.Aggregation_activation_rebuilds_when_the_persisted_projection_version_differs`, `ViewMaintainerAggregationDrainTests.Aggregation_activation_keeps_the_view_when_the_projection_version_matches`. |
| `Swap` | A resize, reshard or restore swaps the source's physical copy | `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. | Yes: `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`, `MaterialisedViewIdentitySwapHealTests.View_tails_new_identity_after_swap`. |
| `Fail` | Lost flip response, failed group write, failed intent retirement or crash; the pass replays from the checkpoint | `AggregationApplier.CompletePendingNumericMaterialisationAsync`, `ViewMaintainerGrain.DrainAggregationAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`, `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`. |
| `Flip` | Complete the prior obligation, drop a source-superseded write, skip an installed membership, else flip membership, slots and intent under the contribution's id | `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.RetractNumericAsync`, `ViewMaintainerGrain.DropWritesSupersededAtSourceAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `AggregationApplierNumericReplayTests.ApplyAsync_regrouped_ordinary_replay_completes_intent_without_changing_the_atomic_key_set`, `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`. |
| `Publish(k,g)` | Rematerialise one affected group from its slots for a source key this pass read | `AggregationApplier.MaterialiseAccumulatorAsync`, `AggregationApplier.CompletePendingNumericSourceAsync`. | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`, `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`. |
| `Advance` | Move past an ordinary unit once its key's obligation is retired | `ViewMaintainerGrain.DrainAggregationAsync`. | Yes: `ViewMaintainerReplayFlushTests.A_suppressed_interrupted_numeric_event_and_empty_successor_complete_repair_before_checkpoint`. |
| `FlushBatch` | Flip a decided batch's captured net slice, public groups included, in one write keyed by the batch; skip an aborted or already-flipped batch | `ViewMaintainerGrain.FlushCompletedAggregationBatchesAsync`. | Yes: `MaterialisedViewAggregationIntegrationTests.Single_tree_atomic_batch_folds_onto_an_existing_group_atomically`, `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`. |
| `Checkpoint` | Bank the pass only after every affected group is republished | `ViewMaintainerGrain.DrainAggregationAsync`, `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Yes: `MaterialisedViewNumericIntentCompositionTests.Source_suppression_and_a_no_output_successor_complete_numeric_intent_before_checkpoint`, `ViewMaintainerReplayFlushTests.A_suppressed_interrupted_numeric_event_and_empty_successor_complete_repair_before_checkpoint`. |
| `Rebind` | A changed version or swapped copy rebuilds from current source under a fresh generation, bound to the current copy | `ViewMaintainerGrain.RebuildAsync`, `ViewMaintainerGrain.EnsureBoundToCurrentSourceIdentityAsync`. | Yes: `ViewMaintainerAggregationDrainTests.Aggregation_activation_rebuilds_when_the_persisted_projection_version_differs`, `MaterialisedViewIdentitySwapHealTests.View_heals_after_source_physical_identity_swap`. |
| `Reconcile` | A live view rebuilds from current source under a fresh generation | `ViewMaintainerGrain.BuildShadowAsync`, `ViewMaintainerGrain.InPlaceRebuildAsync`. | Yes: `MaterialisedViewNumericCompanionTests.Rebuild_reproduces_the_incrementally_maintained_count_and_sum_groups`, `MaterialisedViewShadowSwapTests.Aggregation_rebuild_and_reconcile_preserve_and_repair_group_values`. |
| `Stutter` | No step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `SlotsMatchMembership` | Every observed accumulator slot is the retract/add fold of membership, through regroup, retract and lost responses | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic`, `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`. |
| `IntentCoversStale` | A public group that differs from its slots is named by a durable intent | Yes: `AggregationMembershipAtomicityTests.ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution`, `AggregationMembershipAtomicityTests.ApplyAsync_absent_member_retry_completes_a_failed_retraction_materialisation`. |
| `NoIdentityConflict` | A replay never resubmits an operation id with a changed key set; the id carries the contribution's own HLC | Yes: `AggregationApplierNumericReplayTests.ApplyAsync_regrouped_ordinary_replay_completes_intent_without_changing_the_atomic_key_set`, `AggregationOperationIdTests.OperationId_is_byte_identical_to_the_interpolated_payload`. |
| `QuiescentExact` | A caught-up view is a function of the source alone: delete, filter exit and a reserved group key retract; an empty group has no row; a live zero Sum publishes 0 | Yes: `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`, `AggregationReservedRegroupTests.ApplyAsync_reserved_regroup_retracts_prior_membership_so_incremental_equals_rebuild`, `AggregationLatticeViewProjectionTests.Project_set_failing_filter_emits_retract`. |
| `AbortedContributesNothing` | An aborted batch never reaches membership, slots or groups | Yes: `MaterialisedViewAggregationIntegrationTests.Committed_atomic_batch_folds_into_aggregation_groups_only_after_commit`, `MaterialisedViewAtomicVisibilityTests.Aborted_batch_is_never_surfaced`. |
| `CheckpointAfterPublication` | The resume floor never advances past an unretired publication obligation | Yes: `MaterialisedViewNumericIntentCompositionTests.Source_suppression_and_a_no_output_successor_complete_numeric_intent_before_checkpoint`, `ViewMaintainerReplayFlushTests.A_suppressed_interrupted_numeric_event_and_empty_successor_complete_repair_before_checkpoint`. |
| `Converges` | Once writes, faults and topology changes are spent, fair maintenance and the rebind reach the source's exact Count/Sum | Yes: `MaterialisedViewNumericCompanionTests.Rebuild_reproduces_the_incrementally_maintained_count_and_sum_groups`, `MaterialisedViewNumericCompanionTests.Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly`. |
| `ObligationsRetire` | Every durable affected-group intent is eventually republished and retired, even when the key's next event projects nothing | Yes: `AggregationMembershipAtomicityTests.CompletePendingNumericSourceAsync_repairs_a_source_even_when_projection_emits_nothing`, `AggregationMembershipAtomicityTests.ApplyAsync_absent_member_retry_completes_a_failed_retraction_materialisation`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Property classification (issue #2321)

TypeOK, SlotsMatchMembership, IntentCoversStale, NoIdentityConflict,
QuiescentExact and AbortedContributesNothing are state safety.
CheckpointAfterPublication is action safety. Converges and ObligationsRetire
are temporal liveness under weak fairness of the maintainer steps and the
rebind; the environment steps are unfair and finitely budgeted.

The base checks every property with the filter change, one fault and no
reconcile. The `Swap` variant switches to Count, swaps the physical copy
instead of the filter and allows one reconcile, and checks every property. The
`TwoFaults` variant allows two faults and one reconcile and checks the
invariants and the action property; its liveness is bounded-out for the TLC
budget, with both temporal properties retained on the base and on `Swap`.

Every experiment is an anchored action perturbation except the one fairness
removal:

| Mutation | Classification |
|----------|----------------|
| TypeOKSlotUnderflow | Anchored action: Flip. |
| SlotsMatchMembershipBatchMissesKey | Anchored action: FlushBatch. |
| IntentCoversStaleFlipForgetsIntent | Anchored action: Flip. |
| IntentCoversStalePublishDropsWrite | Anchored action: Publish. |
| IntentCoversStaleCrashForgetsIntent | Anchored action: Fail. |
| NoIdentityConflictDropsHlc | Anchored action: Flip. |
| NoIdentityConflictReplaysInstalled | Anchored action: Flip. |
| NoIdentityConflictReplaysSuperseded | Anchored action: Flip. |
| QuiescentExactRejectKeepsPrior | Anchored action: Flip. |
| QuiescentExactVersionAdoptedForward | Anchored action: ChangeFilter. |
| QuiescentExactRebindWithoutScan | Anchored action: Rebind. |
| QuiescentExactSwapTailsNewLogOnly | Anchored action: Swap, under `Topology = 2`. |
| QuiescentExactReconcileReusesEpoch | Anchored action: Reconcile, under `MaxReconciles = 1`. |
| QuiescentExactZeroSumAbsent | Anchored action: FlushBatch. |
| QuiescentExactWriteNotLogged | Anchored action: Write. |
| AbortedContributesNothingFlushesAbort | Anchored action: FlushBatch. |
| CheckpointAfterPublicationEarly | Anchored action: Checkpoint. |
| ConvergesRebindUnfair | Fairness removal on Rebind. |
| ConvergesAdvanceNeverMoves | Anchored action: Advance. |
| ObligationsRetirePublishKeepsIntent | Anchored action: Publish. |

## Detectors

The two `MaterialisedViewNumericCompanionTests` cases are the caller-level
detectors for extraction and encoding: a real Count view and a real Sum view
over one source, filtered on `Score != 1`, read only through the public view
handle after every step of the same script the model writes, and compared with
values derived from the source's rows alone. The rebuild case then requires a
rebuild to reproduce the incrementally maintained groups exactly and the next
ordinary write to be maintained correctly under the new generation. The
reserved-group retraction is pinned at the applier by
`AggregationReservedRegroupTests`, which compares incremental application with
a fresh application of the current value for Count, Sum and Fold.

`ViewMaintainerAggregationDrainTests` drives the real maintainer's activation
with a persisted projection version that differs from the registered one and
requires a new generation; the matching version is its negative control. The
applier-level detectors (`AggregationMembershipAtomicityTests`,
`AggregationApplierNumericReplayTests`) lose the actual atomic flip response or
fail the actual group write and intent deletion, and bind the atomic key set to
its operation id the way the saga registry does.

Production perturbations for this area are in
[`perturbations/`](perturbations/README.md).

## Defect mutations whose fixes have landed

QuiescentExactRejectKeepsPrior reinstates the defect fixed by #5236: a
contribution rejected for a reserved group key kept the key's prior membership,
so an incrementally maintained view differed from a rebuilt one for the same
source. Its regression test is the reserved-group detector of the
`QuiescentExact` row. IntentCoversStaleFlipForgetsIntent and
IntentCoversStaleCrashForgetsIntent reinstate the publication-obligation loss
fixed for #4890, and NoIdentityConflictReplaysSuperseded the
source-superseded replay fixed for #4885.

## Deliberate abstraction gaps

- **Non-finite and rounded numerics.** A value selector that returns NaN or an
  infinity poisons a Sum group: retracting it cannot restore a finite total.
  Non-integral doubles accumulate by retract/add, so an incrementally maintained
  Sum can differ from a rebuilt one by rounding. The model uses small integers
  and defines no policy for either.
  Production symbols: `AggregationApplier.ContributeNumericAsync`, `AggregationApplier.MaterialiseAccumulatorAsync`.
- **A throwing selector stops the drain.** A group, value or filter selector
  that throws fails the pass before any publication and before the checkpoint
  advances, so the view stays at its last consistent state and retries the same
  entry; there is no dead-letter or skip policy for an unprojectable value, and
  the model has no such event.
  Production symbols: `AggregationLatticeViewProjection.Project`, `ViewMaintainerGrain.DrainAggregationAsync`.
- **Fixed script and shapes.** Two keys, two groups, one reserved group and one
  filter value are a fixed script, not every projection encoding, predicate or
  selector. Fanout greater than one, range deletes lowered to a rebuild,
  min/max/set-union and custom folds are outside this module.
  Production symbols: `AggregationLatticeViewProjection`, `LatticeFoldProjection`.
- **Staging, supersession closure and public snapshots are relied on.** Prepare
  staging before the decision, terminal horizons, the omitted-member replacement
  gate and the multi-group public snapshot of a batch are the obligations of
  `MaterialisedViewAggregation`, `MaterialisedViewHorizon` and
  `MaterialisedViewReadyClosure`; this module decides each batch atomically at
  its write and drops a superseded member without the replacement gate.
  No production symbol: the gap names model boundaries owned by other modules.
- **Rebuild is atomic here.** A rebuild captures the current source, scans it
  and publishes membership, slots, groups, generation, binding and floor in one
  step. Interrupted scans, read holds, generation reclaim and late alias moves
  are `MaterialisedViewRebind` and `MaterialisedViewTail` obligations.
  No production symbol: the gap names model boundaries owned by other modules.
- **Accumulative history is separate.** A revision-keyed history view keeps
  superseded revisions and adopts a changed version forward; that contract is
  checked by `MaterialisedViewAggregation`, not here, where a changed version
  must rebuild.
  Production symbols: `HistoryLatticeViewProjection`.
- **No schema component.** The extraction reads the stored value bytes directly;
  enforcement, version envelopes and upcasting are not installed in this model.
  No production symbol: the gap states the module's independence premise.
