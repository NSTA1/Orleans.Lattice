# TreeLifecycleAdmission refinement

This bounded model checks admission ordering and durable undo clearance, not
the data-plane internals or replication shipping. The competitor abstracts
either a split/fold's migration record or a legacy in-place bootstrap's read
fence. Fresh shadow bootstrap instead uses the resize reservation and held-copy
protocol; it is not a read-fenced import and is not conflated with this path.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `resize`, `resizeUsed` | Durable resize announcement and bounded request budget | `TreeResizeState.InProgress`, `TreeResizeGrain.InitiateResizeStateAsync`; the budget is a modelling device. |
| `competitor`, `competitorUsed` | Published migration/read-fence record and bounded request | `TreeShardSplitState`, `TreeShardConsolidationState`, `LatticeBootstrapCoordinatorGrain`; the budget is a modelling device. |
| `alias` | Before or after resize cutover | `TreeRegistryEntry.PhysicalTreeId`. |
| `undo` | Request, clearance, compensation and completion | `TreeResizeUndoState`, `TreeResizeGrain.UndoResizeCoreAsync`. |
| `cleared` | Persisted once-per-operation unwind clearance | `TreeResizeUndoState.UnwindClearedOperationId`. |
| `replicated`, `enabledReplication` | Replication policy and bounded policy change | `ILatticeReplicationContext.ResolveMergeMode`; the change budget is a modelling device. |
| `running`, `faults` | Finite phase-boundary interruption | Modelling devices; the published durable records survive. |
| `compensated` | Undo has begun changing the serving copy | `TreeResizeGrain.UndoResizeCoreAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `ResizeAnnounce` | Persist resize intent before probing competing records | `TreeResizeGrain.InitiateResizeStateAsync`. | Yes: `TreeResizeGrainTests.InitiateResize_reads_the_migration_records_only_after_its_intent_is_persisted`. |
| `ResizeAdmit` | Enter copy only after competing work is absent | `TreeResizeGrain.InitiateResizeStateAsync`. | Yes: `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`. |
| `ResizeRefuse` | Restore durable predecessor on refusal | `TreeResizeGrain.InitiateResizeStateAsync`. | Yes: `TreeResizeGrainTests.InitiateResize_refused_by_a_split_keeps_a_completed_predecessor_undoable`. |
| `CompetitorAnnounce` | Publish competing record/fence before checking resize | `TreeShardSplitGrain.InitiateSplitStateAsync`, `LatticeBootstrapCoordinatorGrain`. | Yes: `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`, `LatticeBootstrapCoordinatorGrainTests.A_drain_arms_the_read_fence_before_its_first_entry_and_lifts_it_after_its_last`. |
| `CompetitorAdmit` | Start only after resize/undo is absent | `TreeShardSplitGrain`, `TreeShardConsolidationGrain`. | Yes: `TreeShardSplitGrainTests.SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight`, `TreeShardConsolidationGrainTests.StartAsync_refuses_while_a_resize_of_the_tree_is_in_flight`. |
| `CompetitorRefuse` | Clear an unused competing announcement | `TreeShardSplitGrain.InitiateSplitStateAsync`, `LatticeBootstrapCoordinatorGrain`. | Yes: `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`, `LatticeBootstrapCoordinatorGrainTests.A_held_drain_applies_nothing_lifts_a_fence_that_hides_nothing_and_does_not_fail`. |
| `ResizeFinish` | Clear resize's in-progress state | `TreeResizeGrain.CompleteResizeAsync`. | Yes: `TreeResizeGrainTests.CompleteResize_resets_state`. |
| `CompetitorFinish` | Finish migration or complete import and lift fence | `TreeShardConsolidationGrain`, `LatticeBootstrapCoordinatorGrain`. | Yes: `LatticeBootstrapCoordinatorGrainTests.A_drain_arms_the_read_fence_before_its_first_entry_and_lifts_it_after_its_last`. |
| `UndoRequest` | Acknowledge durable undo request before phase work | `TreeResizeGrain.RequestUndoAsync`. | Yes: `TreeResizeGrainTests.RequestUndo_persists_the_intent_in_its_own_slot_and_never_writes_phase_state`. |
| `UndoClearance` | Persist clearance for an unreplicated serving copy | `TreeResizeGrain.ClearUnwindForReplicationAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_an_unreplicated_tree_records_its_clearance_and_runs`. |
| `UndoRefuse` | Refuse replicated after-swap undo before compensation | `TreeResizeGrain.ClearUnwindForReplicationAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed`. |
| `EnableReplication` | Replication policy changes after a decision | `ILatticeReplicationContext.ResolveMergeMode`. | Yes: `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |
| `UndoCompensate` | Begin compensation only after clearance | `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed`. |
| `UndoFinish` | Finish an already-cleared operation despite later replication | `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |
| `LaterCutover` | A later restore or administrative alias change supersedes the resize copy | `ILatticeRegistry.SwapAliasAsync`, `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_a_later_alias_cutover_refuses_before_compensation`. |
| `Crash` | Preserve durable admission and clearance on interruption | `TreeResizeState`, `TreeResizeUndoState`. | Yes: `TreeResizeGrainTests.Failed_release_retains_the_id_for_the_next_idle_pass`, `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |
| `Retry` | Resume from persisted operation state | `TreeResizeGrain`, `TreeShardSplitGrain`. | Yes: `TreeShardSplitGrainTests.A_split_resumed_before_its_drain_abandons_when_a_resize_is_in_flight`, `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |
| `Stutter` | Optional environment inactivity | Modelling device. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `ExclusiveWork` | Publish-before-read prevents simultaneous admitted work | Yes: `TreeResizeGrainTests.InitiateResize_reads_the_migration_records_only_after_its_intent_is_persisted`, `TreeShardSplitGrainTests.InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open`. |
| `UndoExcludesCompetitor` | Pending/running undo prevents new migration or legacy-drain admission | Yes: `TreeResizeGrainTests.HoldsShardMigrations_is_true_while_an_undo_is_pending`, `TreeResizeGrainTests.HoldsShardMigrations_is_true_while_an_undo_runs_and_false_once_it_has_completed`, `LatticeBootstrapCoordinatorGrainTests.A_held_re_drive_keeps_the_fence_over_a_partial_import`. |
| `ClearanceBeforeCompensation` | Replication refusal precedes all compensation | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed`. |
| `RefusedUndoLeavesAlias` | Refusal does not move the serving alias | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed`. |
| `SupersedingCopyPreserved` | A later serving copy is not subject to the old resize's compensation | Yes: `TreeResizeGrainTests.UndoResize_after_a_later_alias_cutover_refuses_before_compensation`, `TreeResizeGrainTests.UndoResize_fences_its_swap_with_the_physical_identity_it_read`. |
| `ClearanceWasUnreplicated` | Only an unreplicated serving copy receives new clearance | Yes: `TreeResizeGrainTests.UndoResize_after_the_swap_of_an_unreplicated_tree_records_its_clearance_and_runs`, `TreeResizeGrainTests.UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed`. |
| `DurableIntentSurvives` | Recovery retains published intent and persisted clearance | Yes: `TreeResizeGrainTests.Failed_release_retains_the_id_for_the_next_idle_pass`, `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |
| `AdmissionsSettle` | Refusal or completion withdraws active admission | Yes: `TreeResizeGrainTests.InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot`, `TreeResizeGrainTests.CompleteResize_resets_state`, `LatticeBootstrapCoordinatorGrainTests.A_held_drain_applies_nothing_lifts_a_fence_that_hides_nothing_and_does_not_fail`. |
| `UndoSettles` | An already-cleared retry finishes despite policy changes | Yes: `TreeResizeGrainTests.UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Limits

The model permits one resize announcement and one competing announcement, one
after-swap undo request, one replication-policy enable, and finite crashes.
An undo request may overtake an already-admitted competitor. The interlock
prevents new admission, not retroactive cancellation of an active operation.
The physical-binding and commit-time map checks for an overtaken migration
belong to the companion lifecycle and ownership models.
Resize refusal abstracts restoring the predecessor rather than its fields.
Competitor admission abstracts the interlock only: a legacy bootstrap's partial
import keeps its fence on failure and retries, whereas an unused fence can be
cleared; imported rows and handoff are the replication modules' scope.
Durable phase boundaries abstract providers and do not prove arbitrary-await
crash safety. The mapped detectors check production seams, not an automatic
refinement proof. A completed resize's mirroring hold and sparse physical maps
are the companion TreeLifecycle and ShardOwnership models' scope.
