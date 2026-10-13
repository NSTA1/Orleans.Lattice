# TreeLifecycle refinement

This is a documented abstraction, not a machine-checked refinement proof.
The model checks control-plane state across the operations; the detectors
exercise the specific production seams, not the complete model in one fixture.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias`, `maps` | Physical copy and virtual-slot maps | `ILatticeRegistry.GetEntryAsync`, `ILatticeRegistry.SwapAliasAsync`, `ILatticeRegistry.ReassignSlotsAsync`. |
| `operation`, `phase` | Reservation owner and durable phase | `TreeResizeState`, `ITreeDeletionGrain.BeginAliasChangeAsync`, `LatticeBackupRestoreService.RestoreAsync`. |
| `source`, `captured`, `copied`, `fenced` | Bound source and acknowledged routed-shard work | `TreeResizeState.ShardIndices`, `TreeResizeGrain.InitiateResizeStateAsync`, `TreeResizeGrain.EnterRejectingOnOldShardsAsync`, `TreeSnapshotGrain.RunSnapshotSliceAsync`. |
| `redirect` | Retained-copy redirect | `TreeResizeGrain.UndoResizeCoreAsync`, `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync`. |
| `mirror`, `undoable` | Completed resize's shard-migration hold | `TreeResizeGrain.HoldsShardMigrationsAsync`. |
| `migration`, `target`, `migrationCopy` | Reshard target and split/fold physical binding | `TreeReshardState`, `TreeShardSplitState.PhysicalTreeId`, `TreeShardConsolidationState.PhysicalTreeId`. |
| `running`, `faults` | Finite phase-boundary crash and retry | Modelling devices, with durable grain state retained. |
| `deleted` | Logical deletion | `ITreeDeletionGrain.DeleteTreeAsync`. |
| `resizeUsed`, `restoreUsed`, `reshardUsed` | Finite optional request budget | Modelling devices; no production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `ResizeBegin` | Reserve and capture sparse source topology after the reshard interlock | `TreeResizeGrain.ResizeAsync`, `TreeResizeGrain.InitiateResizeStateAsync`. | Yes: `TreeResizeGrainTests.InitiateResize_records_every_shard_the_logical_routing_map_names`. |
| `CopyShard` | Bank a snapshot shard slice | `TreeSnapshotGrain.RunSnapshotSliceAsync`. | Yes: `TreeResizeGrainTests.Timer_pass_reserves_legacy_in_flight_work_before_driving_the_snapshot`. |
| `CopyComplete` | Advance only after snapshot completion | `TreeResizeGrain.WaitForSnapshotAsync`. | Yes: `TreeResizeGrainTests.WaitForSnapshot_stays_in_the_snapshot_phase_while_the_slice_reports_work_remaining`, `TreeResizeGrainTests.WaitForSnapshot_advances_phase_to_swap`. |
| `FenceShard` | Fence each old routed shard | `TreeResizeGrain.EnterRejectingOnOldShardsAsync`. | Yes: `TreeResizeGrainTests.RejectOldShards_rejects_a_shard_a_split_allocated_above_the_pinned_count`. |
| `ResizeSwap` | Flip after all old shards reject | `TreeResizeGrain.SwapAliasAsync`. | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`, `TreeResizeGrainTests.SwapAlias_keeps_the_fence_when_a_failed_flip_reached_the_registry`. |
| `ReleaseReservation` | Recover completion-before-release | `TreeResizeGrain.ProcessNextPhaseAsync`, `ITreeDeletionGrain.EndAliasChangeAsync`. | Yes: `TreeResizeGrainTests.A_phase_tick_releases_the_reservation_an_interrupted_completion_left_and_then_reports_idle`. |
| `UndoBegin` | Accept undo under its lifecycle reservation | `TreeResizeGrain.UndoResizeAsync`. | Yes: `TreeResizeGrainTests.Resize_and_undo_refuse_when_lifecycle_cannot_be_reserved`. |
| `UndoRedirect` | Arm replacement before undo's flip | `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `UndoSwap` | Restore old copy and map | `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `UndoClear` | Release old-copy fences and mirrors | `TreeResizeGrain.UndoResizeCoreAsync`. | Yes: `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. |
| `ExpireUndo` | Release the hold once no old mirror remains | `TreeResizeGrain.HoldsShardMigrationsAsync`. | Yes: `TreeResizeGrainTests.HoldsShardMigrations_is_false_once_every_replaced_shard_has_stopped_mirroring`. |
| `ReshardBegin` | Start grow or shrink, refusing undoable resize | `TreeReshardGrain.ReshardAsync`. | Yes: `TreeReshardGrainTests.ReshardAsync_is_refused_as_resize_undoable_while_a_completed_resize_holds_migrations`. |
| `MigrationPublish` | Commit a physically bound split/fold diff | `TreeShardSplitGrain.SwapAsync`, `TreeShardConsolidationGrain.SwapAsync`. | Yes: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`, `TreeShardConsolidationGrainTests.Swap_after_an_alias_cutover_leaves_the_routing_map_untouched_and_abandons_the_fold`. |
| `ReshardComplete` | Complete after target and folds settle | `TreeReshardGrain.EnterCompletePhaseAsync`. | Yes: `TreeReshardGrainTests.A_shrink_does_not_complete_while_a_fold_it_started_is_still_running`, `TreeReshardGrainTests.A_shrink_completes_once_the_map_holds_the_target_and_every_fold_has_finished`. |
| `MigrationRetarget` | Abandon obsolete diff and plan current copy on a later tick | `TreeShardSplitGrain.AbandonRetargetedSplitAsync`, `TreeReshardGrain.MigrateAsync`. | Yes: `TreeShardSplitGrainTests.Split_resumed_on_a_new_activation_after_a_cutover_stays_bound_to_the_tree_it_started_on`, `TreeReshardGrainTests.MigrateAsync_grow_tick_reads_map_and_physical_tree_from_one_registry_entry`. |
| `RestoreBegin` | Reserve a non-deleted tree before shadow build | `LatticeBackupRestoreService.BuildShadowCoreAsync`, `ITreeDeletionGrain.BeginAliasChangeAsync`. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `RestoreSwap` | Publish restored copy's map with alias | `AliasCutoverShardMaps.SwapCutoverAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_tree_keeps_every_key_readable`. |
| `RestoreRedirect` | Arm every retained routed shard | `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_arms_every_retained_shard`. |
| `RevertBegin` | Reserve lifecycle for revert | `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.A_revert_is_refused_while_another_operation_holds_the_trees_alias_reservation`. |
| `RevertSwap` | Restore previous alias and map together | `AliasCutoverShardMaps.RevertAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `RevertRedirect` | Clear previous redirect and arm shadow | `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_arms_every_shard_of_the_restored_copy_to_redirect_back`. |
| `Crash` | Interrupt a phase without erasing durable progress | `TreeResizeGrain.ProcessNextPhaseAsync`, `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `TreeResizeGrainTests.Failed_release_retains_the_id_for_the_next_idle_pass`, `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `Retry` | Resume with own reservation still held | `TreeResizeGrain.RunResizePassAsync`, `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `TreeResizeGrainTests.Idle_pass_releases_a_persisted_orphan_reservation_after_reactivation`, `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `Delete` | Refuse deletion under an alias reservation | `ITreeDeletionGrain.DeleteTreeAsync`. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `Stutter` | Quiescence or optional environment inactivity | Modelling device, no production counterpart. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CopyCoversRoutedShards` | Snapshot enumeration includes sparse routed indices | Yes: `TreeResizeGrainTests.InitiateResize_records_every_shard_the_logical_routing_map_names`. |
| `SwapOnlyAfterCopyAndFence` | No flip before copy and rejection complete | Yes: `TreeResizeGrainTests.SwapAlias_fences_every_old_shard_before_moving_the_alias`. |
| `UndoRedirectBeforeSwap` | Replacement redirects before old copy becomes current | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `UndoReturnsToSource` | Undo restores original physical copy | Yes: `TreeResizeGrainTests.UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it`. |
| `SettledUndoUnfenced` | Undo clears every source shard's rejection | Yes: `TreeResizeGrainTests.UndoResize_after_swap_releases_a_split_allocated_shard`. |
| `CutoverMatchesPhase` | Swap publishes the intended physical copy | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_tree_keeps_every_key_readable`, `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `RedirectInstalledOnCompletion` | Restore and revert finish all redirect work | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_arms_every_shard_of_the_restored_copy_to_redirect_back`. |
| `ExpiredMirrorReleased` | Migration hold tracks remaining mirrors | Yes: `TreeResizeGrainTests.HoldsShardMigrations_is_false_once_every_replaced_shard_has_stopped_mirroring`. |
| `NoMigrationDuringResize` | Resize refuses an active reshard | Yes: `TreeResizeGrainTests.ResizeAsync_refused_while_reshard_in_flight`. |
| `NoReshardWhileMirroring` | Reshard refuses completed-but-undoable resize | Yes: `TreeReshardGrainTests.ReshardAsync_is_refused_as_resize_undoable_while_a_completed_resize_holds_migrations`. |
| `DeleteOutsideReservation` | Reserved alias cannot be deleted | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `PublishedMigrationMatchesTarget` | Obsolete bound migration does not corrupt the current copy map | Yes: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`, `TreeShardConsolidationGrainTests.Swap_after_an_alias_cutover_leaves_the_routing_map_untouched_and_abandons_the_fold`. |
| `ReservationSurvivesCrash` | Failure retains the reservation for recovery | Yes: `TreeResizeGrainTests.Failed_release_retains_the_id_for_the_next_idle_pass`. |
| `OperationReturns` | A persisted completion can release and report idle | Yes: `TreeResizeGrainTests.A_phase_tick_releases_the_reservation_an_interrupted_completion_left_and_then_reports_idle`, `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `ReshardReturns` | Target completion waits for actual migration completion | Yes: `TreeReshardGrainTests.A_shrink_completes_once_the_map_holds_the_target_and_every_fold_has_finished`. |
| `CrashRecovers` | Retry resumes the held operation | Yes: `TreeResizeGrainTests.Idle_pass_releases_a_persisted_orphan_reservation_after_reactivation`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behaviour. |

## Evidence and limits

`Yes` identifies a concrete detector of the mapped seam. It does not claim
an automatic refinement proof or crash injection at every await. Existing
detectors are reused rather than replacing them with tests of a parallel
implementation. The model's per-action mutations demonstrate non-vacuity of
the abstract properties; they are distinct from production perturbations.
The bounded scheduling and data-plane exclusions are stated in the README.

The cited lifecycle detectors ran clean (23 core tests and six backup cutover
integration tests). Removing the production snapshot-completion guard made
`TreeResizeGrainTests.WaitForSnapshot_stays_in_the_snapshot_phase_while_the_slice_reports_work_remaining`
fail on the phase advance and persistence; restoring it returned the two snapshot
completion detectors to green. That perturbation was temporary and is not a
production defect. Other reused seams retain their companion-module detector
evidence; no new machine-checked implementation refinement is claimed.
