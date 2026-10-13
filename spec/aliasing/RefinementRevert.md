# AliasRevert refinement

This is a bounded safety model of shared registry publications interleaving with
the revert service and a physically bound shard migration. It deliberately
allows administrative moves after reservation and return to the same physical
copy. It does not claim the stronger operation/revision guarantee of AliasRouting.

## Variable mapping

| Spec variable | Role | Code counterpart |
|---------------|------|------------------|
| `alias`, `map`, `previous` | Effective physical binding, routing pair and recorded predecessor | `TreeRegistryEntry.PhysicalTreeId`, `ShardMap`, `LatticeRestoreResult.PreviousPhysicalTreeId`. |
| `phase`, `observed`, `lineage` | Service/helper sequence, expected binding and lineage stamp | `LatticeBackupRestoreService.RevertRestoreAsync`, `AliasCutoverShardMaps.RevertAsync`. |
| `reservation`, `deleted` | Logical lifecycle admission | `ITreeDeletionGrain.BeginAliasChangeAsync`, `ITreeDeletionGrain.DeleteTreeAsync`. |
| `migration`, `migrationDone` | Split/fold bound to its initiating copy | `TreeShardSplitState.PhysicalTreeId`, `TreeShardConsolidationState.PhysicalTreeId`. |
| `moves`, `staleRevert`, `staleMigration` | Bounded administrative environment and safety observers | Modelling devices. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Validate` | Refuse an already superseded restore before reservation or lineage | `AliasCutoverShardMaps.ValidateRevertBindingAsync`. | Yes: `LatticeBackupRestoreIntegrationTests.Reverting_a_superseded_restore_preserves_the_later_row_and_takes_no_reservation`. |
| `Reserve` | Take lifecycle reservation unless deleted | `ITreeDeletionGrain.BeginAliasChangeAsync`. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `Restamp` | Stamp lineage before installing the predecessor | `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `LatticeBackupRestoreIntegrationTests.A_shadow_cutover_restamps_the_lineage_and_its_revert_restamps_it_again`. |
| `ReadBinding` | Recheck current shadow/previous identity in core helper | `AliasCutoverShardMaps.RevertAsync`. | Yes: `AliasCutoverShardMapsRefusalTests.RevertAsync_refuses_a_superseding_copy_without_writing`. |
| `Revert` | Compare expected physical identity and atomically restore routing | `ILatticeRegistry.SwapAliasAsync`. | Yes: `AliasCutoverShardMapsRefusalTests.RevertAsync_without_a_recorded_map_still_fences_the_shadow_it_read`, `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `Finish` | Release reservation after redirect completion | `LatticeBackupRestoreService.RevertRestoreAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `MoveAlias` | Administrative alias/map publication | `ILatticeRegistry.SetAliasAsync`. | Yes: `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `Delete` | Delete outside a reservation | `ITreeDeletionGrain.DeleteTreeAsync`. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `BindMigration` | Capture physical copy before split/fold work | `TreeShardSplitState.PhysicalTreeId`. | Yes: `TreeShardSplitGrainTests.Split_resumed_on_a_new_activation_after_a_cutover_stays_bound_to_the_tree_it_started_on`. |
| `PublishMigration` | Obsolete diff leaves current logical routing untouched | `TreeShardSplitGrain.SwapAsync`, `TreeShardConsolidationGrain.SwapAsync`. | Yes: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`, `TreeShardConsolidationGrainTests.Swap_after_an_alias_cutover_leaves_the_routing_map_untouched_and_abandons_the_fold`. |
| `Stutter` | Optional inactivity | Modelling device. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|---------------------|----------|
| `CoherentRow` | A publication carries matching physical identity and map | Yes: `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `NoStaleRevert` | Expected physical identity refuses a racing later copy | Yes: `LatticeRegistryGrainTests.SwapAliasAsync_stale_expected_target_does_not_install_any_redirect`. |
| `BoundMigration` | An obsolete migration never changes current routing | Yes: `TreeShardSplitGrainTests.Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map`. |
| `FinishedReservationReleased` | Completed retry releases its reservation | Yes: `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `EarlyRefusalNeutral` | Already superseded work changes no lineage or reservation | Yes: `LatticeBackupRestoreIntegrationTests.Reverting_a_superseded_restore_preserves_the_later_row_and_takes_no_reservation`. |
| `RevertedBinding` | Restore predecessor's map with its physical identity | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `ReservationBlocksDelete` | Alias reservation excludes deletion | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `DeletionBlocksReservation` | Deleted tree cannot take alias admission | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `CapturedBinding` | Helper captures the copy it actually observed | Yes: `AliasCutoverShardMapsRefusalTests.RevertAsync_without_a_recorded_map_still_fences_the_shadow_it_read`. |
| `CapturedMigration` | Migration keeps its initiating physical identity | Yes: `TreeShardSplitGrainTests.Split_resumed_on_a_new_activation_after_a_cutover_stays_bound_to_the_tree_it_started_on`. |
| `LineageBeforeRevert` | The predecessor is installed only after lineage restamping | Yes: `LatticeBackupRestoreIntegrationTests.A_shadow_cutover_restamps_the_lineage_and_its_revert_restamps_it_again`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain constraint, not a production behavior. |

## Limits

Map tokens abstract an entire copy's routing layout, not individual virtual
slots. Migration publication abstracts only the retargeted-copy guard; the
split/fold algorithms and their slot diffs are checked by companion models.
Redirect fix-up, data visibility, physical WAL consumers and phase crashes are
covered by the cutover, lifecycle and view models, not duplicated here.
The service's early validation is a read, not a reservation or an epoch fence.
A later failure can retain reservation/lineage, and an ABA return to the same
shadow passes the physical check. No liveness guarantee is asserted for such
superseded requests. `AliasRouting` separately checks durable administrative
recovery's stronger operation-id/map-version predecessor checks.
