# Refinement note: BackupCutover to code

This note maps [`BackupCutover.tla`](BackupCutover.tla) - a local shadow-cutover
restore and its revert on one cluster - to the production symbols that play each
role, and to the detector tests that would notice production deviating from it.
It is a documented mapping, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias`, `map` | The registry alias and the shard map held under the tree | The tree registry's alias and the shard map under the logical id, moved together by `AliasCutoverShardMaps.SwapCutoverAsync` and `AliasCutoverShardMaps.RevertAsync` (#4336). |
| `route` | A routing activation's cached alias and map | A `LatticeGrain` StatelessWorker activation's cached routing, refreshed by `LatticeGrain.GetRoutingAsync`. |
| `redir[p]` | Where a retained copy forwards logical-alias traffic | The retained-tree redirect `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync` arms and `LatticeBackupRestoreService.ClearRetainedTreeRedirectAsync` clears. |
| `rp` | The restore or revert in progress | `LatticeBackupRestoreService.RestoreAsync` (`LatticeRestoreMode.ShadowCutover`) and `LatticeBackupRestoreService.RevertRestoreAsync`. |
| `reserved` | The tree's alias reservation | `ITreeDeletionGrain.BeginAliasChangeAsync` / `ITreeDeletionGrain.EndAliasChangeAsync`. |
| `deleted` | The tree is deleted | `ITreeDeletionGrain.DeleteTreeAsync`. |
| `crashed` | The fault budget (one crash) | Modelling device: it bounds the crashes so one cannot repeat forever and starve the retry. No production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Build` | Register the shadow with restore provenance, take the reservation, build | `LatticeBackupRestoreService.BuildShadowCoreAsync`, which registers the shadow before `ITreeDeletionGrain.BeginAliasChangeAsync`. | Yes: `LatticeBackupRestoreIntegrationTests.RestoreAsync_shadow_cutover_stamps_restore_provenance_on_shadow_tree` and `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `Swap` | Move the alias and the map onto the shadow in one registry write | `LatticeBackupRestoreService.CommitShadowCoreAsync` into `AliasCutoverShardMaps.SwapCutoverAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_tree_keeps_every_key_readable`. |
| `ArmRedirect` | Arm the previous copy to forward onto the shadow, then release the reservation | `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync` after the swap, then `ITreeDeletionGrain.EndAliasChangeAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_arms_every_retained_shard`. |
| `RevertBegin` | A revert takes the tree's alias reservation | `LatticeBackupRestoreService.RevertRestoreAsync` calling `ITreeDeletionGrain.BeginAliasChangeAsync` under the restore's revert operation id. **Environment argument:** not fair; a revert is a caller's choice. | Yes: `ShadowCutoverShardMapIntegrationTests.A_revert_is_refused_while_another_operation_holds_the_trees_alias_reservation` pins the reservation, and `LatticeBackupRestoreIntegrationTests.Shadow_restore_lifecycle_targets_live_data_and_revert_refuses_deleted_tree` the refusal of a deleted tree. |
| `RevertSwap` | Move the alias and the map back in one write | `LatticeBackupRestoreService.RevertRestoreAsync` into `AliasCutoverShardMaps.RevertAsync`, which is a no-op swap once the alias is already back. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable`. |
| `RevertRedirect` | Clear the previous copy's redirect, arm the shadow to forward back, release | `LatticeBackupRestoreService.RevertRestoreAsync` calling `LatticeBackupRestoreService.ClearRetainedTreeRedirectAsync` then `LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync`. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_arms_every_shard_of_the_restored_copy_to_redirect_back`. |
| `Refresh` | A routing activation re-resolves alias and map together | `LatticeGrain.GetRoutingAsync` with `forceRefresh`, which reads the alias and the map from one registry row and publishes them as one pair. **Environment argument:** not fair, so an activation may stay stale forever, which is the case the redirects exist for. | Yes: `LatticeGrainTests.GetRoutingAsync_forceRefresh_true_invalidates_alias_and_map_together` and `LatticeGrainTests.GetRoutingAsync_does_not_publish_a_pair_read_before_an_invalidation` (deterministic; #4441 F10), and `ShadowCutoverRoutingSelfHealChaosTests.Cutover_self_heals_all_routing_activations_under_sustained_concurrent_reads` under sustained concurrency (chaos tier, CI only). |
| `Crash` | The restore, or its revert, fails part-way, keeping its reservation and its shadow | Any fault inside `LatticeBackupRestoreService.RestoreAsync` after `ITreeDeletionGrain.BeginAliasChangeAsync`, or inside `LatticeBackupRestoreService.RevertRestoreAsync` after it. **Environment argument:** not fair, at any step between the build and the redirect, and between a revert's reservation and its redirect fix-up. | Yes: `LatticeBackupRestoreIntegrationTests.Shadow_restore_lifecycle_targets_live_data_and_revert_refuses_deleted_tree` for the restore, and `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up` for a revert that failed after its swap. |
| `Retry` | The same request retried resumes, reusing the shadow and the reservation; a retried revert redoes its swap (a no-op) and its whole redirect fix-up | The deterministic operation id `LatticeBackupRestoreService.ResolveShadowTreeId` derives, which makes the build resumable; `LatticeBackupRestoreService.RevertRestoreAsync` re-run with the same result. | Yes: `LatticeBackupCoordinatedRestoreEngineTests.ResolveShadowTreeId_is_deterministic_for_the_same_request`, `LatticeBackupRestoreIntegrationTests.RestoreAsync_rerun_is_a_no_op` and `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up`. |
| `Delete` | Delete the tree, refused while the reservation is held | `ITreeDeletionGrain.DeleteTreeAsync`. **Environment argument:** not fair, any time. | Yes: `TreeDeletionGrainTests.Delete_pending_fences_alias_writes_during_controlled_registry_interleaving` and `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RestoreNeverTorn` | A reader never pairs one copy with another copy's shard map: the cutover and the revert each move the alias and the map in one registry write (#4250, #4336). | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_keeps_every_key_readable_and_reverts`. |
| `CutoverServesRestored` | Once a restore returns, every reader is served by the restored copy, however stale its routing. | Yes: `ShadowCutoverShardMapIntegrationTests.RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_arms_every_retained_shard` pins the redirect a stale route reaches (deterministic; red with no redirect armed, #4441 F10), and `ShadowCutoverRoutingSelfHealChaosTests.Repeated_cutovers_self_heal_routing_to_each_successive_snapshot` under repeated cutovers (chaos tier, CI only). |
| `RevertNeverServesRestored` | Once a revert returns, no reader is served by the restored copy. | Yes: `ShadowCutoverShardMapIntegrationTests.RevertRestoreAsync_arms_every_shard_of_the_restored_copy_to_redirect_back` pins the redirect a stale route reaches; `LatticeBackupRestoreIntegrationTests.RestoreAsync_shadow_cutover_swaps_alias_then_revert_restores_prior_tree` pins a refreshed route. |
| `DeleteNeverMidCutover` | A tree is never deleted while a restore or revert holds its copies in motion, including one that failed part-way. | Yes: `TreeDeletionGrainTests.Alias_reservation_is_idempotent_exclusive_and_explicitly_abandonable`. |
| `RestoreReturns` | A restore whose shadow is built eventually returns, a crash included, because the retry reuses the reservation its failed attempt still holds. | Yes: `LatticeBackupRestoreIntegrationTests.RestoreAsync_rerun_is_a_no_op`. |
| `RevertReturns` | A revert that has taken its reservation eventually returns, a crash after its swap included, because the retried revert completes its redirect fix-up. | Yes: `ShadowCutoverShardMapIntegrationTests.A_revert_retried_after_failing_between_its_swap_and_its_redirect_fix_up_completes_the_fix_up` (red when a retried revert stops once the alias is back, the code analogue of `RevertReturnsRetryStopsAfterSwap`). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy every property is reached and falsifiable by a
protocol-level mutation, and the mutations of the two liveness properties,
`RestoreReturns` and `RevertReturns`, leave `Spec`'s fairness intact. No mutation
adds an action. Since #4441 F9 a crash is modelled at every step of the restore
and of its revert: the revert takes its reservation (`RevertBegin`), swaps
(`RevertSwap`) and fixes up its redirects (`RevertRedirect`) as three steps, and
a crash after any of the first two leaves the reservation held until the
retried revert completes, which `RevertReturns` checks.

## Scope at the seams

Each item below is checked elsewhere, or argued here; none is an unchecked claim.

- **The window between the swap and the redirect.** A stale activation is served
  the retained copy between the alias swap and the redirect being armed, and,
  after a crash there, until the retried request completes. The properties are
  claimed once the restore or revert has returned, which is the claim the
  documentation makes, and `RestoreReturns` and `RevertReturns` check that it
  returns; a write admitted to the retained copy in that window is ordered
  before the restore and discarded with it.
- **Writes, and atomic batches across a cutover.** Values and writers are not
  modelled here. A saga bound to the previous copy across this cutover and its
  revert is checked by `ShardOwnershipCutover`
  ([`spec/shard-ownership/RefinementCutover.md`](../shard-ownership/RefinementCutover.md)).
  That module takes this one's four steps (`Swap`, `ArmRedirect`, `RevertSwap`,
  `RevertRedirect`) as its environment, relying on `RestoreReturns` and
  `RevertReturns` from here for the fairness of the two redirect steps. It adds
  the saga's re-bind, the discard of the prepares the saga leaves on the previous
  copy, and the redirect's admission of the saga's direct calls, and checks
  `AtomicAcrossCutover`, `CommittedBatchOnBoundCopy` and `SagaSettles`. It found
  #4689: a re-bound saga's prepares stay on the previous copy, and a revert serves
  them torn. The chaos test
  `ShadowCutoverAtomicVisibilityChaosTests.Cutovers_and_a_revert_of_a_resharded_tree_never_tear_an_atomic_batch`
  is load coverage only. It does not reach that race.
- **The ownership guard.** A refusal by `ITreeOwnershipGuard` is a failure
  before the swap, which `Crash` covers.