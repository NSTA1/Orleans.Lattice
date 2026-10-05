# Refinement note: BackupRestore to code

This note maps [`BackupRestore.tla`](BackupRestore.tla) - a coordinated restore
of a replicated tree across regions, its per-record admission, and the
replication that resumes after it - to the production symbols that play each
role, and to the detector tests that would notice production deviating from it.
It is a documented mapping, not a machine-checked refinement proof.

The saga's single global decision runs in the extracted core
`CrossClusterSagaDecisionCore.Decide`, which
`CrossClusterSagaCoordinatorGrain` folds its votes through and
`CoordinatedRestoreDecisionModel` drives under Coyote.

## The resume is the rebind-first design of #4490

`Ship(c)` ships only from the copy the alias resolves to, so a shipper
re-resolves its source before it ships again after a fence. Before #4490 was
fixed, production checked its binding only on the alias-change push or once
`LatticeReplicationOptions.ShipSourceIdentityBackstopInterval` had elapsed, so a
shipper whose push was lost resumed from the retired copy's log. Issue #4490
found this, and confirmed the shipper half by execution.
`RestoredCutNotReAdvancedResumeShipsRetiredLog` drops the conjunct and
reproduces production before the fix. The fix (#4498) makes
`ReplicationShipperGrain.ResumeShippingAsync` clear the shipper's
identity-resolved flag, so the first tick after a resume re-resolves and rebinds
before it reads or sends; its code analogue of that mutation, a resume that
leaves the flag set, turns the regression test red (the detector-proof log of
#4517).

## Restored copies are born closed and refuse pre-cutover admissions (#4593)

Delivery is two steps here: Ship(c) admits a write through the peer's
CACHED receive gate, stamping it with the epoch the cached answer carries, and
Land(r) applies it later to whatever copy the peer's alias resolves to then.
Before #4593 was fixed a delivery that passed a stale gate before the pause
reached the tree after the swap and landed on the restored copy;
RestoredCutNotReAdvancedCopyFenceDropped reproduces that. The fix closes the
restored copy before the swap, records the epoch of the restore's pause as its
floor, and refuses at the apply seam any landing on a closed copy or below the
floor. The floor matters after the lift as well: a delivery admitted before the
pause and landing after the copy opened is refused only by it
(RestoredCutNotReAdvancedAdmissionEpochIgnored). A causal-buffer park re-reads
the fence uncached and defers a delivery the fence is paused for, or that a
pause has superseded (RestoredCutNotReAdvancedParkWhilePaused,
RestoredCutNotReAdvancedParkKeepsStaleAdmission), so a parked entry carries
the epoch current when it parked; the drain discards one below the floor
(RestoredCutNotReAdvancedDrainIgnoresEpoch).

**What happens to a refused entry.** A refused live delivery is deferred, never
acknowledged: the sender keeps its cursor and re-ships.
RestoreConvergesRefusalAcked shows the acknowledgement would lose a
post-cutover write a stale cached epoch admitted. A re-ship cannot re-advance
the cut, because the only pre-cutover writes are in a retired log, and Ship
ships only from the copy the alias resolves to (#4490), so after a cluster's
cutover its retired log is never shipped again. A post-cutover write is
re-admitted with a fresh epoch and lands, which is where it belongs. A parked
entry below the floor is discarded: no peer ships a post-cutover write before
the saga completes globally, which is after the receiver's pause, so an entry
parked before that pause is a pre-cutover write and the restore excludes it.

In the model the closed check is subsumed by the floor for a delivery the same
tree's fence admitted: a delivery admitted while the fence was paused never
leaves the sender, so one stamped at the floor epoch was admitted after the
lift. No mutation targets the closed check alone for that reason. Production
keeps it for an apply that carries another tree's admission stamp (a cross-tree
saga's sibling finalize, which this module does not model; a terminal writes no
data and a restored copy holds no pending bucket for a saga whose prepares
predate it), and as the hook the closed-copy age gauge reports.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `alias[c]` | The copy cluster `c`'s alias resolves the tree to | The tree registry alias, moved by `AliasCutoverShardMaps.SwapCutoverAsync` inside `LatticeBackupRestoreService.CommitShadowAsync`. |
| `data[c][p]` | A copy's content | The physical tree behind each id: the previous copy, and the shadow `LatticeBackupRestoreService.BuildShadowAsync` builds. |
| `log[c][p]` | The writes a copy's WAL holds that `c` ships | The physical tree's WAL, read by `ReplicationShipperGrain`. The shadow build writes through the merge and bulk-load seams, so its WAL holds the restored records. |
| `bound[c]` | The copy `c`'s shipper is bound to | `ReplicationShipperState.BoundPhysicalTreeId`. |
| `sent[c]` | The acknowledged writes of the bound log | `ReplicationShipperState.PartitionCursors`, reset on a rebind. |
| `shipOn[c]`, `recvOn[c]` | Outbound shipping and inbound apply are not paused | `SagaWriteFenceGrain`'s shipping pause and the `ITreeReceiveFenceGrain` pause, released together on global completion. |
| `rphase` | The saga | `CrossClusterSagaCoordinatorGrain`'s phase: `CrossClusterSagaPhase.Preparing`, then `CrossClusterSagaPhase.Committed` or `CrossClusterSagaPhase.Aborted`, then completion. |
| `vote[c]` | A participant's vote, and whether it compensated | The `SagaVote` `RestoreParticipant.PrepareAsync` returns; a build that fails garbage-collects its partial shadow before voting abort. |
| `pre`, `post` | Writes made before / after the writer's cutover | Model bookkeeping that names which writes the restore must discard and which it must keep; no production counterpart. |
| `seen[c]` | `c`'s cached receive-gate answer and the epoch it was read under | `ReplicationReceiveGate.ObserveAsync`'s per-silo cache of `ReceiveFenceObservation`. |
| `epoch[c]` | `c`'s receive-fence epoch, bumped by every pause | `TreeReceiveFenceState.Epoch`, returned by `TreeReceiveFenceGrain.PauseAsync`. |
| `closed[c]` | The restored copies a restore holds receive-closed | `CopyReceiveFenceState.ClosedBySagaId` on each `CopyReceiveFenceGrain`, and `SagaWriteFenceState.ReceiveClosedCopies`. |
| `floor[c][p]` | A copy's minimum admission epoch | `CopyReceiveFenceState.MinAdmissionEpoch`. |
| `inflight` | Admitted deliveries that have not landed yet, with their admission stamp | A `ReplicationApplier.ApplyAsync` call between its gate check and the seam's routing check, carrying `ReplicationAdmissionEpoch` in the request context. |
| `parked[c]` | `c`'s causal-apply buffer, each entry with the epoch it parked under | `CausalApplyBufferState.Entries` and `ParkedCausalEntry.AdmissionEpoch`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(w)` | An application write on its author's served copy | Any write through `ILattice`, routed by the alias. **Environment argument:** not fair, any time; the write fence makes the swap one step, so no write interleaves it. | Yes: `SagaWriteFenceGrainTests.Engage_fences_every_shard_and_pauses_shipping_and_receive` pins that the cutover is write-fenced. |
| `Ship(c)` | A shipper sends one unshipped write of its bound log; the peer's cached gate admits it and stamps its epoch, or defers it while it reads paused | `ReplicationShipperGrain`'s drain tick; `ReplicationApplier` consults `IReplicationReceiveGate.ObserveAsync`, defers a receive-fenced entry unacknowledged, and stamps an admitted one with `ReplicationAdmissionEpoch.Stamp`. `ReplicationShipperGrain.ResumeShippingAsync` clears the identity-resolved flag, so the first tick after a resume re-resolves the source and rebinds before its first send (#4498). | Yes: `ReplicationShipperGrainTests.Resume_after_a_saga_pause_rebinds_to_the_restored_copy_before_its_first_send` pins the rebind before the first send, `ReplicationShipperGrainTests.Resume_without_an_alias_change_keeps_shipping_the_same_log` its no-regression arm, and `ReplicationShipperGrainTests.Pump_keeps_cursor_while_receive_fenced_then_reships_and_applies_after_lift` and `ReplicationApplierTests.ApplyAsync_flags_deferred_and_skips_apply_when_receive_fence_engaged` the deferral. |
| `Refresh(c)` | The receive-gate cache re-reads the durable fence | `ReplicationReceiveGate.ObserveAsync` on a cache miss, through `TreeReceiveFenceGrain.ObserveAsync`. **Environment argument:** the cache may stay stale for any number of steps; production bounds it by time, which the model does not rely on. | Yes: `ReplicationReceiveGateTests.An_observation_carries_the_fence_epoch_it_was_read_under` and `ReplicationReceiveGateTests.Repeated_lookups_within_the_window_hit_the_cache_once`. |
| `Land(r)` | An admitted delivery routes to the copy the alias resolves to now; the seam refuses it when that copy is closed or the stamp is below its floor, and the refusal is deferred | `LatticeGrain`'s replication apply seam marks the flow with `ReplicationApplyScope.Enter`, and every routing resolution under it checks the copy (`CopyReceiveFenceGrain.GetStatusAsync`) and throws `CopyReceiveFencedException`; `ReplicationApplier` maps it to a deferral. | Yes: `CoordinatedRestoreCopyReceiveFenceTests.A_peer_write_that_passed_the_receive_gate_before_the_pause_never_lands_on_the_restored_copy`, `CoordinatedRestoreCopyReceiveFenceTests.An_apply_admitted_before_the_restore_paused_receiving_is_refused_after_the_copy_opens`, `CoordinatedRestoreCopyReceiveFenceTests.The_applier_defers_an_entry_its_stale_gate_admitted_before_the_pause_after_the_lift`, `CoordinatedRestoreCopyReceiveFenceTests.A_routing_activation_that_resolved_the_restored_copy_for_a_local_read_still_refuses_the_apply`, and `CoordinatedRestoreCopyReceiveFenceTests.Every_replicated_write_path_refuses_a_closed_copy`. |
| `Park(r)` | An admitted delivery with unmet dependencies is parked and acknowledged, after an uncached fence re-read that defers it when the fence is paused or a pause superseded its admission | `ReplicationApplier`'s park path into `CausalApplyBufferGrain.ParkAsync`. **Environment argument:** not fair; a delivery whose dependencies are met is never parked. | Yes: `CoordinatedRestoreCopyReceiveFenceTests.A_park_while_the_receive_fence_is_paused_is_deferred_not_parked` and `CoordinatedRestoreCopyReceiveFenceTests.A_park_of_an_entry_admitted_before_the_restore_is_deferred_after_the_lift`. |
| `Drain(d, q)` | A parked entry whose dependencies are met drains to the served copy; it stays parked while the copy is closed and is discarded below the floor | `CausalApplyBufferGrain.DrainAsync` stamping each entry's parked epoch through `ReplicationApplier.ApplyDrainedEntryAsync`. | Yes: `CoordinatedRestoreCopyReceiveFenceTests.A_causal_buffer_entry_parked_before_the_restore_is_discarded_and_never_lands` and `ReplicationApplierTests.Causal_buffer_drain_leaves_an_entry_for_a_closed_restored_copy_parked`. |
| `Rebind(c)` | The shipper finds the alias moved, rebinds and resets its cursors | `ReplicationShipperGrain.NotifySourceIdentityChangedAsync` (the push) and `ReplicationShipperGrain.MaybeRefreshSourceIdentityAsync` (the backstop), both through `ReplicationShipperGrain.ApplyResolvedIdentityAsync`. | Yes: `ReplicationShipperGrainTests.NotifySourceIdentityChanged_rebinds_and_resets_cursors_without_registry_read` and `ReplicationShipperGrainTests.SourceIdentity_backstop_elapsed_triggers_re_resolve`. |
| `Build(c)` | Admission pre-flight, then the shadow build of the admitted records; vote | `RestoreParticipant.PrepareAsync` into `LatticeBackupRestoreService.ProbeAdmissionAsync` and `LatticeBackupRestoreService.BuildShadowAsync`, whose apply loops consult `IBackupRestoreAdmission.Admit` per record. **Environment argument:** the vote is unconstrained, covering every probe and build failure. | Yes: `RestoreParticipantTests.PrepareAsync_permanent_build_failure_gcs_shadow_and_votes_abort`, `RestoreParticipantTests.PrepareAsync_admission_probe_failure_votes_abort`, and `TenantBackupRestoreAdmissionTests.Cross_tenant_admission_refuses_every_record`. |
| `Decide` | The single global decision; abort is always possible (a dissent, the prepare deadline, or a lost coordinator) | `CrossClusterSagaCoordinatorGrain` folding the votes through `CrossClusterSagaDecisionCore.Decide`. | Yes: `CrossClusterSagaDecisionCoreTests.Decide_aborts_on_one_abort_and_names_it`, `CrossClusterSagaCoordinatorGrainTests.RunAsync_one_abort_aborts_and_compensates_only_prepared_participants`, and `CoordinatedRestoreDecisionCoyoteTests.Committing_on_any_vote_leaves_the_restore_mixed`. |
| `Engage(c)` | Pause receiving (bumping the epoch), close the restored copy at that epoch, engage the write fence and pause shipping | `RestoreParticipant.CommitAsync` into `SagaWriteFenceGrain.EngageAsync`, which pauses receive first, then calls `CopyReceiveFenceGrain.CloseAsync` for every copy `SagaWriteFenceRequest.ReceiveClosedCopies` names, then fences writes and pauses shipping. **Abstraction argument:** the pause, the close and the fence are one step here; production runs them in that order, and the restored copy is not routable until `CutoverSwap`, so nothing can observe the steps between. A participant that crashes after this step and before `CutoverSwap` is the interleaving in which the coordinator re-drives the idempotent commit (#4441 F9). | Yes: `SagaWriteFenceGrainTests.Engage_closes_the_restored_copy_before_it_fences_writes`, `SagaWriteFenceGrainTests.Each_restored_copy_is_closed_with_its_own_trees_pause_epoch`, `CoordinatedRestoreSetAtomicityTests.Set_restore_commit_closes_every_member_restored_copy_to_peer_writes`, `TreeReceiveFenceGrainEpochTests.Each_new_pause_bumps_the_epoch_and_a_resume_keeps_it`, and `CoordinatedRestoreCopyReceiveFenceTests.A_re_driven_commit_keeps_the_restored_copy_closed_until_the_lift` and `CoordinatedRestoreCopyReceiveFenceTests.An_abort_after_an_engage_whose_swap_never_ran_opens_the_closed_copy` for a participant interrupted between the two steps. |
| `CutoverSwap(c)` | With the fence engaged, swap the alias to the restored copy and unblock writes; the push may be lost | `RestoreParticipant.CommitAsync`: `LatticeBackupRestoreService.CommitShadowAsync`, then `SagaWriteFenceGrain.UnblockWritesAsync`; the push is `ReplicationTreeAliasObserver.OnTreeAliasChangedAsync`, which swallows a failure. **Abstraction argument:** the swap and the write unblock are one step here; the write fence is not modelled (`Write` lands on whichever copy the alias resolves to), and a participant that crashes between them leaves only local writes fenced, which the fence's deadline self-lift releases (pinned in the Detector column) while shipping and receiving stay paused. | Yes: `RestoreParticipantTests.CommitAsync_engages_fence_swaps_then_unblocks_writes`, `SagaWriteFenceGrainTests.UnblockWrites_lifts_the_write_fence_only_and_keeps_shipping_paused`, `SagaWriteFenceGrainTests.Write_fence_self_lifts_on_deadline_but_shipping_stays_paused`, and `ReplicationTreeAliasObserverTests.OnTreeAliasChanged_continues_to_other_peers_when_one_shipper_throws`. |
| `Abort(c)` | A prepared participant reverts and garbage-collects its shadow | `RestoreParticipant.AbortAsync`. | Yes: `RestoreParticipantTests.AbortAsync_after_prepare_reverts_gcs_shadow_and_lifts_fence`. |
| `TimerCompensate(c)` | A prepared participant's cutover-fence timer fires before it cut over; it asks the coordinator for the durable decision and compensates only on an abort, or on no decision, which the coordinator then records as abort | `CrossClusterSagaParticipantGrain`'s fence-expiry auto-compensation into `RestoreParticipant.AbortAsync`. **Environment argument:** not fair; the timer may fire at any moment after the vote, which over-approximates the five-minute bound. Production compensates on the timer alone, without asking the coordinator, and `CrossClusterSagaCoordinatorGrain` does not treat the refusal of the late commit as a failure, so a saga can complete restored on one cluster and not on another (#4637). `RestoreAllOrNothingTimerCompensatesUnilaterally` reproduces production. | Partial: `CrossClusterSagaParticipantGrainTests.Fence_expiry_auto_compensates_on_coordinator_loss` and `CoordinatedRestoreSagaModelTests.Coordinator_loss_before_decision_auto_compensates_prepared_clusters` pin the compensation when no decision exists; nothing pins the query of the coordinator's decision, which production does not make. Gap filed as #4637. |
| `Complete` | Global completion: every cluster cut over, or every prepared cluster compensated | The coordinator reaching its terminal phase, which `SagaWriteFenceGrain`'s poll observes. | Yes: `SagaWriteFenceGrainTests.Laggard_does_not_resume_shipping_until_global_completion`. |
| `Resume(c)` | Shipping and receiving resume together on global completion, and the restored copies open; their floors stay | `SagaWriteFenceGrain`'s release point 2 (`SagaWriteFenceGrain.LiftAsync` and the observed-completion lift), opening each copy through `CopyReceiveFenceGrain.OpenAsync`, which keeps `CopyReceiveFenceState.MinAdmissionEpoch`. | Yes: `SagaWriteFenceGrainTests.Lift_fully_releases_write_fence_shipping_and_receive`, `SagaWriteFenceGrainTests.Observed_global_completion_opens_the_restored_copies`, `SagaWriteFenceGrainTests.Deadline_self_lift_keeps_the_restored_copies_closed_and_touches_them`, and `CoordinatedRestoreCopyReceiveFenceTests.The_restored_copy_opens_when_the_saga_globally_completes`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RestoreAllOrNothing` | No cluster serves its restored copy unless every cluster voted to commit and none compensated: the coordinator commits only on a unanimous commit, and a participant that voted abort has already compensated. | Yes: `CoordinatedRestoreDecisionCoyoteTests.Production_fold_keeps_a_coordinated_restore_all_or_nothing`, `CrossClusterSagaDecisionCoreTests.Decide_aborts_on_one_abort_and_names_it` and `CrossClusterSagaCoordinatorGrainTests.RunAsync_one_abort_aborts_and_compensates_only_prepared_participants`, each red when the decision core commits on any vote. |
| `RestoredCutNotReAdvanced` | No write made before a cluster's cutover reaches a restored copy: receive pauses at the cutover, shipping and receiving resume only on global completion, and a shipper re-ships from its new copy's log. Production violated the last clause when the push was lost, until #4490 was fixed. | Yes: `ReplicationShipperGrainTests.Resume_after_a_saga_pause_rebinds_to_the_restored_copy_before_its_first_send` (red against the code analogue of `RestoredCutNotReAdvancedResumeShipsRetiredLog`) pins the rebind, `SagaWriteFenceGrainTests.Laggard_does_not_resume_shipping_until_global_completion` and `ReplicationApplierTests.ApplyAsync_flags_deferred_and_skips_apply_when_receive_fence_engaged` the pauses, and `CoordinatedRestoreCopyReceiveFenceTests.A_peer_write_that_passed_the_receive_gate_before_the_pause_never_lands_on_the_restored_copy` (red against the code analogue of `RestoredCutNotReAdvancedCopyFenceDropped`) and `CoordinatedRestoreCopyReceiveFenceTests.A_causal_buffer_entry_parked_before_the_restore_is_discarded_and_never_lands` the restored copy's fence (#4593). |
| `RestoreAdmitsOnlyNamespace` | A restore never installs a record outside the restoring tenant's namespace: the per-record admission dead-letters it. | Yes: `LatticeBackupRestoreAdmissionWiringTests.A_bulk_load_restore_dead_letters_a_record_the_admission_refuses` and `LatticeBackupRestoreAdmissionWiringTests.A_merge_restore_dead_letters_a_record_the_admission_refuses` pin that both apply loops consult the admission; `TenantBackupRestoreAdmissionTests.Cross_tenant_admission_refuses_every_record` pins the tenancy admission itself. |
| `AckedWritesServed` | A write made after a cluster's cutover is served by that cluster: writes route by the alias that moved. | Yes: `LatticeBackupRestoreIntegrationTests.RestoreAsync_shadow_cutover_swaps_alias_then_revert_restores_prior_tree`. |
| `RestoreConverges` | A restore followed by resumed replication converges: every cluster eventually serves the same content for good. Fails on a protocol defect under the spec's fairness: a resume that leaves receiving paused or the restored copy closed, a refusal that acknowledges instead of deferring, or a shipper that never re-resolves a source whose push was lost. | Yes: `ReplicationShipperGrainTests.Pump_keeps_cursor_while_receive_fenced_then_reships_and_applies_after_lift`, `ReplicationShipperGrainTests.SourceIdentity_backstop_elapsed_triggers_re_resolve`, `CoordinatedRestoreCopyReceiveFenceTests.The_applier_defers_an_entry_its_stale_gate_admitted_before_the_pause_after_the_lift`, and `ReplicationApplierTests.ApplyAsync_defers_an_entry_that_routed_to_a_closed_restored_copy`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy every property is reached and falsifiable by a
protocol-level mutation, and every liveness mutation leaves `Spec`'s fairness
intact. None needs `DEADLOCK: off`: `Refresh` (the receive-gate cache re-read)
is always enabled, so no mutant deadlocks even when it leaves a write that is
never delivered; each violates `RestoreConverges` as a liveness
counter-example instead. No mutation adds an action.

## Scope at the seams

Each item below is checked elsewhere, or argued here, or cites its open issue.

- **The participant fence timer.** Modelled as `TimerCompensate`, gated on the
  coordinator's durable decision. Production compensates on the timer alone, so
  a commit delivered past the bound leaves one cluster restored and another not;
  that is #4637, whose fix makes the participant ask the coordinator and keep
  the fence up while the coordinator is unreachable. The row above stays partial
  until it lands.
- **Reader atomicity across a set's members.** A backup set restores as one
  group whose aliases one participant step swaps together. Production swaps the
  members one after another inside one write fence, which fences writes but not
  reads, so a reader of two members can observe one restored and one not during
  the swap. The documentation makes no cross-member read-atomicity claim for a
  restore, so there is nothing to check; the guarantee checked is that the group
  commits or aborts as a unit (`RestoreAllOrNothing`).
- **In-place and cold restore.** An in-place restore merges the backup into the
  live tree by last-writer-wins (`LatticeBackupRestoreIntegrationTests.RestoreAsync_merge_into_existing_converges_by_lww_preserving_newer_entries`
  pins it), which is a convergence, not a point-in-time rollback, and is not
  coordinated. A cold restore and a restore to a new tree id build a fresh tree
  with no prior copy. Neither has a cutover, a fence or a resume, so neither has
  a protocol for this module to check; each is a single apply pinned by its
  integration tests.
- **The restored image's own consistency.** That a backup holds no saga torn is
  `BackupCapture.tla`'s; this module restores an image as one value.