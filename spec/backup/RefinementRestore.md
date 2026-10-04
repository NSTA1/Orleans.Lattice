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

## The resume is the intended design of #4490

`Ship(c)` ships only from the copy the alias resolves to, so a shipper
re-resolves its source before it ships again after a fence. Production checks
its binding only on the alias-change push or once
`LatticeReplicationOptions.ShipSourceIdentityBackstopInterval` has elapsed, so a
shipper whose push was lost resumes from the retired copy's log. Issue #4490
found this, and confirmed the shipper half by execution.
`RestoredCutNotReAdvancedResumeShipsRetiredLog` drops the conjunct and
reproduces production. The `Ship` row below cites the issue; when the fix lands
the row is re-pointed and its detector re-proved red against that mutation.

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

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(w)` | An application write on its author's served copy | Any write through `ILattice`, routed by the alias. **Environment argument:** not fair, any time; the write fence makes the swap one step, so no write interleaves it. | Yes: `SagaWriteFenceGrainTests.Engage_fences_every_shard_and_pauses_shipping_and_receive` pins that the cutover is write-fenced. |
| `Ship(c)` | A shipper sends one unshipped write of its bound log, deferred while the peer's receive is paused | `ReplicationShipperGrain`'s drain tick, `ReplicationApplier` deferring a receive-fenced entry unacknowledged. Re-resolving the source before the first post-fence send is the #4490 fix. | Partial: #4490. `ReplicationShipperGrainTests.Pump_keeps_cursor_while_receive_fenced_then_reships_and_applies_after_lift` and `ReplicationApplierTests.ApplyAsync_flags_deferred_and_skips_apply_when_receive_fence_engaged` pin the deferral; the post-fence rebind lands with the fix. |
| `Rebind(c)` | The shipper finds the alias moved, rebinds and resets its cursors | `ReplicationShipperGrain.NotifySourceIdentityChangedAsync` (the push) and `ReplicationShipperGrain.MaybeRefreshSourceIdentityAsync` (the backstop), both through `ReplicationShipperGrain.ApplyResolvedIdentityAsync`. | Yes: `ReplicationShipperGrainTests.NotifySourceIdentityChanged_rebinds_and_resets_cursors_without_registry_read` and `ReplicationShipperGrainTests.SourceIdentity_backstop_elapsed_triggers_re_resolve`. |
| `Build(c)` | Admission pre-flight, then the shadow build of the admitted records; vote | `RestoreParticipant.PrepareAsync` into `LatticeBackupRestoreService.ProbeAdmissionAsync` and `LatticeBackupRestoreService.BuildShadowAsync`, whose apply loops consult `IBackupRestoreAdmission.Admit` per record. **Environment argument:** the vote is unconstrained, covering every probe and build failure. | Yes: `RestoreParticipantTests.PrepareAsync_permanent_build_failure_gcs_shadow_and_votes_abort`, `RestoreParticipantTests.PrepareAsync_admission_probe_failure_votes_abort`, and `TenantBackupRestoreAdmissionTests.Cross_tenant_admission_refuses_every_record`. |
| `Decide` | The single global decision; abort is always possible (a dissent, the prepare deadline, or a lost coordinator) | `CrossClusterSagaCoordinatorGrain` folding the votes through `CrossClusterSagaDecisionCore.Decide`. | Yes: `CrossClusterSagaDecisionCoreTests.Decide_aborts_on_one_abort_and_names_it`, `CrossClusterSagaCoordinatorGrainTests.RunAsync_one_abort_aborts_and_compensates_only_prepared_participants`, and `CoordinatedRestoreDecisionCoyoteTests.Committing_on_any_vote_leaves_the_restore_mixed`. |
| `Commit(c)` | Engage the write fence (pausing writes, shipping and receiving), swap the alias, unblock writes; the push may be lost | `RestoreParticipant.CommitAsync`: `SagaWriteFenceGrain.EngageAsync`, `LatticeBackupRestoreService.CommitShadowAsync`, `SagaWriteFenceGrain.UnblockWritesAsync`; the push is `ReplicationTreeAliasObserver.OnTreeAliasChangedAsync`, which swallows a failure. | Yes: `RestoreParticipantTests.CommitAsync_engages_fence_swaps_then_unblocks_writes`, `SagaWriteFenceGrainTests.UnblockWrites_lifts_the_write_fence_only_and_keeps_shipping_paused`, and `ReplicationTreeAliasObserverTests.OnTreeAliasChanged_continues_to_other_peers_when_one_shipper_throws`. |
| `Abort(c)` | A prepared participant reverts and garbage-collects its shadow | `RestoreParticipant.AbortAsync`. | Yes: `RestoreParticipantTests.AbortAsync_after_prepare_reverts_gcs_shadow_and_lifts_fence`. |
| `Complete` | Global completion: every cluster cut over, or every prepared cluster compensated | The coordinator reaching its terminal phase, which `SagaWriteFenceGrain`'s poll observes. | Yes: `SagaWriteFenceGrainTests.Laggard_does_not_resume_shipping_until_global_completion`. |
| `Resume(c)` | Shipping and receiving resume together on global completion | `SagaWriteFenceGrain`'s release point 2 (`SagaWriteFenceGrain.LiftAsync`). | Yes: `SagaWriteFenceGrainTests.Lift_fully_releases_write_fence_shipping_and_receive`. |
| `Stutter` | Natural termination | Not a protocol step. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RestoreAllOrNothing` | No cluster serves its restored copy unless every cluster voted to commit and none compensated: the coordinator commits only on a unanimous commit, and a participant that voted abort has already compensated. | Yes: `CoordinatedRestoreDecisionCoyoteTests.Production_fold_keeps_a_coordinated_restore_all_or_nothing`, `RestoreSagaDispatcherTests.TryDispatchAsync_saga_abort_throws_all_or_nothing`, and `RestoreSagaDispatcherSetRestoreTests.TryDispatchSetAsync_aborted_saga_throws_all_or_nothing`. |
| `RestoredCutNotReAdvanced` | No write made before a cluster's cutover reaches a restored copy: receive pauses at the cutover, shipping and receiving resume only on global completion, and a shipper re-ships from its new copy's log. Production violates the last clause when the push is lost (#4490). | Partial: #4490. `SagaWriteFenceGrainTests.Laggard_does_not_resume_shipping_until_global_completion` and `ReplicationApplierTests.ApplyAsync_flags_deferred_and_skips_apply_when_receive_fence_engaged` pin the pauses; the rebind-before-resume lands with the fix. |
| `RestoreAdmitsOnlyNamespace` | A restore never installs a record outside the restoring tenant's namespace: the per-record admission dead-letters it. | Yes: `LatticeBackupRestoreAdmissionWiringTests.A_bulk_load_restore_dead_letters_a_record_the_admission_refuses` and `LatticeBackupRestoreAdmissionWiringTests.A_merge_restore_dead_letters_a_record_the_admission_refuses` pin that both apply loops consult the admission; `TenantBackupRestoreAdmissionTests.Cross_tenant_admission_refuses_every_record` pins the tenancy admission itself. |
| `AckedWritesServed` | A write made after a cluster's cutover is served by that cluster: writes route by the alias that moved. | Yes: `LatticeBackupRestoreIntegrationTests.RestoreAsync_shadow_cutover_swaps_alias_then_revert_restores_prior_tree`. |
| `RestoreConverges` | A restore followed by resumed replication converges: every cluster eventually serves the same content for good. Fails on a protocol defect under the spec's fairness: a resume that leaves receiving paused, or a shipper that never re-resolves a source whose push was lost. | Yes: `ReplicationShipperGrainTests.Pump_keeps_cursor_while_receive_fenced_then_reships_and_applies_after_lift` and `ReplicationShipperGrainTests.SourceIdentity_backstop_elapsed_triggers_re_resolve`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy every property is reached and falsifiable by a
protocol-level mutation, and both liveness mutations leave `Spec`'s fairness
intact. `RestoreConvergesNoBackstop` needs `DEADLOCK: off`: with the backstop
gone a shipper whose push was lost never rebinds, so the quiesced state its
stuttering successor requires is never reached. No mutation adds an action.

## Deliberate abstraction gaps

- **The participant fence timer.** A prepared participant that waits more than
  five minutes for the decision auto-compensates and refuses a later commit,
  while the coordinator does not treat that refusal as a failure. The documented
  all-or-nothing guarantee is scoped by that bound
  (`docs/lattice.replication/coordinated-restore.md`), so a commit delivered past
  it can leave one cluster restored and another not. The model's decision always
  reaches every participant; the scoped claim is stated, not checked.
- **A batch in flight across the whole cutover.** A send is atomic here: it is
  applied, or deferred unacknowledged. A real RPC sent from a pre-cutover log
  before the sender's fence and delivered after the receiver's resume is not
  modelled; the receive gate is consulted at apply time, and nothing tags a
  batch with the restore it predates.
- **Reader atomicity across a set's members.** A backup set restores as one
  group whose aliases one participant step swaps together. Production swaps the
  members one after another inside one write fence, which fences writes but not
  reads, so a reader of two members can observe one restored and one not during
  the swap. Claimed nowhere; not checked.
- **In-place and cold restore.** An in-place restore merges the backup into the
  live tree by last-writer-wins (`RestoreAsync_merge_into_existing_converges_by_lww_preserving_newer_entries`
  is its test), which is a convergence, not a point-in-time rollback, and is not
  coordinated. A cold restore and a restore to a new tree id build a fresh tree
  with no prior copy. Neither has a cutover or a resume, so neither is modelled.
- **The restored image's own consistency.** That a backup holds no saga torn is
  `BackupCapture.tla`'s; this module restores an image as one value.
