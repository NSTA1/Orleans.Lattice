# Refinement note: the re-bootstrap module to code

This note maps [`ReplicationReBootstrap.tla`](ReplicationReBootstrap.tla), a
focused companion of [`Replication.tla`](Replication.tla), to the code. It
follows the conventions of [`Refinement.md`](Refinement.md), which maps the main
module and explains the Detector column.

The module answers the one question the main module bounds out by not
modelling tombstone garbage collection. A receiver falls off the source's log
past a delete, and the source then reaps that delete's tombstone. Can an
in-place re-bootstrap still bring the receiver to the source's value? The
export carries every tombstone the source still holds (#4504, fixed by #4544),
but not a reaped one. The receiver reconciles the rest in the drain:

- **A key the source wrote.** The receiver pre-captures its live source-origin
  entries at export open and, for one the export does not carry, applies a
  delete attributed to the source at the captured HLC. That is #4537, built by
  #4647, and the mutation `EventualConvergenceReapedDeleteNotReconciled`
  reproduces production before it.
- **A key another cluster wrote.** The receiver deletes a row the export does
  not carry when its HLC is below the low watermark the source holds for its
  origin. That is #4549, built by #4675, and
  `EventualConvergenceForeignRowNotReconciled` reproduces production before it.
- **A third cluster's write still on its way.** A full re-bootstrap installs a
  drop floor at the source's per-origin applied low watermark (#4549, built
  by #4675). A third origin's late write below it is reflected in the export,
  so it is dropped rather than let resurrect a key the source deleted and
  reaped. The floor is provisional - deferring, not dropping - until its
  import closes stable, and cleared if it closes unstable; it never covers
  the source's own origin; and installing it arms the receiver's shard roots,
  so a write admitted before it cannot land after the reconcile scan.
  `EventualConvergenceNoBootstrapFloor` reproduces production before it.

The base configuration has two clusters, s and r. The Floor variant
(`ReplicationReBootstrap.Floor.cfg`, `Third = TRUE`) adds a third cluster q,
whose writes reach s and r over their own edges, with two writes and one
detach (`Reattach = FALSE`) to stay inside the per-run budget. Each of the
floor's mechanisms - the floor itself, defer-not-drop, the clear on an
unstable close, the write gate, and the per-origin reconcile - has a mutation
that fires there.

The module also states three contracts the reconcile depends on, each with a
mutation reproducing production before its fix: a tombstone is reaped only
once no write it beats is on its way and every attached peer has it (#4615,
built by #4678), an aligned receiver refuses a batch read under a source
lineage it has left (#4673, built by #4681), and a receiver restore forces a
re-seed (#4586 part 2b, #4663).

### The source-restore contract

A unilateral source restore (a restore or revert, a purge and recreate, or an
alias rebind, run on one cluster only) drops rows without deleting them. The
contract is:

- **The reconcile never turns that absence into a delete.**
  `ReconcileDeletesOnlyDeleted` holds on every behaviour, the restore
  included. Peers keep the source-origin rows the restore dropped, so the
  clusters may diverge.
- **A coordinated restore is the remedy.** It cuts every cluster to the same
  restore point, after which the replicas agree.
- **`EventualConvergence` holds on every behaviour with no unilateral source
  restore**, with no other carve-out; after one, the replicas agree once a
  coordinated restore has run, and until then the receiver keeps every write
  of another origin (`Kept`): nothing the restore did, the drop floor
  included, makes it lose one.

Every row below is Yes against that contract. Saga atomicity across a
bootstrap - a cross-tree sub-saga a per-tree import settles, purges or splits
(#4683, #4684, #4685) - is not this module's: it carries no sagas, and those
issues belong to the atomic-commit cross-cluster module and its cross-tree
bootstrap fix. No row here depends on them.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `authored` | History of every write | Not a production variable: the set the properties quantify over. |
| `wal[x]` | A cluster's own writes | The per-tree WAL a shipper drains. |
| `cursor[e]` | A shipper's acknowledged position | `ReplicationShipperState.PartitionCursors`. |
| `reg[x]` | A replica's value of the key | The leaf's `LwwValue`. `Absent` is a key with no entry, which is what a reaped tombstone leaves. `fab` marks a tombstone the reconcile fabricated, for the safety property only. |
| `clk[x]` | A leaf's clock | The leaf HLC. A reap does not lower it. |
| `reaped` | Highest reaped tombstone HLC | Not a production variable: it states that a write authored after a reap stamps above it. The reap guard that makes that true is #4615's (`Reap`). |
| `fellOff` | A re-seed of the receiver is owed | `ReplicationShipperState.ReseedRequiredEpoch`, which the shipper persists when a shipping read finds the source trimmed past its cursor (since #4599, the fix for #4587), when the peer is detached from the log, and when the receiver's lineage changes (#4586). Every push carries it until a full bootstrap from a later export clears it. |
| `booted` | The requested re-bootstrap completed | The terminal phase of `LatticeBootstrapCoordinatorGrain`. |
| `scopedDone` | The one range-scoped export has run | Not a production variable: it bounds the anti-entropy bootstrap fallback to one export. |
| `topo` | The source's shard-map version | `ShardMap.Version`, which the export records at open and close (#4647). |
| `gen` | The source's lineage token | `TreeRegistryEntry.Lineage`, re-stamped by every operation that changes content without tombstones (a create after purge, a shadow-cutover restore and its revert, an alias rebind) and by none that preserves it (a resize, a reshard, a split). |
| `restored` | The source lost the key without a delete | Not a production variable: it scopes the liveness property to the source-restore contract. |
| `phys` | The source's physical tree | The physical tree id behind the alias, which a resize changes. Compared at export open and close only, never with the aligned token. |
| `deleted` | The source tree is soft-deleted | The tree-deletion state that `ILattice.DeleteTreeAsync` sets and `ILattice.RecoverTreeAsync` clears. |
| `delDone` | The source's soft-delete epoch | `TreeDeletionState.DeletionEpoch`, which every soft delete advances, so a delete and recovery inside one export is visible. |
| `owed` | A pass is owed | The bootstrap coordinator's durable owed retry, repaid by `ILatticeBootstrapCoordinatorGrain.RetryOwedReconcileAsync`. |
| `aligned` | The source lineage the receiver's copy is aligned with | The aligned source lineage the bootstrap coordinator records on a stable pass that orphans no source key (#4647). |
| `restoredR` | A receiver restore removed the key | Not a production variable: it bounds the receiver restore to the one disruption. |
| `coordDone` | A coordinated restore has run | Not a production variable: it scopes the liveness property to the source-restore contract. |
| `lost[o]` | Origin o's writes the source lost to its restore | Not a production variable: it makes the source's low watermark for each origin per lineage, which the source's tree frontier provides by re-deriving its watermarks at every lineage change (`IReplicationTreeFrontierGrain.OnLineageChangingAsync`, #4586 part 2b). |
| `oldS` | The source's log entries written before its lineage was re-stamped | The per-partition boundary a shipper reads when its binding's lineage changes (`ReplicationShipperState.SourceLineageBoundary`, #4681); it consumes the records below it without shipping them. |
| `detached` | The receiver is detached as a peer of the source's tree | `ReplicationShipperGrain.DetachedFromLog`, set by `ReplicationShipperGrain.DetachFromLogAsync` when the peer is removed from the topology (#4534). |
| `fl[o]` | The receiver's bootstrap drop floor for third origin o | `ReplicationHighWaterMarkState.BootstrapFloor` (`ReplicationBootstrapFloor.LowWatermarks`), installed by `LatticeBootstrapCoordinatorGrain.InstallBootstrapFloorAsync` from the export's frontier for every origin but the source, and read per delivery as `ReplicationApplyAdmission.BootstrapFloor` (#4675). Zero is no floor. The held-write exemption (`ReplicationBootstrapFloor.Held`) is not exercised: the model's source holds back no write. |
| `prov` | The floor is provisional | `ReplicationBootstrapFloor.Provisional`, set by the install and cleared by `IReplicationHighWaterMarkGrain.FinalizeBootstrapFloorAsync` once the reconcile has closed against a stable source. |
| `adm[o]` | The admission at the receiver of o's next write | The floor epoch the applier stamps on a write it admits (`ReplicationFloorAdmission.Stamp`, from `ReplicationApplyAdmission.FloorEpoch`) while the write is on its way to a shard root: `fresh` under the epoch in force, `stale` under one a later install has since raised (`TreeRegistryEntry.ReplicationFloorEpoch`, armed by `ShardRootGrain.ArmReplicationFloorEpochAsync`). |
| `ex` | The export in progress | The coordinator's drain over `LatticeSnapshotProvider.ExportAsync`, with the reconcile's pre-capture, carried-key bookkeeping and gates (`BootstrapDeleteReconcile.Decide`, #4647). `lwm0` is the per-origin low watermark the export carries at open (`SnapshotSourceFrontier`), which the foreign reconcile reads (`BootstrapForeignDeleteReconcile`, #4675). |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(o, h, d)` | A cluster commits a write, or the source deletes the key | A leaf commit or `DeleteAsync`. The HLC is above the leaf clock, which the merge path advances past every merged timestamp, and above every reaped tombstone (`reaped`). | Yes: `BPlusLeafGrainTests.MergeMany_advances_local_clock_past_incoming_max`. |
| `Deliver(e)` | A shipper delivers its next entry | The ship and apply path, which `Replication.tla` checks in full. The receiver merges by `LwwValue.Merge`: the HLC, then a tombstone wins the tie, then the value. Delivery here is FIFO and exactly once. An entry the source logged before its lineage was re-stamped never applies at a receiver aligned with the new lineage (since #4681, the fix for #4673). The shipper stamps every push with the lineage its binding was read under (`ReplicationBatch.SourceLineage`) and, when that lineage changes, consumes the records below the boundary it reads without shipping them and re-seeds the peer; the receiver refuses a batch stamped with another lineage than the one its last whole-tree drain recorded (`ILatticeBootstrapCoordinatorGrain.GetDrainedLineageAsync`), or arriving after its own contents were replaced. The check runs at the applier's admission seam (`ReplicationSourceLineageGate.AdmitAsync`), which every apply entry passes through: a push, whether it applies per entry or as batched runs; an entry the causal-apply buffer drains; and a dead-letter replay. A parked or dead-lettered entry keeps the stamp it arrived under (`ParkedCausalEntry.SourceLineage`, `DeadLetterEntry.SourceLineage`); the drain discards it once refused, and a refused replay leaves it parked for the operator. The gRPC service only hands the sender's stamp to the applier and answers a refused result with a not-accepted, lineage-refused ack. Before #4681 the batch carried no lineage and applied, and a later pass reconciled it as a delete (`ReconcileDeletesOnlyDeletedStaleLineageApplied`). Until the fix for #4707 only the gRPC push checked the stamp, so an entry parked or dead-lettered before the realign applied when the drain or a replay reached it - the same stale delivery, delayed. A third origin's delivery to the receiver is `Admit` then `Land` instead. | Yes: `ReapedSourceDeleteReconcileIntegrationTests.A_batch_read_under_the_pre_restore_lineage_is_refused_once_the_receiver_drained_the_restored_lineage`, `ReapedSourceDeleteReconcileIntegrationTests.A_pre_restore_batch_arriving_after_a_coordinated_restore_cutover_is_refused`, `ReapedSourceDeleteReconcileIntegrationTests.A_multi_entry_batch_read_under_the_pre_restore_lineage_is_refused_whole`, `ReapedSourceDeleteReconcileIntegrationTests.An_entry_parked_under_the_pre_restore_lineage_is_discarded_when_the_drain_reaches_it_after_the_realign`, `ReapedSourceDeleteReconcileIntegrationTests.A_dead_letter_read_under_the_pre_restore_lineage_is_not_applied_by_a_replay_after_the_realign`, `ReapedSourceDeleteReconcileIntegrationTests.An_entry_dead_lettered_during_a_stamped_push_keeps_the_stamp_it_arrived_under`, `LatticeReplicationGrpcServiceTests.Push_hands_the_sender_stamped_lineage_to_the_applier_seam`, `LatticeReplicationGrpcServiceTests.Push_answers_a_lineage_refused_apply_with_a_not_accepted_lineage_refused_ack`, `CrossClusterAtomicVisibilityTests.An_alias_move_to_a_new_lineage_stamps_the_new_lineage_and_never_ships_what_the_restored_log_held`, `CrossClusterAtomicVisibilityTests.A_purge_and_recreate_over_the_same_log_never_ships_its_old_lineage_records`, `CrossClusterAtomicVisibilityTests.A_refusal_for_the_source_lineage_on_a_current_binding_re_seeds_the_peer`, `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break` and `LwwValueMergeCrossSiteConvergenceTests.Merge_converges_across_sites_when_only_one_side_authored_locally`. |
| `Admit(o)` | The receiver's applier admits a third origin's next write | `ReplicationApplier.ApplyAsync` and, on the batch path, `ApplyOriginRunAsync` read the origin's admission (`IReplicationHighWaterMarkGrain.GetAdmissionAsync`). Outside the bootstrap drain, a write below the bootstrap drop floor and not held (`ReplicationApplyAdmission.Drops`) is dropped, acknowledged without being merged (outcome `bootstrap-floor-dropped`), once the floor is final, and deferred, not acknowledged so the sender re-ships it, while it is provisional (`bootstrap-floor-deferred`); any other write is stamped with the floor epoch it was admitted under (#4675, the fix for #4549). A dropped write is never re-sent, so it must be one the export reflects: below the source's applied watermark (`EventualConvergenceFloorAtOriginClock`) and after a stable close (`EventualConvergenceFloorDropsWhileProvisional`). Without the floor a third origin's write still in flight resurrects a reaped delete (`EventualConvergenceNoBootstrapFloor`). | Yes: `ReplicationApplierTests.ApplyAsync_drops_a_write_below_the_bootstrap_floor_so_it_cannot_resurrect_a_deleted_key`, `ReplicationApplierTests.ApplyBatchAsync_drops_only_the_entries_below_the_floor`, `ReplicationApplierTests.ApplyAsync_defers_a_write_below_a_provisional_floor`, `ReplicationApplierTests.ApplyBatchAsync_defers_the_run_when_an_entry_is_below_a_provisional_floor`, `ReplicationApplierTests.ApplyAsync_applies_a_held_write_and_a_write_at_or_above_the_floor`, `ReplicationApplierTests.ApplyAsync_stamps_the_write_with_the_floor_epoch_it_was_admitted_under` and `ReapedSourceDeleteReconcileIntegrationTests.Bootstrap_drop_floor_stops_an_in_flight_third_origin_write_resurrecting_a_reaped_delete`. |
| `Land(o)` | The admitted write reaches a shard root | The shard root's floor gate (`ShardRootGrain.ReplicationFloorAdmission`): a write stamped with an older floor epoch than the one the shard root is armed with is refused (`ReplicationFloorAdmissionStaleException`), which the applier maps to a deferral, so the sender re-ships it and it is admitted against the new floor. `ArmReplicationFloorEpochAsync` also waits out every write already past the check, which the atomic step stands for, and a shard root activated later reads the epoch from the registry before its first stamped write. Without the gate a write admitted before the install lands after the reconcile scan and resurrects the key (`EventualConvergenceNoFloorWriteGate`). | Yes: `ShardRootGrainOptimisticReadTests.An_armed_shard_refuses_an_older_stamped_write_and_admits_current_and_unstamped_ones`, `ShardRootGrainOptimisticReadTests.Arming_waits_for_an_interleaved_write_admitted_under_an_older_epoch`, `ShardRootGrainOptimisticReadTests.A_point_write_that_waits_out_the_arming_turn_is_refused`, `ShardRootGrainOptimisticReadTests.A_shard_activated_after_the_epoch_was_raised_reads_it_from_the_registry`, `ReplicationApplierTests.ApplyAsync_defers_a_write_a_shard_refuses_as_admitted_before_a_later_floor` and `ReapedSourceDeleteReconcileIntegrationTests.A_write_admitted_before_the_floor_and_arriving_after_the_scan_is_refused_then_dropped`. |
| `Trim` | The source trims its log past the receiver, and its shipper requests a re-seed | A `WalRetention` trim past the receiver's cursor. The shipper's next read finds the page starting above the requested sequence and `ReplicationShipperGrain.MarkReseedRequiredAsync` records the export epoch; `ReplicationReseedResponder` starts a full bootstrap from the sender unless one from an export opened after it has completed (since #4599, the fix for #4587). | Yes: `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`, `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has` and `ReplicationReseedResponderTests.Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch`. |
| `Detach` | The receiver is detached as a peer and later re-attached | `ReplicationShipperGrain.DetachFromLogAsync` (#4534): the shipper stops holding the log, so the trim and the reap no longer wait for it, and it takes the peer off the log, so its pushes ask for a re-seed once it is re-attached. A reaped delete it missed is then repaired by the reconcile. Without the re-seed the receiver keeps the value (`EventualConvergenceReattachWithoutReseed`). The Floor variant takes one detach (`Reattach = FALSE`): every re-attach installs a fresh floor, which refuses the write then in flight, so only an operator who never stopped re-attaching could starve it. | Yes: `WalGcShipperOffsetFloorTests.Detaching_a_removed_peers_shipper_releases_its_purge_hold_and_the_log` and `ReplicationDriverActivationServiceTests.ExecuteAsync_detaches_a_runtime_removed_peers_shippers_from_the_log_without_tearing_them_down`. |
| `Reap` | The source garbage-collects a tombstone | `BPlusLeafGrain.CompactTombstonesBelowAsync`: past `TombstoneGracePeriod` and strictly below the replication ceiling `ITombstoneReapGate` answers (since #4678, the fix for #4615). `ReplicationTombstoneReapGate` takes the minimum of D - the receiver frontier's applied low watermark of every origin, a third origin's included (`EventualConvergenceReapIgnoresThirdOrigin`), clamped below the writes it holds unapplied (`IReplicationOriginFrontierGrain.GetMinHeldForTreeAsync`) - and P - every attached peer's vouched watermark (`IReplicationShipperGrain.GetReapLowWatermarkAsync`), zero while the peer is off the log, so a re-seed's export carries the tombstone first. A detached peer holds nothing back. Before #4678 the reap waited for the wall clock alone, and a write delayed past the grace period resurrected the key (`EventualConvergenceReapInsideGrace`). | Yes: `TombstoneReapGateIntegrationTests.A_late_write_older_than_a_delete_does_not_resurrect_the_key_while_its_origin_is_not_covered`, `TombstoneReapGateIntegrationTests.A_tombstone_is_reaped_once_every_origin_covers_it`, `CrossClusterAtomicVisibilityTests.The_reap_ceiling_is_zero_while_a_peer_is_off_the_log_and_resumes_after_its_re_seed`, `CrossClusterAtomicVisibilityTests.The_reap_ceiling_stays_below_a_parked_or_dead_lettered_write_and_ignores_a_lost_one` and `CrossClusterAtomicVisibilityTests.The_reap_gate_fails_closed_on_every_origin_it_cannot_vouch_for`. |
| `Restore` | A unilateral source restore | A restore or revert, a purge and recreate, or an alias rebind on the source alone. It re-stamps `TreeRegistryEntry.Lineage`, which the reconcile's generation gate needs (`ReconcileDeletesOnlyDeletedRestoreUnseen`), and its tree frontier hears the change through `IReplicationTreeFrontierGrain.OnLineageChangingAsync` and re-derives its low watermarks from the new contents (`ReconcileDeletesOnlyDeletedWatermarkKeptAcrossRestore`). Peers keep what the restore dropped, and synthesise no delete for it; a dependent of a write the restore destroyed is released, not parked for ever; and `ReplicationTreeLineageObserver` counts a replacement no coordinated restore fences (`orleans.lattice.replication.source_restore.uncoordinated`) and logs it (#4586 part 2b-2, #4674). | Yes: `ReapedSourceDeleteReconcileIntegrationTests.A_unilateral_source_restore_that_drops_source_rows_leaves_them_on_the_receiver_and_synthesises_no_delete`, `CrossClusterAtomicVisibilityTests.A_dependent_of_a_write_an_uncoordinated_source_restore_destroyed_is_released_not_parked_forever`, `ReplicationTreeLineageObserverTests.A_replacement_outside_a_coordinated_restore_is_counted`, `LatticeBackupRestoreIntegrationTests.A_shadow_cutover_restamps_the_lineage_and_its_revert_restamps_it_again`, `BootstrapDeleteReconcileTests.Decide_lineage_mismatch_with_an_orphaned_source_key_skips_without_owed_retry` and `ReapedSourceDeleteReconcileIntegrationTests.A_source_lineage_change_that_orphans_nothing_realigns_the_receiver`. |
| `CoordinatedRestore` | Every cluster cuts over to the same restore point | The coordinated-restore saga: every cluster's receive fence is held and every cluster cuts over to the same cut, after which the streams resume. The receiver aligns with the new lineage, and a batch read before the cut is refused because the cutover replaced the receiver's contents (#4681). A cut applied to one cluster only leaves them apart (`EventualConvergenceCoordinatedRestoreCutsOneCluster`). The cutover replaces the receiver's contents, which clears its drop floor. | Yes: `CoordinatedRestoreAfterDivergenceTests.A_coordinated_restore_after_an_uncoordinated_one_converges_every_cluster`, `ReplicationTreeLineageObserverTests.A_replacement_a_coordinated_restore_fences_is_not_counted`, `ReapedSourceDeleteReconcileIntegrationTests.A_pre_restore_batch_arriving_after_a_coordinated_restore_cutover_is_refused`, `CoordinatedRestoreConvergenceChaosTests.Randomized_vote_outcomes_always_converge_all_or_nothing` and `CoordinatedRestoreConvergenceChaosTests.Peer_dropping_between_prepare_and_commit_converges_to_a_full_commit`. Both Chaos tests run only in CI's Chaos lane. |
| `RestoreR` | A receiver restore | A restore on the receiver re-stamps its lineage. Its tree frontier re-mints its epoch and caps every origin, its next acknowledgement carries the new lineage, and every sender answers it with a forced re-seed and a rewind after the echo (#4586). Without the re-seed the receiver never regains what its restore removed (`EventualConvergenceNoReseedOnReceiverRestamp`). The replacement also clears the receiver's drop floor, which belongs to its lineage of the tree (`ReplicationHighWaterMarkGrain.ResetAppliedIdentitiesAsync`, #4675). A third origin's re-seed is modelled as a rewind of its edge to the receiver: with only the source's re-seed, a third origin's write the restore removed would never come back. | Yes: `CrossClusterAtomicVisibilityTests.A_new_receiver_lineage_after_acknowledgements_re_seeds_the_peer_and_vouches_again_only_after_the_replay`, `CrossClusterAtomicVisibilityTests.A_first_receiver_lineage_after_unvouched_acknowledgements_forces_a_re_seed_and_releases_nothing` and `ReplicationTreeFrontierGrainTests.A_replacement_re_mints_the_epoch_zeroes_and_caps_every_origin_and_forgets_identities`; the floor's clear by `ReplicationHighWaterMarkGrainTests.ResetAppliedIdentitiesAsync_clears_the_floor_durably` and `ReplicationApplierTests.A_lineage_reset_lifts_the_floor_at_the_applier`. |
| `Reshard` | The source's shard map changes | A split or reshard. A scan in progress may pass a key that was present throughout, so the export records the shard-map version at open and close and an unequal pass skips the reconcile and owes a retry. The lineage is unchanged (`ReconcileDeletesOnlyDeletedReshardUnseen`). | Yes: `BootstrapDeleteReconcileTests.Decide_open_close_mismatch_skips_and_owes_retry`, `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_source_generation_owes_a_retry_that_re_drains_and_reconciles` and `TreeLineageIntegrationTests.A_reshard_keeps_the_lineage`. |
| `Resize` | The source moves to a new physical tree | `ILattice.ResizeAsync`: an online copy into a new physical tree and an alias swap. Content is preserved, so the lineage token does not change, but a scan in progress may pass a key, so the physical tree is compared at export open and close (`ReconcileDeletesOnlyDeletedResizeUnseen`). | Yes: `BootstrapDeleteReconcileTests.Decide_open_close_mismatch_skips_and_owes_retry` and `TreeLineageIntegrationTests.A_resize_keeps_the_lineage`. |
| `SoftDelete` | The source tree is soft-deleted | `ILattice.DeleteTreeAsync`, which advances `TreeDeletionState.DeletionEpoch`. Production fails a read of a soft-deleted tree; the model lets the export carry nothing instead, which is the more dangerous shape, so the gate is checked against it (`ReconcileDeletesOnlyDeletedDeleteEpochUnbumped`). | Yes: `TreeLineageIntegrationTests.Every_soft_delete_advances_the_deletion_epoch_and_a_recover_keeps_it`, `TreeLineageIntegrationTests.A_logical_delete_of_an_aliased_tree_advances_its_deletion_epoch`, `BootstrapDeleteReconcileTests.Decide_deleted_at_open_skips_and_owes_retry` and `BootstrapDeleteReconcileTests.Decide_deleted_at_close_skips_and_owes_retry`. |
| `Recover` | The soft-deleted tree is recovered | `ILattice.RecoverTreeAsync`, which brings back every key written before the delete. The reconcile relies on that: a recovery that lost a key without re-stamping the lineage token would leave an absence no gate sees (`ReconcileDeletesOnlyDeletedRecoverLosesKeys`). | Yes: `TreeDeletionIntegrationTests.RecoverTree_restores_access_to_data`. |
| `BeginExport(kind)` | An export opens and the receiver pre-captures | `LatticeBootstrapCoordinatorGrain` opening `LatticeSnapshotProvider.ExportAsync`: full for a re-seed, range-scoped for the anti-entropy bootstrap fallback, and a retry pass (`reconcile`) when one is owed, which drains the rows again. The receiver pre-captures its live source-origin entries only (`ReconcileDeletesOnlyDeletedAnyOrigin`), and the export records the shard-map version, physical tree, lineage, deletion state and epoch at open (#4647). It also carries the source's per-origin applied low watermark and held writes (`SnapshotSourceFrontier`, read after the generation opens), which the receiver installs only under the opening lineage (#4674, #4675). Two frontiers are checked separately: the one the export opens with drives the drop floor and the reconcile (`BootstrapForeignDeleteReconcile.FrontierMatchesOpen`), and the one its close trailer carries is pinned on the tree frontier at the drain's end (`BootstrapFrontierInstall.Decide`, called by the coordinator). A full or retry pass installs the drop floor from it, provisional, for every origin but the source's own, bumps the tree's floor epoch and arms every shard root with it (`InstallBootstrapFloorAsync`, #4675); a range-scoped export installs none, because `BootstrapFallbackPlanner` re-ships its rows through the source's own write path rather than the receiver's drain. A retry's backoff (`OwedRetryInitialDelay`, doubling to `OwedRetryMaxDelay`) outlasts an admitted write's landing, so the model opens no retry pass while a third origin's write is admitted and in flight: one that always opened in that window would refuse it for ever. | Yes: `ReapedSourceDeleteReconcileIntegrationTests.A_completed_bootstrap_pins_the_exported_frontier_only_when_it_was_read_under_the_opening_lineage`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_keeps_a_third_origin_key_the_source_export_omits`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, `ReapedSourceDeleteReconcileIntegrationTests.A_frontier_read_under_another_lineage_installs_no_floor_and_reconciles_nothing`, `SnapshotSourceFrontierPlumbingTests.A_stable_export_installs_its_frontier`, `SnapshotSourceFrontierPlumbingTests.A_frontier_read_under_another_lineage_or_missing_generations_install_nothing` and `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_routes_snapshot_drain_through_IReplicationApplier`; the install's floor epoch reaching the shard roots (the registry raise and the arming) by `ReapedSourceDeleteReconcileIntegrationTests.A_write_admitted_before_the_floor_and_arriving_after_the_scan_is_refused_then_dropped`. |
| `ExportRow` | The scan reaches the key | The export's committed-projection pass, which also carries a retained tombstone as a committed tombstone row (since #4544, the fix for #4504), and the drain applying it. A retry pass applies its rows too (`EventualConvergenceRetryPassSkipsRows`). | Yes: `InPlaceReBootstrapDeleteIntegrationTests.Re_bootstrap_over_a_populated_receiver_deletes_a_key_the_source_deleted_while_the_receiver_was_behind`, `LatticeSnapshotProviderTests.ExportAsync_ships_a_tombstoned_entry_as_a_committed_tombstone_row`, `LeafSnapshotProviderTests.StreamAsync_projects_a_committed_tombstone_as_a_committed_delete` and `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_source_generation_owes_a_retry_that_re_drains_and_reconciles`. |
| `EndExport` | The drain completes and reconciles | `BootstrapDeleteReconcile.Decide` (#4647): a pre-captured source-origin key the export did not carry is deleted at its captured HLC when the key is in scope, the source was stable across the export and the receiver is aligned with its lineage; an unstable pass owes a retry; a lineage mismatch with an orphaned source key is skipped for good; and a stable pass that orphans no source key, at open or at the end of the drain, realigns the receiver (`ReconcileDeletesOnlyDeletedRealignOnOpenCaptureOnly`). A live row of another origin the export did not carry, read at the end of the drain, is deleted when its HLC is below the source's watermark for that origin and not held, and owes a retry while it is not (`BootstrapForeignDeleteReconcile`, since #4675, the fix for #4549; `EventualConvergenceForeignRowNotReconciled`, `ReconcileDeletesOnlyDeletedForeignAboveWatermark`), each against its own origin's watermark (`ReconcileDeletesOnlyDeletedForeignRowReadsReceiversWatermark`). A full or retry pass's drop floor is then made final when the pass closed stable (`IReplicationHighWaterMarkGrain.FinalizeBootstrapFloorAsync`), and cleared when it did not, so the deliveries it deferred apply when re-shipped (`EventualConvergenceUnstableImportKeepsFloor`). | Yes: `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_deletes_a_third_origin_key_the_source_applied_then_deleted_and_reaped`, `ReapedSourceDeleteReconcileIntegrationTests.A_third_origin_orphan_above_the_watermark_owes_a_retry_that_reconciles_it_once_the_watermark_passes`, `ReapedSourceDeleteReconcileIntegrationTests.A_third_origin_write_the_source_holds_is_neither_dropped_nor_reconciled`, `ReapedSourceDeleteReconcileIntegrationTests.A_source_row_taken_during_the_drain_that_the_export_lacks_blocks_alignment`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_a_never_aligned_receiver_skips_the_reconcile`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_keeps_a_newer_write_that_lands_on_a_captured_key_during_the_drain`, `BootstrapForeignDeleteReconcileTests.Classify_owes_a_retry_for_an_orphan_at_or_above_the_watermark_or_of_an_unknown_origin`, `BootstrapDeleteReconcileTests.Decide_never_aligns_over_a_source_row_taken_during_the_drain_that_the_export_lacks`, `BootstrapDeleteReconcileTests.Decide_all_gates_pass_reconciles` and `BootstrapDeleteReconcileTests.Decide_scoped_export_skips_reconcile`; the floor's close by `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_export_clears_the_drop_floor_and_infers_no_third_origin_delete` and `ReplicationHighWaterMarkGrainTests.A_floor_is_provisional_until_finalized_and_finalizing_is_durable`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ReconcileDeletesOnlyDeleted` | A tombstone the reconcile fabricates is dominated by a delete the source really authored, so absence from an export for any other reason - outside the scope, passed over during a reshard or resize, lost to a restore, purge or rebind, a write the source never applied, or a write the source applied under a lineage it has since left - is never turned into a delete. It holds on every behaviour, a unilateral source restore included. | Yes: `ReapedSourceDeleteReconcileIntegrationTests.A_unilateral_source_restore_that_drops_source_rows_leaves_them_on_the_receiver_and_synthesises_no_delete`, `ReapedSourceDeleteReconcileIntegrationTests.A_batch_read_under_the_pre_restore_lineage_is_refused_once_the_receiver_drained_the_restored_lineage`, `ReapedSourceDeleteReconcileIntegrationTests.A_third_origin_write_the_source_holds_is_neither_dropped_nor_reconciled`, `ReapedSourceDeleteReconcileIntegrationTests.A_frontier_read_under_another_lineage_installs_no_floor_and_reconciles_nothing`, `BootstrapDeleteReconcileTests.Decide_lineage_mismatch_with_an_orphaned_source_key_skips_without_owed_retry` and `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_a_never_aligned_receiver_skips_the_reconcile`; its dominance argument rests on `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break`. |
| `EventualConvergence` | Once writing stops, both replicas hold the value of every write, the deletes included, on every behaviour with no unilateral source restore, even when the source reaped a delete the receiver missed; after a unilateral source restore, they agree once a coordinated restore has run, and until then the receiver keeps every write of another origin (`Kept`): it holds that write or a later one, or the source authored a delete that beats it. | Yes: `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_deletes_a_third_origin_key_the_source_applied_then_deleted_and_reaped`, `TombstoneReapGateIntegrationTests.A_late_write_older_than_a_delete_does_not_resurrect_the_key_while_its_origin_is_not_covered`, `CoordinatedRestoreAfterDivergenceTests.A_coordinated_restore_after_an_uncoordinated_one_converges_every_cluster` and `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`; with a third origin's write in flight, `ReapedSourceDeleteReconcileIntegrationTests.Bootstrap_drop_floor_stops_an_in_flight_third_origin_write_resurrecting_a_reaped_delete`, `ReapedSourceDeleteReconcileIntegrationTests.A_write_admitted_before_the_floor_and_arriving_after_the_scan_is_refused_then_dropped` and `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_export_clears_the_drop_floor_and_infers_no_third_origin_delete`, which also pins `Kept`: a third origin's write below the floor during an unstable drain is kept, not lost. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Defects found and fixed

| Issue | Former production shape | Fixed by | Reproducing mutation |
|-------|-------------------------|----------|----------------------|
| #4537 | The drain applied only the rows the export carried, so a delete behind the trim point whose tombstone the source had reaped reached the receiver by no path. | #4647 | `EventualConvergenceReapedDeleteNotReconciled` |
| #4549 | A reaped delete of a key the receiver held under another origin was not reconciled, and a third origin's write still in flight to the receiver could resurrect it. | #4675 | `EventualConvergenceForeignRowNotReconciled`, `EventualConvergenceNoBootstrapFloor` |
| #4615 | A tombstone was reaped on the wall clock alone, so a write it beats that arrived after the grace period resurrected the key. | #4678 | `EventualConvergenceReapInsideGrace` |
| #4673 | A batch the source read under a lineage it had left applied at a receiver already aligned with the new one, and a later pass reconciled it as a delete. | #4681 | `ReconcileDeletesOnlyDeletedStaleLineageApplied` |
| #4707 | The same stale batch, parked in the causal-apply buffer or dead-lettered before the receiver realigned, applied when the drain or an operator replay reached it: only the gRPC push checked the stamp. The model's `Deliver` abstracts every delayed delivery, so the same mutation reproduces it. | #4717 | `ReconcileDeletesOnlyDeletedStaleLineageApplied` |

No open issue owns territory in this module. Every fix's detectors were proven
red against the reproducing mutation's production shape, and green once the
fix landed, in the fix's pull request. The source-restore contract's pins
landed with #4674 (#4586 part 2b-2), which also carried the export's
per-origin watermark.

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkReBootstrapHlcRunaway` |
| ReconcileDeletesOnlyDeleted | Only a source-origin value is pre-captured; the source-origin reconcile is gated on scope, a stable source across the export (shard map, physical tree, soft-delete state and epoch, lineage) and the aligned lineage; the foreign reconcile on the end-scan row being below the source's per-lineage watermark at open for the row's own origin; realignment on no source key being orphaned at open or at the end of the drain; and an aligned receiver refuses a stale-lineage batch. | Faithfully inexpressible | `ReconcileDeletesOnlyDeletedOutOfScope`, `ReconcileDeletesOnlyDeletedGenerationAtExportOnly`, `ReconcileDeletesOnlyDeletedReshardUnseen`, `ReconcileDeletesOnlyDeletedResizeUnseen`, `ReconcileDeletesOnlyDeletedRestoreUnseen`, `ReconcileDeletesOnlyDeletedSoftDeleteUnseen`, `ReconcileDeletesOnlyDeletedDeleteEpochUnbumped`, `ReconcileDeletesOnlyDeletedRecoverLosesKeys`, `ReconcileDeletesOnlyDeletedAnyOrigin`, `ReconcileDeletesOnlyDeletedForeignAboveWatermark`, `ReconcileDeletesOnlyDeletedWatermarkKeptAcrossRestore`, `ReconcileDeletesOnlyDeletedRealignOnOpenCaptureOnly`, `ReconcileDeletesOnlyDeletedStaleLineageApplied`, `ReconcileDeletesOnlyDeletedForeignRowReadsReceiversWatermark` |
| EventualConvergence | Merges are confluent; a trim, a detach and a receiver restore each force a re-seed; the export carries retained tombstones; the reconcile covers reaped ones of every origin; an unstable pass and a foreign row not yet below the watermark are retried in full; the aligned comparison survives a resize; a tombstone is reaped only behind D, over every origin, and P; a third origin's late write is dropped only below the source's applied watermark once its import has closed stable, deferred before that, admitted afresh after an unstable close, and refused by a shard root armed past the epoch it was admitted under; and a coordinated restore cuts every cluster to the same point. | Faithfully inexpressible | `EventualConvergenceReapedDeleteNotReconciled`, `EventualConvergenceForeignRowNotReconciled`, `EventualConvergenceReapInsideGrace`, `EventualConvergenceDeliverOverwrites`, `EventualConvergenceFallOffUndetected`, `EventualConvergenceReattachWithoutReseed`, `EventualConvergenceNoReseedOnReceiverRestamp`, `EventualConvergenceCoordinatedRestoreCutsOneCluster`, `EventualConvergenceReshardSkipNotRetried`, `EventualConvergenceSoftDeleteSkipNotRetried`, `EventualConvergenceRetryPassSkipsRows`, `EventualConvergenceAlignedComparesPhysicalTree`, `EventualConvergenceNoBootstrapFloor`, `EventualConvergenceFloorAtOriginClock`, `EventualConvergenceFloorDropsWhileProvisional`, `EventualConvergenceUnstableImportKeepsFloor`, `EventualConvergenceNoFloorWriteGate`, `EventualConvergenceReapIgnoresThirdOrigin` |

No mutation adds an action, and every `EventualConvergence` mutation leaves the
fairness intact. The floor's mutations run in the Floor variant's instance
(their `BOUNDS`); `EventualConvergenceReapIgnoresThirdOrigin` needs a third
write and runs with `MaxHlc = 2` instead of the variant's write bound.

`EventualConvergenceReBootstrapSkipsTombstones`, an export that carries no
retained tombstone, was retired: with the any-origin reconcile in the model, a
missing tombstone is repaired by the reconcile, so the mutation no longer
reproduces a reachable failure here. The export's tombstone rows stay checked
by the main module's `SnapshotDropsDeletes` mutation and by the `ExportRow`
detectors.

### What writing the module changed in the design

#4537's first proposal:

- **The fabricated delete is at the captured HLC t, not `succ(t)`.** A
  tombstone wins the HLC tie, so it still removes the captured value, and it
  is never above the source's delete.
- **An unstable pass must be retried, and the retry must re-apply its rows**
  (`EventualConvergenceReshardSkipNotRetried`,
  `EventualConvergenceSoftDeleteSkipNotRetried`,
  `EventualConvergenceRetryPassSkipsRows`).
- **A soft delete needs an epoch, not only a flag**
  (`ReconcileDeletesOnlyDeletedDeleteEpochUnbumped`).
- **The aligned comparison uses the lineage token alone**
  (`EventualConvergenceAlignedComparesPhysicalTree`).
- **The generation gate compares with the aligned lineage, not only export
  open with close** (`ReconcileDeletesOnlyDeletedGenerationAtExportOnly`).

The extension to #4549, #4615, #4586 and the source-restore contract:

- **A foreign row is read at the end of the drain, not pre-captured.** A
  write another cluster made during the drain is otherwise missed; while its
  HLC is not yet below the watermark, a retry is owed.
- **The watermark is per lineage.** A source restore drops what it had
  applied, so a watermark carried across it vouches for writes the new
  contents lack (`ReconcileDeletesOnlyDeletedWatermarkKeptAcrossRestore`).
  With nothing undelivered it is one past the origin's clock, not unbounded.
- **Realignment checks the end of the drain too.** A source key taken during
  the drain and not carried is an orphan the open capture cannot see
  (`ReconcileDeletesOnlyDeletedRealignOnOpenCaptureOnly`).
- **An aligned receiver must refuse a stale-lineage batch** (#4673), or a
  realigned receiver applies a write the restore dropped and a later pass
  deletes it. The refusal must hold on every apply entry, the causal-buffer
  drain and a dead-letter replay included (#4707), since either can deliver
  the batch after the realign.
- **The reap waits for every attached peer (P), and a detach releases it.**
  Without the detach, P alone would keep every reaped delete from a peer and
  the reconcile would be dead code.
- **A coordinated restore merges the leaf clock and resets the watermark** at
  the cut, or a later write stamps below the cut's value.

Porting #4549's drop floor into the module (the Floor variant), for the
#4439 confirmation pass's finding C2:

- **`EventualConvergence` needed a clause for the unremedied restore.** With
  the source's watermark exact, a provisional floor that dropped instead of
  deferring, or an unstable import that kept its floor, lost nothing the
  property checked: a retry pass re-carries the row, and after a unilateral
  source restore the property claimed nothing. `Kept` - the receiver keeps
  every write of another origin until a coordinated restore - is what makes
  `EventualConvergenceFloorDropsWhileProvisional` and
  `EventualConvergenceUnstableImportKeepsFloor` fire.
- **A receiver restore is answered by every sender's re-seed.** With only the
  source's, a third origin's write the restore removed never comes back.
  Production already re-seeds from every sender (#4586); the model now
  rewinds the third origin's edge in `RestoreR`.
- **The write gate is live only because retries back off and re-attaches are
  finite.** Every install refuses the write then in flight. A retry pass, or
  a re-attach, that always landed in that window would refuse it for ever.
  The retry's backoff (a minute, doubling) and a bounded number of operator
  re-attaches rule that out; the model encodes the first as a guard on
  `BeginExport("reconcile")` and bounds the second in the Floor variant.
- **The reap's D covers every origin, a third one's included**
  (`EventualConvergenceReapIgnoresThirdOrigin`), as production's
  `ReplicationTombstoneReapGate` already did.

## Deliberate abstraction gaps

- Everything [`Replication.tla`](Replication.tla) covers - loss, duplication
  and reordering, the cycle-break, the identity cache, the causal buffer,
  dead-lettering and restarts - is out of scope here; see
  [`Refinement.md`](Refinement.md#deliberate-abstraction-gaps).
- **One key, two clusters; three in the Floor variant.** In the base the
  receiver's own write stands for every non-source origin. The Floor variant
  adds a third cluster q that only writes: its replica, the reap's hold for
  it as a peer (P over q) and its own exports are not modelled, and its
  re-seed of the receiver is a rewind of its edge. The variant bounds writes
  at two and detaches at one to fit the per-run budget; the one floor
  mutation that needs three writes runs with fewer HLCs instead.
- **The floor's held-write exemption** is not exercised: the model's source
  applies every write it receives, so it holds none back. The exemption is
  pinned by `ReplicationApplierTests.ApplyAsync_applies_a_held_write_and_a_write_at_or_above_the_floor`,
  and held writes themselves are the low-watermark companion's
  ([`ReplicationLowWatermark.Refinement.md`](ReplicationLowWatermark.Refinement.md)).
- **The source's own origin is never floored**, as production's is not. In
  this instance flooring it would drop nothing the export does not reflect -
  the source's later writes stamp above its own watermark at open - so no
  mutation expresses it, and no detector pins the exclusion.
- **The floor is read under the export's opening lineage only.** The model
  reads the watermark atomically with the export's open, so a frontier read
  under another lineage never arises; production refuses one
  (`BootstrapForeignDeleteReconcile.FrontierMatchesOpen`).
- **The source's deliveries to the receiver are not split** into admission
  and landing. A source write a shard root refuses as stale is re-shipped,
  which only delays it, and the source's origin is never floored.
- **CRDT trees.** The reconcile is last-writer-wins only. The model has no
  CRDT delete, so it neither needs nor checks that gate.
- **The reap guard's D and P** are stated over the model's single key and
  its edges into the source; their production form is #4615's ceiling over every origin and peer.
- **At most one disruption** of either tree per behaviour: a source restore,
  a reshard, a resize, a soft delete or a receiver restore. Each gate is
  checked against the disruption it exists for; combinations are bounded out.
- **A read of a soft-deleted tree** fails in production. The model lets the
  export carry nothing instead, the shape that would mislead the reconcile.
- **A receiver restore** is taken only once the receiver's own writes have
  reached the source: one that destroys unshipped writes is a unilateral
  restore of their source, which the source-restore contract covers.
- **The coordinated restore** is one atomic step; its saga's prepare, vote
  and commit are covered by the chaos suite, not this module.
- **Receiver reaps** are not modelled, so a receiver tombstone is never
  garbage-collected.
