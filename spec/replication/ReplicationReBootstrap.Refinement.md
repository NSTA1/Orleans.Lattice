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
  origin. That is #4549, not yet merged, and
  `EventualConvergenceForeignRowNotReconciled` reproduces current production.

The module also states three contracts the reconcile depends on, each with a
reproducing mutation: a tombstone is reaped only once no write it beats is on
its way and every attached peer has it (#4615), an aligned receiver refuses a
batch read under a source lineage it has left (#4673), and a receiver restore
forces a re-seed (#4586).

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
  coordinated restore has run.

Every row below is Yes against that contract, or Partial citing the open issue
whose fix it waits on.

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
| `lostR` | The receiver's writes the source lost to its restore | Not a production variable: it makes the source's low watermark for the receiver's origin per lineage, which the source's tree frontier provides by re-deriving its watermarks at every lineage change (#4586). |
| `oldS` | The source's log entries written before its lineage was re-stamped | Not a production variable: it lets `Deliver` recognise a batch read under an earlier source lineage, which production cannot yet do (#4673). |
| `detached` | The receiver is detached as a peer of the source's tree | `ReplicationShipperGrain.DetachedFromLog`, set by `ReplicationShipperGrain.DetachFromLogAsync` when the peer is removed from the topology (#4534). |
| `ex` | The export in progress | The coordinator's drain over `LatticeSnapshotProvider.ExportAsync`, with the reconcile's pre-capture, carried-key bookkeeping and gates (`BootstrapDeleteReconcile.Decide`, #4647). `lwm0` is the low watermark the export carries for each origin at open, which #4549 consumes. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(o, h, d)` | A cluster commits a write, or the source deletes the key | A leaf commit or `DeleteAsync`. The HLC is above the leaf clock, which the merge path advances past every merged timestamp, and above every reaped tombstone (`reaped`). | Yes: `BPlusLeafGrainTests.MergeMany_advances_local_clock_past_incoming_max`. |
| `Deliver(e)` | A shipper delivers its next entry | The ship and apply path, which `Replication.tla` checks in full. The receiver merges by `LwwValue.Merge`: the HLC, then a tombstone wins the tie, then the value. Delivery here is FIFO and exactly once. An entry the source logged before its lineage was re-stamped, reaching a receiver already aligned with the new lineage, is refused: a batch would carry the source lineage it was read under. Production carries none, so it applies the entry and a later pass reconciles it as a delete (`ReconcileDeletesOnlyDeletedStaleLineageApplied`). | Partial (#4673): the batch carries no source lineage. The merge is covered by `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break` and `LwwValueMergeCrossSiteConvergenceTests.Merge_converges_across_sites_when_only_one_side_authored_locally`. |
| `Trim` | The source trims its log past the receiver, and its shipper requests a re-seed | A `WalRetention` trim past the receiver's cursor. The shipper's next read finds the page starting above the requested sequence and `ReplicationShipperGrain.MarkReseedRequiredAsync` records the export epoch; `ReplicationReseedResponder` starts a full bootstrap from the sender unless one from an export opened after it has completed (since #4599, the fix for #4587). | Yes: `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`, `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has` and `ReplicationReseedResponderTests.Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch`. |
| `Detach` | The receiver is detached as a peer and later re-attached | `ReplicationShipperGrain.DetachFromLogAsync` (#4534): the shipper stops holding the log, so the trim and the reap no longer wait for it, and it takes the peer off the log, so its pushes ask for a re-seed once it is re-attached. A reaped delete it missed is then repaired by the reconcile. Without the re-seed the receiver keeps the value (`EventualConvergenceReattachWithoutReseed`). | Yes: `WalGcShipperOffsetFloorTests.Detaching_a_removed_peers_shipper_releases_its_purge_hold_and_the_log` and `ReplicationDriverActivationServiceTests.ExecuteAsync_detaches_a_runtime_removed_peers_shippers_from_the_log_without_tearing_them_down`. |
| `Reap` | The source garbage-collects a tombstone | `BPlusLeafGrain.CompactTombstonesAsync` past `TombstoneGracePeriod`. The guard is #4615's design: the tombstone is below the source's applied frontier D, so no write it beats is still on its way, and below every attached peer's vouched watermark P, which a peer owed a re-seed holds back until the re-seed's export, which carries the tombstone, completes. **Production reaps on the wall clock alone**, so a write delayed past the grace period resurrects the key (`EventualConvergenceReapInsideGrace`). | Partial (#4615): no production mechanism enforces the guard. The wall-clock window itself is covered by `BPlusLeafGrainTests.CompactTombstones_does_not_block_future_passes_when_tombstones_remain_in_grace` and `BPlusLeafGrainTests.CompactTombstones_in_grace_tombstones_still_suppress_the_completeness_stamp`. |
| `Restore` | A unilateral source restore | A restore or revert, a purge and recreate, or an alias rebind on the source alone. It re-stamps `TreeRegistryEntry.Lineage`, which the reconcile's generation gate needs (`ReconcileDeletesOnlyDeletedRestoreUnseen`), and its tree frontier hears the change through `IReplicationTreeFrontierGrain.OnLineageChangingAsync` and re-derives its low watermarks from the new contents (`ReconcileDeletesOnlyDeletedWatermarkKeptAcrossRestore`). Peers keep what the restore dropped. | Partial (#4586, #4549): the contract's pins - peers keep the dropped source-origin rows and synthesise no delete, a dependent of a destroyed write is released and the uncoordinated restore is counted - and the export's per-lineage watermark are not merged. The re-stamp and the lineage gate are covered by `LatticeBackupRestoreIntegrationTests.A_shadow_cutover_restamps_the_lineage_and_its_revert_restamps_it_again`, `BootstrapDeleteReconcileTests.Decide_lineage_mismatch_with_an_orphaned_source_key_skips_without_owed_retry` and `ReapedSourceDeleteReconcileIntegrationTests.A_source_lineage_change_that_orphans_nothing_realigns_the_receiver`. |
| `CoordinatedRestore` | Every cluster cuts over to the same restore point | The coordinated-restore saga: every cluster's receive fence is held and every cluster cuts over to the same cut, after which the streams resume. The receiver aligns with the new lineage. A cut applied to one cluster only leaves them apart (`EventualConvergenceCoordinatedRestoreCutsOneCluster`). | Partial (#4586): convergence after a diverging unilateral restore is not pinned. All-or-nothing commit of the cut is covered by `CoordinatedRestoreConvergenceChaosTests.Randomized_vote_outcomes_always_converge_all_or_nothing` and `CoordinatedRestoreConvergenceChaosTests.Peer_dropping_between_prepare_and_commit_converges_to_a_full_commit`. |
| `RestoreR` | A receiver restore | A restore on the receiver re-stamps its lineage. Its tree frontier re-mints its epoch and caps every origin, its next acknowledgement carries the new lineage, and every sender answers it with a forced re-seed and a rewind after the echo (#4586). Without the re-seed the receiver never regains what its restore removed (`EventualConvergenceNoReseedOnReceiverRestamp`). | Yes: `CrossClusterAtomicVisibilityTests.A_new_receiver_lineage_after_acknowledgements_re_seeds_the_peer_and_vouches_again_only_after_the_replay`, `CrossClusterAtomicVisibilityTests.A_first_receiver_lineage_after_unvouched_acknowledgements_forces_a_re_seed_and_releases_nothing` and `ReplicationTreeFrontierGrainTests.A_replacement_re_mints_the_epoch_zeroes_and_caps_every_origin_and_forgets_identities`. |
| `Reshard` | The source's shard map changes | A split or reshard. A scan in progress may pass a key that was present throughout, so the export records the shard-map version at open and close and an unequal pass skips the reconcile and owes a retry. The lineage is unchanged (`ReconcileDeletesOnlyDeletedReshardUnseen`). | Yes: `BootstrapDeleteReconcileTests.Decide_open_close_mismatch_skips_and_owes_retry`, `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_source_generation_owes_a_retry_that_re_drains_and_reconciles` and `TreeLineageIntegrationTests.A_reshard_keeps_the_lineage`. |
| `Resize` | The source moves to a new physical tree | `ILattice.ResizeAsync`: an online copy into a new physical tree and an alias swap. Content is preserved, so the lineage token does not change, but a scan in progress may pass a key, so the physical tree is compared at export open and close (`ReconcileDeletesOnlyDeletedResizeUnseen`). | Yes: `BootstrapDeleteReconcileTests.Decide_open_close_mismatch_skips_and_owes_retry` and `TreeLineageIntegrationTests.A_resize_keeps_the_lineage`. |
| `SoftDelete` | The source tree is soft-deleted | `ILattice.DeleteTreeAsync`, which advances `TreeDeletionState.DeletionEpoch`. Production fails a read of a soft-deleted tree; the model lets the export carry nothing instead, which is the more dangerous shape, so the gate is checked against it (`ReconcileDeletesOnlyDeletedDeleteEpochUnbumped`). | Yes: `TreeLineageIntegrationTests.Every_soft_delete_advances_the_deletion_epoch_and_a_recover_keeps_it`, `TreeLineageIntegrationTests.A_logical_delete_of_an_aliased_tree_advances_its_deletion_epoch`, `BootstrapDeleteReconcileTests.Decide_deleted_at_open_skips_and_owes_retry` and `BootstrapDeleteReconcileTests.Decide_deleted_at_close_skips_and_owes_retry`. |
| `Recover` | The soft-deleted tree is recovered | `ILattice.RecoverTreeAsync`, which brings back every key written before the delete. The reconcile relies on that: a recovery that lost a key without re-stamping the lineage token would leave an absence no gate sees (`ReconcileDeletesOnlyDeletedRecoverLosesKeys`). | Yes: `TreeDeletionIntegrationTests.RecoverTree_restores_access_to_data`. |
| `BeginExport(kind)` | An export opens and the receiver pre-captures | `LatticeBootstrapCoordinatorGrain` opening `LatticeSnapshotProvider.ExportAsync`: full for a re-seed, range-scoped for the anti-entropy bootstrap fallback, and a retry pass (`reconcile`) when one is owed, which drains the rows again. The receiver pre-captures its live source-origin entries only (`ReconcileDeletesOnlyDeletedAnyOrigin`), and the export records the shard-map version, physical tree, lineage, deletion state and epoch at open (#4647). It also records the source's low watermark for every origin, which #4549 consumes. | Partial (#4549): the per-origin watermark the foreign reconcile reads is not merged. The pre-capture is covered by `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_keeps_a_third_origin_key_the_source_export_omits` and `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, and the drain's source stamp by `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_routes_snapshot_drain_through_IReplicationApplier`. |
| `ExportRow` | The scan reaches the key | The export's committed-projection pass, which also carries a retained tombstone as a committed tombstone row (since #4544, the fix for #4504), and the drain applying it. A retry pass applies its rows too (`EventualConvergenceRetryPassSkipsRows`). | Yes: `InPlaceReBootstrapDeleteIntegrationTests.Re_bootstrap_over_a_populated_receiver_deletes_a_key_the_source_deleted_while_the_receiver_was_behind`, `LatticeSnapshotProviderTests.ExportAsync_ships_a_tombstoned_entry_as_a_committed_tombstone_row`, `LeafSnapshotProviderTests.StreamAsync_projects_a_committed_tombstone_as_a_committed_delete` and `ReapedSourceDeleteReconcileIntegrationTests.An_unstable_source_generation_owes_a_retry_that_re_drains_and_reconciles`. |
| `EndExport` | The drain completes and reconciles | `BootstrapDeleteReconcile.Decide` (#4647): a pre-captured source-origin key the export did not carry is deleted at its captured HLC when the key is in scope, the source was stable across the export and the receiver is aligned with its lineage; an unstable pass owes a retry; a lineage mismatch with an orphaned source key is skipped for good; and a stable pass that orphans no source key, at open or at the end of the drain, realigns the receiver (`ReconcileDeletesOnlyDeletedRealignOnOpenCaptureOnly`). A row of another origin the export did not carry is deleted when its HLC is below the source's watermark at open, and a retry is owed while it is not (#4549, `EventualConvergenceForeignRowNotReconciled`, `ReconcileDeletesOnlyDeletedForeignAboveWatermark`). | Partial (#4549): the foreign reconcile and the end-of-drain realignment check are not merged. The source-origin reconcile is covered by `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_a_never_aligned_receiver_skips_the_reconcile`, `ReapedSourceDeleteReconcileIntegrationTests.A_receiver_holding_only_its_own_and_third_origin_rows_aligns_on_its_first_bootstrap`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_keeps_a_newer_write_that_lands_on_a_captured_key_during_the_drain`, `BootstrapDeleteReconcileTests.Decide_all_gates_pass_reconciles`, `BootstrapDeleteReconcileTests.Decide_scoped_export_skips_reconcile` and `BootstrapDeleteReconcileTests.Decide_never_aligned_with_an_orphaned_source_key_skips_reconcile`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ReconcileDeletesOnlyDeleted` | A tombstone the reconcile fabricates is dominated by a delete the source really authored, so absence from an export for any other reason - outside the scope, passed over during a reshard or resize, lost to a restore, purge or rebind, a write the source never applied, or a write the source applied under a lineage it has since left - is never turned into a delete. It holds on every behaviour, a unilateral source restore included. | Partial (#4549, #4673): the foreign reconcile is not merged and a batch carries no source lineage. The source-origin reconcile is covered by `BootstrapDeleteReconcileTests.Decide_lineage_mismatch_with_an_orphaned_source_key_skips_without_owed_retry`, `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_a_never_aligned_receiver_skips_the_reconcile` and `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_keeps_a_third_origin_key_the_source_export_omits`; its dominance argument rests on `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break`. |
| `EventualConvergence` | Once writing stops, both replicas hold the value of every write, the deletes included, on every behaviour with no unilateral source restore, even when the source reaped a delete the receiver missed; after a unilateral source restore, they agree once a coordinated restore has run. | Partial (#4549, #4615, #4586): the foreign reconcile, the reap guard and the coordinated-restore convergence pin are not merged. The reaped source delete is covered by `ReapedSourceDeleteReconcileIntegrationTests.Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped`, and a trim past the receiver by `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Territory owned by other open issues

| Issue | What it owns | Reproducing mutation |
|-------|--------------|----------------------|
| #4549 | The reconcile of a row another cluster wrote, below the source's low watermark for its origin, and the end-of-drain realignment check. | `EventualConvergenceForeignRowNotReconciled` |
| #4615 | A tombstone reaped on the wall clock alone, while a write it beats is still on its way or a peer owed a re-seed lacks it, which then resurrects the key. | `EventualConvergenceReapInsideGrace` |
| #4673 | An aligned receiver applying a batch the source read under a lineage it has left, which a later pass reconciles as a delete. | `ReconcileDeletesOnlyDeletedStaleLineageApplied` |
| #4586 | The source-restore contract's pins: peers keep a unilateral restore's dropped rows and count the restore, a dependent of a destroyed write is released, and a coordinated restore after divergence converges every cluster; and the export's per-lineage watermark. | None: the model states the contract, and production's current behaviour already matches it; the pins are detectors still to merge. |

When a fix lands, its row here and the matching gap text in the tables above
are removed, and the rows' detectors are proven red against the mutation's
production shape. #4537 landed as #4647: its rows are Yes, and its detectors
were proven red against `EventualConvergenceReapedDeleteNotReconciled` and the
gate mutations.

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkReBootstrapHlcRunaway` |
| ReconcileDeletesOnlyDeleted | Only a source-origin value is pre-captured; the source-origin reconcile is gated on scope, a stable source across the export (shard map, physical tree, soft-delete state and epoch, lineage) and the aligned lineage; the foreign reconcile on the end-scan row being below the source's per-lineage watermark at open; realignment on no source key being orphaned at open or at the end of the drain; and an aligned receiver refuses a stale-lineage batch. | Faithful for the intended design; blind for production until #4549 and #4673 land | `ReconcileDeletesOnlyDeletedOutOfScope`, `ReconcileDeletesOnlyDeletedGenerationAtExportOnly`, `ReconcileDeletesOnlyDeletedReshardUnseen`, `ReconcileDeletesOnlyDeletedResizeUnseen`, `ReconcileDeletesOnlyDeletedRestoreUnseen`, `ReconcileDeletesOnlyDeletedSoftDeleteUnseen`, `ReconcileDeletesOnlyDeletedDeleteEpochUnbumped`, `ReconcileDeletesOnlyDeletedRecoverLosesKeys`, `ReconcileDeletesOnlyDeletedAnyOrigin`, `ReconcileDeletesOnlyDeletedForeignAboveWatermark`, `ReconcileDeletesOnlyDeletedWatermarkKeptAcrossRestore`, `ReconcileDeletesOnlyDeletedRealignOnOpenCaptureOnly`, `ReconcileDeletesOnlyDeletedStaleLineageApplied` |
| EventualConvergence | Merges are confluent; a trim, a detach and a receiver restore each force a re-seed; the export carries retained tombstones; the reconcile covers reaped ones of every origin; an unstable pass and a foreign row not yet below the watermark are retried in full; the aligned comparison survives a resize; a tombstone is reaped only behind D and P; and a coordinated restore cuts every cluster to the same point. | Faithful for the intended design; blind for production until #4549, #4615 and #4586 land | `EventualConvergenceReapedDeleteNotReconciled`, `EventualConvergenceForeignRowNotReconciled`, `EventualConvergenceReapInsideGrace`, `EventualConvergenceDeliverOverwrites`, `EventualConvergenceFallOffUndetected`, `EventualConvergenceReattachWithoutReseed`, `EventualConvergenceNoReseedOnReceiverRestamp`, `EventualConvergenceCoordinatedRestoreCutsOneCluster`, `EventualConvergenceReshardSkipNotRetried`, `EventualConvergenceSoftDeleteSkipNotRetried`, `EventualConvergenceRetryPassSkipsRows`, `EventualConvergenceAlignedComparesPhysicalTree` |

No mutation adds an action, and every `EventualConvergence` mutation leaves the
fairness intact.

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
  deletes it.
- **The reap waits for every attached peer (P), and a detach releases it.**
  Without the detach, P alone would keep every reaped delete from a peer and
  the reconcile would be dead code.
- **A coordinated restore merges the leaf clock and resets the watermark** at
  the cut, or a later write stamps below the cut's value.

## Deliberate abstraction gaps

- Everything [`Replication.tla`](Replication.tla) covers - loss, duplication
  and reordering, the cycle-break, the identity cache, the causal buffer,
  dead-lettering and restarts - is out of scope here; see
  [`Refinement.md`](Refinement.md#deliberate-abstraction-gaps).
- **One key, two clusters.** The receiver's own write stands for every
  non-source origin, a third cluster's included. A third cluster's write in
  flight to the receiver during a bootstrap, which #4549's drop floor
  handles, is not modelled.
- **CRDT trees.** The reconcile is last-writer-wins only. The model has no
  CRDT delete, so it neither needs nor checks that gate.
- **The reap guard's D and P** are stated over the model's single key and
  edge; their production form is #4615's ceiling over every origin and peer.
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
