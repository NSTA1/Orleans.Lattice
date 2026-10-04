# Refinement note: cross-cluster TLA+ spec to code

This note maps [`AtomicCommitCrossCluster.tla`](AtomicCommitCrossCluster.tla)
to the Orleans.Lattice code that replicates an atomic-write saga to a peer
cluster and keeps it all-or-nothing visible there, so the abstract model and the
runtime artefact are traceably the same protocol. Like
[`Refinement.md`](Refinement.md), the single-cluster note it complements, it is
a **documented mapping, not a machine-checked refinement proof**.

Its scope is the **receiver**. The origin's saga is
[`AtomicCommit.tla`](AtomicCommit.tla)'s, instanced unchanged, and its own
properties are checked and mapped there. Nothing below re-maps the origin's
protocol steps; the origin rows map only what each origin step writes for
replication. Coverage of one half must not be read as coverage of the other
(issue #2324).

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `phase`, `vote`, `decision`, `terminal`, `pend`, `orphanDone`, `forgotten`, `masked`, `revision` | The origin saga and its registry | `AtomicCommit.tla`'s variables, mapped in [`Refinement.md`](Refinement.md). Only saga `t1` moves here; `t2` stays in `init`. |
| `xtree` | Shape of the replicated saga | Whether the saga's terminals carry `WalRecord.CrossTreeOperationId` (a `LatticeCrossTreeTxGrain` sub-saga) or not (a single-tree `AtomicWriteGrain` saga). Fixed per behaviour. |
| `oext[tr]` | Authoring-side delegation | The origin tree's `TxRegistryState.ExternalAuthorities` row for the saga, written by `TxRegistryGrain.RegisterExternalDecisionAuthorityAsync` and dropped when the sub-saga's finalize records the decision locally. |
| `orcv[tr]` | Receiving-side delegation on the origin | The origin tree's `TxRegistryState.ReceiverDecisionAuthorities` row. Only the public apply seam handed the origin's own terminal could write one, and the registry refuses it. |
| `outbox` | Unacknowledged replication records | The origin's WAL records past the shipper's durable per-partition cursor for this peer (`ReplicationShipperGrain`): every record is re-shipped until the receiver acknowledges it. |
| `dlv` | Delivery history | The records the receiver has applied at least once. Modelling device for stating the ordering assumption; no production counterpart. |
| `rconn` | Receiver attached to the stream | A receiver in live incremental replication, as opposed to one that will join through a snapshot bootstrap. |
| `rpend[k]` | Receiver leaf's prepared bucket | The receiver leaf's pending-tx bucket for the saga, installed by `IReplicationApplyGrain.ApplyPreparedSetAsync`. |
| `rterm[k]` | Receiver leaf's applied terminal | The receiver leaf's recently-terminal memory (`BPlusLeafGrain.ApplyTxCommit` records it even with no bucket); the late-prepare refusal's and the orphan guard's input. |
| `rproj[k]` | Receiver leaf's materialised projection | Whether the receiver leaf's committed rows hold the saga's write. |
| `rarr[tr]`, `rexp[tr]` | Receiver tally | `TxRegistryState.TerminalArrivals` and `TxRegistryState.ExpectedTerminals` on the receiver tree's registry. `rexp = 0` stands for "no entry". |
| `rdec[tr]` | Receiver registry's local decision | `TxRegistryState.Decisions` on the receiver tree's registry. |
| `rout[tr]`, `rstage[tr]` | Cross-tree hand-off progress | The outcome and position of `LatticeGrain.ApplyTxTerminalAsync`'s barrier hand-off after the tree's tally completes: register the delegation, then notify the barrier. |
| `rdeleg[tr]` | Receiver delegation to the barrier | The receiver tree's `TxRegistryState.ReceiverDecisionAuthorities` row, written by `TxRegistryGrain.RegisterReceiverDecisionAuthorityAsync`. |
| `rdial[tr]` | Barrier unreachable | A failing grain call from `TxRegistryGrain.ResolveReceiverDelegatedAsync` to the barrier. |
| `carr[tr]`, `cdec` | Barrier state | `CrossTreeReceiverState.Arrived`, and `CrossTreeReceiverState.Decided` with `CrossTreeReceiverState.Committed`, published by `LatticeCrossTreeReceiverGrain.GetDecisionAsync` once durable. |
| `rfin`, `rtodo` | Owed receiver work | Trees the barrier's decided notify told to finalize, and leaves owed the terminal by a finalized registry. Production carries both inside the apply call and re-drives them through redelivery; the model keeps them as durable obligations. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `OriginPrepare` | The origin's prepare fan-out, as replication records | `AtomicCommit`'s `PrepareTx` for the saga, plus every prepared write appended to the origin WAL with `WalRecord.IsPrepared` and the transaction id, which the shipper ships and `ReplicationApplier.ApplyAsync` routes to `IReplicationApplyGrain.ApplyPreparedSetAsync`. A cross-tree sub-saga registers its `TxRegistryState.ExternalAuthorities` row first (`TxRegistryGrain.RegisterExternalDecisionAuthorityAsync`). | Yes: `ReplicationApplierTests.ApplyAsync_routes_prepared_set_through_apply_grain_with_full_atomic_batch_metadata` pins a replicated prepare reaching the receiver as a prepared, transaction-scoped write. |
| `OriginDecide` | The origin records its decision | `AtomicCommit`'s `DecideTx`, `AtomicWriteGrain.RecordTerminalDecisionAsync`. Nothing is replicated by the decision itself: a receiver learns the outcome only from terminals. | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals`. |
| `OriginBroadcast(k)` | One source shard's terminal, as a replication record | `AtomicCommit`'s `BroadcastStep`, plus the terminal record `ShardRootGrain.AppendTxTerminalAsync` appends before its leaf fan-out, stamped with `WalRecord.AtomicShardCount` from `LatticeAtomicShardCountContext.Current`, which `AtomicWriteGrain.BroadcastTerminalsAsync` sets to the saga's touched-shard count. Every other caller of `ShardRootGrain.AppendTxTerminalAsync` (the split's retroactive sweep among them) writes a count of 0; the model has no such terminal, and `RAllOrNothingLegacyNoTallyMultiShard` shows what one does. A cross-tree sub-saga's local mark drops its `TxRegistryState.ExternalAuthorities` row before its terminals. | Yes: `AtomicWriteGrainTests.Committing_saga_stamps_its_touched_shard_count_on_every_terminal_it_broadcasts` and `AtomicWriteGrainTests.Aborting_saga_stamps_its_touched_shard_count_on_every_terminal_it_broadcasts`. |
| `OriginForget` | The origin retires the saga's registry row | `AtomicCommit`'s `ForgetDecision`, `ITxRegistryGrain.ForgetAsync`. Its only receiver consequence is what a later bootstrap export reports. | Yes: `TxRegistryGrainTests.ForgetAsync_drops_recorded_decision`. |
| `DeliverPrepare(m)` | A replicated prepare reaches its receiver leaf | `ReplicationApplier.ApplyAsync` -> `IReplicationApplyGrain.ApplyPreparedSetAsync`, which first settles the prepare against the receiver registry (`LatticeGrain.TrySettleReplicatedPrepareAsync`: `GetStatusAsync`, read through to `GetRecordedStatusAsync` on `Indeterminate`): applied as a committed write under `Committed`, dropped under `Aborted`, and otherwise staged in the leaf's pending bucket (issue #4482's fix, #4510). A prepare trailing the saga's terminal on that leaf is refused (`BPlusLeafGrain.IsLatePrepareForTerminalTransactionAsync`); the receiver registry never forgets in the model, so the settle stands in front of that refusal, which `RNoStrandedPrepareLatePrepareStaged` removes together with it. The environment the action stands in for - delivery in any order, loss, duplicates from a lost ack - is weaker than production's at-least-once, per-tree stream, so every delivery production makes is one the model makes. | Yes: `CrossClusterAtomicVisibilityTests.Cross_cluster_prepared_set_lands_invisible_until_TxCommit_arrives` (staged and hidden), `BPlusLeafGrainTests.Prepared_set_trailing_its_sagas_terminal_is_refused` (the refusal), `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_bootstrap_of_an_aborted_saga_is_dropped` and `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_the_receivers_decision_retention_still_settles_against_the_exported_abort` (the settle). |
| `DeliverTerminal(m)` | A replicated source-shard terminal reaches the receiver registry | `IReplicationApplyGrain.ApplyTxTerminalAsync` -> `ITxRegistryGrain.RecordTerminalArrivalAsync`: the ungated legacy path (`TerminalArrivalTally.IsUngated`), the dedup set, `TerminalArrivalTally.MergeExpected`, `TerminalArrivalTally.IsFinalArrival`, and the mixed-outcome guard `TerminalDecisionGuard.Classify`. On a final tally a single-tree saga marks the registry (`TxRegistryWriteRetry.MarkDecisionAsync`) and owes the fan-out; a cross-tree saga hands off to the barrier. **The guard assumes a shard's terminal is delivered after that shard's outstanding prepares.** Production provides the stronger, saga-wide form: `ReplicationShipperGrain.MergeOneBatchAsync` pulls every terminal out of its per-tick merge into a hold, released only once the peer has acknowledged every prepare of that saga the WAL holds (`ReplicationShipperGrain.TerminalHold`), and a held terminal caps its partition's durable cursor so a restart re-reads it. Stated over outstanding records, so a prepare that was never shipped cannot hold its terminal back for ever. | Yes: `CrossClusterAtomicVisibilityTests.Multi_shard_saga_through_the_applier_stays_invisible_until_every_source_shard_terminal_arrives` and `TxRegistryGrainTerminalArrivalTests.RecordTerminalArrivalAsync_with_partial_tally_reports_not_final` pin the tally; `ReplicationShipperGrainTests.Terminal_does_not_overtake_a_prepare_landing_in_a_partition_read_empty_earlier_in_the_tick`, `ReplicationShipperGrainTests.Terminal_does_not_overtake_a_prepare_queued_behind_a_higher_hlc_entry`, `ReplicationShipperGrainTests.Terminal_is_not_applied_ahead_of_a_failed_pipelined_batch_that_carried_its_prepare` and `CrossClusterAtomicVisibilityTests.Saga_shipped_from_a_skewed_multi_partition_log_is_visible_whole_on_the_receiver` pin the ordering the guard assumes, each red with the hold switched off - the production analogue of `RAllOrNothingTerminalOvertakesPrepare` (issue #4480, fixed). |
| `ReceiverRegister(tr)` | Step (a) of the barrier hand-off | `LatticeGrain.ApplyTxTerminalAsync` calling `TxRegistryGrain.RegisterReceiverDecisionAuthorityAsync`, strictly before the notify. A local decision already recorded supersedes it. | Yes: `LatticeGrainReplicationApplyTests.A_cross_tree_terminal_delegates_its_tree_to_the_barrier_until_every_tree_arrives` observes the delegation on the first tree's registry while the barrier waits. |
| `ReceiverNotify(tr)` | Step (b): the barrier records the tree and decides | `LatticeCrossTreeReceiverGrain.NotifyTerminalAsync`, through `CrossTreeReceiverBarrier.IsComplete` and `CrossTreeReceiverBarrier.CommitsAll`; it persists before it returns, which the model's single step stands for. | Yes: `LatticeCrossTreeReceiverGrainTests.NotifyTerminalAsync_first_of_two_trees_is_in_flight`, `LatticeCrossTreeReceiverGrainTests.NotifyTerminalAsync_any_abort_makes_the_global_verdict_aborted` and `CrossTreeReceiverBarrierTests.The_barrier_is_incomplete_until_every_wait_set_tree_has_arrived`. |
| `ReceiverFinalize(tr)` | A decided tree materializes its slice | `LatticeGrain.FinalizeCrossTreeTerminalCoreAsync` (inline for the notifying tree, `IReplicationApplyGrain.FinalizeCrossTreeTerminalAsync` for a sibling): `TxRegistryWriteRetry.MarkDecisionAsync` records the barrier's verdict, which drops the delegation row, then the fan-out is owed. | Yes: `LatticeGrainReplicationApplyTests.A_cross_tree_terminal_delegates_its_tree_to_the_barrier_until_every_tree_arrives`, which reads each tree's recorded decision before anything dials the barrier, because a dial caches the verdict itself and would hide a finalise that never marked the registry. |
| `ReceiverFanOut(k)` | One receiver leaf applies the recorded terminal | `LatticeGrain.ApplyTerminalPostGateAsync` -> `ShardRootGrain.AppendTxTerminalAsync` -> `BPlusLeafGrain.ApplyTxTerminalAsync`, whose bucket disposition is `MigrationTerminalCore.DecideBucketAction`. A leaf with no bucket records the terminal and materializes nothing (`BPlusLeafGrain.ApplyTxCommit`). | Yes: `LatticeGrainReplicationApplyTests.A_replicated_commit_terminal_drains_the_bucket_into_the_projection` and `LatticeGrainReplicationApplyTests.A_replicated_abort_terminal_discards_the_bucket`, which look at the leaves' buckets because a read cannot tell a drained bucket from one a committed registry surfaces. |
| `DialFault(tr)` | The receiver registry cannot reach the barrier | `TxRegistryGrain.ResolveReceiverDelegatedAsync`'s catch, answering `TxStatus.Indeterminate` and keeping the delegation row. Unfair and unordered: a failed grain call needs no ordering, so the model's guard - any time a delegation exists - is no stronger than production's. The point path answers this way, and since issue #4448's fix (#4461) so do the snapshot read paths (`TxRegistryGrain.SnapshotAsync`, `TxRegistryGrain.SnapshotWithRevisionAsync`), which carry the txid as `Indeterminate` rather than leave it out. | Yes: `TxRegistryGrainTests.GetStatusAsync_reports_indeterminate_when_the_receiver_coordinator_cannot_be_dialled` pins the point path, and `TxRegistryGrainTests.SnapshotAsync_reports_indeterminate_for_an_unreachable_receiver_coordinator` and `TxRegistryGrainTests.SnapshotWithRevisionAsync_reports_indeterminate_for_an_unreachable_receiver_coordinator` the snapshot paths. `RAllOrNothingSnapshotReadsUnresolvableAsInFlight` restores the omission and stands as #4448's regression check. |
| `ForeignOriginClaim(tr)` | The origin is handed its own cross-tree terminal | A caller of the public apply seam supplying the origin's own cluster id, which `ReplicationApplier.ApplyAsync` drops and `LatticeGrain.ApplyTxTerminalAsync` does not check; `TxRegistryGrain.ThrowIfWouldCoexist` refuses the registration. Enabled for the whole window the authoring row exists, which is every moment a coexistence could arise: before it there is no saga, and after it the tree's local decision supersedes any registration. | Yes: `TxRegistryGrainTests.RegisterReceiverDecisionAuthorityAsync_rejects_a_txid_already_delegated_outbound`. |
| `Bootstrap` | A fresh receiver joins through a snapshot | `LatticeSnapshotProvider.ExportAsync`: snap0 frozen first, then the prepared-row pass (`LatticeSnapshotProvider.EnumeratePreparedAsync`) and the committed-projection pass. A decided saga ships as committed rows, its still-resident buckets included, plus a decision row (`SnapshotEntry.SettledDecision`) the drain records in the receiver registry and never forgets (`LatticeBootstrapCoordinatorGrain.ApplySettledDecisionAsync`); a saga whose decision has aged out but is still stored ships by its recorded verdict (`ITxRegistryGrain.GetRecordedStatusAsync`, issue #4481's fix, #4501); an in-flight saga ships its buckets as prepared rows. The origin keeps a forgotten saga's row until its WAL can no longer re-ship a prepare of it (`TxRegistryGrain.IsWalPurgeCleared`, issue #4508's fix, #4553; the guard conjunct in `Bootstrap`). The shipper then resumes from its own cursors, so any retained subset of the pre-cut records is shipped again, and `DeliverPrepare`'s settle consumes it (issue #4482's fix, #4510). The model takes the export atomically. | Yes: `BootstrapAtomicVisibilityTests.ExportAsync_emits_prepared_rows_for_in_flight_saga` (the prepared rows); `BootstrapAtomicVisibilityTests.ExportAsync_ships_the_recorded_commit_when_the_decision_has_aged_out`, `BootstrapAtomicVisibilityTests.Receiver_bootstrapped_over_an_aged_out_commit_with_a_stranded_prepare_serves_the_saga_whole` and `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_bootstrap_settles_against_the_exported_commit` in its aged-out case (the recorded verdict, #4501); `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_bootstrap_of_an_aborted_saga_is_dropped` and `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_the_receivers_decision_retention_still_settles_against_the_exported_abort` (the decision rows and the settle, #4510); `TxRegistryWalPurgeGuardTests.Replicated_tree_holds_an_aged_out_decision_until_the_wal_trims_past_its_prepare` and `TxRegistryWalPurgeGuardTests.Replicated_tree_with_zero_retention_still_holds_the_decision_until_the_wal_trims` (the purge guard, #4553). `RAllOrNothingExportOverStrandedPrepare`, `RNoStrandedPrepareBootstrapReshipsPreCut` and `RNoStrandedPrepareDedupeOverPurgedDecision` restore production before each fix and stand as its regression check. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at quiescence so TLC does not report termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `RAllOrNothing` | A receiver reader never sees one of a replicated saga's keys post-saga and another pre-saga, within one tree (the tally) or across trees (the barrier). Hidden is compatible with either, as in `AtomicCommit`. | Partial: `CrossClusterAtomicVisibilityTests.Multi_shard_saga_through_the_applier_stays_invisible_until_every_source_shard_terminal_arrives` and `CrossClusterCrossTreeAtomicVisibilityTests.Cross_tree_batch_stays_invisible_until_every_tree_terminal_arrives`. Production reaches a split view the base does not where its transport loses a prepare to a peer while the saga's terminal still ships, which the base's transport never does: a dead-lettered batch (issue #4494), a WAL trim past a lagging peer under the retention TTL (issue #4534), and a trim of an unread entry whose HLC is at or below the shipper's reported cursor (issue #4579). `RCommittedEventuallyVisiblePrepareNotShipped` reaches the same cell and fires this property too. The snapshot read paths' split (issue #4448) is fixed by #4461. |
| `RStrictIsolation` | The receiver never surfaces a saga the origin did not commit: a replicated abort leaves every staged write invisible. | Yes: `CrossClusterAtomicVisibilityTests.Cross_cluster_prepared_set_remains_invisible_after_TxAbort` and `LatticeGrainReplicationApplyTests.ApplyPreparedSetAsync_followed_by_abort_terminal_keeps_value_invisible`. |
| `RLinearizedTerminals` | A receiver leaf applies the outcome the receiver registry recorded, and only after it is recorded: `LatticeGrain.ApplyTxTerminalAsync` marks before it fans out, and the fan-out carries the recorded verdict. | Yes: `LatticeGrainReplicationApplyTests.A_replicated_abort_terminal_discards_the_bucket`, which addresses the key's own source shard, so a fan-out carrying the wrong verdict reaches the bucket and surfaces the aborted write. |
| `DelegationsDisjoint` | No registry holds both delegation rows for one txid (issue #2353's premise), enforced at both registration sites. | Yes: `TxRegistryGrainTests.RegisterReceiverDecisionAuthorityAsync_rejects_a_txid_already_delegated_outbound` and `TxRegistryGrainTests.RegisterExternalDecisionAuthorityAsync_rejects_a_txid_already_delegated_inbound`. |
| `RMonotonicVisibility` | A replicated committed value, once served on the receiver, is never served pre-saga again: the fan-out drains the bucket into the projection rather than consuming it, so the value survives the registry ever ceasing to answer for the saga. | Yes: `LatticeGrainReplicationApplyTests.A_replicated_commit_terminal_drains_the_bucket_into_the_projection`. A read-only test cannot detect this row: while the registry answers Committed the gate surfaces an undrained bucket too, so the detector inspects the bucket. |
| `RCommittedEventuallyVisible` | Under at-least-once delivery every replicated committed saga is eventually materialized on every receiver leaf. Stated over materialization because a failing dial hides a delegated key for as long as it lasts. | Yes: `LatticeGrainReplicationApplyTests.A_replicated_commit_terminal_drains_the_bucket_into_the_projection`, for the reason given in the `RMonotonicVisibility` row. |
| `RNoStrandedPrepare` | Every bucket the receiver stages is eventually consumed by the saga's terminal. | Partial: `LatticeGrainReplicationApplyTests.A_replicated_commit_terminal_drains_the_bucket_into_the_projection` and `LatticeGrainReplicationApplyTests.A_replicated_abort_terminal_discards_the_bucket`. A pre-cut prepare re-shipped after a bootstrap is settled against the exported decision (issue #4482, fixed by #4510), which the origin keeps while the prepare can still be re-shipped (issue #4508, fixed by #4553): `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_bootstrap_of_an_aborted_saga_is_dropped` and `BootstrapAtomicVisibilityTests.Pre_cut_prepare_reshipped_after_the_receivers_decision_retention_still_settles_against_the_exported_abort`. Production still strands a staged bucket where its transport loses the saga's terminal to a peer, which the base's transport never does: a dead-lettered batch (issue #4494) and a WAL trim past a lagging peer under the retention TTL (issue #4534). `RNoStrandedPrepareShipperDropsTerminal` reaches that cell. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## The Detector column

The rules are [`Refinement.md`'s](Refinement.md#the-detector-column): a detector
is a test over production code, never a TLA+ mutation; the names are resolved by
`RefinementDetectorMappingTests`; and no census of the column is recorded here.
Every detector named above was shown to go red by perturbing the production code
it covers; the perturbations are listed in the pull request that added this note.

## Deliberate abstraction gaps

- **The origin is one saga.** `AtomicCommit`'s second saga never starts, so
  inter-saga last-writer-wins on a shared key is not exercised on the receiver
  either. That is the single-cluster note's per-saga-projection gap, unchanged.
- **One source shard per key, and the same layout on both clusters.** Production
  tallies source-shard indices independently of the receiver's layout and fans
  each observed index out through its split-forward closure
  (`TerminalFanOutResolver`); the model identifies a shard with a key, so a
  receiver whose shards have split since is not modelled.
- **Unstamped terminals.** The model's terminals all carry the touched-shard
  count. Production writes a count of 0 on every terminal outside the saga
  coordinator's broadcast, including the split's retroactive-sweep terminals, and
  the receiver takes them down the legacy path; for a multi-shard saga that marks
  the saga early (`RAllOrNothingLegacyNoTallyMultiShard`). That is harmless
  once every prepare of the saga has arrived, which the shipper's terminal hold
  now guarantees for every terminal, unstamped ones included: checked in TLC by
  tightening `DeliverTerminal`'s guard to that saga-wide form, under which
  `RAllOrNothing` holds with every terminal unstamped. The base keeps the
  weaker per-shard guard as the minimum contract, so the mutation still fires.
- **The bootstrap.** Modelled for the single-tree shape and taken atomically.
  Production's export is two passes against a frozen snap0 (the residual race
  `LatticeSnapshotProvider` documents, which the at-least-once re-ship covers),
  and its drain applies the rows one at a time while the receiver may serve
  reads (issue #4526). A stranded origin bucket whose decision row has already
  been purged is exported as the split the origin itself serves (#2318's
  premise): the base reaches no stranded origin bucket, for the reason
  `AtomicCommit`'s base does not, and the stranding half of
  `RAllOrNothingExportOverStrandedPrepare` alone reaches that split through
  the purged answer. The base exports a decided saga's still-resident buckets
  as committed rows; that is what keeps the settle atomic, and without it a
  reader between two prepare arrivals sees the saga split
  (`RAllOrNothingSettleKeyByKey`).
- **The transport never loses a record.** It reorders, duplicates and drops
  deliveries, but keeps every record until it is acknowledged. Production loses
  one to a peer on a dead-lettered batch (issue #4494), on a WAL trim past a
  lagging peer under the retention TTL (issue #4534), and on a trim of an
  unread entry whose HLC is at or below the shipper's reported cursor (issue
  #4579). A lost prepare whose terminal still ships splits the receiver
  (`RCommittedEventuallyVisiblePrepareNotShipped` fires `RAllOrNothing` too), and
  a lost terminal strands its staged buckets
  (`RNoStrandedPrepareShipperDropsTerminal`).
- **The receiver registry is never masked by retention.** No receiver-side
  `ForgetAsync` runs, so a receiver decision row is never tombstoned; the only
  `Indeterminate` a receiver registry gives is the failed dial.
- **No crash, reactivation or redelivery mechanics.** Production re-drives a
  partially applied terminal by redelivery; the model holds the owed work as
  durable obligations. A receiver leaf's recently-terminal memory is per
  activation, but since #4461 its late-prepare refusal also consults the
  registry's decision (issue #4445's fix), and on the replication path the
  settle stands in front of it. A reactivation is not modelled.
- **No snapshot read path as such.** The model has one read view, the point
  path's. Since #4461 the snapshot read paths give the same answer for an
  undiallable delegation (issue #4448); `RAllOrNothingSnapshotReadsUnresolvableAsInFlight`
  is the regression check.
