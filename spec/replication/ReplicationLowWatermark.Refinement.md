# Refinement note: the low-watermark module to code

This note maps [`ReplicationLowWatermark.tla`](ReplicationLowWatermark.tla), a
focused companion of [`Replication.tla`](Replication.tla), to the code. It
follows the conventions of [`Refinement.md`](Refinement.md), which maps the main
module and explains the Detector column.

The module checks the causal-dependency check #4586 built. A dependency names
one write, `(o, t)`: the write of origin `o` at HLC `t` that the author held.
The receiver meets it either on the exact identity it remembers, or when `t`
is strictly below the low watermark `S_o` the origin ships and `(o, t)` is not
one of the writes the receiver acknowledged without applying (parked,
dead-lettered or discarded as a lost mark). The per-origin high-water mark the
check used before cannot answer the question: it is the maximum HLC applied,
and an origin's HLCs reach a receiver out of order (#1060), so it released a
dependent before its dependency. `Replication.tla` and
`ReplicationCausalDelivery.tla` check `CausalOrder` with the exact-identity
half of the check; this module checks the watermark half, which is what makes
`S_o` sound.

`S_o` is downward-closed because each WAL partition seals a clock floor `F` and
refuses a fresh local stamp below it (#4586 part 1). A shipping read publishes
`(F, O)`, the floor and the offset it was in force from, and once the
shipper's acknowledged cursor reaches `O` every fresh write stamped below `F`
on that partition has been acknowledged. The tree's watermark is the minimum
over its partitions, clamped below every acknowledged prepare whose terminal is
not acknowledged (part 2b). The floor is enforced only once a capability gate
sees every active silo on a build that refuses; before that the check is
identity-only. A dead-letter queue at capacity refuses rather than evicts, and
a discarded dead letter leaves a lost mark, which dead-letters its dependents
(#4603, fixed by #4612).

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `aAuth` | Every HLC origin a authored | Not a production variable: the history the properties quantify over. |
| `aCom` | a's writes that are visible | A plain write is visible at once; a saga's prepared write once its terminal lands. |
| `alog[p]` | a's WAL partition `p` of the tree | The per-partition `WalShardGrain` log, holding plain writes, saga prepares (`prep`) and terminals (`term`). |
| `adr[p]` | The shipper's drained position | The batch the shipper has read and sent but the peer has not acknowledged. |
| `aack[p]` | The shipper's acknowledged cursor | `ReplicationShipperState.PartitionCursors`, which only acknowledgements move. |
| `floor[p]` | Partition `p`'s clock floor | The persisted floor `WalShardGrain` enforces through `WalClockFloorCore.IsSubjectToFloor`, trailing the wall clock by a lag. |
| `pub[p]` | The latest `(F, O)` a shipping read published | The pair `WalShardGrain`'s shipping read returns, which the shipper records per partition (`ReplicationShipperGrain.NoteClockFloor`). |
| `spart[p]` | The floor the acknowledged cursor has covered | The newest recorded pair whose offset the durable cursor has reached, in `ReplicationShipperGrain.ComputeTreeLowWatermark`. |
| `capable[p]` | `p`'s silo runs a build that refuses | The silo advertises `IWalClockFloorCapable` in the cluster manifest. |
| `floorOn` | The capability gate is open | `IWalClockFloorGate.IsOpen`, answered by `ClusterManifestWalClockFloorGate`. A persisted floor stays enforced after a reactivation even with the gate closed, so the model latches it. |
| `bdeps` | b's writes, each naming the a-write it depends on | A write stamped with a `LatticeVectorClockContext`; `CausalApplyBuffer.RequiredDependencies` turns it into the exact foreign writes it names. |
| `back` | b's shipper position | b's acknowledged cursor toward the receiver. |
| `rApplied` | The a-writes merged at the receiver | The receiver's replica. |
| `ids` | The identities the receiver remembers | `CausalAppliedIdentityRecord`, filled by `ReplicationHighWaterMarkGrain.AdvanceAppliedAsync`; bounded, so it forgets (`Forget`). |
| `dlq` | The receiver's dead letters of a | `ReplicationDeadLetterGrain`, bounded by `DlqCap`. |
| `lost` | Lost marks | The lost set on `ReplicationOriginFrontierGrain`, recorded by `ReplicationHighWaterMarkGrain.RecordLostAsync` before a discard removes the dead letter. |
| `dls` | Dead letters so far | Fault budget: not a production variable. |
| `bApplied` | b's dependents merged | The receiver's replica. |
| `bBuf` | b's dependents parked | `CausalApplyBufferGrain`. |
| `bDead` | b's dependents dead-lettered as `dependency_lost` | `ReplicationDeadLetterGrain`. |
| `srcv` | The receiver's recorded `S_a` | `ReplicationOriginFrontierGrain`'s effective low watermark for a: the aggregate a's shippers report across the trees, below every tree cap. |
| `wake` | A drain is pending | A pending `CausalApplyBufferGrain.DrainAsync`. |
| `losses` | Transport losses so far | Fault budget: not a production variable. |
| `booted` | The receiver bootstrapped | The terminal phase of `LatticeBootstrapCoordinatorGrain`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AuthorA(p, h, saga)` | a commits a plain write, or prepares a saga write, on partition `p` | A leaf write appended to the partition's WAL. Once the gate is open, a floor-capable silo refuses a fresh local stamp below the floor (`WalClockFloorCore.IsSubjectToFloor`), and the leaf re-stamps above it and commits once. Carried and foreign stamps are exempt. | Yes: `WalShardGrainTests.A_published_floor_refuses_an_older_fresh_append_without_assigning_an_offset`, `WalClockFloorCoreTests.A_fresh_local_stamp_below_the_floor_is_refused`, `WalClockFloorCoreTests.A_prepare_this_leaf_minted_its_original_stamp_for_is_governed` and `BPlusLeafGrainTests.SetAsync_restamps_above_a_refused_clock_floor_and_commits_once`. The first three pin the refusal; the leaf test pins only the re-stamp and single commit that follow one, and stays green with the refusal removed. |
| `CommitA(h, p)` | a saga's terminal | The saga terminal, appended to the WAL like any fresh write; the write keeps its prepare's HLC (#4566). Until the receiver acknowledges it, the shipped watermark stays below the prepare. | Yes: `WalClockFloorCoreTests.Saga_terminals_and_range_deletes_are_governed` and `CrossClusterAtomicVisibilityTests.An_acknowledged_prepare_whose_terminal_is_not_acknowledged_caps_the_watermark`. |
| `Upgrade(p)` | `p`'s silo moves to the floor-capable build | A silo starts on a build that advertises `IWalClockFloorCapable`. The gate is the cluster's, read from the manifest, never the silo's own (`CausalOrderSiloOpensItsOwnGate`). | Yes: `ClusterManifestWalClockFloorGateTests.Stays_closed_while_any_active_silo_runs_an_older_build` and `ClusterManifestWalClockFloorGateTests.Opens_when_every_active_silo_is_capable`. |
| `EnableFloor` | The capability gate opens | `ClusterManifestWalClockFloorGate` opens once every active silo's manifest advertises the marker. While it is closed no floor is published or enforced, and the shipper vouches for nothing. | Yes: `ClusterManifestWalClockFloorGateTests.IsOpen_stays_closed_while_any_active_silo_runs_an_older_build`, `ClusterManifestWalClockFloorGateTests.IsOpen_opens_once_the_last_older_build_silo_advertises_the_marker`, `ClusterManifestWalClockFloorGateTests.Stays_closed_while_an_active_silo_manifest_has_not_arrived`, `ClusterManifestWalClockFloorGateIntegrationTests.The_gate_is_open_on_every_silo_of_a_floor_capable_cluster` and `WalShardGrainTests.A_closed_gate_publishes_no_floor_and_refuses_nothing`. The `IsOpen_*` tests read the gate the way its consumers do; the `Stays_closed_*` tests call its `Evaluate` directly and so cannot see an `IsOpen` that skips it. |
| `AdvanceFloor(p)` | The floor follows the clock and is published | `WalShardGrain` persists the new floor (`WalShardGrain.PersistClockFloorAsync`), then a shipping read publishes it paired with the next offset, atomically under the append gate (`WalShardGrain.PublishClockFloorAsync`). A failed write publishes nothing. | Yes: `WalShardGrainTests.An_open_gate_persists_the_floor_before_publishing_it_with_the_next_offset`, `WalShardGrainTests.A_failed_floor_write_publishes_nothing_and_refuses_nothing` and `WalClockFloorCoreTests.The_target_trails_the_wall_clock_by_the_lag`. The persist-before-publish order is pinned by the failed-write test alone: the first test stays green when the floor is published before it is persisted. |
| `Pass(p)` | The acknowledged cursor reaches a published offset | `ReplicationShipperGrain.ComputeTreeLowWatermark` takes, per partition, the newest recorded pair whose offset the durable cursor has reached. The drained position does not count (`CausalOrderLowWatermarkFromDrainedCursor`). | Yes: `CrossClusterAtomicVisibilityTests.Shipper_vouches_for_the_published_floor_once_its_acknowledged_cursor_passes_it` and `CrossClusterAtomicVisibilityTests.Shipper_vouches_for_nothing_the_peer_has_not_acknowledged`. |
| `ShipA(p)` | The shipper sends the next entry | `ReplicationShipperGrain`'s ship paths. The cursor moves on the acknowledgement, never on the send (`CausalOrderShipAcksBeforeReceipt`). | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_does_not_advance_partition_cursor_when_ack_rejected` and `CrossClusterAtomicVisibilityTests.Shipper_vouches_for_nothing_the_peer_has_not_acknowledged`. |
| `LoseA(p)` | A transport loss | An unacknowledged batch is re-sent from the durable cursor (`CausalOrderLossCountedAsDelivered`). Bounded by `MaxLoss`. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_does_not_advance_cursor_on_negative_ack`. |
| `ReceiveA(p)` | The receiver runs the next a-entry | `ReplicationApplier.ApplyAsync`: a prepare is held invisible; a visible write merges and its identity is recorded (`ReplicationHighWaterMarkGrain.AdvanceAppliedAsync`), or, on an apply failure, is dead-lettered. A full dead-letter queue refuses, so the entry stays unacknowledged (since #4612, the fix for #4603; `EventualConvergenceDeadLetterEvictsOldest`). | Yes: `ReplicationDeadLetterGrainTests.EnqueueAsync_refuses_rather_than_evicts_when_capacity_reached`, `ReplicationApplyIntegrationTests.A_full_dead_letter_queue_defers_instead_of_evicting_and_a_discarded_write_blocks_its_dependents` and `ReplicationHighWaterMarkGrainTests.AdvanceAppliedAsync_records_identities_and_advances_only_when_asked`. |
| `Replay(h)` | An operator replays a dead letter | `ILatticeReplicationDeadLetters.ReplayAsync`: the entry runs back through the applier and is removed only once applied (`CausalOrderReplayDropsTheDeadLetter`). | Yes: `DeadLetterIntegrationTests.Replay_routes_through_inner_and_removes_parked_entry`. |
| `Discard(h)` | An operator discards a dead letter | `ReplicationDeadLetterGrain.DiscardAsync` records a foreign-origin entry as lost before removing it, and keeps it when the mark cannot be recorded (`CausalOrderDiscardLeavesNoMark`). | Yes: `ReplicationDeadLetterGrainTests.DiscardAsync_records_a_foreign_origin_entry_as_lost_before_removing_it` and `ReplicationDeadLetterGrainTests.DiscardAsync_keeps_the_entry_when_the_lost_mark_cannot_be_recorded`. |
| `Forget(h)` | The identity record forgets an identity | `CausalAppliedIdentityRecord` forgets an origin's oldest identity past its capacity; the write stays merged, and a dependency on it is met once `S_a` passes it. | Yes: `CausalAppliedIdentityRecordTests.Past_its_capacity_the_record_forgets_the_oldest_identity_of_the_origin` and `ReplicationHighWaterMarkGrainTests.A_forgotten_identity_is_met_once_the_origin_low_watermark_passes_it`. |
| `AuthorB(t)` | b writes a dependent of an a-write it holds | A write under a `LatticeVectorClockContext` naming the write the author held; `CausalApplyBuffer.RequiredDependencies` names each foreign write exactly (`CausalOrderDependencyNamesAnUnheldWrite` names one it does not hold). | Yes: `CausalApplyBufferTests.RequiredDependencies_names_each_foreign_write_exactly` and `PublicApiContractTests.LatticeVectorClockContext_With_stamps_VectorClock_on_mutation`. |
| `ReceiveB` | The receiver runs b's next dependent | `ReplicationHighWaterMarkGrain.CheckDependenciesAsync`: a lost dependency dead-letters the dependent as `dependency_lost`; a dependency met on the identity record, or by `CausalFrontierCore.Decide` (strictly below `S_a` and not held), merges it; anything else parks it. | Yes: `ReplicationApplyIntegrationTests.An_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first`, `ReplicationApplyIntegrationTests.A_batched_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first`, `CausalFrontierCoreTests.The_low_watermark_is_strict`, `CausalFrontierCoreTests.A_held_write_below_the_low_watermark_is_unmet`, `ReplicationApplyIntegrationTests.A_write_held_in_another_trees_buffer_keeps_its_dependent_parked_past_the_low_watermark` and `ReplicationApplyIntegrationTests.A_dependent_of_a_discarded_write_is_dead_lettered_even_when_the_high_water_mark_passed_it`. |
| `Drain` | The buffer releases a dependent | `CausalApplyBufferGrain.DrainAsync`, under the same check. One lost mark holds back only its own dependents (`EventualConvergenceLostMarkPinsLowWatermark`). | Yes: `CausalFrontierCoreTests.One_lost_mark_does_not_pin_other_writes_of_the_origin`, `ReplicationApplyIntegrationTests.A_dependency_on_a_write_another_tree_applied_waits_for_the_origin_low_watermark` and `ReplicationApplierTests.ApplyAsync_drains_buffered_entries_when_dependency_arrives`. |
| `Heartbeat` | The receiver records a new `S_a` and a drain runs | The watermark rides on every batch and, on an idle link, on a liveness probe. `ReplicationTreeFrontierGrain.ObserveAsync` accepts it and forwards the aggregate to `IReplicationOriginFrontierGrain.RecordLowWatermarkAsync`, the minimum over a's trees. The shipper computes it as the minimum over partitions, clamped below acknowledged prepares without terminals (`CausalOrderLowWatermarkMaxOverShippers`, `CausalOrderPrepareClampOmitted`). A parked dependent is re-checked by the drain every `ReplicationMaintenanceGrain` tick runs, which the model's re-arm on record over-approximates in timing only (`EventualConvergenceRecordedLowWatermarkDoesNotRearm`). | Yes: `CrossClusterAtomicVisibilityTests.An_idle_link_carries_an_advanced_watermark_on_a_liveness_probe`, `LatticeReplicationGrpcServiceSourceFrontierTests.A_configured_peers_frontier_is_recorded_and_the_ack_reports_the_epoch`, `ReplicationSourceFrontierAggregateGrainTests.Aggregate_is_the_minimum_over_every_replicated_tree_and_zero_while_one_has_none`, `ReplicationOriginFrontierGrainTests.The_low_watermark_only_rises` and `ReplicationMaintenanceGrainTests.ProcessNextPhaseAsync_drains_the_causal_apply_buffer_every_tick`. |
| `Bootstrap` | The receiver installs a snapshot | `LatticeBootstrapCoordinatorGrain` installs the export's per-origin applied watermarks at the handoff through `ReplicationTreeFrontierGrain.PinAsync`, as a pointwise maximum, after publishing the writes the source held back (#4586 part 2b-2). The source's own origin is omitted from its export, so the snapshot source here is a peer holding every committed a-write. Installing the source's high-water mark instead is `CausalOrderBootstrapInstallsHighWaterMark`. | Yes: `ReplicationTreeFrontierGrainTests.A_pin_installs_the_export_watermark_and_keeps_the_cap_until_the_origin_re_covers_the_tree`, `ReplicationTreeFrontierGrainTests.A_pin_publishes_the_writes_the_source_held_before_any_watermark` and `SnapshotSourceFrontierPlumbingTests.A_stable_export_installs_its_frontier`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `CausalOrder` | The receiver never merges a dependent before the write its dependency names: the identity record names only writes it merged, and `S_a` vouches only for writes acknowledged and not held back. | Yes: `ReplicationApplyIntegrationTests.An_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first`, `ReplicationApplyIntegrationTests.A_batched_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first`, `CausalFrontierCoreTests.A_held_write_below_the_low_watermark_is_unmet`, `CrossClusterAtomicVisibilityTests.Shipper_vouches_for_nothing_the_peer_has_not_acknowledged` and `WalShardGrainTests.A_published_floor_refuses_an_older_fresh_append_without_assigning_an_offset`. |
| `EventualConvergence` | Every committed a-write is eventually merged or lost, and every dependent eventually merges, or is dead-lettered because its dependency was lost: the floor follows the clock, so `S_a` passes every acknowledged write; a forgotten identity is met by it; one lost mark blocks only its own dependents; a full dead-letter queue defers rather than evicts. | Yes: `ReplicationHighWaterMarkGrainTests.A_forgotten_identity_is_met_once_the_origin_low_watermark_passes_it`, `CausalFrontierCoreTests.One_lost_mark_does_not_pin_other_writes_of_the_origin`, `ReplicationApplyIntegrationTests.A_full_dead_letter_queue_defers_instead_of_evicting_and_a_discarded_write_blocks_its_dependents` and `ReplicationMaintenanceGrainTests.ProcessNextPhaseAsync_drains_the_causal_apply_buffer_every_tick`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Defects found and fixed

| Issue | Former production shape | Fixed by | Reproducing mutation |
|-------|-------------------------|----------|----------------------|
| #4586 | A dependency was met once the origin's high-water mark reached its HLC; the bootstrap pin installed the source's high-water vector. Neither is downward-closed, so a dependent was released before its dependency. | #4650, #4640, #4663, #4658, #4674 | `CausalOrderBootstrapInstallsHighWaterMark` here; `CausalOrderMaxHlcFrontier` in the main and causal-delivery modules |
| #4603 | A full dead-letter queue evicted its oldest entry, and a discarded dead letter left no trace, so its dependents were released or stranded. | #4612 | `EventualConvergenceDeadLetterEvictsOldest`, `CausalOrderDiscardLeavesNoMark` |

Every fix's detectors were proven red against the reproducing mutation's
production shape and green once the fix landed, in the fix's pull request.

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkLowWatermarkRunaway` |
| CausalOrder | A dependency is met only on an identity the receiver merged, or strictly below a watermark that is the minimum over partitions of a floor the acknowledged cursor covered, clamped below open prepares, published atomically with its offset, enforced only behind a cluster-wide gate, and excluding every write held back; a dependent names a write its author held. | Faithfully inexpressible | `CausalOrderFloorGuardDropped`, `CausalOrderGateOpensBeforeEverySiloCapable`, `CausalOrderSiloOpensItsOwnGate`, `CausalOrderFloorPublishedBeforeLastAppend`, `CausalOrderLowWatermarkFromDrainedCursor`, `CausalOrderShipAcksBeforeReceipt`, `CausalOrderLossCountedAsDelivered`, `CausalOrderLowWatermarkMaxOverShippers`, `CausalOrderPrepareClampOmitted`, `CausalOrderHeldBackOmitted`, `CausalOrderLowWatermarkInclusive`, `CausalOrderDiscardLeavesNoMark`, `CausalOrderReplayDropsTheDeadLetter`, `CausalOrderEvictionForgetsTheWrite`, `CausalOrderDependencyNamesAnUnheldWrite`, `CausalOrderBootstrapInstallsHighWaterMark` |
| EventualConvergence | Floors advance and are published, passed and shipped fairly; a terminal follows every prepare; a full queue defers; held writes are excluded per identity rather than as a minimum; recording a watermark re-arms the drain. | Faithfully inexpressible | `EventualConvergenceDeadLetterEvictsOldest`, `EventualConvergenceLostMarkPinsLowWatermark`, `EventualConvergenceRecordedLowWatermarkDoesNotRearm`, `EventualConvergenceCommitLogsNoTerminal` |

Most mutations raise only the bounds of the feature they need through their
`BOUNDS:` header; each variant configuration checks one such feature on one
partition with every property. No mutation adds an action, and every
`EventualConvergence` mutation leaves the fairness intact.

## Deliberate abstraction gaps

- **One origin, one dependent writer, one receiver, one tree.** Production's
  watermark is per (tree, lineage, origin) and aggregated over the trees
  (`ReplicationSourceFrontierAggregateGrain`); the aggregate is a minimum, so
  one tree stands for it. The per-tree identity record and the per-tree
  causal buffers are one here.
- **Lineage.** A receiver re-stamp caps the origin's watermark until it is
  re-seeded, and a source restore re-derives it; both are the re-bootstrap
  companion's ([`ReplicationReBootstrap.Refinement.md`](ReplicationReBootstrap.Refinement.md)),
  and a newer aggregate generation, which may lower the value, is never
  exhibited here: `srcv` only rises.
- **The shipper's other clamps and freezes.** A prepare it could not track, a
  batch the cursor passed without delivering, a peer off the log, a replay
  filter, an unreported lineage and a key-filtered tree each clamp or withhold
  the watermark. Each only lowers it, so the model's watermark is an upper
  bound of production's.
- **The drain re-arm.** The model re-arms a drain when a watermark is
  recorded; production re-checks the buffer on every maintenance tick. Both
  are fair, so only timing differs.
- **Clock and lag.** The floor rises by one per step and is enforced at once;
  production's trails the wall clock by a lag and moves only once it trails
  its target by half a lag. Re-stamping a refused write and the caller-fixed
  stamps (idempotency keys, nested range deletes) are covered by their own
  detectors, not modelled.
- **Bounds.** Two partitions, two HLCs and two a-writes in the base; sagas,
  transport loss, a second dead letter and the bootstrap each on one partition
  in a variant.
