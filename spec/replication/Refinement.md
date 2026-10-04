# Refinement note: the replication TLA+ module to code

This note maps the TLA+ specification in [`Replication.tla`](Replication.tla)
to plain (non-saga) cross-cluster replication as it exists in
`src/lattice.replication/`, so the model and the running code are traceably
the same protocol and any divergence is visible.

It is a **documented mapping, not a machine-checked refinement proof.** The
value is that a change to the replication design shows here which spec action,
which production seam and which detector must move together.

## A note on what the module models

The module models the **intended** design. Three production defects found
while writing it were fixed by their own issues (#4463, #4464 and #4465), so
production now implements the design the module checks. Each defect keeps a
mutation that reproduces its former production shape and makes a property
fire: the standing check that reintroducing it is caught. The list is under
[defects found and fixed](#defects-found-and-fixed).

The decisions the module checks run in production through pure cores:
`ReplicationShipEligibility` (the shipper's cycle-break and its legacy scalar
cursor filter), `ReplicationReceiveDedup` (the receiver's cycle-break and
the monotone high-water mark) and the existing
`CausalApplyBuffer.DependenciesSatisfied`. The Coyote models in
`test/lattice.replication/Coyote/` execute the same cores.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `authored` | History of every write | Not a production variable: the set of writes the properties quantify over. |
| `wal[x]` | A cluster's write-ahead log | The per-tree WAL a shipper drains. It holds local writes and, because the WAL is the sole durability boundary, every write the cluster applied from a peer with its authoring origin preserved (`LatticeGrain.ReplicationApply` stamps it through `LatticeOriginContext`). One sequence per cluster stands for production's several key-hashed partitions; see [abstraction gaps](#deliberate-abstraction-gaps). |
| `cursor[e]` | A shipper's acknowledged position | The durable per-partition sequence cursor `ReplicationShipperState.PartitionCursors`, advanced in `ReplicationShipperGrain.AdvanceCursorAsync`. The scalar HLC cursor `ReplicationShipperState.Cursor` is not a skip criterion outside the legacy-migration tick (`ReplicationShipEligibility.IsBelowLegacyScalarCursor`), so the model has no variable for it. |
| `val[x][k]` | A replica's state for a key | The set of writes merged into the replica. Each merge mode is a join-homomorphism from that set (`ValueAt`): last-writer-wins is the maximum under (HLC, origin rank), as `LwwValue` merges; the grow-only counter takes each origin's highest contribution, as `GCounter.MergeFrom` does pointwise. |
| `hwm[x][o]` | Per-origin high-water vector | `ReplicationHighWaterMarkState.Vector`, advanced by `ReplicationHighWaterMarkGrain.TryAdvanceAsync` through `ReplicationReceiveDedup.AdvancesHighWaterMark`. It is the local vector `CausalApplyBuffer.DependenciesSatisfied` reads, and never a drop threshold (#1060). |
| `pinned[x][o]` | Snapshot-pinned drop floor | `ReplicationHighWaterMarkState.PinnedFloor`. Since #4476 (the fix for #4463) production installs no floor and the applier never reads one; the slot is kept for rolling upgrades. The model keeps the variable, always zero, so `BootstrapHandoffLosesNothingPinnedFloor` can still express the removed design. |
| `cache[x]` | Shadow-forward identity cache | `RecentApplyCache`, keyed by (origin, HLC, key, op), one per tree in the applier's memory. The model's cache is unbounded; eviction is covered by the idempotent merge every identity-cache miss falls through to. |
| `parking[x]` | Entries between the dependency check and the buffer insert | The window inside `ReplicationApplier.ApplyAsync` between the `GetVectorAsync` read that finds a dependency unmet and the completion of `ReplicationApplier.ParkAsync`. |
| `buf[x]` | Causal apply buffer | `CausalApplyBufferGrain`, one durable grain per tree. Its `CausalApplyBuffer` is written through to `CausalApplyBufferState` before a park returns and is restored on activation (since #4483, the fix for #4464). |
| `wake[x]` | A drain is pending | A pending `CausalApplyBufferGrain.DrainAsync`. It is requested by `ReplicationApplier.DrainBufferAsync` after an apply that advanced the vector (and on the first advance a silo sees after it starts), by the park's own re-check, by `LatticeBootstrapCoordinatorGrain.PinAndCompleteAsync` after the pin, and by every `ReplicationMaintenanceGrain` tick. |
| `dlq[x]` | Dead-letter queue | `ReplicationDeadLetterGrain`, fed by `DeadLetterTrackingReplicationApplier` and by buffer eviction in `CausalApplyBufferGrain.ParkAsync`. |
| `faults` | Environment fault budget | Modelling device only: the fairness assumption that faults do not happen forever. |
| `booted` | Bootstrap has run | The terminal `LiveIncremental` phase of `LatticeBootstrapCoordinatorGrain`, at most once in the instance. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Author(o, k, h, d)` | A cluster commits a local write | A leaf commit under `BPlusLeafGrain.AdvanceClockOrOverride`. The guard `h > Ver(o, k)` is the leaf clock's receive rule: the merge path advances the leaf clock past every merged timestamp, so a later local write on that leaf stamps higher. Nothing relates two leaves' clocks, or two clusters', which is production's guarantee and the source of the #1060 non-monotonicity. The dependency `d` is an application-supplied `LatticeVectorClockContext` frontier. **Over-approximation:** the HLC is any value above the leaf's version stamp, where production's is the next tick of a wall-clock-driven clock. | Yes: `BPlusLeafGrainTests.MergeMany_advances_local_clock_past_incoming_max`. |
| `ShipSkip(e)` | The shipper consumes an entry it must not ship | `ReplicationShipperGrain.ShouldShip` returning false (`ReplicationShipEligibility.IsShipEligible`: a foreign or empty origin, or a tombstone reap) and `ReplicationShipperGrain.FoldFilteredOnlyConsumedCursorsAsync` folding the partition cursor past it without an acknowledgement. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_advances_partition_cursor_past_filtered_only_foreign_suffix`. |
| `Deliver(e, i)` | Ship, receive pipeline, acknowledgement | Shipper: `ReplicationShipperGrain.MergeOneBatchAsync` and the ship paths, guarded by `ReplicationShipEligibility.IsShipEligible`. Receiver: `ReplicationApplier.ApplyAsync` and `ReplicationApplier.ApplyOriginRunAsync`, in order: the cycle-break `ReplicationReceiveDedup.IsOwnOrigin`, `RecentApplyCache.TryAdd`, the dependency check `CausalApplyBuffer.DependenciesSatisfied`, the merge, `ReplicationHighWaterMarkGrain.TryAdvanceAsync`. The cursor advances only on the acknowledgement of the next entry, as the pipelined ack path does. **Over-approximation:** any entry above the cursor may be delivered at any time, in any order, again after an out-of-order acknowledgement, and a peer on another build may echo the destination's own write back (the guard's second disjunct), so the receiver's cycle-break is load-bearing. A deferred acknowledgement stalls production's shipper at that entry, where the model lets later entries through: a superset of deliveries, and the one deferral the intended design has (an in-flight duplicate) ends when its park completes, so no liveness is lost. Head-of-line blocking where it can deadlock is checked by the companion module ([`ReplicationCausalDelivery.Refinement.md`](ReplicationCausalDelivery.Refinement.md)). The guard `~InFlight(x, w)` is production's since #4477 (the fix for #4465): `RecentApplyCache.TryAdd` distinguishes an in-flight reservation from a completed one, and a duplicate of an in-flight delivery gets a deferred, not-accepted acknowledgement. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_skips_entries_originating_from_peer`, `ReplicationApplierTests.ApplyAsync_skips_local_origin_entries_as_no_op`, `ReplicationApplierTests.ApplyAsync_dedupes_duplicate_identity_tuple_without_invoking_apply_grain`, `ReplicationApplierTests.ApplyAsync_defers_a_duplicate_while_the_first_delivery_is_between_its_dependency_check_and_park` and `ReplicationShipperGrainTests.PumpOnceAsync_does_not_advance_cursor_on_negative_ack`. |
| `Park(x, p)` | The parked entry enters the causal buffer and is acknowledged | `ReplicationApplier.ParkAsync` hands the entry to `CausalApplyBufferGrain.ParkAsync`, which inserts it (`CausalApplyBuffer.TryAdd`), persists it before returning, and then re-checks the vector and drains in the same turn, so a dependency met between the check and the insert cannot strand it (#4464, fixed by #4483). The transport then acknowledges it `Accepted`. Production also releases the entry's identity reservation here and lets the durable buffer recognise the parked entry's re-deliveries, where the model keeps the reservation until eviction. The two differ only on a re-delivery whose dependencies have since been met: production merges it again, idempotently, and the buffered copy later drains as a no-op, while the model acknowledges it as already held. Every value agrees. | Yes: `ReplicationApplierTests.ApplyAsync_parks_entry_when_vector_clock_dependency_missing` and `ReplicationApplierTests.ApplyAsync_parked_entry_whose_dependency_was_met_before_the_insert_is_drained_by_the_park`. |
| `Drain(x)` | Release of buffered entries whose dependencies are met | `CausalApplyBufferGrain.DrainAsync`: a fixed point over `CausalApplyBuffer.DrainSatisfied` that applies each released entry through `ReplicationApplier.ApplyDrainedEntryAsync` and persists its removal only after the apply (or its dead-lettering) returned. One entry per step is finer-grained than production's pass, which only adds interleavings. | Yes: `ReplicationApplierTests.ApplyAsync_drains_buffered_entries_when_dependency_arrives`. |
| `ApplyFails(e, i)` | An apply fails into the dead-letter queue | `DeadLetterTrackingReplicationApplier.ApplyAsync` parking the entry once its retry budget is exhausted, after `ReplicationApplier.ApplyAsync`'s catch rolled back the identity reservation. Bounded by the fault budget, and only at the fault site. | Yes: `DeadLetterTrackingReplicationApplierTests.ApplyAsync_parks_entry_when_threshold_reached` and `ReplicationApplierTests.ApplyAsync_rolls_back_cache_when_apply_grain_throws_so_retry_can_apply`. |
| `Evict(x, w)` | The bounded buffer displaces an entry | `CausalApplyBuffer.TryAdd` over its caps; `CausalApplyBufferGrain.ParkAsync` dead-letters the displaced entry before the write that removes it from the durable buffer. Its identity reservation was released when it was parked, so a replay is applied rather than dropped as a duplicate (#3630). Bounded by the fault budget. | Yes: `ReplicationApplierTests.ApplyAsync_releases_dedupe_reservation_of_entry_evicted_to_dead_letter_queue_so_replay_applies_it` and `ReplicationApplierTests.Overflow_dead_letters_the_evicted_entry_before_its_removal_is_persisted`. |
| `Replay(x, r)` | An operator replays a dead letter | `ILatticeReplicationDeadLetters.ReplayAsync`, which runs the entry back through the applier. Production never replays on its own; the liveness property's only assumption about operators is that one eventually does. | Yes: `ReplicationApplierTests.ApplyAsync_releases_dedupe_reservation_of_entry_evicted_to_dead_letter_queue_so_replay_applies_it` and `DeadLetterIntegrationTests.Replay_routes_through_inner_and_removes_parked_entry`. |
| `Restart(x)` | A receiving silo restarts | The applier singleton is rebuilt: the identity cache and every call in progress are lost; an unacknowledged park is re-sent by its shipper. The causal buffer survives in `CausalApplyBufferState`, and a drain is re-armed by the first high-water-mark advance the restarted silo sees (`ReplicationApplier.DrainBufferAsync`) and by every `ReplicationMaintenanceGrain` tick, without further traffic (since #4483, the fix for #4464). Bounded by the fault budget. | Yes: `ReplicationApplierTests.ApplyAsync_parked_entry_survives_a_restart_and_drains_when_its_dependency_arrives`, `ReplicationApplierTests.Reloaded_buffer_drains_an_entry_whose_dependency_was_met_before_a_restart_without_further_traffic`, `ReplicationApplierTests.ApplyAsync_first_advance_after_a_restart_drains_entries_parked_before_it` and `ReplicationMaintenanceGrainTests.ProcessNextPhaseAsync_drains_the_causal_apply_buffer_every_tick`. |
| `Bootstrap` | Snapshot bootstrap and the stream handoff | `LatticeSnapshotProvider.ExportAsync` (the frontier is the source's own vector, because no production caller reports one to `IWalCursorRegistry`), the drain in `LatticeBootstrapCoordinatorGrain.DrainSnapshotOnceAsync` under `LatticeBootstrapApplyContext`, and `LatticeBootstrapCoordinatorGrain.PinAndCompleteAsync` sealing the source coordinate at the cut. The pin installs no drop floor (since #4476, the fix for #4463), joins the frontier into the vector already held instead of replacing it (`ReplicationHighWaterMarkGrain.MergeBootstrapFrontierAsync`), and then re-arms a drain of the causal buffer (since #4483, the fix for #4464). **Over-approximation argument for collapsing the export into one step:** with no drop floor a row only adds a value the source held, every write in which the target also receives over its own peer edges; a seeded identity can only name a write the row contains; and an earlier frontier is a smaller one. | Yes: `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_pin_seals_source_coordinate_and_preserves_other_origins`, `ReplicationHighWaterMarkGrainTests.MergeBootstrapFrontierAsync_clears_a_legacy_drop_floor`, `ReplicationHighWaterMarkGrainTests.MergeBootstrapFrontierAsync_takes_the_pointwise_maximum_and_never_regresses` and `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_pin_merges_the_frontier_then_drains_the_causal_buffer`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `NoRelay` | A cluster ships only writes it authored, so a -> b -> c is never a path and nothing ping-pongs: `ReplicationShipEligibility.IsShipEligible` refuses every foreign-origin entry the WAL captured from an apply. Stated over the delivery step, because in a full mesh a relayed write is also delivered directly and no state distinguishes it. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_skips_entries_originating_from_third_cluster_other_than_local`, `ReplicationShipEligibilityTests.IsShipEligible_rejects_an_entry_applied_from_a_peer` and `ReplicationConvergenceCoyoteTests.Both_cycle_breaks_prevent_relay_and_reflection`. |
| `NoReflection` | A cluster never admits its own write, received from a peer, into its apply pipeline: `ReplicationReceiveDedup.IsOwnOrigin` in both the per-entry and the batched path, independent of the sender's build. | Yes: `ReplicationApplierTests.ApplyAsync_skips_local_origin_entries_as_no_op`, `ReplicationApplierTests.ApplyBatchAsync_local_origin_run_classifies_all_as_dedup_without_grain_calls` and `ReplicationConvergenceCoyoteTests.Without_the_receiver_guard_an_echoed_own_write_is_applied`. |
| `CursorNeverSkipsUnshipped` | The shipper's partition cursor never passes a ship-worthy entry the peer has not absorbed: it advances only on a positive acknowledgement of the next batch, and the scalar HLC cursor is not a skip criterion (the shipper half of #1060), and a duplicate of an in-flight delivery is deferred rather than acknowledged (#4465, fixed by #4477). | Yes: `ReplicationShipperGrainTests.DrainBatchAsync_cold_partition_new_below_cursor_entry_is_shipped_not_dropped`, `ReplicationApplierTests.ApplyAsync_defers_a_duplicate_while_the_first_apply_is_in_flight_and_applies_it_after_an_abort`, `ReplicationShipperGrainTests.PumpOnceAsync_does_not_advance_partition_cursor_when_ack_rejected` and `ReplicationConvergenceCoyoteTests.Partition_cursor_ships_every_entry`. |
| `DedupNeverDropsNew` | The receiver drops an entry as a duplicate only if its value already reflects it: neither the incremental diagonal (the receiver half of #1060) nor a bootstrap pin (#4463) is a drop threshold, and every identity reservation of an entry that was not applied is released (#3630 and the apply-failure rollback). | Yes: `ReplicationApplierTests.ApplyAsync_applies_point_write_when_timestamp_below_hwm`, `ReplicationApplierTests.ApplyAsync_releases_dedupe_reservation_of_entry_evicted_to_dead_letter_queue_so_replay_applies_it` and `ReplicationConvergenceCoyoteTests.Identity_and_merge_dedup_never_drops_a_new_write_and_converges`. |
| `BootstrapHandoffLosesNothing` | After a bootstrap handoff, every write the target did not author is either absorbed or still on its way and will be accepted: the pin installs no drop floor, so a write the snapshot does not hold is applied when it arrives, whether it is the source's own (at or below the sealed source coordinate) or a third origin's (below its non-monotonic frontier coordinate). | Yes: `ReplicationApplierTests.ApplyAsync_applies_a_source_write_absent_from_the_snapshot_at_the_sealed_source_coordinate` and `ReplicationApplierTests.ApplyAsync_applies_a_third_origin_write_below_its_non_monotonic_frontier_coordinate`. |
| `EventualConvergence` | Once writing stops, over a fair transport and with dead letters eventually replayed, every replica of every key reaches the value of all its writes: the merges are confluent (the #2891 tie-break), every write is delivered or re-sent, and every parked entry is eventually applied, which production has guaranteed since #4483 (the fix for #4464). | Yes: `LwwValueMergeCrossSiteConvergenceTests.Merge_converges_across_sites_when_only_one_side_authored_locally`, `ReplicationApplierTests.ApplyAsync_drains_chain_of_dependent_entries_in_one_call`, `ReplicationApplierTests.Reloaded_buffer_drains_an_entry_whose_dependency_was_met_before_a_restart_without_further_traffic` and `ReplicationConvergenceCoyoteTests.Identity_and_merge_dedup_never_drops_a_new_write_and_converges`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## The Detector column

The rules are those of the atomic-commit note
([`../atomic-commit/Refinement.md`](../atomic-commit/Refinement.md#the-detector-column)):
a detector is a test over production code, never a TLA+ mutation; every test
named resolves under `test/`; every row admitting a gap cites the issue that
closes it. No tally is recorded here; derive one from the column.

Every detector named above was proven red by perturbing the production seam
its row abstracts, running the one test, and restoring with
`git diff --exit-code` clean. The proof log is in the pull request that added
this note (#4438).

## Defects found and fixed

| Issue | Former production shape | Fixed by | Reproducing mutation |
|-------|-------------------------|----------|----------------------|
| #4463 | The bootstrap pin installed a drop floor at the frontier, losing writes the snapshot did not hold. | #4476 | `BootstrapHandoffLosesNothingPinnedFloor` |
| #4464 | The causal buffer stranded or lost parked entries: the lost wakeup between check and park, the in-memory buffer, the pin replacing the vector, and the pin not draining. | #4483 | `EventualConvergenceParkLostWakeup`, `EventualConvergenceVolatileCausalBuffer`, `EventualConvergencePinRegressesVector`, `EventualConvergencePinSkipsDrain` |
| #4465 | A duplicate of an entry still being parked was acknowledged, so the entry could be lost. | #4477 | `CursorNeverSkipsUnshippedDuplicateOfParkingAcked` |

Every fix's detectors were proven red against the reproducing mutation's
production shape and green once the fix landed.

## Property classification

Per #2321's taxonomy, for each property: why it holds on the base, and what
the firing mutation shows.

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkHlcRunaway` |
| NoRelay | `Deliver`'s guard is the cycle-break production has. | Faithfully inexpressible | `NoRelayShipsForeignWrites` |
| NoReflection | `Deliver`'s receiver branch is production's guard. | Faithfully inexpressible | `NoReflectionReceiverAdmitsOwnWrites` |
| CursorNeverSkipsUnshipped | Only the next entry's acknowledgement moves the cursor, and the scalar cursor is not a skip criterion. An in-flight duplicate is deferred, as production's has been since #4477. | Faithfully inexpressible | `CursorNeverSkipsUnshippedScalarHlcFilter`, `CursorNeverSkipsUnshippedAckJumpsGap`, `CursorNeverSkipsUnshippedDuplicateOfParkingAcked` |
| DedupNeverDropsNew | The only thresholds are the zero floor and identities of absorbed writes, and every unapplied reservation is released. | Faithfully inexpressible | `DedupNeverDropsNewIncrementalDiagonal`, `DedupNeverDropsNewFailedApplyKeepsReservation`, `DedupNeverDropsNewEvictionKeepsReservation` |
| BootstrapHandoffLosesNothing | The pin installs no floor, as production's has since #4476. | Faithfully inexpressible | `BootstrapHandoffLosesNothingPinnedFloor` |
| EventualConvergence | Merges are confluent, delivery is fair, and park, restart and pin re-arm drains and keep the buffer, as production's have since #4483. | Faithfully inexpressible | `EventualConvergenceObserverRelativeTieBreak`, `EventualConvergenceDrainDropsReleasedEntry`, `EventualConvergenceReplayDiscards`, `EventualConvergenceParkLostWakeup`, `EventualConvergenceVolatileCausalBuffer`, `EventualConvergencePinRegressesVector`, `EventualConvergencePinSkipsDrain` |

No mutation adds an action: every one perturbs an existing action or, in
`EventualConvergenceObserverRelativeTieBreak`'s case, the merge definition.
`EventualConvergence` fails on protocol defects under the fairness the
specification asserts, not only without it: every one of its mutations leaves
`Fairness` untouched.

## Deliberate abstraction gaps

What the module does **not** cover, stated so coverage of one half is never
read as coverage of another (#2324):

- **Saga replication.** Prepared atomic-batch entries, saga terminals and
  cross-cluster atomic visibility are epic #4430's cross-cluster module
  (#4436). The floor bypass and the causal-park bypass for prepared entries are
  not modelled.
- **Empty-origin records and the change feed.** Every write in the model
  carries an origin, which is what `WalCommitLogWriter` stamps on a replicated
  tree. Durability-only records with an empty origin, which the shipper drops
  and `ChangeFeed` deliberately keeps for a local bootstrap consumer (#2324),
  are not modelled, so the model says nothing about the change feed.
- **Range deletes, tombstone reaps, key filters, coalescing, content-hash
  elision.** Range deletes bypass the point-write dedup; tombstone reaps never
  ship; key filters and pre-ship coalescing reduce what ships without changing
  what converges; elision lets the receiver advance on a manifest. None is
  modelled.
- **One WAL partition per cluster.** Production's key-hashed partitions have
  independent cursors. One partition already interleaves independent leaf
  clocks, and a single cursor never advances faster than per-partition ones
  would, so every entry production may still re-deliver, the model may too.
- **The legacy-migration tick.** A state persisted before partition cursors
  existed drops entries at or below its scalar cursor once. The model starts
  from a fresh state, so that tick never runs.
- **The apply window of the in-flight duplicate.** The model splits only the
  park window; the apply itself is atomic, so a duplicate arriving during an
  apply is not exhibited. Production defers it on both windows (#4477), and
  `ReplicationApplierTests.ApplyAsync_defers_a_duplicate_while_the_first_apply_is_in_flight_and_applies_it_after_an_abort`
  pins the apply window.
- **Topology.** One instance: a and b ship to each other and to c, which only
  receives and is the only bootstrap target and fault site. Relays over longer
  chains, more than one fault and more than two writes are bounded out.
- **Receive fence, enrolment and tenant gates, merge-mode mismatch.** These
  defer or refuse entries for reasons outside convergence and are not modelled.
