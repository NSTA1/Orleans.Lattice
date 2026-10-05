# Refinement note: WAL durability lifecycle to production

This note maps [`WalDurability.tla`](WalDurability.tla) - the leaf-side WAL
durability lifecycle with crash-anywhere recovery - to the Orleans.Lattice code
that plays each role, so the model and the runtime artefact are traceably the same
protocol and any divergence is visible. The shard-move half of the WAL lives in
[`WalMove.tla`](WalMove.tla) and has its own note,
[`MoveRefinement.md`](MoveRefinement.md).

It is a **documented mapping, not a machine-checked refinement proof**. Three gates
keep it honest about what it names: the staleness gate resolves every backticked
`Type.Member` against `src/`; the detector gate resolves every test the Detector
column names against `test/` and requires every admitted gap to cite an issue; and
the coverage gates require a row for every action in `Next` and every property the
cfg checks. None of them checks that a row's claim is TRUE. That was done by reading
production and, for every Detector, by perturbing production and watching the named
test go red (the log is in the pull request that introduced this note).

## Model scope in one paragraph

One WAL partition of three offsets is shared by two leaves. Every write belongs to
`l1`; `l2` owns nothing in the partition, which is the common production case
(keys hash across every partition) and the reason a checkpoint must be a read
position. A budget of one fault - a shard crash, a leaf stop, a failed persist or a
failed capture - bounds the environment; the two fault-free intended-design stutters
(`ActivateLoadFail`, `ActivateRowless`, `ReplayFaultRearm`) cost nothing. The base model
checks 111,154 distinct states to depth 27 with every safety and liveness property.
The `TwoFaults` variant (`WalDurability.TwoFaults.cfg`) checks every safety invariant
with a budget of two faults: 680,122 distinct states to depth 32. The `SnapshotLoss`
variant lets the environment destroy a leaf's snapshot or its row, and the operator
purge a tree, at two faults: 1,365,609 distinct states to depth 32.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next` | The partition's offset allocator | The WAL shard's next sequence, advanced by `WalOffsetAllocationCore.Assign` under the shard's state gate inside `WalShardGrain.AppendAsync` and reported by `WalShardGrain.GetNextSequenceAsync`. |
| `inflight` | Appends assigned an offset whose flush has not landed | The shard's in-flight flush chain (`WalShardGrain.StartFlush`, `WalShardGrain.FlushAsync`); the oldest in-flight start is what `WalShardGrain.DurableContiguousTailOffset` feeds `WalShippingWatermark.DurableContiguousTail`. |
| `durable` | Offsets the store holds | Entries the partition's `IWalStorageProvider` has durably flushed and the GC has not trimmed. |
| `tail` | Oldest readable offset | The provider's oldest retained offset, read by the fall-off checks through `ICommitLogReader.GetTailOffsetAsync` and `ILeafReplayCoordinatorGrain.GetTailOffsetAsync`. |
| `acked` | Writes acknowledged to their writer | Appends whose `WalShardGrain.AppendAsync` completed, which happens only after their flush landed. |
| `orphans` | Abandoned appends that may still land | Provider calls `WalAbandonedFlushRegistry` holds: flushes `WalShardGrain` stopped waiting for at their deadline or under an expired drain budget, never acknowledged (issue #4621). |
| `up[l]` | A leaf activation exists | A live `BPlusLeafGrain` activation whose activation replay has been armed (`BPlusLeafGrain.ExecuteActivationReplayAsync`). |
| `cache[l]` | The in-memory projection | `BPlusLeafGrain` entry cache (`LeafEntryCache`), rebuilt on every activation from a snapshot and the WAL. Modelled as the set of owned offsets it holds; values are abstracted away. |
| `rp[l]` | The leaf's read position (pending or persisted) | The pending checkpoint map in `BPlusLeafGrain.Projection` (`_pendingCheckpointOffsetsByPartition`) over the persisted slot; `max(rp, stCp)` is `BPlusLeafGrain.GetCurrentCheckpointForPartition`. It is a READ position: `BPlusLeafGrain.ReplayPartitionAsync` advances it over entries it skips as another leaf's work (#2270). |
| `stCp[l]` | The activation's belief about its persisted checkpoint | `LeafNodeState.ProjectionCheckpointOffset` / `LeafNodeState.ProjectionCheckpointOffsetsByPartition` in `state.State`, read through `BPlusLeafGrain.GetPersistedCheckpointForPartition`. |
| `durCp[l]` | What grain storage holds | The checkpoint carried by the last successful `WriteStateAsync` of the leaf's state. |
| `anchor[l]` | The checkpoint an activation started from | The value `BPlusLeafGrain.TryRehydrateFromSnapshotAsync` writes into `state.State` from the snapshot's coverage without persisting it, or the stored checkpoint on a cold start. Modelling device: it exists so `PersistedBeliefHonest` can admit that deliberate, unpersisted rehydrate belief and nothing else. |
| `clk[l]` | The leaf's persisted clock is past Zero | `LeafNodeState.Clock`; `BPlusLeafGrain.ReportCursorIfActiveAsync` publishes nothing while it is Zero. The model treats the clock as live when it is persisted past Zero or the projection holds a row. |
| `cov[l]` | Snapshot coverage the activation has recorded | `BPlusLeafGrain.DurableSnapshotCoverageForPartition`, raised only by `BPlusLeafGrain.RecordDurableSnapshotCoverage` from a load or a capture the store kept (#3440). Per activation: lost on stop. |
| `snapCov[l]`, `snapRows[l]` | The durable snapshot | The blob `ILeafSnapshotStorageGrain.SaveAsync` keeps: `LeafSnapshotBlob.SnapshotOffsetsByPartition` and its rows. |
| `stale[l]` | Leaf latched stale (fail closed) | `LeafProjectionStaleException` raised by the cold-replay guard in `BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync` or by a `LatticeFallOffLogDetector.ClassifyAsync` loss decision; operator rebuild required. |
| `hadSnap[l]` | The leaf's durable record that it has held snapshot coverage | `LeafNodeState.SnapshotCoveredPartitions` (issue #4634): one-way per-partition flags `BPlusLeafGrain.RecordDurableSnapshotCoverage` raises on a kept capture or a load, written by `FlushDurableMaterialiserFrontierCoreAsync` in the leaf row before the publish that may rely on them. Grouped with the pin variables because it is written in the same step. Tracked only under `SnapshotLoss`. |
| `pinOff[l]`, `pinHlc[l]` | The leaf's durable materialiser pin | The pin store's per-consumer offset and frontier, merged by monotone max in `WalMaterialiserPinGrain.Merge`. `pinHlc = "zero"` with no offset is the block pin seeded by `BPlusLeafGrain.SeedDurableMaterialiserBlockPinAsync`; `pinHlc = "gone"` is an entry the GC retired (`LatticeWalGcScheduler`, `PinRetire`). |
| `row[l]` | The leaf's state row exists | `LeafNodeState` in grain storage (issue #4654). A leaf without it serves data, and writes a first row, only when the call carries a create intent naming it (`LatticeNewLeafIntentContext.IsFor`); the separate row record (`ILeafRowRecordGrain`) is defence in depth the model does not need, since it closes no case the intent leaves open. Changes only under `SnapshotLoss`. |
| `purgeBegun` | The shard recorded that its purge began clearing leaves | `ShardRootState.LeafClearsBegun`, made durable by `ShardRootGrain.ClearTopologyAsync` before its first leaf clear; the recovery reseed passes a create intent only when it is set. One flag, because every modelled leaf shares one shard. |
| `cleared[l]` | The purge deliberately cleared the leaf | A ghost: no production counterpart. It exists so `ClearRecorded` can state the order of the purge's record and its clears. |
| `faults` | Environment fault budget | Modelling device only: the fairness ceiling (faults do not happen for ever). No production counterpart. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Append` | Offset allocation | `WalShardGrain.AppendAsync` assigns the next offset through `WalOffsetAllocationCore.Assign` under the shard's state gate. **Over-approximation:** the model appends whenever an offset remains; production appends only on a client write, a subset. | Yes: `WalShardGrainTests.AppendAsync_assigns_monotonically_increasing_sequence_numbers` and `WalOffsetContiguityCoyoteTests.Atomic_assign_keeps_every_offset_unique_and_dense`. |
| `FlushAck(o)` | Flush lands, write acknowledged, owner folds it in | `WalShardGrain.FlushAsync` completes the append's acknowledgement only after the provider flush; flushes of different appends complete in any order. The owning leaf applies its own write to its cache on the foreground path without advancing its checkpoint. **Over-approximation:** the model also acknowledges a write whose owner is not active, without the apply; production routes a write through an active leaf, so the model's set of acknowledged-but-unapplied writes is a superset. | Yes: `WalShardGrainTests.AppendAsync_hung_provider_flush_faults_with_timeout_when_deadline_elapses` - an append whose flush never lands does not complete. |
| `ShardCrash` | WAL shard activation lost | Unflushed appends are lost with the activation; the next activation recovers the allocator from the provider's highest stored offset through `WalOffsetAllocationCore.RecoveredNextOffset`. An abandoned provider call does not survive to land below a reader: the in-memory and file providers' writes die with the process, and the Azure Table provider commits phase 2 in offset order and rolls a late phase-1 row forward in order or back (`ReconcileAsync`), so it lands only at or above the committed tail. **Over-approximation:** the crash may happen between any two actions. | Yes: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization`, which drives the same core through the grain's test seam. |
| `ReadStep(l)` | Read the next entry of the shared stream | Activation replay and the starvation drive, both through `BPlusLeafGrain.ReplayPartitionAsync`: shown only offsets below the durable-contiguous watermark (`WalShippingWatermark.IsOffsetExposable`), which is also bounded by every unsettled abandoned call and by one past the highest stored offset (`WalShardGrain.GetReadableHeadAsync`, issue #4621), folding owned entries and advancing the read position over every entry read (#2270). A read below the tail returns the surviving suffix. | Yes: `BPlusLeafGrainTests.ProjectionCheckpointOffset_advances_over_entries_the_leaf_skips`, `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is`; the head bound by `WalShardGrainTests.A_head_a_reader_resumes_from_never_passes_an_offset_a_recovered_allocator_can_reissue`. |
| `Abandon(o)` | A flush misses its deadline, or the drain budget expires under it | `WalShardGrain` faults the append's acknowledgement (an unknown outcome to its caller) and registers the still-running provider call in `WalAbandonedFlushRegistry`, process-wide and keyed by provider, tree and shard, so a reactivation in the same process sees it too. Until the call settles, `WalShardGrain.DurableContiguousTailOffset` exposes nothing at or above it (issue #4621). Costs a fault. | Yes: `WalShardGrainTests.ReadAsync_never_exposes_an_offset_above_an_abandoned_flush_that_can_still_land` and `WalShardGrainTests.A_reactivated_shard_is_held_below_its_predecessors_abandoned_flush_until_it_settles`, red when the watermark ignores an abandoned window or the registry is per activation. |
| `LateLand(o)` | An abandoned call lands | The provider writes the entry. Every provider refuses a write at an occupied offset and at or below its trim watermark (`InMemoryWalStorageProvider`, `FileWalShard`, the Azure Table provider's transactional `Add`), so it lands only in a gap no reader has passed. | Yes: `WalShardGrainTests.ReadAsync_never_exposes_an_offset_above_an_abandoned_flush_that_can_still_land` (the entry is shown in order once it lands) and `InMemoryWalStorageProviderTrimWatermarkTests.An_append_at_or_below_the_watermark_is_refused_and_allocation_stays_above_it`. |
| `SettleHole(o)` | An abandoned call settles without landing | The registry retires the window and the offset is a permanent hole; readers pass it, and the provider's trim watermark (`IWalStorageProvider.GetLowestOffsetAsync`, raised durably before any delete) tells it from a trim. **Over-approximation:** production's `HandleFlushFailureAsync` resyncs the allocator to one past the highest stored offset when the window settles, so the hole is never trailing; the model keeps the allocator where it was, and bounds the watermark by `RecoveredNext` instead. | Yes: `WalShardGrainTests.ReadAsync_releases_a_hole_once_the_abandoned_flush_settles_without_landing`, `WalShardGrainTests.A_hole_directly_above_the_trim_watermark_is_not_a_fall_off_for_the_leaf_or_the_subscriber` and `CrossClusterAtomicVisibilityTests.Shipper_tells_a_hole_above_the_trim_point_from_a_trim` (a hole is not a trim), `WalShardGrainTests.A_trim_past_a_readers_position_is_still_a_fall_off_with_the_trim_watermark` (a trim still is), and `WalShardGrainTests.Without_a_trusted_trim_watermark_a_reader_treats_the_hole_above_a_trim_point_as_trimmed` (until every silo writes the watermark). |
| `PersistCheckpoint(l)` | Persist the pending read position | `BPlusLeafGrain.FlushPendingCheckpointAsync` commits the pending advance through `BPlusLeafGrain.ApplyPendingCheckpointAdvance` and persists it. The coalescing thresholds only delay this step; the model persists any pending advance (the #3608 residual tick makes that eventual). | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_retains_the_pending_advance_for_the_next_flush`. |
| `PersistFail(l)` | A checkpoint persist that fails | The persist throws and `BPlusLeafGrain.RollbackCheckpointCommit` restores `state.State` and keeps the advance pending (#4017). | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint` and `BPlusLeafGrainTests.Failed_deactivation_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`. |
| `Capture(l)` | Capture a snapshot | `BPlusLeafGrain.CaptureSnapshotCoreAsync`: proceeds once any partition is proven checkpointed, declines a claim covering nothing (#2725), writes through `ILeafSnapshotStorageGrain.SaveAsync`, and records coverage only from a kept capture. The store's `LeafSnapshotStorageGrain.MergeMonotone` never lowers stored coverage; where production MERGES a regressing claim that carries every stored key, the model keeps the old snapshot outright - the same coverage over a subset of the rows - so every property that holds over the model's snapshot holds over production's. The claim refines the model's read position: while the cache is unanchored (a cold rebuild, or a rehydrate in flight) `BPlusLeafGrain.BuildUnanchoredCoverage` claims per partition only what the rebuild has provably re-read, and `BPlusLeafGrain.UnanchoredCoverageIsCapturable` declines a claim that covers nothing or regresses durable coverage (issue #4451, fixed); an anchored cache claims `BPlusLeafGrain.BuildCheckpointCoverage`. | Yes: `BPlusLeafGrainTests.Inline_capture_the_store_merges_still_advances_durable_coverage` (coverage recorded from a kept capture). For the #4451 claim, `BPlusLeafGrainTests.Capture_part_way_through_a_cold_rebuild_banks_only_the_re_read_frontier` pins the claim itself (`BuildUnanchoredCoverage`): it is the one detector red when only the claim reverts to the checkpoint. `BPlusLeafGrainTests.Capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack`, `BPlusLeafGrainTests.Deactivation_capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack` and `BPlusLeafGrainTests.Capture_while_the_snapshot_rehydrate_is_in_flight_never_claims_coverage_its_rows_lack` go red only when the claim AND the decline gate (`UnanchoredCoverageIsCapturable`) revert together. Removing only the gate is undetected by design: with the claim correct, the gate only skips a claim that covers nothing or regresses stored coverage, which the store's monotone merge would never record as a coverage regression anyway. With the claim reverted, the gate still declines the captures those three tests make, which is why only the frontier test reports a claim-only revert. |
| `CaptureFail(l)` | A capture that fails or is declined | The store declines or the write fails; the leaf records no coverage (#3440). | Yes: `BPlusLeafGrainTests.Inline_capture_declined_by_the_store_does_not_advance_durable_coverage` and `BPlusLeafGrainTests.Staged_capture_whose_commit_is_declined_does_not_advance_durable_coverage`. |
| `PublishPin(l)` | Publish the durable materialiser pin | `BPlusLeafGrain.ResolveDurablePinForPartition` resolves the pin through `LeafDurablePinCore.Resolve` and the pin store merges it (`WalMaterialiserPinGrain.Merge`). **Over-approximation:** the model may publish between any two steps; production publishes from the flush tail, the deactivation barrier, the starvation drive and the Zero-clock seed. Because the store merges by monotone max, a publication at any step yields an entitlement at least as high as any production schedule's. The never-written release is bounded by the snapshot coverage the leaf holds (issue #4456), and fires only when the leaf holds durable coverage at all: with no snapshot the block stands until a capture records coverage (issue #4523). The empty release of a leaf with a live clock (the `(clock, -1)` arm) is withheld for every partition until the replay barrier latches (`BPlusLeafGrain.WithUnreplayedPartitionsLive`, issue #4669); the model's single partition cannot reach that arm (see the gaps), and `WalPartitionReleaseModel` checks it. | Yes: `LeafEmptyReleaseBeforeReplayTests.A_cold_leaf_never_releases_a_partition_its_replay_has_not_read` (the replay barrier, real grains), `LeafDurablePinCoreTests.A_pending_checkpoint_never_reaches_the_pin_issue_3476` and `BPlusLeafGrainTests.Batched_pin_flush_over_an_unpersisted_advance_publishes_no_pin_past_the_persisted_checkpoint` (the covered arm), and `LeafDurablePinCoreTests.The_never_written_release_is_bounded_by_snapshot_coverage_issue_4456`, `LeafDurablePinCoreTests.The_never_written_release_needs_durable_snapshot_coverage_issue_4523`, `BPlusLeafGrainTests.Never_written_leaf_holding_a_snapshot_releases_no_further_than_its_coverage`, `BPlusLeafGrainTests.Never_written_leaf_without_a_snapshot_keeps_its_block_pin` and `BPlusLeafGrainTests.Never_written_leaf_releases_once_a_kept_capture_covers_its_persisted_checkpoint` (the never-written arm). |
| `GcTrim` | Trim the stream's prefix | `LatticeWalGc.ApplyDurableMaterialiserFloorAsync` computes the offset floor and marks the partition of a standing block pin blocked, which withholds both the cursor predicate and the offset admission there; `LatticeWalGc.TrimShardAsync` trims the prefix whose every entry `WalGcTrimCore.IsEntryEligible` admits under `WalGcOffsetAdmission.Admits`, and its scan stops at the durable offset floor itself. A block pin whose leaf reports no durable offset at all is held more widely still: it leaves the offset census incomplete, which holds the whole tree before any scan. A consumer holding an override hold whose offset is still `-1` is read as a block pin too (issue #4641): `LatticeWalGc` reads each partition's head bound, then the holds, then the offset census, and that order is what keeps a hold raised mid-pass from being missed by a trim that reaches its write. The model has no stamps, so it cannot express the hold; `WalPartitionReleaseModel` checks it. **Over-approximation:** the model trims to the durable offset floor alone; production additionally bounds the trim by uncovered consumers' cursors, causal frontiers and buffer pins, and a configured retention TTL is a retention event the model does not take, whose overtaken consumers detect and recover (see "Retention TTL"). | Yes: `LatticeWalGcOffsetEntitlementTests.RunOnceAsync_offset_admission_still_stops_at_the_floor_and_attributes_it` (the scan stops at the durable offset floor), `LatticeWalGcOffsetEntitlementTests.RunOnceAsync_a_block_pin_holds_its_own_partition_while_the_pass_trims_the_others` (a block pin that abstains from the offset floor holds its own partition on a pass that does reach the scan) and `WalGcTrimFloorCoyoteTests.Min_cursor_floor_never_trims_past_the_slowest_consumer` (the trim core never passes the slowest consumer); the override hold by `WalOverrideHoldTests.A_trim_keeps_a_replicated_write_stamped_below_an_empty_release_frontier` (red when the GC ignores holds) and `WalMaterialiserPinGrainOverrideHoldTests.A_hold_stands_until_a_real_offset_lands`. |
| `LeafStop(l)` | Leaf activation ends | A crash, a silo restart or a deactivation: the entry cache, the pending checkpoint map and the recorded coverage are per activation and lost; grain state and the snapshot survive. The next activation sees an empty cache and takes the `-1` cold override unless a snapshot rehydrates. | Yes: `LeafReplayStartPolicyTests.An_absent_snapshot_over_an_empty_cache_replays_cold` and `BPlusLeafGrainTests.Cold_rebuild_over_a_lost_prefix_throws_and_never_captures`. |
| `Activate(l)` | Activation and its replay start | `BPlusLeafGrain.ExecuteActivationReplayAsync`: `BPlusLeafGrain.TryRehydrateFromSnapshotAsync` loads the snapshot, records its coverage and lowers the checkpoint to it; otherwise `LeafReplayStartPolicy.Decide` takes the cold override and replays the whole readable WAL, or fails closed when a snapshot that exists failed to load. A leaf with no state row activates only under a create intent its surviving evidence does not refuse (`ActivateRowless`, issue #4654). Both fall-off checks - `LatticeFallOffLogDetector.ClassifyAsync` on the warm path and the cold-replay guard - route through `WalFallOffCore.IsPrefixLost`. | Yes: `BPlusLeafGrainTests.Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint`, `BPlusLeafGrainTests.Activation_hydrates_empty_cache_from_snapshot_older_than_persisted_checkpoint`, `WalFallOffCoreTests.The_first_needed_offset_is_the_one_after_the_checkpoint`, and for a checkpoint of 0 (`FallsOff`'s `cp >= 0`, issue #4433) `WalFallOffCoreTests.A_zero_checkpoint_still_needs_offset_one_issue_4433`, `LatticeFallOffLogDetectorTests.ClassifyAsync_with_zero_checkpoint_and_offset_one_trimmed_triggers_WAL_trim` and `BPlusLeafGrainTests.Cold_rebuild_over_a_durable_zero_checkpoint_whose_next_offset_was_trimmed_throws`. |
| `ActivateLoadFail(l)` | A snapshot that exists fails to load, or one that existed has vanished | **A stutter:** the activation fails closed and is retried. Production has matched this since issue #4450 was fixed: `LeafReplayStartPolicy.Decide` answers `FailClosed` for a failed load over an empty cache, the activation throws `LeafSnapshotUnavailableException` rather than cold-replaying a WAL trimmed under that snapshot, and `ILattice.RebuildLeafProjectionAsync` is the operator's recovery path. A storage fault on a leaf with no snapshot also fails closed; nothing changes, which the spec admits as a stutter. Since issue #4634 an ABSENT snapshot on a leaf whose row records held snapshot coverage (`LeafNodeState.SnapshotCoveredPartitions`) fails closed the same way, whatever the WAL's tail reads: the pin store keeps the entitlement the vanished snapshot licensed, so a cold rebuild started while the tail still reads 0 can have its prefix trimmed under it. | Yes: `LeafReplayStartPolicyTests.A_failed_load_over_an_empty_cache_fails_closed`, `BPlusLeafGrainTests.Failed_snapshot_load_over_a_trimmed_wal_prefix_fails_the_replay_instead_of_coming_up_from_the_suffix` and `BPlusLeafGrainTests.Failed_snapshot_load_over_an_intact_wal_fails_the_replay_closed_too`; for a vanished snapshot `VanishedSnapshotColdStartIntegrationTests.A_cold_start_over_a_vanished_snapshot_does_not_silently_lose_the_prefix_it_covered` (real grains), `BPlusLeafGrainTests.Vanished_snapshot_over_a_wal_trimmed_under_it_fails_the_replay_instead_of_coming_up_from_the_suffix`, `BPlusLeafGrainTests.Vanished_snapshot_at_checkpoint_zero_over_a_trimmed_wal_fails_the_replay`, `BPlusLeafGrainTests.Vanished_snapshot_over_an_untrimmed_wal_still_fails_the_replay_closed` and `VanishedSnapshotColdStartIntegrationTests.A_cold_start_over_a_vanished_snapshot_fails_closed_even_when_the_tail_is_untrimmed_at_activation` (real grains: the tail reads 0 at activation and the GC trims under the would-be rebuild); the record reaching the leaf row before the pin by `BPlusLeafGrainTests.Kept_snapshot_record_is_durable_in_the_leaf_row_before_any_pin_is_published` and `BPlusLeafGrainTests.Failed_record_write_publishes_no_pin_and_the_next_flush_retries_it`; the operator rebuild accepting the loss by `BPlusLeafGrainTests.Rebuild_over_a_vanished_snapshot_drops_the_record_and_the_leaf_comes_back_accepting_the_loss`. |
| `SnapshotVanish(l)` | The environment destroys a leaf's snapshot | Storage loss or an operator deleting the blob `ILeafSnapshotStorageGrain` keeps (issue #4634). Enabled only under `SnapshotLoss`, the `SnapshotLoss` variant configuration. Nothing tells the leaf: its live coverage and its published pin, which may license a trim behind the snapshot, are untouched. **An environment fault, not a protocol step**: the leaf row survives it, and the row's own loss is `LeafRowVanish`, which composes with it at two faults. | Yes, for what the step means to the leaf: `VanishedSnapshotColdStartIntegrationTests.A_cold_start_over_a_vanished_snapshot_does_not_silently_lose_the_prefix_it_covered` destroys the blob of a real leaf and asserts its next activation fails closed rather than silently. |
| `ActivateRowless(l)` | A leaf whose row is gone activates with no create intent | **A stutter:** the activation fails closed (issue #4654). `BPlusLeafGrain` refuses every data operation of a rowless, unbound activation not admitted by a create intent (`LeafStateRowLostException`, an `ILatticeLeafUnavailable`) and writes no row. The intent comes only from a create path - shard bootstrap, a split's sibling, a bulk load or append, and the recovery reseed on a shard that recorded `ShardRootState.LeafClearsBegun` - and is refused while the leaf's snapshot or row record survives. The #1744 write-path re-bind passes none. | Yes: `VanishedLeafRowIntegrationTests.Vanished_leaf_row` (real grains: the row alone, the row with its snapshot, and the row with its record, all fail closed rather than reporting acknowledged keys absent), `VanishedLeafRowIntegrationTests.A_binding_without_a_create_intent_is_refused_on_a_leaf_whose_row_was_lost`, `VanishedLeafRowIntegrationTests.The_write_path_self_heal_does_not_re_create_a_leaf_whose_row_was_lost`, `UnboundLeafSelfHealIntegrationTests.CrdtWrite_to_a_routed_leaf_with_no_row_fails_closed`, `TreeDeletionIntegrationTests.RecoverTree_does_not_re_create_a_rowless_leaf_no_purge_cleared`, `BPlusLeafGrainTests.Rowless_activation_without_a_create_intent_fails_every_data_operation_closed`, `BPlusLeafGrainTests.Create_intent_for_a_leaf_whose_snapshot_survives_is_refused` and `BPlusLeafGrainTests.Create_intent_for_a_leaf_whose_row_record_survives_is_refused`; the intended re-create by `TreeDeletionIntegrationTests.RecoverTree_rebinds_a_root_leaf_left_unseeded_by_an_interrupted_purge` and `TreeDeletionIntegrationTests.RecoverTree_rebinds_a_non_root_leaf_left_unseeded_by_an_interrupted_purge`. |
| `LeafRowVanish(l)` | The environment destroys a leaf's state row | Storage loss or a row deleted outside the lattice while routing and the pin store still name the leaf (issue #4654). The row's checkpoint, clock and snapshot record go with it; the snapshot does not. The step also ends the activation, since a live one would rewrite the row from memory on its next write. Enabled only under `SnapshotLoss`. **An environment fault, not a protocol step.** | Yes, for what the step means to the leaf: `VanishedLeafRowIntegrationTests.Vanished_leaf_row` deletes the row of a real leaf, with and without its snapshot and its record, and asserts its next activation fails closed. |
| `PinRetire(l)` | The GC retires a rowless leaf's pin | `LatticeWalGcScheduler` retires the pin of a blocking consumer whose leaf reports no tree id (#3101), and its orphan sweep does the same. Not a fault, and reachable only after a `LeafRowVanish`: it is why evidence from the pin store cannot stand in for a create intent, since a retired entry reads exactly like a leaf never created. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pin_of_a_leaf_that_reports_no_tree_id` and `LatticeWalGcSchedulerCadenceTests.The_sweep_never_retires_a_pin_whose_leaf_is_still_live`. |
| `PurgeClear(l)` | The operator's purge clears a leaf | `ShardRootGrain.ClearTopologyAsync` (`PurgeTreeAsync`) records `ShardRootState.LeafClearsBegun` before its first leaf clear, then clears each routed leaf's row, snapshot and record. The model records the flag in the same step as each clear; a crash between the two only lets recovery re-create a rowless leaf with no snapshot, inside the purge carve-out. Enabled only under `SnapshotLoss`. | Yes: `ShardRootGrainPurgeTests.PurgeAsync_records_that_it_began_clearing_leaves_before_the_first_leaf_clear` and `ShardRootGrainPurgeTests.PurgeAsync_that_cannot_write_its_record_clears_no_leaf`. |
| `ReplayFaultRearm(l)` | A cold rebuild faults and re-arms in one activation | **A stutter:** the re-armed replay stays cold from its re-read frontier. Production has matched this since issue #4467 was fixed: `LeafReplayStartPolicy.Decide` treats a cache whose cold rebuild is still pending as unanchored, so `BPlusLeafGrain.ExecuteActivationReplayAsync` retries a faulted cold rebuild cold rather than resuming warm from the persisted checkpoint. | Yes: `BPlusLeafGrainTests.A_cold_rebuild_that_faults_part_way_is_retried_cold_not_resumed_warm` (within one activation) and `BPlusLeafGrainTests.Cancelled_cold_replay_lets_the_next_activation_resume_above_the_banked_frontier` (across activations). |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `AckedWriteDurable` | An acknowledged write survives a crash of its owner: the owner's durable snapshot and the readable WAL rebuild it. | Yes: `WalDurabilityLifecycleCoyoteTests.The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery`, which drives `LeafDurablePinCore`, `WalGcTrimCore`, `WalShippingWatermark`, `WalOffsetAllocationCore` and `WalFallOffCore` under crash-anywhere interleavings. |
| `TrimCoveredBySnapshot` | The GC never trims an acknowledged write its owner's durable snapshot does not hold (#4017's invariant). | Yes: `LeafDurablePinCoreTests.A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage`, `BPlusLeafGrainTests.Capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack` and `WalDurabilityLifecycleCoyoteTests.The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery`, whose model resolves the pin through `LeafDurablePinCore` and trims through `WalGcTrimCore`. |
| `ReadPositionHonest` | A leaf never serves a projection whose read position has passed an acknowledged write it owns and does not hold. Its one carve-out is a leaf whose shard has begun a purge (`Purging`, `ShardRootState.LeafClearsBegun`): the operator deleted that data, so recovery re-creating the leaf empty is the contract, not a loss (issue #4654). | Yes: `VanishedLeafRowIntegrationTests.Vanished_leaf_row` and `TreeDeletionIntegrationTests.RecoverTree_does_not_re_create_a_rowless_leaf_no_purge_cleared` (a rowless leaf on a shard that has not begun a purge fails closed), `BPlusLeafGrainTests.ProjectionCheckpointOffset_advances_past_the_last_scanned_entry_not_the_last_applied_one`, `BPlusLeafGrainTests.Cold_rebuild_over_a_lost_prefix_throws_and_never_captures`, `BPlusLeafGrainTests.Failed_snapshot_load_over_a_trimmed_wal_prefix_fails_the_replay_instead_of_coming_up_from_the_suffix` and `BPlusLeafGrainTests.A_cold_rebuild_that_faults_part_way_is_retried_cold_not_resumed_warm`. |
| `ClearRecorded` | Every leaf a purge deliberately clears is on a shard that recorded the purge first, so recovery can re-create it; a clear without the record leaves a leaf no create path may name, failing closed for ever. | Yes: `ShardRootGrainPurgeTests.PurgeAsync_records_that_it_began_clearing_leaves_before_the_first_leaf_clear` and `ShardRootGrainPurgeTests.PurgeAsync_that_cannot_write_its_record_clears_no_leaf`. |
| `ShippingNeverSkips` | No reader is shown an offset above a still-unfilled prefix hole. | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is`. Also `WalDurabilityLifecycleCoyoteTests.The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery`: its reader asks the watermark about exactly the first in-flight offset, so a watermark that exposes that offset (an off-by-one) is reported by `[ShippingNeverSkips]`. `WalShippingWatermarkCoyoteTests` lands a flush atomically and cannot see that boundary; the boundary unit test pins it directly. |
| `LogPrefixApplied` | No entry ever becomes readable below a reader's position: every readable entry a leaf owns at or below its read position is in its projection, acknowledged or not, so an abandoned append that lands late lands only above every reader (issue #4621). | Yes: `WalShardGrainTests.ReadAsync_never_exposes_an_offset_above_an_abandoned_flush_that_can_still_land`, `WalShardGrainTests.A_reactivated_shard_is_held_below_its_predecessors_abandoned_flush_until_it_settles` and `WalShardGrainTests.A_head_a_reader_resumes_from_never_passes_an_offset_a_recovered_allocator_can_reissue`. |
| `OffsetContiguity` | No acknowledged offset is reissued; every assigned offset is below the allocator. | Yes: `WalOffsetContiguityCoyoteTests.Atomic_assign_keeps_every_offset_unique_and_dense` and `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset`. |
| `RecoveryNeverFallsOffLog` | No leaf latches `LeafProjectionStaleException` under the protocol's own operation. | Yes: `WalFallOffCoreTests.The_first_needed_offset_is_the_one_after_the_checkpoint`, `LeafDurablePinCoreTests.The_never_written_release_is_bounded_by_snapshot_coverage_issue_4456`, `BPlusLeafGrainTests.Never_written_leaf_holding_a_snapshot_releases_no_further_than_its_coverage` and `BPlusLeafGrainTests.Never_written_leaf_is_not_latched_stale_by_a_cold_capture_below_its_published_release`, the end-to-end form of issue #4523's two-fault composition, which the `TwoFaults` variant checks in the model; and `WalDurabilityLifecycleCoyoteTests.A_never_written_leaf_is_never_latched_stale_under_crash_anywhere_recovery`, which drives `LeafDurablePinCore`, `WalGcTrimCore` and `WalFallOffCore` with one leaf owning nothing. |
| `PersistedBeliefHonest` | `state.State`'s checkpoint is what storage holds, or the anchor the activation deliberately started from; a failed persist is rolled back (#4017). | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint` asserts the rolled-back checkpoint directly. |
| `ReleaseBackedBySnapshot` | Every trim entitlement the pin store has published is backed by the leaf's durable snapshot coverage, which only grows. The root-cause form of `RecoveryNeverFallsOffLog`'s two-fault violation, reachable with no fault. | Yes: `LeafDurablePinCoreTests.A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage` (a data-bearing leaf), `LeafDurablePinCoreTests.The_never_written_release_is_bounded_by_snapshot_coverage_issue_4456` (a never-written leaf holding a snapshot) and `LeafDurablePinCoreTests.The_never_written_release_needs_durable_snapshot_coverage_issue_4523` (a never-written leaf holding none); end to end, `WalDurabilityLifecycleCoyoteTests.The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery`, whose model asserts it after every step. |
| `SnapshotCoverageMonotonic` | Durable snapshot coverage never regresses (`LeafSnapshotStorageGrain.MergeMonotone`). | Yes: `LeafSnapshotStorageGrainTests.SaveAsync_still_merges_a_regressing_capture_that_carries_every_stored_key`. |
| `PublishedPinWithinPersistedBelief` | A newly published pin never exceeds the persisted checkpoint (#3476). | Yes: `LeafDurablePinCoreTests.A_pending_checkpoint_never_reaches_the_pin_issue_3476` and `BPlusLeafGrainTests.Batched_pin_flush_over_an_unpersisted_advance_publishes_no_pin_past_the_persisted_checkpoint`. |
| `EveryAckedWriteMaterialised` | Every acknowledged write is eventually held by its owner's projection. | Yes: `LeafReplayStartPolicyTests.An_absent_snapshot_over_an_empty_cache_replays_cold` and the bounded-progress assertion of `WalDurabilityLifecycleCoyoteTests.The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery`. |
| `ReclamationEventuallyAdvances` | Once every write is in and the pins can release, everything appended is reclaimed from storage and no append or abandoned call is left outstanding. Stated over storage rather than the tail: a settled hole at the end of the bounded instance holds the tail below the stream's end only because the model has no later write (issue #4621). | Yes: `BPlusLeafGrainTests.Failed_checkpoint_persist_retains_the_pending_advance_for_the_next_flush` (a failed persist does not strand the advance) and `BPlusLeafGrainTests.ProjectionCheckpointOffset_advances_over_entries_the_leaf_skips` (a leaf that owns nothing still advances). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

Every property above is made to fire by a mutation. Classified by what the
mutation does:

- **Perturbs an existing action.** Every mutation but four. For these the
  property holds because a guard in the modelled protocol prevents its violation,
  and the mutation is the evidence that the guard is load-bearing.
- **Splices a transition into a stutter.** `ActivateLoadFail` and
  `ReplayFaultRearm` change no state: they are production failing closed (#4450)
  and a re-armed replay staying cold (#4467). `ReadPositionHonestLoadFailureColdReplays`,
  `ReadPositionHonestLoadFailureColdStartsOverIntactWal`,
  `ReadPositionHonestFaultedColdReplayResumesWarm` and the second edit of
  `SnapshotCoverageMonotonicStoreAcceptsRegression` replace a stutter with a new
  transition, so they ADD an action. Their limit: they show that taking that step
  would be unsafe - each is production's behaviour before its fix - not that a
  guard inside an existing step is load-bearing. The mutation-coverage gate is
  satisfied for those two rows by construction; the fixes are pinned by the
  production tests in the rows' Detector column.

Two properties are classified differently:

- **`SnapshotCoverageMonotonic` is unreached in the base, not guarded.** With
  only the store's monotone refusal dropped (`rp[l] >= snapCov[l]` removed from
  `Capture`), the property still holds over the whole base state space: a capture
  below durable coverage needs an activation that starts below its own snapshot,
  and the base has none (a rehydrate restarts at the snapshot's coverage, and a
  failed load fails closed). `SnapshotCoverageMonotonicStoreAcceptsRegression`
  reaches it only by splicing such an activation in, so it is NOT evidence that the
  refusal is load-bearing here. The evidence for the store is the row's detector,
  `LeafSnapshotStorageGrainTests`, red with the refusal perturbed.
- **`EveryAckedWriteMaterialised` is subsumed in this instance.** Its
  protocol-defect pairing, `EveryAckedWriteMaterialisedColdStartResumesFromCheckpoint`,
  also violates `ReadPositionHonest` (at depth 9), and no liveness-only protocol
  defect was found for it; `EveryAckedWriteMaterialisedLeafStopsUnbounded` perturbs
  the environment's fault assumption instead. The module's protocol-defect
  liveness evidence is `ReclamationEventuallyAdvances`, whose two pairings violate
  no safety invariant.

What the finite instance can hide is **bounded-out** cells, and they are named
rather than assumed absent:

- **Two faults in one behaviour.** The main configuration's budget is one. The
  `TwoFaults` variant raises it to two for every invariant and both action
  properties; that is the check that reaches issue #4523, which the one-fault
  instance hid. **Bounded-out at two faults, by the TLC budget:** the liveness
  properties `EveryAckedWriteMaterialised` and `ReclamationEventuallyAdvances`. The
  full configuration at two faults takes about ten minutes, past the five-minute
  run ceiling, so they are checked at one fault only. A liveness defect that needs
  two faults would not be found here.
- **Three faults in one behaviour.** Outside every configuration.
- **More than three offsets, two leaves or one partition.** Per-partition checkpoint
  arrays, coverage widening (#3157) and cross-partition clamps are not modelled.
- **A leaf that owns some writes and skips others.** `l1` owns every write and `l2`
  none. The mixed instance (owners `0 -> l1, 1 -> l2, 2 -> l1`) was checked with
  every property during development: clean, 153,455 distinct states to depth 27.
  It is not the checked instance, because it cannot reach the never-written latch
  of #4456 (now fixed) within one fault.

Three pairings perturb the environment rather than the protocol, and deserve a note.
`ReadPositionHonestLostRowReadsAsFresh` hides the loss of a leaf row, so the leaf
reads as one never written (issue #4654 before its fix), and
`ReadPositionHonestSnapshotRecordLostWithSnapshot` keeps the snapshot record with the
snapshot instead of in the row. And `EveryAckedWriteMaterialisedLeafStopsUnbounded`
perturbs the ENVIRONMENT assumption that faults do not recur for ever. It shows the
property needs that assumption. The protocol-defect liveness pairings, which keep
the spec's fairness intact, are `EveryAckedWriteMaterialisedColdStartResumesFromCheckpoint`
(subsumed, above), `ReclamationEventuallyAdvancesReadPositionTracksOwnEntries` (#2270)
and `ReclamationEventuallyAdvancesResidualAdvanceNeverPersisted` (#3608).

## Detectors

The Detector column names tests over production code, never a TLA+ mutation: a
mutation perturbs the spec, so it cannot notice production regressing. Every
`Yes` and `Partial` detector was shown to go red by perturbing the production code
it names and restored afterwards; the log is in the pull request.

## Defect mutations whose fixes have landed

These mutations reproduced open defects until their fixes landed, and are now ordinary
regression mutations. Each fix's detectors were proven red with production perturbed back
to the old behaviour, and green on the fix:

| Mutation | Behaviour it reproduced | Issue | Detectors |
|----------|-------------------------|-------|-----------|
| `ReadPositionHonestLoadFailureColdReplays` | A failed snapshot load cold-replays a WAL trimmed under that snapshot. | #4450 | the `ActivateLoadFail` row's |
| `TrimCoveredBySnapshotColdCaptureOverclaims` | A capture mid cold rebuild claims the persisted checkpoint over a partial projection. | #4451 | the `Capture` row's |
| `ReadPositionHonestFaultedColdReplayResumesWarm` | A faulted cold rebuild re-arms warm from the persisted checkpoint. | #4467 | the `ReplayFaultRearm` row's |
| `RecoveryNeverFallsOffLogNeverWrittenReleaseUnbounded` | A never-written leaf releases its persisted checkpoint above its snapshot's coverage. | #4456 | the `PublishPin` row's |
| `ReleaseBackedBySnapshotNoSnapshotReleasesCheckpoint` | A never-written leaf with no snapshot releases its block at its persisted checkpoint, with no durable coverage behind it. | #4523 | the `ReleaseBackedBySnapshot` row's |
| `RecoveryNeverFallsOffLogNoSnapshotReleaseTwoFaults` | The same release, composed over two faults into a stale latch; the mutation raises the fault budget to two. | #4523 | the `RecoveryNeverFallsOffLog` row's |
| `LogPrefixAppliedWatermarkIgnoresAbandoned` | An abandoned append leaves the watermark, a reader passes it, and the call then lands below the reader. | #4621 | the `Abandon` and `LogPrefixApplied` rows' |
| `ReadPositionHonestLostRowReadsAsFresh` | A lost leaf row reads back as a fresh one, so the leaf starts cold over a WAL trimmed under its lost snapshot. | #4654 | the `LeafRowVanish` and `ActivateRowless` rows' |
| `ReadPositionHonestRowlessActivationStartsCold` | A rowless leaf activates with no create intent at all. | #4654 | the `ActivateRowless` row's |
| `ShippingNeverSkipsTrailingHoleExposed` | A trailing hole is exposed; after a crash the recovered allocator reissues it below the reader that passed it. Two faults. | #4621 | the `ReadStep` row's head-bound detector |

## Retention TTL

An operator-configured `WalRetention` is a retention event, not a defect: the GC's
TTL arm admits an entry older than the window however the consumer clauses read
(`WalGcTrimCore.ClassifyEntry` keeps it independent of the cursor and
`consumerOffsetFloor` arms). The model takes no such step. What is verified instead
is what the trim can overtake, and that every consumer it overtakes detects the
trim and recovers.

| Consumer | What a TTL trim can overtake | Detection | Recovery | Detector |
|----------|------------------------------|-----------|----------|----------|
| Leaf materialiser with a durable checkpoint | Nothing. The scan stops at the partition's durable materialiser offset floor, the minimum of every leaf's `min(checkpoint, coverage)`, before eligibility is evaluated (`LatticeWalGc.TrimShardAsync`), so the TTL arm never reaches an entry a leaf has not durably applied or snapshotted. | Not needed. | Not needed. | `LatticeWalGcRetentionLeafFloorTests.RunOnceAsync_retention_ttl_never_trims_past_a_leafs_durable_offset_floor` (red when the floor stop is dropped). |
| Leaf materialiser with no durable checkpoint on the partition (a standing `Zero` block pin: a data-bearing leaf that has never checkpointed, present in the registry or not, including a tree none of whose leaves has checkpointed yet) | Nothing. Every durable pin at or below `Zero` that the offset floor does not cover holds the partition it names against every admitting arm, the TTL arm included (`DurableMaterialiserFloor.IsPartitionHeldByBlockPin`, issue #4622), because such a leaf replays from the `-1` sentinel on a cold activation and could not detect a trimmed prefix. Every other uncovered leaf pin caps the partition's TTL ceiling at its frontier (`DurableMaterialiserFloor.RetentionCeilingFor`): the frontier was published by an empty release, when the leaf held no row in the partition and had applied nothing there, so every entry the leaf has written there since is stamped above it, although the pin store's max merge keeps the frontier once that write turns the partition into a block. That precondition is the empty-release arm's, and two production rules keep it true. No partition releases empty before the leaf's replay has read it: until the replay barrier latches, `BPlusLeafGrain.WithUnreplayedPartitionsLive` counts every partition as data-bearing (issue #4669, fixed by #4677). And a write stamped below the leaf's clock - a replication apply, a range delete's issue stamp, a carried copy, a reap - is preceded by a durable override hold that the GC reads as a block until the consumer's first real offset lands (issue #4641, fixed by #4679; `BPlusLeafGrain.NeedsOverrideHold`, `WalMaterialiserPinState.OverrideHolds`). | Not needed. | Not needed. | `WalRetentionBlockPinTests.A_retention_trim_keeps_an_acknowledged_write_a_never_checkpointed_leaf_owns` (real grains, silo lost; red when the TTL arm passes a block pin), `WalRetentionBlockPinTests.A_retention_trim_keeps_a_write_a_leaf_applied_after_it_released_the_partition_empty` (real grains: an empty release by a graceful deactivation, then a write, then the silo is lost; red when only a `Zero` pin holds the TTL arm), `LatticeWalGcBlockPinHoldTests.A_retention_ceiling_does_not_trim_a_partition_a_standing_block_pin_holds` (red when the TTL arm passes a block pin), `LatticeWalGcBlockPinHoldTests.A_retention_ceiling_is_capped_at_an_uncovered_leaf_pins_frontier` (red when the cap is dropped), `LatticeWalGcBlockPinHoldTests.A_registered_leafs_cursor_does_not_trim_a_partition_its_standing_block_pin_holds` (red when only registry-absent pins hold) and `LeafDurablePinCoreTests.Live_never_checkpointed_rows_over_an_unproven_wal_keep_the_block` (the empty-release precondition: a partition holding a live row never releases empty). At the model level, `WalPartitionReleaseCoyoteTests` drives `LeafDurablePinCore` and `WalGcTrimCore` on a leaf spanning two partitions: `A_ttl_that_yields_only_to_zero_pins_trims_a_write_made_after_an_empty_release` is red without the cap, and `Empty_releases_lose_no_acknowledged_write_under_the_replay_barrier_and_the_frontier_capped_ttl` holds with it. The two rules that keep the cap's premise true are detected end to end on real grains, silo lost: `LeafEmptyReleaseBeforeReplayTests.A_cold_leaf_never_releases_a_partition_its_replay_has_not_read` (red on the code before #4677 and with the gate disabled), and `WalOverrideHoldTests.A_trim_keeps_a_replicated_write_stamped_below_an_empty_release_frontier` (cursor and retention arms), `WalOverrideHoldTests.A_trim_never_resurrects_keys_a_replicated_range_delete_removed`, `WalOverrideHoldTests.A_hold_survives_a_restart_between_the_write_and_its_coverage`, `WalOverrideHoldTests.A_block_report_after_the_write_does_not_release_the_hold` and `WalOverrideHoldTests.A_carried_stamp_saga_terminal_holds_the_touched_leaf_on_the_terminal_partition` (each red when the leaf never raises, when the GC ignores holds, when a report clears the hold, or when the shard root never raises, as the case applies); the trigger by `BPlusLeafGrainOverrideHoldTriggerTests.A_saturated_merge_that_leaves_the_stamp_at_the_clock_still_needs_a_hold`. In the model, `WalPartitionReleaseCoyoteTests.An_empty_release_published_before_the_partition_is_replayed_trims_an_unread_write`, `WalPartitionReleaseCoyoteTests.Skipping_the_override_hold_lets_the_gc_trim_an_override_stamped_write_issue_4641`, `WalPartitionReleaseCoyoteTests.Dropping_the_override_hold_without_a_real_offset_lets_the_gc_trim_an_override_stamped_write_issue_4641`, `WalPartitionReleaseCoyoteTests.Clearing_the_override_hold_on_a_persisted_checkpoint_lets_the_gc_trim_an_override_stamped_write_issue_4641`, `WalPartitionReleaseCoyoteTests.Reading_the_holds_before_the_head_bound_lets_the_gc_trim_an_override_stamped_write_issue_4641` and `WalPartitionReleaseCoyoteTests.A_trigger_on_a_stamp_below_the_clock_alone_misses_a_saturated_merge_issue_4641` are each red with one rule removed, and the design run holds with override-stamped and saturated writes. |
| Replication shipper | Entries it has not durably acknowledged. | A shipping read whose first sequence is above the one requested (`ReplicationShipperGrain.ForcedGap`). | The shipper withholds saga records and asks the peer to re-seed (`ReplicationBatch.ReseedAfterEpoch`); the receiver bootstraps from an export taken after the gap, and the shipper then rewinds every partition to its lowest retained entry (#4534, #4599). | `CrossClusterAtomicVisibilityTests.Saga_whose_prepare_was_trimmed_unshipped_is_never_delivered_torn`, `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has` and `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`. |
| View maintainer | Entries past its durable read position. | `WalLogSubscriber` reports a fall-off when the tail has passed the next offset it needs, probed before the read and again whenever a read jumps an offset. | The maintainer logs the warning `View '{ViewName}' fell off the WAL on source '{SourceTree}'; rebuilding.` (`ViewMaintainerGrain.DrainAsync`; the aggregation drain logs its own), rebuilds from current source state and resumes tailing from the heads it captured. An accumulative (history) view's rebuild collapses its timeline to one revision per key: that is its contract, since a history view's timeline is bounded by WAL retention (`docs/lattice/history-views.md`), and the warning makes the collapse observable. | `WalGcViewOffsetFloorTests.A_view_a_retention_trim_overtakes_falls_off_and_rebuilds_from_source_state` (red when the maintainer ignores the fall-off), `HistoryViewWalFallOffTests.A_history_view_a_retention_trim_overtakes_logs_the_fall_off_and_collapses_to_current_state` (asserts the warning and the collapse; red likewise), `WalLogSubscriberTests.DrainAsync_reports_fell_off_log_when_a_trim_lands_between_the_tail_probe_and_the_read` and `ViewMaintainerAggregationDrainTests.Aggregation_drain_rebuilds_when_the_source_WAL_trimmed_past_the_checkpoint`. |
| Incremental backup capture | Its delta window (it is not a retention reader). | `LatticeBackupCaptureService.HasFallenOffAsync` before the drain, and `WalLogSubscriber` during it. | A full capture, whose cut is read from leaf state and is newer than the window it replaces. | `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_falls_back_to_a_full_when_the_base_resume_point_fell_off_the_wal` and `IncrementalDeltaCollectorTests.A_trim_landing_after_the_tail_probe_makes_the_capture_fall_back_rather_than_skip_entries`. |

## Deliberate abstraction gaps

These are modelled abstractly or not at all. **No conclusion about them may be drawn
from this specification.**

- **Values, keys, HLCs and CRDT merge.** A write is an offset and a projection is a
  set of offsets, so replay idempotence (applying an entry twice leaves the
  projection unchanged) is true by construction here and asserts nothing. It is the
  LWW and CRDT cores' property, not this model's; see the excluded candidate below.
- **The checkpoint is NOT durably monotone.** The issue proposed
  `CheckpointMonotonicDurable`. It is false of production by design: rehydrating a
  snapshot older than the persisted checkpoint lowers the checkpoint to the
  snapshot's coverage (#2280, #2404), and the next persist writes the lower value.
  The durable quantities that ARE monotone are the pin store (by max merge) and
  snapshot coverage (`SnapshotCoverageMonotonic`); the model states those instead.
- **Saga state.** Prepared buckets, deferred terminals, the unresolved-replay-work
  ledger and the prepare clamp on checkpoint advance (`BPlusLeafGrain.PendingTx`)
  are not modelled; the atomic-commit module covers the saga.
- **Splits, merges and resharding.** Split handoff, checkpoint hints
  (`BPlusLeafGrain.ApplyCheckpointHintAsync`), moved-away slots and the warm-cache
  rescue are not modelled; the shard ownership module (#4434) owns them.
- **Replication, view maintainers, log subscribers and backup capture.** They are
  WAL consumers the offset floor does not speak for. They are absent here, which
  only over-approximates the trim, so nothing here speaks for them. Production
  bounds the trim for each retention reader in offset space, on every silo, as
  `WalMove.tla`'s consumer states (`t - 1 <= cons`): the replication shipper and
  every view maintainer register with the log's durable
  `IWalOffsetConsumerRegistryGrain` before they read it, and each GC pass refuses
  any entry at or above the lowest per-partition read position they publish
  (`WalGcTrimCore.ClassifyEntry`, `consumerOffsetFloor`; issues #4579, #4584).
  Their HLC cursors, which are not an offset bound and which only the silo they
  run on can see, no longer carry that guarantee. An incremental backup capture is
  deliberately not a retention reader: the GC may trim past it, and its fall-off
  decision is exact by offset and made against what was actually read
  (`WalLogSubscriber` probes the tail again when a read jumps an offset), so a
  trim it misses makes the capture fall back to a full backup, whose cut is read
  from leaf state the durable pins protect and is newer than the window it replaces
  (`IncrementalDeltaCollectorTests.A_trim_landing_after_the_tail_probe_makes_the_capture_fall_back_rather_than_skip_entries`).
  A configured retention TTL may trim past a reader; see "Retention TTL" below.
- **HLC stamps, several partitions and the empty release: checked by a Coyote
  model.** Stamps and frontiers are abstracted away here and the instance has one
  partition, so the GC arms that admit by stamp (the cursor, the offset admission's
  uncovered cursor, the retention ceiling capped at an uncovered frontier), the
  `ReleaseEmpty` arm of `LeafDurablePinCore.Resolve` for a leaf whose clock is live
  but which holds no row in a partition (the abstaining `(clock, -1)` release,
  #1490's narrowest arm; issue #4433, finding F08), the `walProvenEmpty` arm (#3103),
  and writes stamped below the leaf's clock (issue #4641) are not expressible in
  TLC. `WalPartitionReleaseModel` checks them together on a leaf whose writes span
  two partitions, driving the production cores `LeafDurablePinCore.Resolve` and
  `WalGcTrimCore.IsEntryEligible` with the replay barrier of #4677 and the override
  hold of #4679 (trigger, store-side prune, and the GC's three reads as separate
  steps): `WalPartitionReleaseCoyoteTests.Empty_releases_lose_no_acknowledged_write_under_the_replay_barrier_and_the_frontier_capped_ttl`
  holds, from a normal start and from a WAL reset, and each of its guards is red with
  one rule removed (see the Retention TTL table). The production detectors are
  `LeafEmptyReleaseBeforeReplayTests`, `WalOverrideHoldTests`, and for the two
  arms `LeafDurablePinCoreTests.An_empty_partition_with_nothing_applied_releases_with_its_sentinel_checkpoint`
  and `LeafDurablePinCoreTests.Live_never_checkpointed_rows_over_a_proven_empty_wal_release_the_block_issue_3103`.
- **A purge is a contract, not a loss (issue #4654).** Once a shard has recorded
  that its purge began clearing leaves (`ShardRootState.LeafClearsBegun`), the
  operator has deleted that data, and a leaf recovery re-creates empty there is the
  intended outcome. `ReadPositionHonest` is therefore quantified over leaves whose
  shard has not begun a purge (`Purging`); that is its only carve-out, and
  `RecoveryNeverFallsOffLog` has none: a re-create over a surviving snapshot is
  refused, not latched stale (`RecoveryNeverFallsOffLogCreateIntentOverSurvivingSnapshot`).
  The checked configurations apply the action constraint `PurgeFreezesProtocol`:
  once a purge has begun only its clears and the recovery's re-creates (or their
  refusals) are explored, because a deleted tree refuses data operations and every
  property the purged data could falsify is carved out for it. The flag is
  absorbing; production clears it after the reseed, which begins a fresh lifecycle.
  The model also folds the record into each clear's step (see `PurgeClear`).
- **The row record.** `ILeafRowRecordGrain` is not modelled. It is defence in
  depth: a lost row with a surviving record already fails closed on the intent rule
  alone, and the model shows the intent rule closes the double loss of the row and
  its record without it.
- **Shard moves.** Specified in `WalMove.tla`, not here.
- **Atomicity of a grain turn: a modelling abstraction, covered at the
  implementation level.** Each action is one atomic step. Production interleaves
  grain turns at await points (`[AlwaysInterleave]` methods, timers, the background
  replay task of #2909); the model's actions are coarser, so an interleaving
  *inside* a persist, a capture or a rehydrate is not explored here. Those
  interleavings are covered where they live: a capture racing the replay or the
  rehydrate by `BPlusLeafGrainTests.Capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack`
  and `BPlusLeafGrainTests.Capture_while_the_snapshot_rehydrate_is_in_flight_never_claims_coverage_its_rows_lack`;
  `#4017`'s persist that failed across an await by
  `BPlusLeafGrainTests.Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint`
  and the lifecycle Coyote guard `NoRollbackOnFailedPersist`; and the WAL shard's own
  in-turn races by the Coyote models that explore them -
  `WalOffsetContiguityCoyoteTests.Split_read_advance_hands_two_appends_the_same_offset`
  (offset assignment), `WalMoveQuiesceCoyoteTests.Split_fence_check_strands_an_offset_past_the_fence`
  (the fence check and the assignment), `WalShippingWatermarkCoyoteTests.Raw_tail_without_watermark_strands_an_in_flight_offset`
  (flush completions against reader polls) and
  `WalCommitLogWriterDrainCoyoteTests.Checking_the_token_before_parking_loses_a_wakeup`
  (the drain against parked callers).
- **The replay barrier and data operations: a modelling abstraction.** Readers of
  the projection are not modelled; `ReadPositionHonest` is stated over the
  projection a reader WOULD be served once the barrier releases, which is stricter
  than production's barrier needs while a replay is still running. That no data
  operation is served before the barrier releases is pinned by
  `BPlusLeafGrainTests.Every_data_entry_point_waits_for_the_replay_barrier`.

## Excluded candidates from the issue

| Candidate | Why it is not a checked property |
|-----------|----------------------------------|
| `ReplayIdempotent` | Faithfully inexpressible in this abstraction: projections are sets, so re-applying an entry cannot change one. Idempotence of a real apply is a property of the LWW and CRDT cores (`LwwValue<T>` merge), outside this model. |
| `CheckpointMonotonicDurable` | False of production by design (see the gaps above); replaced by `SnapshotCoverageMonotonic` and the pin store's monotone merge, which are the monotone durable quantities. |
