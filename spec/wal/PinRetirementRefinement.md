# Refinement note: WAL orphan-pin retirement to production

This note maps [`WalPinRetirement.tla`](WalPinRetirement.tla) - the WAL garbage
collector retiring a leaf's durable materialiser pins, racing the leaf's own
lifecycle and its re-creation under the same grain id - to the Orleans.Lattice code
that plays each role. The leaf-side durability lifecycle (append, flush, replay,
checkpoint, snapshot, crash recovery) is mapped in [`Refinement.md`](Refinement.md);
this module takes its floor, block pin and trim as given. Like that note it is a
documented mapping, not a refinement proof: the gates check its names, detectors
and coverage, and its claims were checked by reading production and by perturbing
it under each detector.

## Model scope in one paragraph

One leaf grain of one tree publishes a durable materialiser pin under one consumer
id into a pin store spread over two routing keys, on one WAL partition of two
offsets. The leaf writes, checkpoints, publishes and seeds; the tree is
soft-deleted and recovered, purged (mark, row clear, own pin retirement, any of
which can be interrupted), folded away, or discarded as an undone resize's copy;
and once a purge or a fold has cleared the leaf's row its grain id can be populated
again once, re-binding its tree id and seeding a block pin under the same consumer
id. Against that, the
starvation drive's retirement and one orphan sweeper (two in the `TwoSweepers`
variant) each take a token per key, read the pins, read the leaf's durable row
once, and decide each found pin through the consumer-id gate and the removal fence,
as separate steps; the deleted-tree hold retires a discarded tree's pins; a completed
purge's tree-level tail trims the log, clears the pin store and frees the id, and is
re-driven until its record lands; and the GC trims to WalDurability's floor. Routing changes, unreadable reads and abandoned
runs are environment faults, zero in the base and one in the `OneFault` variant.
The design checked is the fenced one (issue #5358), with a purge tail that acts only
on its own registry lineage (issue #5417); production's earlier behaviour is
reproduced by mutations.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `tail` | The WAL partition's allocator head and oldest readable offset | The partition's `WalShardGrain` sequence and retained prefix, as in [`Refinement.md`](Refinement.md). |
| `phase` | The leaf grain's durable row and its tree's lifecycle | `LeafNodeState.TreeId` on the leaf's row (bound: `live`, `deleted`, `marked`, `draining`; `husk`: no record or no tree id, as `WalFloorHolderReader.Classify` reads it); the tree's soft delete and recovery (`TreeDeletionGrain.RecoverPhysicalAsync`); the purge marker (`ILeafRowRecordGrain.MarkPurgeClearedAsync`); a fold's drain before `BPlusLeafGrain.ClearGrainStateCoreAsync`; and a discarded copy (`ITreeDeletionGrain.DiscardIfAbandonedDerivedCopyAsync`). |
| `inc` | How often the leaf's grain id was populated again | A modelling device: production has no incarnation number, which is the defect. The consumer id (`BPlusLeafGrain.ResolveConsumerIdBase`) is the same for every incarnation. |
| `seeded` | The incarnation's birth block pin has landed | `BPlusLeafGrain.SeedDurableMaterialiserBlockPinAsync`, awaited by `BPlusLeafGrain.SetTreeIdAsync` and `BPlusLeafGrain.InitializeSiblingAsync` before the routed writes that follow. |
| `cp` | The incarnation's durable checkpoint | The persisted projection checkpoint and the snapshot coverage `LeafDurablePinCore.Resolve` bounds the published offset by. |
| `need` | Writes the incarnation must still replay from the WAL | Acknowledged writes above the durable checkpoint. A purge's row clear deletes them by design; a fold moved them to a survivor; a discard abandons the copy. |
| `purgeMark` | Whether the leaf's row record admits re-creating the rowless leaf | `LeafRowRecordState.PurgeCleared` after a purge's clear (`BPlusLeafGrain.AdmitPurgeClearedAsync`, `BPlusLeafGrain.AdmitCreateIntentAsync`), or no row record at all after a fold's clear deleted it (`BPlusLeafGrain.ClearRowRecordAsync`), which `BPlusLeafGrain.AdmitCreateIntentAsync` admits under a create intent when no snapshot survives either. Recovery and re-creation reset it. |
| `owed` | The leaf's own pin retirements still owed by its terminal step | `BPlusLeafGrain.RetireMaterialiserPinsAsync`, one `ILeafCursorReporter.UnregisterAsync` per consumer, each fanning out to every read key. |
| `route` | The key the leaf's pins are written under now | `WalMaterialiserPinRouting.ShardKey` under the shard count `WalMaterialiserPinRouting.ResolveShardCount` resolves. |
| `pinOff` | The pin store's entries | `WalMaterialiserPinState` per key, merged by monotone max in `WalMaterialiserPinGrain.Merge`; `-1` is the Zero block pin, `NoPin` an absent entry. |
| `pinInc` | Which incarnation last merged into an entry | A ghost: the store does not record it, and a birth seed into a surviving entry changes nothing a read can see. |
| `pc`, `seen`, `found`, `verdict` | One retirement run | The sweep's `located` map and leaf reads in `LatticeWalGcScheduler.SweepOrphanedMaterialiserPinsCoreAsync`, and the drive's in `LatticeWalGcScheduler.RetireConsumerFencedAsync`. |
| `touched` | Tokens a later merge invalidated | `WalMaterialiserPinGrain.RecordMerge` stamping the consumer's merge after the token from `WalMaterialiserPinGrain.GetRetirementTokenAsync`. |
| `faults` | Environment budget | Modelling device only. |
| `tailOwed`, `tailLin` | A completed purge's owed tail and the registry lineage it recorded | `TreeDeletionState.RegistryUnregisterPending` and `TreeDeletionState.PurgedRegistryLineage` (with `TreeDeletionState.PurgedRegistryLineageRecorded`), written with the purge's completion by `TreeDeletionGrain.ReadPurgedRegistryLineageAsync`. A registry entry's `TreeRegistryEntry.Lineage` is modelled by `inc`: every new registration stamps a fresh one. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | A write reaches a live leaf only after its birth seed | `BPlusLeafGrain.SetTreeIdAsync` awaits `BPlusLeafGrain.SeedDurableMaterialiserBlockPinAsync` before the shard root's routed merge; a split sibling's `BPlusLeafGrain.InitializeSiblingAsync` likewise. | Yes: `BPlusLeafGrainTests.InitializeSibling_birth_seeds_block_pin_so_wal_gc_does_not_trim_past_split_sibling_frontier` and `BPlusLeafGrainTests.Cleared_leaf_SetTreeIdAsync_seed_still_persists`. |
| `Checkpoint` | The leaf's durable checkpoint covers its writes | The persisted checkpoint and snapshot coverage that `LeafDurablePinCore.Resolve` reads; the published offset never passes the persisted checkpoint (#3476). | Yes: `LeafDurablePinCoreTests.A_pending_checkpoint_never_reaches_the_pin_issue_3476` and `BPlusLeafGrainTests.Batched_pin_flush_over_an_unpersisted_advance_publishes_no_pin_past_the_persisted_checkpoint`. |
| `Publish` | The leaf merges its checkpoint into its pin at its route | `BPlusLeafGrain.ResolveDurablePinForPartition` through `ILeafCursorReporter.NoteDurableMaterialiserFrontier` into `WalMaterialiserPinGrain.Merge`. **Over-approximation:** a report that changes nothing is not modelled; it would only add a fence taint, which refuses more removals. | Yes: `LeafDurablePinCoreTests.A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage` and `WalMaterialiserPinGrainTests.ReportAsync_lower_frontier_is_coalesced_without_write`. |
| `Seed` | The birth seam seeds a Zero block pin | `BPlusLeafGrain.SeedDurableMaterialiserBlockPinAsync` through `ILeafCursorReporter.SeedDurableMaterialiserBlockManyAsync` into `WalMaterialiserPinGrain.SeedManyAsync`, which merges by monotone max and stamps the merge for the fence even when it changes nothing. A split sibling seeds its donor's head rather than the sentinel; the model's block is the stronger pin. | Yes: `LeafCursorReporterDurableSeedTests.Seed_block_writes_the_pin_through_before_returning` and `WalMaterialiserPinGrainTests.RemoveIfUnmergedSinceAsync_refuses_after_a_seed_that_changes_nothing`. |
| `RouteChange` | The routing of the consumer id changes between publications | `WalMaterialiserPinRouting.ShardKey` under a changed `WalMaterialiserPinRouting.ResolveShardCount`; the old entry is never deleted, and `WalMaterialiserPinRouting.EnumerateReadKeys` reads every key a pin may sit under. **Environment:** its guard is only the fault budget, no stronger than production, which changes the shard count by configuration at any time. | Yes: `WalMaterialiserPinRoutingTests.EnumerateReadKeys_covers_every_shardkey_target` and `LatticeWalGcSchedulerCadenceTests.The_sweep_retires_a_pin_from_every_key_it_was_found_under`. |
| `SoftDelete` | A soft delete keeps the leaf's row and pins | A deleted tree's retention is `PhysicalTreeRetention.Deleted`; `LatticeWalGcScheduler.HoldDeletedTreeAsync` retires nothing for it and reactivates nothing. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_does_not_reactivate_a_blocked_leaf_of_a_deleted_tree` and `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_reports_a_deleted_trees_blocked_floor_once_per_episode_at_error_level`. |
| `RecoverTree` | Recovery hands the tree back, including a marked leaf whose row survived | `TreeDeletionGrain.RecoverPhysicalAsync` re-seeds the shards' bindings; `BPlusLeafGrain.RecoverBindingAsync` re-binds a rowful leaf and resets its marker. | Yes: `TreeDeletionGrainTests.Recover_reseeds_node_bindings_on_all_shards` and `PurgeRecoveryIntegrationTests.A_marked_leaf_whose_purge_was_interrupted_before_its_row_clear_recovers_without_latching_stale`. |
| `MarkPurge` | The purge commits to the leaf, marker first | `BPlusLeafGrain.ClearGrainStateCoreAsync` (purge) calls `ILeafRowRecordGrain.MarkPurgeClearedAsync` before anything is cleared, and only names the pins. | Yes: `BPlusLeafGrainTests.Purge_clear_marks_the_row_record_before_the_row_is_cleared_and_keeps_it` and `BPlusLeafGrainTests.ClearGrainStateForPurge_interrupted_at_the_row_clear_keeps_the_pin`. |
| `Fold` | An empty-leaf fold has moved the leaf's rows and retires its pins | The fold's drain precedes `BPlusLeafGrain.ClearGrainStateCoreAsync` (non-purge), which retires the pins through `BPlusLeafGrain.UnregisterMaterialiserPinsAsync` before the clear (#3101). The drain itself is spec/bplustree's (`BPlusReclaimRecovery`). | Yes: `BPlusLeafGrainTests.ClearGrainState_retires_the_pin_before_the_state_is_cleared` and `BPlusLeafGrainTests.ClearGrainState_retires_one_pin_per_wal_partition`. |
| `ClearRow` | The row is cleared: after the mark for a purge, after the pin retirement for a fold | `BPlusLeafGrain.ClearGrainStateCoreAsync`: `state.ClearStateAsync` under the persist gate, the latch that refuses any later write of the row, then (purge only) `BPlusLeafGrain.RetireMaterialiserPinsAsync` last (#4700). A fold's clear then deletes the row record (`BPlusLeafGrain.ClearRowRecordAsync`) and a purge's keeps it marked; either leaves the row re-creatable, so both set `purgeMark`. | Yes: `BPlusLeafGrainTests.ClearGrainStateForPurge_retires_the_pin_only_after_the_row_is_cleared` and `BPlusLeafGrainTests.ClearGrainState_leaves_the_leaf_reading_as_having_no_durable_state`. |
| `SelfRetire(k)` | The terminal step removes its own pin under one key | `BPlusLeafGrain.RetireMaterialiserPinsAsync` calls `ILeafCursorReporter.UnregisterAsync`, which fans out to every read key. It is the publisher retiring itself in its own turn, so it is not fenced. | Yes: `BPlusLeafGrainTests.ClearGrainState_retires_the_materialiser_pin` and `BPlusLeafGrainTests.ClearGrainStateForPurge_retires_the_pin_only_after_the_row_is_cleared`. |
| `SelfRetireFail(k)` | The terminal step's removal fails or is lost; the failure is swallowed | `BPlusLeafGrain.RetireMaterialiserPinsAsync` logs and continues; `LeafCursorReporter.UnregisterAsync` swallows a pin removal failure. **Environment:** enabled for every owed key, no stronger than production, where any provider fault or silo loss can strand one. | Yes: `BPlusLeafGrainTests.ClearGrainState_completes_when_the_reporter_faults` and `LeafCursorReporterDurableSeedTests.Unregister_swallows_and_logs_a_pin_removal_failure`. |
| `Recreate` | A cleared grain id is populated again | `BPlusLeafGrain.RecoverBindingAsync` admits a rowless leaf only by its purge marker (`BPlusLeafGrain.AdmitPurgeClearedAsync`); `BPlusLeafGrain.AdmitCreateIntentAsync` admits one under a create intent - a reused tree id's deterministic root leaf, a bulk load re-using a leaf id - when its record is purge-marked or absent (a fold deleted it) and no snapshot survives; each then binds the tree id through `BPlusLeafGrain.SetTreeIdAsync` before seeding. The dying activation cannot do it: its clear latches it against any later row write and deactivates it. **Over-approximation:** enabled from every husk whose own retirement has ended, no stronger than production's two admissions. Nothing waits for a retirement in flight. | Yes: `BPlusLeafGrainTests.Create_intent_re_creates_a_purge_cleared_leaf` and `PurgeRecoveryIntegrationTests.A_leaf_whose_purge_clear_was_interrupted_part_way_is_re_created_by_recovery`. |
| `Discard` | An undone resize's unrecoverable copy is discarded | `LatticeWalGcScheduler.TryDiscardAbandonedCopyAsync` asks `ITreeDeletionGrain.DiscardIfAbandonedDerivedCopyAsync`; `TreeDeletionGrain.RecoverPhysicalAsync` refuses a discarded copy. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_discards_a_deleted_tree_that_is_an_abandoned_resize_copy` and `UndoneResizeWalReleaseIntegrationTests.A_destination_retired_by_an_earlier_build_is_discarded_once_no_resize_names_it`. |
| `DiscardRetire` | The deleted-tree hold retires every pin of a discarded tree | `LatticeWalGcScheduler.HoldDeletedTreeAsync` calls `ILeafCursorReporter.UnregisterTreeAsync` for `PhysicalTreeRetention.Discarded` only. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pins_of_a_discarded_tree_without_touching_its_leaves` and `TreeDeletionGrainTests.Discard_retires_every_leaf_materialiser_pin_on_the_tree`. |
| `PurgeTailRemove` | A completed purge's tail: trim the log, clear the pin store, remove the registry entry - only while the entry carries the recorded lineage | `TreeDeletionGrain.FinishCompletedPurgeAsync`: `TreeDeletionGrain.TrimPurgedWalAsync`, `TreeDeletionGrain.DeregisterLeafCursorsAsync` (pins before the registry entry, #5416), `TreeDeletionGrain.SettleRegistryEntryAsync`, gated by `TreeDeletionGrain.ClassifyPurgedRegistryEntryAsync` (#5417). **Over-approximation:** the model may re-drive it with no fault between; production re-drives only after the record's write, or the loop stop before it, threw, or the removal's reply was lost. | Yes: `TreeDeletionGrainTests.A_redriven_purge_leaves_a_reused_tree_id_untouched`, `TreeDeletionGrainTests.A_redriven_purge_whose_removal_never_landed_still_removes_the_purged_entry` and `TreeDeletionGrainTests.PurgeNow_clears_the_pin_store_before_the_registry_frees_the_tree_id`. |
| `PurgeTailSettle` | A re-drive that finds the id registered again under another lineage only records the removal | `TreeDeletionGrain.RecordLandedRegistryRemovalAsync`, with the lineage compared against the one recorded at completion, never re-read. | Yes: `TreeDeletionGrainTests.A_reused_tree_id_is_recognised_once_a_redriven_purge_has_settled`, `TreeDeletionGrainTests.A_redriven_purge_of_an_entry_without_a_lineage_recognises_a_reuse` and `TreeDeletionGrainTests.A_redriven_purge_whose_removal_landed_only_records_it`. |
| `GcTrim` | Trim to the durable floor | WalDurability's `GcTrim`: `LatticeWalGc.ApplyDurableMaterialiserFloorAsync` and `LatticeWalGc.TrimShardAsync`. **Over-approximation:** the model ignores the in-memory cursors production also consults, whose registry is per silo, so a GC pass on a silo that does not hold a leaf's cursor trims the shared WAL without it. | Yes: `WalGcTrimFloorCoyoteTests.Min_cursor_floor_never_trims_past_the_slowest_consumer` and `WalPinRetirementCoyoteTests.Fenced_retirement_never_retires_the_pin_of_a_recreated_leaf`. |
| `Start(p)` | A sweep begins, or the drive's NotDriven verdict starts a retirement | `LatticeWalGcScheduler.SweepOrphanedMaterialiserPinsAsync` (local, rate-limited) or `WalOrphanPinSweepGrain.ProcessNextPhaseAsync` through `IWalOrphanPinSweepRunner.RunAsync`; `LatticeWalGcScheduler.RetireOrphanedPinAsync` after `LeafStarvationDriveOutcome.NotDriven`. | Yes: `LatticeWalGcSchedulerCadenceTests.A_silo_with_a_coordinator_does_not_sweep_locally` and `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pin_of_a_leaf_that_reports_no_tree_id`. |
| `Enum(p, k)` | Take a key's token, then read its pins | `WalMaterialiserPinGrain.GetRetirementTokenAsync` then `WalMaterialiserPinGrain.GetPinsAsync` per key of `WalMaterialiserPinRouting.EnumerateReadKeys`, in both retirement paths. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_retires_a_pin_from_every_key_it_was_found_under` and `LatticeWalGcSchedulerCadenceTests.The_sweep_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into`. |
| `Read(p)` | One durable read of the leaf's row, without activating it | `LatticeWalGcScheduler.ReadLeafSnapshotOrNullAsync` and `WalFloorHolderReader.ReadLeafCheckpointSnapshotAsync`, classified by `WalFloorHolderReader.Classify` and `WalOrphanPinRetirementCore.IsHusk`. | Yes: `LatticeWalGcSchedulerCadenceTests.A_husk_leaf_is_classified_as_orphaned_rather_than_repairable` and `LatticeWalGcSchedulerCadenceTests.The_drive_does_not_remove_a_pin_whose_leaf_row_is_live_again`. |
| `ReadFail(p)` | The read faults and fails closed | `LatticeWalGcScheduler.ReadLeafSnapshotOrNullAsync` maps a non-timeout fault to an unreadable snapshot. **Environment:** charged to the fault budget, no stronger than production, where any read can fault. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_fails_closed_when_a_leaf_state_read_throws` and `WalOrphanPinRetirementCoreTests.Decide_keeps_every_pin_whose_leaf_read_is_not_a_husk`. |
| `Decide(p, c, k)` | Decide one found pin: husk read, gate, fence | `WalOrphanPinRetirementCore.Decide` with `LatticeWalGcScheduler.RetirementVerdict`, then `LatticeWalGcScheduler.RemovePinFromKeysAsync` through `WalMaterialiserPinGrain.RemoveIfUnmergedSinceAsync` and `WalOrphanPinRetirementCore.MayRemove` (#5358). | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into`, `LatticeWalGcSchedulerCadenceTests.The_drive_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into`, `LatticeWalGcSchedulerCadenceTests.The_sweep_never_retires_a_pin_whose_leaf_is_still_live` and `LatticeWalGcSchedulerCadenceTests.The_orphan_sweep_never_retires_a_pin_whose_leaf_id_is_not_a_guid_leaf_key`. |
| `Done(p)` | A run ends once every found pin is decided | The sweep's batch loop decides every located consumer before `CreateOutcome`; the drive's `LatticeWalGcScheduler.RemovePinFromKeysAsync` visits every key the pin was found under. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_retires_far_more_orphans_in_one_pass_than_the_floor_can_report` and `LatticeWalGcSchedulerCadenceTests.The_sweep_retires_a_pin_from_every_key_it_was_found_under`. |
| `Abandon(p)` | Cancellation, a timeout or the coordinator's loss ends a run part-way | The stopping token and `OrphanSweepOperationBudget` in `LatticeWalGcScheduler.SweepOrphanedMaterialiserPinsCoreAsync`; a lost `WalOrphanPinSweepGrain` activation loses only in-memory demand. **Environment:** charged to the fault budget, no stronger than production. | Yes: `LatticeWalGcSchedulerCadenceTests.A_stalled_sweep_releases_admission_on_cancellation_or_timeout` and `WalOrphanPinSweepGrainTests.A_failed_sweep_keeps_its_demand_owed`. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `NoTrimPastLiveNeed` | The WAL is never trimmed past a write a live, or recoverable, leaf still needs to replay. | Yes: `WalPinRetirementCoyoteTests.Fenced_retirement_never_retires_the_pin_of_a_recreated_leaf` (the production core under every interleaving with a re-created leaf), `WalPinRetirementCoyoteTests.Unfenced_retirement_retires_the_pin_of_a_recreated_leaf` and `LatticeWalGcSchedulerCadenceTests.The_sweep_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into`. |
| `LivePublisherPinned` | A seeded incarnation whose row is bound always holds a pin. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into`, `LatticeWalGcSchedulerCadenceTests.The_drive_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into` and `BPlusLeafGrainTests.ClearGrainStateForPurge_interrupted_at_the_row_clear_keeps_the_pin`. |
| `RetireOnlyHusk` | A GC retirement removes a pin only if its publisher was cleared after publishing it and nothing merged into it since. | Yes: `WalMaterialiserPinGrainTests.RemoveIfUnmergedSinceAsync_refuses_after_a_seed_that_changes_nothing`, `WalOrphanPinRetirementCoreTests.MayRemove_refuses_a_consumer_merged_after_the_token` and `LatticeWalGcSchedulerCadenceTests.A_live_leaf_pin_on_a_tree_pinned_to_more_partitions_than_configured_is_never_retired`. |
| `UnreadableNeverRetired` | A leaf read that faults never retires a pin. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_fails_closed_when_a_leaf_state_read_throws` and `WalOrphanPinRetirementCoreTests.Decide_keeps_every_pin_whose_leaf_read_is_not_a_husk`. |
| `OrphanEventuallyRetired` | A gone leaf's pins are eventually all retired, unless its grain id is populated again first. | Yes: `LatticeWalGcSchedulerCadenceTests.The_sweep_retires_far_more_orphans_in_one_pass_than_the_floor_can_report`, `LatticeWalGcSchedulerCadenceTests.The_sweep_removes_the_same_pin_when_no_leaf_was_recreated` and `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pins_of_a_discarded_tree_without_touching_its_leaves`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiments. |

<a id="property-classification"></a>

## Property classification (issue #2321)

`TypeOK`, `NoTrimPastLiveNeed` and `LivePublisherPinned` are state safety;
`RetireOnlyHusk` and `UnreadableNeverRetired` are action safety; and
`OrphanEventuallyRetired` is temporal liveness. All are reached and checked on the
base. The `OneFault` and `TwoSweepers` variants re-check every state and action
property with one environment fault and with a second sweeper; liveness is
**bounded-out** of both for the TLC budget (about eight minutes with two sweepers),
and stays checked on the base, where an interrupted self-retirement still leaves
real orphans. `UnreadableNeverRetired` is **unreached** on the base, which has no
fault to make a read unreadable; it is reached in `OneFault`, and its mutation
raises the bound. Nothing is faithfully- or blindly-inexpressible.

`OrphanEventuallyRetired` can fail on a protocol defect under the fairness the spec
asserts: `OrphanEventuallyRetiredReadNeverHusk`, `OrphanEventuallyRetiredEnumFindsNothing`
and `OrphanEventuallyRetiredDoneSkipsDecisions` each leave every fairness conjunct
intact. Fairness is on storage and turn completion - the leaf's own retirement, the
deleted-tree hold, a started run's reads and per-pin decisions, and a sweeper being
started again - never on what a decision decides: `Decide`'s outcome is fixed by its
inputs, and the drive starts only on its own NotDriven evidence.

Every experiment is an anchored action perturbation; none splices in an action:

| Mutation | Classification |
|----------|----------------|
| `NoTrimPastLiveNeedUnfencedRemoval` | Anchored action: `Decide`. Production before #5358. |
| `NoTrimPastLiveNeedDriveRemovesOnProbe` | Anchored actions: `Read`, `Decide`. Production's drive path before #5358. |
| `RetireOnlyHuskUnfencedRemoval` | Anchored action: `Decide`. |
| `RetireOnlyHuskIgnoresIdGate` | Anchored action: `Decide`, with `Foreign` raised. |
| `UnreadableNeverRetiredFaultReadsAsHusk` | Anchored action: `Decide`, at one fault. |
| `LivePublisherPinnedFailedReadReadsAsHusk` | Anchored action: `ReadFail`, at one fault. |
| `LivePublisherPinnedReadIgnoresTreeId` | Anchored action: `Read`. |
| `OrphanEventuallyRetiredReadNeverHusk` | Anchored action: `Read`; a protocol defect with fairness intact. |
| `OrphanEventuallyRetiredEnumFindsNothing` | Anchored action: `Enum`; a protocol defect with fairness intact. |
| `OrphanEventuallyRetiredDoneSkipsDecisions` | Anchored action: `Done`; a protocol defect with fairness intact. |
| `TypeOkStartInvalidPhase` | Anchored action: `Start`. |
| `TypeOkAbandonOverspendsFaults` | Anchored action: `Abandon`, at one fault. |
| `TypeOkSelfRetireFailInvalidKey` | Anchored action: `SelfRetireFail`. |
| `NoTrimPastLiveNeedWriteBeforeSeed` | Anchored action: `Write`. |
| `NoTrimPastLiveNeedCheckpointNotDurable` | Anchored action: `Checkpoint`. |
| `NoTrimPastLiveNeedPublishPastCheckpoint` | Anchored action: `Publish`. |
| `LivePublisherPinnedSeedSkipped` | Anchored action: `Seed`. |
| `LivePublisherPinnedRerouteDropsPin` | Anchored action: `RouteChange`, at one fault. |
| `LivePublisherPinnedSoftDeleteRetiresPin` | Anchored action: `SoftDelete`. |
| `LivePublisherPinnedRecoverClearedRow` | Anchored action: `RecoverTree`. |
| `LivePublisherPinnedPurgeRetiresPinFirst` | Anchored action: `MarkPurge`; the #4700 ordering reversed. |
| `NoTrimPastLiveNeedFoldKeepsRows` | Anchored action: `Fold`. |
| `NoTrimPastLiveNeedClearKeepsWrites` | Anchored action: `ClearRow`. |
| `LivePublisherPinnedSelfRetireUnowed` | Anchored action: `SelfRetire`. |
| `LivePublisherPinnedRecreateBeforeRetireEnds` | Anchored action: `Recreate`. |
| `NoTrimPastLiveNeedDiscardKeepsWrites` | Anchored action: `Discard`. |
| `LivePublisherPinnedHoldRetiresRecoverableTree` | Anchored action: `DiscardRetire`; #3930's Deleted/Discarded distinction. |
| `NoTrimPastLiveNeedTrimIgnoresBlockPin` | Anchored action: `GcTrim`. |
| `NoTrimPastLiveNeedPurgeRedriveIgnoresLineage` | Anchored action: `PurgeTailRemove`. Production before #5417. |
| `NoTrimPastLiveNeedRedriveReReadsLineage` | Anchored action: `PurgeTailSettle`. |

## Detectors

`WalPinRetirementModel` (`test/lattice/BPlusTree/Coyote/`) executes the production
decision core, `WalOrphanPinRetirementCore.Decide` and
`WalOrphanPinRetirementCore.MayRemove`, against a purge-cleared leaf being re-created
under every interleaving of the retirement's token, read and removal with the
re-creation's tree-id bind, seed and write. Its fixed-design test holds; its two
guard tests remove the fence (`Unfenced`) and take the token after the read
(`TokenAfterRead`), and Coyote re-finds the retired pin of a re-created leaf in
each. The gate verdict, budget and pin-grain epoch are pinned to the arms that
remove, with a comment saying why (#2319); `WalOrphanPinRetirementCoreTests` covers
the other arms.

The scheduler fixtures drive the real `LatticeWalGcScheduler` with a fake pin store
and leaf-state provider whose read hook re-creates the leaf between the
retirement's read and its removal. The production perturbation
[`perturbations/UnfencedPinRemoval.perturbation`](perturbations/UnfencedPinRemoval.perturbation)
replaces the fenced removal with `WalMaterialiserPinGrain.RemoveAsync`: both
re-creation fixtures go red, and the controls stay green.

## Defect mutations whose fixes have landed

`NoTrimPastLiveNeedUnfencedRemoval`, `RetireOnlyHuskUnfencedRemoval` and
`NoTrimPastLiveNeedDriveRemovesOnProbe` reproduce issue #5358, found by this
model: the GC removed an orphaned pin by consumer id after a husk read taken in a
separate step, so a purge-cleared leaf re-created in between lost the block pin its
birth seeded under the same consumer id, and the trim could pass its
un-checkpointed writes. The fix in this change fences the removal through the pin
grain's per-activation merge stamps and moves the drive onto the sweep's fenced
sequence; the mutations are ordinary regression checks.

`NoTrimPastLiveNeedPurgeRedriveIgnoresLineage` reproduces issue #5417, found by the
independent review of this model: a completed purge's tail - WAL trim, pin-store
clear, registry removal - was re-driven whenever its registry removal was still
recorded as owed, and that record is cleared only by a write after the removal. A
removal that landed with its record unwritten was re-driven against an id a write or
a create had registered again, trimming the new tree's acknowledged writes and
clearing its pins. The purge now records the lineage of the entry it removes with
its completion, and a re-drive acts only while the entry still carries it.

## Deliberate abstraction gaps

- **One leaf, one partition, two keys.** The model has one leaf grain publishing one
  consumer id per partition on a single partition, and two pin-store keys. Several
  leaves, several partitions per leaf and the per-leaf read memo of a sweep are
  not modelled; the fence is per consumer per key, so they compose. The sweep's
  floor-holder census and its sampling budgets decide nothing here.
  Production symbols: `LatticeWalGcScheduler.ClassifyFloorHolderPinsAsync`, `LatticeWalGcScheduler.ReadBlockingPinStateMemoisedAsync`.
- **Frontier and offset as one value.** A pin is one offset with `-1` as the Zero
  block; the HLC frontier, override holds and the offset census's completeness
  checks are WalDurability's and `WalPartitionReleaseModel`'s.
  Production symbols: `WalMaterialiserPinGrain.RaiseOverrideHoldsAsync`, `LatticeWalGc.ApplyDurableMaterialiserFloorAsync`.
- **In-memory cursors.** The trim ignores the in-memory cursor registry. That
  registry is per silo and keeps a cursor until an unregister or a silo restart,
  so it protects a leaf only from GC passes on the silo holding its cursor; a pass
  on any other silo trims the shared WAL without it. The model therefore reports a
  trim where production loses the write only once the re-created leaf also replays
  cold - its activation ends before it checkpoints and republishes.
  Production symbols: `InMemoryWalCursorRegistry`, `ILeafCursorReporter.UnregisterAsync`.
- **Late delivery of an abandoned retirement call.** A dying activation's own
  retirement completes or is dropped before the grain id can be populated again;
  a removal message still in flight from a lost silo that lands after the
  re-creation's seed is not modelled, and neither is a duplicate activation of
  the dying leaf (Orleans keeps one activation per grain only eventually, across a
  membership change) whose retirement runs after the seed. Both need a silo or
  directory failure; the self-retirement is unfenced, so either would erase the seed.
  Production symbols: `BPlusLeafGrain.RetireMaterialiserPinsAsync`.
- **Pin-grain reactivation.** The fence's epoch is fixed in the model; a pin grain
  reactivated between a token and a removal refuses it, which only retains more.
  Production symbols: `WalMaterialiserPinGrain.GetRetirementTokenAsync`.
- **A swallowed birth seed.** A re-created leaf whose seed fails takes writes with
  no block pin until it publishes; that window is the leaf lifecycle's, not the
  retirement's, and the model's seed always lands.
  Production symbols: `LeafCursorReporter.SeedDurableMaterialiserBlockManyAsync`.
- **The tree-level purge tail, by step.** The model's purge tail is one step that
  trims, clears the pin store and frees the id, so it cannot express the order of
  those three: that the pin store is cleared before the registry entry is removed
  (issue #5416) is pinned by
  `TreeDeletionGrainTests.PurgeNow_clears_the_pin_store_before_the_registry_frees_the_tree_id`
  and `TreeDeletionGrainTests.CompletePurge_clears_the_pin_store_before_the_registry_frees_the_tree_id`,
  not by the model. Its single leaf completes the purge's walk with its own row
  clear, and a purge completed by a build that predates the lineage record, or
  whose entry was already gone, is re-driven without the check.
  Production symbols: `WalMaterialiserPinGrain.ClearAsync`, `ILeafCursorReporter.UnregisterTreeAsync`, `TreeDeletionState.PurgedRegistryLineageRecorded`.
