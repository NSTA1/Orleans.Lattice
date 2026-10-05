# Refinement note: WAL shard move to production

This note maps [`WalMove.tla`](WalMove.tla) - a WAL shard move running concurrently
with appends, out-of-order flushes, a consumer, the GC, a shard crash and the loss of
the move's coordinator - to the Orleans.Lattice code that plays each role. The
leaf-side lifecycle is mapped in [`Refinement.md`](Refinement.md). Like that note it
is a documented mapping, not a refinement proof; the same gates check its names,
detectors and coverage, and none checks its claims - that was done by reading
production and by perturbing it under each detector.

The model's fence has two halves: a DURABLE fence, a record in the WAL placement pin
under a lease (`WalMoveFence`, carried by `WalPlacementPin`), and the source
ACTIVATION's in-memory fence, which every new activation re-derives from the durable
record. Production has had both since issue #4525 was fixed; before that it had only
the activation half, the fence was lost with the activation, and the flip re-checked
nothing about the source.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next` | The stream's offset allocator | The WAL shard's next sequence, advanced by `WalOffsetAllocationCore.Assign` inside `WalShardGrain.AppendAsync`. |
| `inflight` | Appends whose flush has not landed | The shard's in-flight flush chain (`WalShardGrain.FlushAsync`). |
| `durable` | What the stream's current home holds | The partition's provider entries; after a move's flip, the target provider's. |
| `tail` | Oldest readable offset | The current home's oldest retained offset. |
| `acked` | Acknowledged writes | Appends whose `WalShardGrain.AppendAsync` completed. |
| `cons` | Every reader of the stream | One cursor standing for leaf replay, the replication shipper, view maintainers and log subscribers, each of which reads below `WalShippingWatermark.DurableContiguousTail` and holds the GC floor. |
| `move[m]` | Each coordinator's phase | The phase of move `m`'s `LatticeAdminGrain.RunMoveCopyPhasesAsync`: `"idle"` before it starts, `"fenced"` once the source is quiesced, `"copied"` once the delta is copied, `"done"` after the flip, an abort or the loss of its coordinator. It belongs to the coordinator, so a shard crash does not reset it. `MoveIds` is the moves an operator starts, each under its own move id (`LatticeAdminGrain.NewWalMoveId`): one in the base, two in the `TwoMoves` variant. |
| `moveCopy[m]` | What move `m`'s target holds | Entries `LatticeAdminGrain.RunMoveCopyPhasesAsync` has copied to the target provider. |
| `dfence`, `lapsed` | The durable fence and its lease | The partition's `WalMoveFence` in the placement pin (`WalPlacementPin`): `dfence` is the fence's move id (`NoMove` when none is held), and the record also carries the source provider key and the lease expiry. `lapsed` is that expiry having passed. |
| `afence` | The source activation's fence | `WalShardGrain._moveFenced`, raised by `WalShardGrain.QuiesceForMoveAsync` or, at activation, from the durable fence (`LatticeOptionsResolver.ResolveWalShardPlacementAsync`), and lost with the activation. |
| `crashes`, `coordCrashes` | Environment budgets | Modelling devices only: crashes do not recur for ever. Moves are finite because each move id runs once. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Append` | Append admitted only when the activation is unfenced | `WalShardGrain.AppendAsync` refuses through two checks that are each sufficient alone: `WalShardGrain.ThrowIfMoveFenced` on entry, and `WalMoveFenceCore.IsAppendAdmitted` under the same state gate that assigns the offset. The second is the one that closes the race with a quiesce raised after the first check ran; for an append arriving after the fence is up, the two are redundant, so removing either alone is not observable. | Yes: `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence` (the in-gate check closes the race) and `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move`, which goes red when both checks are removed together or when the fence is never raised. |
| `FlushAck(o)` | Flush lands, write acknowledged | `WalShardGrain.FlushAsync` completes an append only after its provider flush; completions in any order. | Yes: `WalShardGrainTests.AppendAsync_hung_provider_flush_faults_with_timeout_when_deadline_elapses`. |
| `Consume` | A reader advances below the watermark | Every cursor-advancing reader is shown offsets below `WalShippingWatermark.DurableContiguousTail` only (`WalShippingWatermark.IsOffsetExposable`). | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is` (the boundary, which the Coyote model's atomic flush cannot reach). |
| `GcTrim` | Trim below what readers have consumed | `LatticeWalGc.TrimShardAsync` trims only entries `WalGcTrimCore.IsEntryEligible` admits. The model's consumer floors the trim in offset space (`t - 1 <= cons`), and so does production, on every silo: leaf replay through the durable materialiser pins, and every other retention reader (the replication shipper, view maintainers) through the per-partition read position it publishes as an `IWalOffsetConsumer` in the log's durable `IWalOffsetConsumerRegistryGrain`, which `LatticeWalGc.ReadOffsetConsumerFloorsAsync` reads on every pass and `WalGcTrimCore.ClassifyEntry` enforces (`consumerOffsetFloor`), failing closed when a position cannot be read (issues #4579, #4584). **Over-approximation:** one consumer floors the trim; production floors it under the minimum across every consumer. A configured retention TTL may trim past a consumer; that is a retention event the model does not take, and every consumer it overtakes detects it and recovers (see `Refinement.md`, "Retention TTL"). | Yes: `WalGcTrimCoreConsumerOffsetFloorTests.ClassifyEntry_refuses_an_entry_at_the_consumer_floor_that_the_cursor_admits` and `WalGcTrimCoreConsumerOffsetFloorTests.ClassifyEntry_refuses_an_entry_above_the_consumer_floor_that_the_materialiser_offset_admission_admits` (red when the clause is dropped); `WalGcShipperOffsetFloorTests.Gc_retains_an_unshipped_entry_stamped_below_the_shipper_cursor_after_its_leaf_checkpoints` and `WalGcShipperOffsetFloorTests.Gc_holds_every_entry_for_a_registered_shipper_that_has_acknowledged_nothing` (red when the shipper does not register, when the clause is dropped, and when the registry is keyed per silo); `WalGcViewOffsetFloorTests.Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints` (two silos; red when the view does not register, when the clause is dropped, and when the registry is keyed per silo); and `WalGcTrimFloorCoyoteTests.Min_cursor_floor_never_trims_past_the_slowest_consumer` (the trim core never passes the slowest consumer). |
| `MoveFence` | Raise the durable fence, then quiesce the source | An operator starts the move through `LatticeAdminGrain.ExecuteWalMoveAsync`; the coordinator first raises the durable fence with a compare-and-swap on the placement version (`ILatticeRegistry.RaiseWalMoveFencesAsync`, decided by `WalMoveFenceCore.EvaluateRaise`, which refuses another move's live fence and takes over a lapsed one), then `WalShardGrain.QuiesceForMoveAsync` fences the activation. | Yes: `LatticeAdminGrainWalMoveTests.A_move_raises_its_fence_before_the_first_quiesce_renews_it_and_flips_under_the_same_move_id`, `LatticeRegistryGrainTests.RaiseWalMoveFencesAsync_refuses_a_partition_another_move_holds_a_live_fence_on`, `LatticeRegistryGrainTests.RaiseWalMoveFencesAsync_takes_over_another_moves_lapsed_fence` (the takeover the `TwoMoves` variant checks), `WalMoveFenceCoreTests.EvaluateRaise_raises_renews_takes_over_and_refuses` and `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` (the activation half). |
| `MoveCopy` | Copy once quiesced, renewing the fence | The coordinator copies the delta once the fenced stream has drained: `inflight = {}` is `WalShardGrain.DrainInFlightAsync`, which waits out the in-flight appends within `LatticeOptions.WalDrainBudget`; on expiry it force-faults them, and their provider calls, which may still land, stay outstanding. The quiesce separately aborts when the activation's placement version is ahead of the coordinator's (`WalMoveFenceCore.ShouldAbortStaleQuiesce`), a check on placement versions, not on the drain. The drain is complete only when no provider work the activation stopped waiting for can still land (`WalShardGrain.HasOutstandingProviderWork`: force-faulted flushes and abandoned provider calls); otherwise the quiesce reports `DrainIncomplete` and the coordinator aborts. Before every convergence re-quiesce the coordinator renews its fence, and aborts if it is gone. **Abstraction:** production bulk-copies before the fence and copies the delta after it; the model's single copy is that final, quiesced step, which is what determines what the target holds. | Yes: `WalShardGrainTests.QuiesceForMoveAsync_is_not_quiesced_while_a_force_faulted_append_is_still_outstanding` and `LatticeAdminGrainWalMoveTests.A_move_aborts_when_the_source_reports_its_drain_incomplete` (the drain), `LatticeAdminGrainWalMoveTests.A_move_whose_fence_was_released_before_the_renewal_aborts_without_flipping` and `LatticeRegistryGrainTests.RaiseWalMoveFencesAsync_renewal_refuses_once_the_fence_was_released` (the renewal), `WalShardGrainTests.QuiesceForMoveAsync_aborts_without_fencing_when_activation_is_ahead_of_coordinator` and `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy`. |
| `MoveSwitch` | Flip to the target, clearing the fence | The coordinator verifies and flips the placement, then deactivates the source (`LatticeAdminGrain.ForceDeactivateAfterFlipAsync`, `WalShardGrain.DeactivateForMoveAsync`), so the next activation serves from the target unfenced. The flip is `ILatticeRegistry.FlipFencedWalPlacementAsync`: one compare-and-swap that checks the placement version AND that every moved partition still carries this move's fence (`WalMoveFenceCore.IsFlipAdmitted`), and clears the fence with the placement change. Just before it the coordinator re-reads the source's durable tail and requires it equal to the quiesced tail. | Yes: `LatticeRegistryGrainTests.FlipFencedWalPlacementAsync_refuses_a_flip_whose_fence_was_released`, `LatticeRegistryGrainTests.FlipFencedWalPlacementAsync_refuses_a_flip_whose_fence_another_move_took_over`, `LatticeRegistryGrainTests.FlipFencedWalPlacementAsync_flips_and_clears_the_moves_fence_in_one_write`, `WalMoveDurableFenceIntegrationTests.A_flip_after_the_source_fence_lapsed_and_served_an_append_is_refused` (real grains), `LatticeAdminGrainWalMoveTests.A_move_flips_the_placement_of_its_partition_to_the_target` (the batch path's compare-and-swap names the target), `LatticeAdminGrainWalMoveTests.A_single_partition_move_flips_the_placement_of_its_partition_to_the_target` (the single-partition path, which `LatticeTreeAdmin`, gRPC, MCP and the tracked move call, flips through its own registry call) and `LatticeAdminGrainWalMoveTests.A_flipped_move_tolerates_a_source_that_cannot_be_deactivated` (the source is deactivated after the flip). |
| `MoveAbort` | Abandon the move, release the fence, unfence the source | The failure path of `LatticeAdminGrain.RunMoveCopyPhasesAsync` deactivates the fenced source (`WalShardGrain.DeactivateForMoveAsync`) so it resumes unfenced, after releasing the move's durable fence (`ILatticeRegistry.ReleaseWalMoveFenceAsync` with this move's id). | Yes: `LatticeAdminGrainWalMoveTests.An_aborted_move_releases_its_fence_before_it_deactivates_the_source`, `LatticeAdminGrainWalMoveTests.A_refused_flip_releases_the_fence_and_the_source_and_surfaces_the_refusal`, `LatticeAdminGrainWalMoveTests.A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source` and `LatticeRegistryGrainTests.ReleaseWalMoveFenceAsync_ignores_a_fence_held_by_another_move` (an abort releases only its own fence, which the `TwoMoves` variant needs). |
| `LeaseLapse` | The fence's lease lapses | The durable fence's `LeaseExpiresUtc` passes, and the activation's in-memory quiesce lease lapses in `WalShardGrain.ThrowIfMoveFenced`. **Over-approximation:** the lapse is unconditional, so safety is checked however the clocks fall, even while the coordinator renews; production's safety likewise never depends on a clock, only its liveness does. | Yes: `WalMoveDurableFenceIntegrationTests.A_fence_abandoned_by_a_dead_coordinator_refuses_appends_until_its_lease_lapses_then_is_released` and `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever`. |
| `Release` | An activation releases a lapsed fence and serves unfenced | A source activation that finds a lapsed fence releases it in the registry (`ILatticeRegistry.ReleaseWalMoveFenceAsync` with `onlyIfExpired`, admitted by `WalMoveFenceCore.IsReleaseAdmitted`) and resolves its provider from the pin the release returned (`LatticeOptionsResolver.ResolveWalShardPlacementAsync`); a released fence is what the flip then refuses. | Yes: `LatticeOptionsResolverWalShardPlacementTests.A_lapsed_fence_is_released_and_the_provider_is_resolved_from_the_pin_the_release_returned`, `LatticeRegistryGrainTests.ReleaseWalMoveFenceAsync_only_if_expired_keeps_a_live_fence_and_releases_a_lapsed_one`, `WalMoveFenceCoreTests.IsReleaseAdmitted_requires_the_moves_own_fence_and_a_lapsed_lease_when_asked` and `WalMoveDurableFenceIntegrationTests.A_fence_abandoned_by_a_dead_coordinator_refuses_appends_until_its_lease_lapses_then_is_released`. |
| `ShardCrash` | Shard activation lost mid-anything | In-flight appends and `WalShardGrain._moveFenced` are lost with the activation; the next activation recovers the allocator through `WalOffsetAllocationCore.RecoveredNextOffset`. The move's coordinator is a different grain and survives. The next activation re-derives its fence from the durable record: it comes up fenced while the fence is live (`WalMoveFenceCore.EvaluateActivationFence`). | Yes: `WalMoveDurableFenceIntegrationTests.A_source_activation_lost_after_the_final_quiesce_cannot_acknowledge_an_append_the_flip_strands` (real grains), `LatticeOptionsResolverWalShardPlacementTests.A_live_fence_on_the_resolved_provider_fences_the_activation_without_releasing_it`, `WalMoveFenceCoreTests.EvaluateActivationFence_fences_an_activation_of_the_source_while_the_lease_holds`, `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization`. |
| `CoordinatorCrash` | The move's coordinator is lost | The grain running `LatticeAdminGrain.ExecuteWalMoveAsync` is lost; the move is abandoned and its fence is left for its lease to govern. **Abstraction:** production may re-drive the move, resuming the copy past what the target holds (`WalMoveResumeCore.ResumeCursor`), which `WalMoveRedriveModel` checks; here a lost coordinator's move is abandoned. | Yes: `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` (a fence does not outlive its coordinator for ever) and `WalMoveRedriveCoyoteTests.Resume_past_target_copies_each_offset_exactly_once` (the re-drive). |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `MovedStreamKeepsAckedWrites` | A move never loses an acknowledged write: every acknowledged entry is on the stream's current home or below what every reader has consumed. | Yes: `WalMoveDurableFenceIntegrationTests.A_source_activation_lost_after_the_final_quiesce_cannot_acknowledge_an_append_the_flip_strands` and `WalMoveDurableFenceIntegrationTests.A_flip_after_the_source_fence_lapsed_and_served_an_append_is_refused` (the source lost after the copy, on real grains), `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `CopyTakenQuiesced` | While the move still holds its durable fence, a copied stream has no append in flight, so nothing can land on the old home between the copy and a flip that is allowed to happen. | Yes: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `ReaderNeverPassesHole` | No reader passes an append still in flight. | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is`. |
| `AllocatorNeverReissues` | A recovered allocator never reissues an acknowledged offset. | Yes: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization`. |
| `StreamEventuallyComplete` | A move's fence is always eventually lowered, so appends are not refused for ever. | Yes: `LatticeAdminGrainWalMoveTests.A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source` (an aborted move releases its source) and `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` (a lapsed lease retires the fenced activation however the coordinator fares). |
| `FenceEventuallyReleased` | A durable fence is never held for ever, including once its coordinator is lost: its lease lapses unconditionally and a source activation releases it. | Yes: `WalMoveDurableFenceIntegrationTests.A_fence_abandoned_by_a_dead_coordinator_refuses_appends_until_its_lease_lapses_then_is_released` (a dead coordinator's fence is released once its lease lapses), `LatticeOptionsResolverWalShardPlacementTests.A_lapsed_fence_is_released_and_the_provider_is_resolved_from_the_pin_the_release_returned` and `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Defect mutations whose fixes have landed

This mutation reproduced production's behaviour until issue #4525's fix landed, and is
now an ordinary regression mutation. The fix's detectors were proven red with
production perturbed back to the old behaviour, and green on the fix:

| Mutation | Behaviour it reproduced | Issue | Detectors |
|----------|-------------------------|-------|-----------|
| `MovedStreamKeepsAckedWritesFenceInMemoryOnly` | The fence lives only in the activation's memory and the flip checks only the placement version, so a source re-activated after the copy acknowledges writes the flip discards. | #4525 | the `ShardCrash`, `MoveSwitch` and `Release` rows' |

Two further mutations show that each half of the fix is load-bearing on its own:
`MovedStreamKeepsAckedWritesActivationIgnoresDurableFence` (the flip is guarded but a
re-activation ignores the record) and `MovedStreamKeepsAckedWritesFlipIgnoresReleasedFence`
(re-activation honours the record but the flip is unguarded) both lose a write, and
`MovedStreamKeepsAckedWritesFlipAcceptsAnyMovesFence` (the flip requires a fence but not
this move's) loses one once a second move raises its own fence after the first's was
released.

## Property classification (issue #2321)

Every property is made to fire by a mutation that perturbs an existing action, so
each holds because a guard in the modelled move protocol prevents it. One mutation,
`MovedStreamKeepsAckedWritesFlipAcceptsAnyMovesFence`, also declares the second move
the `TwoMoves` variant checks, because the defect it reproduces needs one.

Two moves contending for the stream are checked by the `TwoMoves` variant
(`WalMove.TwoMoves.cfg`), with every safety and liveness property: a move may take over
another move's lapsed fence while the other is still copying, and the flip, the
renewal and the abort each act only on the move's own fence. Taking over a LIVE fence
is refused (`WalMoveFenceCore.EvaluateRaise`), but the model shows that this refusal
is not what keeps a write safe: were a live fence taken over, the overtaken move's
flip and renewal would still be refused by the ownership check. It protects the
overtaken move's progress, and is pinned by its unit detectors rather than by a
property. Bounded-out cells, named rather than assumed absent: a third move, a second
shard crash, a second coordinator crash, a move re-driven rather than abandoned, and
more than one consumer.

## Deliberate abstraction gaps

- **Late-landing flushes.** Specified in `WalDurability.tla` (`Abandon`, `LateLand`,
  `SettleHole`; issue #4621), not here: a move's quiesce waits out every abandoned call
  (the `MoveCopy` row), so none lands under a copy.
- **Placement, catalogs and providers.** Which provider a partition lives on, the
  placement pin's other contents and its audit, and content verification are not
  modelled; the move is reduced to the stream's contents before and after the flip
  and the fence record it carries.
- **Re-drive.** A move whose coordinator is lost is abandoned here; its re-drive
  arithmetic is `WalMoveResumeCore`'s, checked by `WalMoveRedriveModel`, not by this
  module.
- **Source reclamation.** Trimming the orphaned source after the flip
  (`LatticeAdminGrain.ReclaimMovedWalSourceAsync`) is not modelled.
- **Leaves.** The leaf lifecycle is `WalDurability.tla`'s; a move touches none of
  its state.
