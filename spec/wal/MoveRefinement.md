# Refinement note: WAL shard move to production

This note maps [`WalMove.tla`](WalMove.tla) - a WAL shard move running concurrently
with appends, out-of-order flushes, a consumer, the GC, a shard crash and the loss of
the move's coordinator - to the Orleans.Lattice code that plays each role. The
leaf-side lifecycle is mapped in [`Refinement.md`](Refinement.md). Like that note it
is a documented mapping, not a refinement proof; the same gates check its names,
detectors and coverage, and none checks its claims - that was done by reading
production and by perturbing it under each detector.

The model's fence has two halves: a DURABLE fence, a record in the WAL placement pin
under a lease, and the source ACTIVATION's in-memory fence, which every new
activation re-derives from the durable record. That is the intended design of issue
#4525. Production today has only the activation half: the fence is lost with the
activation, and the flip re-checks nothing about the source. The rows that depend on
the durable half say so, and the standing check below keeps production's behaviour
firing until the fix lands.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next` | The stream's offset allocator | The WAL shard's next sequence, advanced by `WalOffsetAllocationCore.Assign` inside `WalShardGrain.AppendAsync`. |
| `inflight` | Appends whose flush has not landed | The shard's in-flight flush chain (`WalShardGrain.FlushAsync`). |
| `durable` | What the stream's current home holds | The partition's provider entries; after a move's flip, the target provider's. |
| `tail` | Oldest readable offset | The current home's oldest retained offset. |
| `acked` | Acknowledged writes | Appends whose `WalShardGrain.AppendAsync` completed. |
| `cons` | Every reader of the stream | One cursor standing for leaf replay, the replication shipper, view maintainers and log subscribers, each of which reads below `WalShippingWatermark.DurableContiguousTail` and holds the GC floor. |
| `move` | The coordinator's phase | The phase of `LatticeAdminGrain.RunMoveCopyPhasesAsync`: `"fenced"` once the source is quiesced, `"copied"` once the delta is copied, `"idle"` after the flip or an abort. It belongs to the coordinator, so a shard crash does not reset it. |
| `moveCopy` | What the target holds | Entries `LatticeAdminGrain.RunMoveCopyPhasesAsync` has copied to the target provider. |
| `dfence`, `lapsed` | The durable fence and its lease | The fence record in the partition's placement pin and its lease expiry, the intended design of issue #4525. Production has no such record yet. |
| `afence` | The source activation's fence | `WalShardGrain._moveFenced`, raised by `WalShardGrain.QuiesceForMoveAsync` and lost with the activation. |
| `moves`, `crashes`, `coordCrashes` | Environment budgets | Modelling devices only: moves are operator-initiated and finite, crashes do not recur for ever. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Append` | Append admitted only when the activation is unfenced | `WalShardGrain.AppendAsync` refuses through two checks that are each sufficient alone: `WalShardGrain.ThrowIfMoveFenced` on entry, and `WalMoveFenceCore.IsAppendAdmitted` under the same state gate that assigns the offset. The second is the one that closes the race with a quiesce raised after the first check ran; for an append arriving after the fence is up, the two are redundant, so removing either alone is not observable. | Yes: `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence` (the in-gate check closes the race) and `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move`, which goes red when both checks are removed together or when the fence is never raised. |
| `FlushAck(o)` | Flush lands, write acknowledged | `WalShardGrain.FlushAsync` completes an append only after its provider flush; completions in any order. | Yes: `WalShardGrainTests.AppendAsync_hung_provider_flush_faults_with_timeout_when_deadline_elapses`. |
| `Consume` | A reader advances below the watermark | Every cursor-advancing reader is shown offsets below `WalShippingWatermark.DurableContiguousTail` only (`WalShippingWatermark.IsOffsetExposable`). | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is` (the boundary, which the Coyote model's atomic flush cannot reach). |
| `GcTrim` | Trim below what readers have consumed | `LatticeWalGc.TrimShardAsync` trims only entries `WalGcTrimCore.IsEntryEligible` admits under the minimum consumer floor. **Over-approximation:** one consumer floors the trim; production floors it under the minimum across every consumer. | Yes: `WalGcTrimFloorCoyoteTests.Min_cursor_floor_never_trims_past_the_slowest_consumer`. |
| `MoveFence` | Raise the durable fence, then quiesce the source | An operator starts the move through `LatticeAdminGrain.ExecuteWalMoveAsync`; `WalShardGrain.QuiesceForMoveAsync` fences the activation. **Production diverges:** it raises no durable fence first (issue #4525). | Partial: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` covers the activation half (an append after the quiesce is refused); the durable half is issue #4525's. |
| `MoveCopy` | Copy once quiesced, renewing the fence | The coordinator copies the delta once the fenced stream has drained, and aborts a quiesce whose observation is stale (`WalMoveFenceCore.ShouldAbortStaleQuiesce`). **Abstraction:** production bulk-copies before the fence and copies the delta after it; the model's single copy is that final, quiesced step, which is what determines what the target holds. **Production diverges:** no durable fence is renewed, and a quiesce can report the stream drained while a force-faulted append is still outstanding (issue #4525). | Partial: `WalShardGrainTests.QuiesceForMoveAsync_aborts_without_fencing_when_activation_is_ahead_of_coordinator` and `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy`; the renewal and the drain arm are issue #4525's. |
| `MoveSwitch` | Flip to the target, clearing the fence | The coordinator verifies and flips the placement, then deactivates the source (`LatticeAdminGrain.ForceDeactivateAfterFlipAsync`, `WalShardGrain.DeactivateForMoveAsync`), so the next activation serves from the target unfenced. **Production diverges:** the flip's compare-and-swap checks only the placement version, not that the move's fence is still held (issue #4525). | Partial: `LatticeAdminGrainWalMoveTests.A_move_flips_the_placement_of_its_partition_to_the_target` (the compare-and-swap names the target) and `LatticeAdminGrainWalMoveTests.A_flipped_move_tolerates_a_source_that_cannot_be_deactivated` (the source is deactivated after the flip); the fence check is issue #4525's. |
| `MoveAbort` | Abandon the move, release the fence, unfence the source | The failure path of `LatticeAdminGrain.RunMoveCopyPhasesAsync` deactivates the fenced source (`WalShardGrain.DeactivateForMoveAsync`) so it resumes unfenced. **Production diverges:** there is no durable fence to release (issue #4525). | Partial: `LatticeAdminGrainWalMoveTests.A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source` covers releasing the source; the durable release is issue #4525's. |
| `LeaseLapse` | The fence's lease lapses | Production has the activation's in-memory quiesce lease, which lapses in `WalShardGrain.ThrowIfMoveFenced`. **Over-approximation:** the lapse is unconditional, so safety is checked however the clocks fall, even while the coordinator renews. **Production diverges:** no durable lease (issue #4525). | Partial: `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` covers the in-memory lease; the durable one is issue #4525's. |
| `Release` | An activation releases a lapsed fence and serves unfenced | Production's lapsed activation requests deactivation in `WalShardGrain.ThrowIfMoveFenced`, and the next activation comes up unfenced. **Production diverges:** there is no record to release, so nothing tells the flip the fence is gone (issue #4525). | Partial: `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` covers the deactivation; the registry release is issue #4525's. |
| `ShardCrash` | Shard activation lost mid-anything | In-flight appends and `WalShardGrain._moveFenced` are lost with the activation; the next activation recovers the allocator through `WalOffsetAllocationCore.RecoveredNextOffset`. The move's coordinator is a different grain and survives. **Production diverges:** the next activation comes up unfenced instead of re-deriving the fence from a durable record (issue #4525). | Partial: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization` cover recovery; re-deriving the fence is issue #4525's. |
| `CoordinatorCrash` | The move's coordinator is lost | The grain running `LatticeAdminGrain.ExecuteWalMoveAsync` is lost; the move is abandoned and its fence is left for its lease to govern. **Abstraction:** production may re-drive the move, resuming the copy past what the target holds (`WalMoveResumeCore.ResumeCursor`), which `WalMoveRedriveModel` checks; here a lost coordinator's move is abandoned. | Yes: `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` (a fence does not outlive its coordinator for ever) and `WalMoveRedriveCoyoteTests.Resume_past_target_copies_each_offset_exactly_once` (the re-drive). |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `MovedStreamKeepsAckedWrites` | A move never loses an acknowledged write: every acknowledged entry is on the stream's current home or below what every reader has consumed. | Partial: `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. Production violates it when the source crashes after the copy, until issue #4525 is fixed. |
| `CopyTakenQuiesced` | While the move still holds its durable fence, a copied stream has no append in flight, so nothing can land on the old home between the copy and a flip that is allowed to happen. | Yes: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `ReaderNeverPassesHole` | No reader passes an append still in flight. | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole` and `WalShippingWatermarkTests.The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is`. |
| `AllocatorNeverReissues` | A recovered allocator never reissues an acknowledged offset. | Yes: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization`. |
| `StreamEventuallyComplete` | A move's fence is always eventually lowered, so appends are not refused for ever. | Yes: `LatticeAdminGrainWalMoveTests.A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source` (an aborted move releases its source) and `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` (a lapsed lease retires the fenced activation however the coordinator fares). |
| `FenceEventuallyReleased` | A durable fence is never held for ever, including once its coordinator is lost: its lease lapses unconditionally and a source activation releases it. | Partial: `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` covers the in-memory lease; the durable fence and its release are issue #4525's. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Standing defect check

`MovedStreamKeepsAckedWritesFenceInMemoryOnly` reproduces **current production
behaviour** rather than a historical defect, and must be re-proven against the
detectors issue #4525's fix adds when it lands in the bucket:

| Mutation | Production behaviour it keeps | Issue |
|----------|-------------------------------|-------|
| `MovedStreamKeepsAckedWritesFenceInMemoryOnly` | The fence lives only in the activation's memory and the flip checks only the placement version, so a source re-activated after the copy acknowledges writes the flip discards. | #4525 |

Two further mutations show that each half of the fix is load-bearing on its own:
`MovedStreamKeepsAckedWritesActivationIgnoresDurableFence` (the flip is guarded but a
re-activation ignores the record) and `MovedStreamKeepsAckedWritesFlipIgnoresReleasedFence`
(re-activation honours the record but the flip is unguarded) both lose a write.

## Property classification (issue #2321)

Every property is made to fire by a mutation that perturbs an existing action, so
each holds because a guard in the modelled move protocol prevents it. Bounded-out
cells, named rather than assumed absent: a second move, a second shard crash, a
second coordinator crash, a move re-driven rather than abandoned, a foreign move
taking over an expired fence, and more than one consumer.

## Deliberate abstraction gaps

- **Placement, catalogs and providers.** Which provider a partition lives on, the
  placement pin's other contents and its audit, and content verification are not
  modelled; the move is reduced to the stream's contents before and after the flip
  and the fence record it carries.
- **Re-drive.** A move whose coordinator is lost is abandoned here; its re-drive
  arithmetic is `WalMoveResumeCore`'s, checked by `WalMoveRedriveModel`, not by this
  module.
- **Fence takeover.** With one move there is no foreign fence to take over once
  expired; the compare-and-swap that refuses a live foreign fence is not exercised.
- **Source reclamation.** Trimming the orphaned source after the flip
  (`LatticeAdminGrain.ReclaimMovedWalSourceAsync`) is not modelled.
- **Leaves.** The leaf lifecycle is `WalDurability.tla`'s; a move touches none of
  its state.