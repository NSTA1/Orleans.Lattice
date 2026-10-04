# Refinement note: WAL shard move to production

This note maps [`WalMove.tla`](WalMove.tla) - a WAL shard move running concurrently
with appends, out-of-order flushes, a consumer, the GC and a shard crash - to the
Orleans.Lattice code that plays each role. The leaf-side lifecycle is mapped in
[`Refinement.md`](Refinement.md). Like that note it is a documented mapping, not a
refinement proof; the same gates check its names, detectors and coverage, and none
checks its claims - that was done by reading production and by perturbing it under
each detector.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next` | The stream's offset allocator | The WAL shard's next sequence, advanced by `WalOffsetAllocationCore.Assign` inside `WalShardGrain.AppendAsync`. |
| `inflight` | Appends whose flush has not landed | The shard's in-flight flush chain (`WalShardGrain.FlushAsync`). |
| `durable` | What the stream's current home holds | The partition's provider entries; after a move's flip, the target provider's. |
| `tail` | Oldest readable offset | The current home's oldest retained offset. |
| `acked` | Acknowledged writes | Appends whose `WalShardGrain.AppendAsync` completed. |
| `cons` | Every reader of the stream | One cursor standing for leaf replay, the replication shipper, view maintainers and log subscribers, each of which reads below `WalShippingWatermark.DurableContiguousTail` and holds the GC floor. |
| `move` | Move phase | `WalShardGrain._moveFenced` and the coordinator's phase in `LatticeAdminGrain.RunMoveCopyPhasesAsync`: `"fenced"` is the fence raised by `WalShardGrain.QuiesceForMoveAsync`, `"copied"` the delta copied after quiescing, `"idle"` after the flip. |
| `moveCopy` | What the target holds | Entries `LatticeAdminGrain.RunMoveCopyPhasesAsync` has copied to the target provider. |
| `moves`, `crashes` | Environment budgets | Modelling devices only: moves are operator-initiated and finite, crashes do not recur for ever. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Append` | Append admitted only when unfenced | `WalShardGrain.AppendAsync` refuses through `WalShardGrain.ThrowIfMoveFenced` (`WalMoveFenceCore.IsAppendAdmitted`) under the same state gate that assigns the offset. | Yes: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `FlushAck(o)` | Flush lands, write acknowledged | `WalShardGrain.FlushAsync` completes an append only after its provider flush; completions in any order. | Yes: `WalShardGrainTests.AppendAsync_hung_provider_flush_faults_with_timeout_when_deadline_elapses`. |
| `Consume` | A reader advances below the watermark | Every cursor-advancing reader is shown offsets below `WalShippingWatermark.DurableContiguousTail` only (`WalShippingWatermark.IsOffsetExposable`). | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole`. |
| `GcTrim` | Trim below what readers have consumed | `LatticeWalGc.TrimShardAsync` trims only entries `WalGcTrimCore.IsEntryEligible` admits under the minimum consumer floor. **Over-approximation:** one consumer floors the trim; production floors it under the minimum across every consumer. | Yes: `WalGcTrimFloorCoyoteTests.Min_cursor_floor_never_trims_past_the_slowest_consumer`. |
| `MoveFence` | Raise the fence | `WalShardGrain.QuiesceForMoveAsync` sets the fence; an operator starts the move through `LatticeAdminGrain.ExecuteWalMoveAsync`. | Yes: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` (an append after the quiesce is refused). |
| `MoveCopy` | Copy once quiesced | The coordinator copies the delta once the fenced stream has drained, and aborts a quiesce whose observation is stale (`WalMoveFenceCore.ShouldAbortStaleQuiesce`). **Abstraction:** production bulk-copies before the fence and copies the delta after it; the model's single copy is that final, quiesced step, which is what determines what the target holds. | Yes: `WalShardGrainTests.QuiesceForMoveAsync_aborts_without_fencing_when_activation_is_ahead_of_coordinator` and `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy`. |
| `MoveSwitch` | Flip to the target, lower the fence | The coordinator verifies and flips the placement, then deactivates the source (`LatticeAdminGrain.ForceDeactivateAfterFlipAsync`, `WalShardGrain.DeactivateForMoveAsync`), so the next activation serves from the target unfenced. | Yes: `LatticeAdminGrainWalMoveTests.A_move_flips_the_placement_of_its_partition_to_the_target` (the compare-and-swap names the target) and `LatticeAdminGrainWalMoveTests.A_flipped_move_tolerates_a_source_that_cannot_be_deactivated` (the source is deactivated after the flip). |
| `ShardCrash` | Shard activation lost mid-anything | In-flight appends and the in-memory fence are lost with the activation; the next activation recovers the allocator from the provider through `WalOffsetAllocationCore.RecoveredNextOffset`. **Abstraction:** the model abandons a move a crash interrupts; production may re-drive it, resuming the copy past what the target holds (`WalMoveResumeCore.ResumeCursor`), which `WalMoveRedriveModel` checks. | Yes: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalMoveRedriveCoyoteTests.Resume_past_target_copies_each_offset_exactly_once`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `MovedStreamKeepsAckedWrites` | A move never loses an acknowledged write: every acknowledged entry is on the stream's current home or below what every reader has consumed. | Yes: `LatticeAdminGrainWalMoveTests.A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `CopyTakenQuiesced` | The final copy is taken from a quiesced stream: no append is in flight once it is taken. | Yes: `WalShardGrainTests.AppendAsync_throws_quiescing_while_fenced_for_a_move` and `WalMoveQuiesceCoyoteTests.Atomic_fence_check_never_assigns_after_the_fence`. |
| `ReaderNeverPassesHole` | No reader passes an append still in flight. | Yes: `WalShippingWatermarkCoyoteTests.Watermark_never_ships_an_offset_above_a_prefix_hole`. |
| `AllocatorNeverReissues` | A recovered allocator never reissues an acknowledged offset. | Yes: `WalOffsetAllocationCoreTests.A_recovered_allocator_resumes_one_past_the_highest_stored_offset` and `WalShardGrainTests.AppendAsync_recovers_offset_counter_from_provider_on_initialization`. |
| `StreamEventuallyComplete` | A move's fence is always eventually lowered, so appends are not refused for ever. | Yes: `LatticeAdminGrainWalMoveTests.A_cancel_signalled_at_the_flip_stops_the_move_and_releases_the_source` (an aborted move releases its source) and `WalShardGrainTests.An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever` (a lapsed lease retires the fenced activation however the coordinator fares). |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification (issue #2321)

Every property is made to fire by a mutation that perturbs an existing action, so
each holds because a guard in the modelled move protocol prevents it. Bounded-out
cells, named rather than assumed absent: a second move, a second crash, a crash that
re-drives rather than abandons the move, and more than one consumer.

## Deliberate abstraction gaps

- **Placement, catalogs and providers.** Which provider a partition lives on, the
  placement pin and its audit, and content verification are not modelled; the move
  is reduced to the stream's contents before and after the flip.
- **Re-drive.** A crashed move is abandoned here; its re-drive arithmetic is
  `WalMoveResumeCore`'s, checked by `WalMoveRedriveModel`, not by this module.
- **Source reclamation.** Trimming the orphaned source after the flip
  (`LatticeAdminGrain.ReclaimMovedWalSourceAsync`) is not modelled.
- **Leaves.** The leaf lifecycle is `WalDurability.tla`'s; a move touches none of
  its state.
