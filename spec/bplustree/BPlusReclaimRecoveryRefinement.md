# Refinement note: durable leaf-reclaim recovery

`BPlusReclaimRecovery` isolates the crash boundary after a leaf has been
latched for retirement. It models one middle leaf, its predecessor, one
successor, one parent-route entry, and one bounded activation loss. It checks
the ordering that prevents an unlinked but still-routed leaf from being cleared
before both the successor back link and the durable recovery path are complete.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `linked` | The predecessor still points to the victim in the sibling chain. | `LeafNodeState.NextSibling`. |
| `routed` | Parent traversal can still select the victim. | `BPlusInternalGrain` child routing. |
| `backLinkCorrect` | The successor points back to the predecessor rather than the removed victim. | `LeafNodeState.PrevSibling`, persisted by `BPlusLeafGrain.SetPrevSiblingAsync`. |
| `victimExists` | The victim's persisted state and WAL materialiser pin remain. | `ClearGrainStateAsync` and leaf activation state. |
| `retired`, `retirementGranted` | Durable mutation refusal and the shard root's accepted retirement decision. | `LeafNodeState.ReclaimRetired`, `TryBeginRetirementAsync`, and `EnterMutationScope`. |
| `pending` | The predecessor's durable completion obligation for an unlinked successor. | `PendingReclaimSuccessorId` and `PendingReclaimNextId` on `LeafNodeState`. |
| `ready`, `crashes` | Whether the current activation can advance the protocol, and the finite crash budget. | Grain deactivation/reactivation with persisted state retained. |

The model abstracts keys, values, sibling identities, parent height and the
successor back pointer. The cascading-split model covers level-indexed parent
growth separately; this module focuses on the leaf-removal recovery order.

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `BeginRetirement` | Persist the latch before accepting the decision. | `BPlusLeafGrain.TryBeginRetirementAsync`. | Yes: `BPlusLeafGrainTests.The_retirement_latch_survives_reactivation_and_refuses_the_merge`. |
| `UnlinkAndRecord` | Atomically bypass the victim and persist the retry marker. | `BPlusLeafGrain.TryUnlinkSuccessorAsync`. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_throwing_compare_and_swap_keeps_the_leaf_latched_for_an_ambiguous_commit`. |
| `RetireRoute` | Remove the victim from the parent only after the predecessor absorbs its range. | `ShardRootGrain.RetireRoutingAsync`. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_later_pass_finishes_route_retirement_before_clearing_the_victim`. |
| `RepairBackLink` | Persist the successor's link to the predecessor; failure retains the marker and victim for retry. | `BPlusLeafGrain.SetPrevSiblingAsync` before `ClearRemovedLeafAsync` and `CompletePendingReclaimAsync`. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_failed_successor_back_pointer_repair_keeps_the_reclaim_marker`. |
| `ClearVictim` | Clear the now-unrouted victim only after its successor link is repaired. | `ShardRootGrain.ClearRemovedLeafAsync`. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_failed_successor_back_pointer_repair_keeps_the_reclaim_marker`. |
| `Complete` | Retire the predecessor marker only after the successor link and victim clear succeed. | `IBPlusLeafGrain.CompletePendingReclaimAsync`. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_failed_successor_back_pointer_repair_keeps_the_reclaim_marker`. |
| `Crash` | Lose activation-local state without losing persisted state. | Orleans grain deactivation after a partial reclaim. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_later_pass_finishes_route_retirement_before_clearing_the_victim`. |
| `Recover` | Resume the persisted obligation in a new activation. | `TryCompletePendingReclaimAsync` in the next reclaim pass. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_later_pass_finishes_route_retirement_before_clearing_the_victim`. |
| `Stutter` | No protocol progress. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `TypeOK` | The latch, marker, route, and activation state remain in their finite domains. | Yes: `BPlusLeafGrainTests.The_retirement_latch_survives_reactivation_and_refuses_the_merge`. |
| `RetirementGrantIsLatched` | A shard root cannot proceed on a retirement decision unless writes are durably refused. | Yes: `BPlusLeafGrainTests.The_retirement_latch_survives_reactivation_and_refuses_the_merge`. |
| `UnlinkedVictimHasMarker` | An unlinked victim remains named by durable state until successor-link repair and cleanup complete. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_failed_successor_back_pointer_repair_keeps_the_reclaim_marker`. |
| `NoRouteToClearedVictim` | Grain state is never cleared while a parent route may still select the victim. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_routing_entry_that_will_not_retire_keeps_the_victim_and_recovery_marker`. |
| `ClearedVictimHasRepairedBackLink` | The successor never points at a victim whose state has been cleared. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_failed_successor_back_pointer_repair_keeps_the_reclaim_marker`. |
| `CompletedReclaimCleared` | The predecessor does not forget the obligation before link repair and victim clear. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_clear_that_keeps_failing_stays_owed_across_passes`. |
| `PendingEventuallyCompletes` | Given another reclaim pass and eventual storage/route availability, a durable obligation completes. | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_later_pass_finishes_route_retirement_before_clearing_the_victim`. |

## Bounds and assumptions

The checked instance contains one predecessor, victim and parent route, with
one successor back link and one activation loss. The temporal property uses
weak fairness for route retirement, back-link repair, state clearing, marker
completion and reactivation. It therefore states convergence when retry
opportunities continue and the required storage and parent calls eventually
succeed; it does not claim progress through a permanent outage.

The model does not include the complete leaf split transfer, arbitrary
internal-node identity, or orphan discovery. Those are separate protocol
surfaces; the six-level cascade model and the existing topology model cover
their respective bounded claims.
