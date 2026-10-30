# Refinement note: split-gate wait-for cycles

`SplitGateCycles` checks the cross-grain wait-for graph that `SplitLinkTurns`
deliberately abstracts. It models two adjacent leaves, B and C, and every turn
that can hold or wait on their split gates:

- a foreground split on B, which holds B's gate while it calls C's
  `SetPrevSiblingAsync`;
- concurrent capture turns on both leaves;
- a capture nested inside the foreground split's checkpoint-hint loop;
- the shard root's link gate, with its defer-and-drain path for a busy capture
  link;
- a parent overflow whose new sibling seeds both leaves, and a cascade one level
  further up;
- a consolidation turn that takes C's gate;
- deactivation of either leaf.

`NoWaitCycle` requires that some in-flight step can always be taken, so no set
of turns ends up waiting only on each other. Two such cycles were found in the
shipped code and fixed: #4918, the nested capture link, and the sibling-setter
cycle. #4914's cycle is reproduced here by mutation as well.

## Combined parent/digest dispatch

`SeedChildParentReadAsync` now calls the `[AlwaysInterleave]`
`SetParentAndGetChildDigestSnapshotAsync` on either child kind. The wrapper
awaits the existing `SetParentAsync` before `GetChildDigestSnapshotAsync`.
The model's reparent admission, gate acquisition, durable write, gate release
and digest service still occur in that order. Digest admission is a stuttering
step inside the already admitted combined call, rather than a second queued
message. The separate-call model therefore conservatively retains an extra
admission boundary; neither a durable action nor a gate is elided.

Leaf replay and persist-write barriers remain in the delegated methods. The
internal-node release still drains deferred child publications before building
the snapshot. Neither implementation publishes back to the parent awaiting its
seed. A binding failure still prevents digest service and preserves retry
semantics. Caller tests assert one combined dispatch per child; interleave,
replay, persist-race, clear-row and deferred-fold tests guard the retained seams.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `fg` | B's foreground split, from taking the gate through releasing it. | `BPlusLeafGrain.SplitAsync`, reached from an interleaving `SetAsync`. |
| `cap` | Each leaf's capture turn: the split it attempts under `Wait(0)`, then its link. | `BPlusLeafGrain.TrySplitForByteOverflowAsync` and `LinkCaptureSplitAsync`. |
| `cons` | A consolidation turn on C that seals moved slots under C's split gate. | `BPlusLeafGrain.MarkSlotsMovedAwayAsync`. |
| `gate` | Which turn holds each leaf's split gate. | `BPlusLeafGrain._splitGate`. |
| `sib` | The `SetPrevSiblingAsync` call that a split of B makes into C. | `IBPlusLeafGrain.SetPrevSiblingAsync`, which waits on C's `_splitGate`. |
| `ch` | Each link chain's progress: at the root, deferred, at the parent, seeding, at the grandparent, cascading, returning, or done. | `ShardRootGrain.LinkLeafSplitFromCaptureAsync` and the split-link path, plus `BPlusInternalGrain.AcceptSplitAsync`. |
| `lg` | The chain holding the root's link gate. | `ShardRootGrain._splitLinkGate`. |
| `seed` | The new internal sibling's callbacks into each leaf. | `BPlusInternalGrain.SeedChildParentReadAsync` calling `SetParentAsync`, then `GetChildDigestSnapshotAsync`. |
| `overflowed`, `cascaded` | The parent has divided, and the division has climbed one level further. | `BPlusInternalGrain.AcceptSplitAsync` returning a division. |
| `linked` | Each chain's split is linked. | The completed link call. |
| `dep`, `deacts` | Each leaf's activation state, and how many deactivations it has had. | Orleans activation collection; the test-only `ForceDeactivateAsync` seam. |

The four origins are `fg` (the foreground split's result, which the root links
in its own turn), `nest` (the nested capture), `capB` and `capC`. Keys, values,
child lists and digest contents are abstracted.

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `BeginForeground` | An interleaving write overflows B and starts a split. | `BPlusLeafGrain.SetAsync` reaching `SplitAsync`. | Yes: `BPlusLeafGrainSplitGateTests.Split_proceeds_normally_when_gate_is_free`. |
| `ForegroundTakeGate` | The split waits for B's split gate. | `_splitGate.WaitAsync` in `SplitAsync`. | Yes: `BPlusLeafGrainSplitGateTests.Split_proceeds_normally_when_gate_is_free`. |
| `ForegroundCallSibling` | Under the gate, the split calls the old successor's `SetPrevSiblingAsync`. | `SplitAsync` re-pointing C's previous-sibling link. | Yes: `BPlusLeafGrainTests.SetPrevSibling_persists_sibling_id`. |
| `ForegroundSiblingDone` | The sibling call returns. | `SplitAsync` continuing after the sibling call. | Yes: ``SiblingSetterInterleaveIntegrationTests.Sibling_setters_are_admitted_while_a_capture_turn_is_suspended``. |
| `ForegroundSkipNested` | The checkpoint-hint loop reaches no capture, or a capture that leaves the unlinked-split marker in place. | `TrySplitForByteOverflowAsync` testing `_splitGate.CurrentCount > 0`. | Yes: `BPlusLeafGrainTests.Nested_capture_under_a_held_split_gate_defers_the_unlinked_link_instead_of_deadlocking`. |
| `ForegroundNestedLink` | The nested capture calls the root while B's gate is held. Unreachable in the base model; the defect A mutation enables it. | The pre-#4918 drain in `TrySplitForByteOverflowAsync`. | Yes: `BPlusLeafGrainTests.Nested_capture_under_a_held_split_gate_defers_the_unlinked_link_instead_of_deadlocking`. |
| `ForegroundNestedDone` | The nested link returns or is deferred. Unreachable in the base model. | The pre-#4918 drain returning. | Yes: `BPlusLeafGrainTests.Nested_capture_under_a_held_split_gate_defers_the_unlinked_link_instead_of_deadlocking`. |
| `ForegroundRelease` | The split releases B's gate and returns its result for the root to link. | `SplitAsync` releasing `_splitGate` in `finally`. | Yes: `BPlusLeafGrainSplitGateTests.Split_publishes_digest_to_parent_only_after_releasing_the_gate`. |
| `BeginCapture` | A non-interleaving capture turn starts on a leaf. | The snapshot capture reaching `TrySplitForByteOverflowAsync`. | Yes: `BPlusLeafGrainCaptureSeamByteOverflowTests.Capture_divides_a_leaf_that_is_already_over_the_byte_bound`. |
| `CaptureTakeGate` | The capture takes a free split gate with `Wait(0)`. | `_splitGate.Wait(0)` in `TrySplitForByteOverflowAsync`. | Yes: `BPlusLeafGrainCaptureSeamByteOverflowTests.Capture_divides_a_leaf_that_is_already_over_the_byte_bound`. |
| `CaptureGateBusy` | The capture finds the gate held and skips the division. | `TrySplitForByteOverflowAsync` returning when `Wait(0)` fails. | Yes: `BPlusLeafGrainSplitGateTests.Overflowing_write_skips_split_when_gate_is_held`. |
| `CaptureCallSibling` | A capture split of B calls C's `SetPrevSiblingAsync` under B's gate. | The capture split re-pointing C's previous-sibling link. | Yes: ``SiblingSetterInterleaveIntegrationTests.Sibling_setters_are_admitted_while_a_capture_turn_is_suspended``. |
| `CaptureSiblingDone` | The sibling call returns; the capture releases B's gate and links its split. | The capture split releasing `_splitGate`, then `LinkCaptureSplitAsync`. | Yes: ``SiblingSetterInterleaveIntegrationTests.Sibling_setters_are_admitted_while_a_capture_turn_is_suspended``. |
| `CaptureSplitC` | A capture split of C, the last leaf, has no successor to call; it releases C's gate and links. | The capture split with no next sibling. | Yes: `BPlusLeafGrainCaptureSeamByteOverflowTests.Capture_divides_a_leaf_that_is_already_over_the_byte_bound`. |
| `CaptureLinkReturns` | The capture turn ends once its link lands or is deferred. | `LinkCaptureSplitAsync` returning. | Yes: `BPlusLeafGrainCaptureSeamByteOverflowTests.Capture_retries_a_pending_split_link_before_resuming_byte_overflow_repair`. |
| `AdmitSibling` | The runtime admits `SetPrevSiblingAsync` into C, or queues it behind C's running turns. | `[AlwaysInterleave]` on `IBPlusLeafGrain.SetPrevSiblingAsync`. | Yes: ``SiblingSetterInterleaveIntegrationTests.Sibling_setters_are_admitted_while_a_capture_turn_is_suspended``. |
| `SiblingTakeGate` | The sibling setter waits for C's split gate. | `SetPrevSiblingAsync` taking `_splitGate`. | Yes: `BPlusLeafGrainTests.SetNextSiblingAsync_does_not_race_etag_when_concurrent_with_SetPrevSiblingAsync`. |
| `SiblingFinish` | The sibling setter persists and releases C's gate. | `SetPrevSiblingAsync` releasing `_splitGate`. | Yes: `BPlusLeafGrainTests.SetPrevSibling_persists_sibling_id`. |
| `BeginConsolidation` | Consolidation sends C a moved-away seal. | `TreeShardConsolidationGrain` calling `MarkSlotsMovedAwayAsync`. | Yes: `BPlusLeafGrainTests.MarkSlotsMovedAway_persists_sorted_distinct_slots`. |
| `AdmitConsolidation` | The non-interleaving seal call is admitted once C is idle. | Orleans turn scheduling for `MarkSlotsMovedAwayAsync`. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |
| `ConsolidationTakeGate` | The seal waits for C's split gate. | `MarkSlotsMovedAwayAsync` taking `_splitGate`. | Yes: `BPlusLeafGrainTests.MarkSlotsMovedAway_persists_sorted_distinct_slots`. |
| `ConsolidationFinish` | The seal persists and releases C's gate. | `MarkSlotsMovedAwayAsync` releasing `_splitGate`. | Yes: `BPlusLeafGrainTests.MarkSlotsMovedAway_persists_sorted_distinct_slots`. |
| `RootTakeLinkGate` | The root takes its link gate for a chain. | `ShardRootGrain._splitLinkGate` acquisition. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `RootDeferLink` | A capture link that finds the link gate busy is parked, and the capture returns at once. | `LinkLeafSplitFromCaptureAsync` deferring a contended link. | Yes: `ShardRootGrainSplitLinkTests.A_capture_link_during_parent_seeding_is_deferred_without_waiting_for_the_root_gate`. |
| `RootDrainDeferred` | The root drains a parked link once the gate is free. | The root's deferred-link drain. | Yes: `ShardRootGrainSplitLinkTests.A_capture_split_during_root_promotion_is_deferred_until_the_root_is_installed`. |
| `ParentAccept` | A full parent accepts the split and divides; its new sibling queues seeding callbacks into both leaves. | `BPlusInternalGrain.AcceptSplitAsync`, then `SeedChildParentReadAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `ParentAcceptNoOverflow` | The parent accepts the split without dividing. | `BPlusInternalGrain.AcceptSplitAsync` returning no division. | Yes: `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds`. |
| `SeedComplete` | Seeding finishes in both leaves and the root carries the division up. | The root resuming the link after the seeding returns. | Yes: `ShardRootGrainSplitLinkTests.A_parent_division_is_linked_before_the_next_division_below_it_descends`. |
| `GrandparentCascade` | The grandparent accepts the division and divides too; its new sibling reparents internal nodes only. | The root linking the parent's division a level up. | Yes: `ShardRootGrainSplitLinkTests.A_root_that_divides_on_accept_is_wrapped_under_a_new_root`. |
| `GrandparentAbsorbs` | The grandparent accepts the division without dividing. | The root linking the parent's division a level up. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `CascadeSeeded` | The cascade's internal reparenting finishes. | The new internal sibling seeding its internal children. | Yes: `ShardRootGrainSplitLinkTests.A_root_that_divides_on_accept_is_wrapped_under_a_new_root`. |
| `ReturnLink` | The link lands and the root releases its link gate. | The end of the root's link call. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `AdmitSetParent` | The runtime admits `SetParentAsync` into a busy leaf, or queues it. | `[AlwaysInterleave]` on `IBPlusLeafGrain.SetParentAsync`. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `SetParentTakeGate` | The reparent waits for the leaf's split gate. | `BPlusLeafGrain.SetParentAsync` taking `_splitGate`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `SetParentFinish` | The reparent persists and releases the gate. | `SetParentAsync` releasing `_splitGate`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `ServeDigest` | The runtime admits the digest read and the leaf serves it. | `[AlwaysInterleave]` on `IBPlusLeafGrain.GetChildDigestSnapshotAsync`. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `Deactivate` | An idle leaf starts deactivating and admits nothing more. | Orleans activation collection, or the test-only `ForceDeactivateAsync`. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |
| `FinishDeactivation` | Deactivation completes and the next call activates afresh. | The leaf's deactivation hook returning after `BPlusLeafGrain.FlushPendingCheckpointOnDeactivateAsync` and its other barriers. | Yes: `BPlusLeafGrainTests.Deactivation_records_no_barrier_failure_when_every_barrier_succeeds`. |
| `Stutter` | No protocol progress. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `TypeOK` | Turn, gate, chain, seeding and activation state stay in their finite domains. | Yes: `BPlusLeafGrainSplitGateTests.Split_proceeds_normally_when_gate_is_free`. |
| `GateOwnership` | Each split gate is held only by the turn that took it, and is released when that turn ends. | Yes: `BPlusLeafGrainSplitGateTests.Overflowing_write_skips_split_when_gate_is_held`. |
| `SingleParentMutation` | At most one link mutates the parents at a time. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `LinkGateMatchesLinking` | The root holds its link gate exactly for the chain that is past it. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `ClimbsOnlyAfterSeed` | A division is carried upward only after the seeding it caused has finished in both leaves. | Yes: `ShardRootGrainSplitLinkTests.A_parent_division_is_linked_before_the_next_division_below_it_descends`. |
| `DeferredOnlyForCapture` | Only a capture link is deferred, and a deferred link never holds the link gate. | Yes: `ShardRootGrainSplitLinkTests.A_capture_link_during_parent_seeding_is_deferred_without_waiting_for_the_root_gate`. |
| `NoWaitCycle` | No set of in-flight turns waits only on each other, so no call timeout is needed to make progress. | Yes: `BPlusLeafGrainTests.Nested_capture_under_a_held_split_gate_defers_the_unlinked_link_instead_of_deadlocking`. |
| `EveryLinkLands` | Every split that reaches the root is eventually linked. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |

## Bounds and assumptions

The checked instance has two leaves, one foreground split, one capture per leaf,
one nested capture, one consolidation turn, one parent overflow, one cascade and
at most one deactivation per leaf. That is the smallest instance in which every
gate holder can meet every gate waiter. With the base flags it has 229,031
distinct states.

The flags at the top of the module hold the shipped fixes in place, and each
mutation turns one of them off:

- `SetParentInterleaves` and `DigestInterleaves` are #4914.
- `SiblingSettersInterleave` is the sibling-setter fix.
- `NestedCaptureDefersLink` is #4918.

With `NestedCaptureDefersLink` TRUE, `ForegroundNestedLink` and
`ForegroundNestedDone` cannot fire. Their mutations enable them directly, so
both rows are still exercised. `RootDefersBusyLink` is defence in depth: the
model holds without it, because no capture holds a split gate while it waits
on the root.

`DeactivationOnlyWhenIdle` is an environment assumption, not a code fix. Orleans
collects only idle activations, and a deactivating activation admits no message,
even an `[AlwaysInterleave]` one. A deactivation forced on a busy leaf would
recreate the cycle; the leaf's `ForceDeactivateAsync` is a test seam that is
only called on a quiescent leaf.

Weak fairness covers `InFlightStep`, every step of work already in flight. The
environment steps that start work (the Begin actions and `Deactivate`) are not
fair, so `EveryLinkLands` assumes the root, the parents and storage keep
answering. It does not claim progress through a storage outage.
