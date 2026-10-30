# Refinement note: capture-path split-link turns

`SplitLinkTurns` models the grain turns and the wait-for graph of one split
that a leaf links from inside its own capture turn (issue #4914). The donor
leaf awaits the shard root inline; the root records the link under its link
gate and hands it to the parent internal node. A parent already at its fan-out
limit splits as well, and its new sibling seeds the children it took over by
calling back into each of them, including the donor whose turn started the
chain. The model checks that those callbacks never queue behind that turn, that
admitting them does not let two storage writes overlap on the leaf row, and
that admitting them does not let a write land on a cleared leaf.

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
| `leafTurn` | The donor leaf's exclusive capture turn. | The `BPlusLeafGrain` turn that reaches `BPlusLeafGrain.Split.cs` and awaits `IShardRootGrain.LinkLeafSplitFromCaptureAsync`. |
| `chain` | How far the link has travelled through root and parent. | `ShardRootGrain.LinkLeafSplitFromCaptureAsync` and the parent accept in `BPlusInternalGrain`. |
| `seed` | The new internal sibling's callbacks into the donor leaf: queued, admitted, blocked behind the turn, writing, or done. | `BPlusInternalGrain.SeedChildParentReadAsync` calling `IBPlusLeafGrain.SetParentAsync` then `GetChildDigestSnapshotAsync`. |
| `linkGate` | The shard root's link gate, held while the link is outstanding. | `ShardRootGrain._splitLinkGate`. |
| `linked` | The split is linked into its parent. | The completed `LinkLeafSplitFromCaptureAsync` call. |
| `lost` | The call timeout failed the capture after its write was acknowledged. | Orleans `ResponseTimeout` on the capture's root call. |
| `gate`, `writes` | The leaf's persist-write gate holder and the storage writes in flight. | `BPlusLeafGrain._persistWriteGate` and the `WriteStateAsync` call inside `BPlusLeafGrain.PersistAsync`. |
| `flush` | A persist from another interleaved turn that holds no split gate. | Any `PersistAsync` caller other than `SetParentAsync`, such as a checkpoint flush. |
| `cleared`, `resurrected` | The leaf was cleared, and whether a write later landed on the cleared row. | `BPlusLeafGrain.ClearGrainStateAsync` setting `_leafStateCleared` inside the persist-write gate. |

The model holds one donor leaf, one parent that overflows, one new internal
sibling and one interleaved flush. It abstracts keys, values, the child list
and the digest contents, and treats the link's internal recursion (an internal
division linked a level up) as a single parent step.

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `BeginCapture` | The capture turn splits the leaf and starts the link. | `BPlusLeafGrain.Split.cs` calling `LinkLeafSplitFromCaptureAsync`. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |
| `RootRecordLink` | The root records the link under its link gate. | `ShardRootGrain.LinkLeafSplitFromCaptureAsync`. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `ParentOverflow` | The parent accepts the split, overflows and divides; its new sibling queues the seeding callbacks. | `BPlusInternalGrain` accept and split, then `SeedChildParentReadAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `AdmitSetParent` | The runtime admits `SetParentAsync` into the busy leaf, or queues it. | `[AlwaysInterleave]` on `IBPlusLeafGrain.SetParentAsync`. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `BeginSetParent` | The reparent persist takes the persist-write gate. | `BPlusLeafGrain.SetParentAsync` into `PersistAsync`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `FinishSetParent` | The reparent write lands and releases the gate. | `PersistAsync` releasing `_persistWriteGate` in `finally`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `AdmitDigest` | The runtime admits the digest read into the busy leaf, or queues it. | `[AlwaysInterleave]` on `IBPlusLeafGrain.GetChildDigestSnapshotAsync`. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `ServeDigest` | The leaf returns its digest; seeding completes and the link heads back. | `BPlusLeafGrain.GetChildDigestSnapshotAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `ReturnLink` | The link returns, the root releases its gate and the capture turn ends. | The end of `LinkLeafSplitFromCaptureAsync`. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `BeginFlush` | Another interleaved turn persists, waiting on the gate and re-checking the clear latch. | `PersistAsync` checking `_leafStateCleared` inside `_persistWriteGate`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `FinishFlush` | That write lands and releases the gate. | `PersistAsync` releasing `_persistWriteGate`. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `ClearLeaf` | The leaf is cleared once no write is in flight. | `ClearGrainStateAsync` inside `_persistWriteGate`. | Yes: `BPlusLeafGrainTests.A_clear_waits_for_an_interleaved_persist_already_in_flight`. |
| `Timeout` | The call timeout breaks a genuine wait cycle and fails the capture. | Orleans `ResponseTimeout` on the capture's root call. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |
| `Stutter` | No protocol progress. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `TypeOK` | Turn, chain, seeding, gate and write state stay in their finite domains. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `NoTurnWaitCycle` | No seeding callback queues behind the capture turn that is waiting on it. | Yes: `IBPlusLeafGrainReparentInterleaveTests.Reparent_callback_is_marked_AlwaysInterleave`. |
| `NoAcknowledgedWriteLost` | The call timeout never fails a capture whose write was acknowledged. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |
| `NoConcurrentLeafWrites` | At most one storage write is in flight on the leaf row. | Yes: `BPlusLeafGrainTests.Interleaved_SetParentAsync_persist_never_overlaps_an_ungated_persist`. |
| `NoResurrectionAfterClear` | No write lands on a leaf after it has been cleared. | Yes: `BPlusLeafGrainTests.A_clear_waits_for_an_interleaved_persist_already_in_flight`. |
| `LinkGateMatchesLinking` | The root holds its link gate exactly while the link is outstanding. | Yes: `ShardRootGrainSplitLinkTests.Concurrent_links_are_serialised_so_the_second_descends_after_the_first_lands`. |
| `LinkedOnlyAfterSeed` | The link is reported only after the new sibling has reparented and read the donor. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `CaptureEventuallyLinked` | A capture that starts a link eventually has it linked. | Yes: `ShardConsolidationChaosTests.Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing`. |

## Bounds and assumptions

The checked instance has one capture link, one parent overflow and one
interleaved flush, which is the smallest instance in which the wait cycle and
both gate hazards can occur. `SetParentInterleaves` and `DigestInterleaves`
are `TRUE` in the base model, matching the shipped `[AlwaysInterleave]`
attributes; the mutations that queue either callback behind the capture turn
reproduce the #4914 deadlock.
Weak fairness covers every step except starting a capture, starting a flush,
clearing the leaf and stuttering, so the liveness property assumes the root,
the parent and storage keep answering. It does not claim progress through a
storage outage. Concurrent links from other leaves are covered by the root's
link gate and the existing split-link tests; the cascading-split model covers
divisions that climb more than one level.