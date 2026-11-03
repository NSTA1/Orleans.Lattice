# Refinement note: the live-advance core to production

This note maps [`WalLiveAdvanceCore.tla`](WalLiveAdvanceCore.tla) - when one leaf's
activation may advance its read position over its own applied appends without reading
the WAL, and what that does to the shared snapshot and the GC trim - to the
Orleans.Lattice code that plays each role. It is module C of the five modules promoted
from the design model [`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla) for epic
#5605, and the sink of their assume-guarantee graph
([`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)). The design authority is
[`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md): section 1's "Arming" list,
section 4's unstamped watermark and self-report, and "The model". Production landed in
#5704. Like the other notes in this directory it is a documented mapping, not a
refinement proof: the gates check its names, detectors and coverage, and its claims were
checked by reading production.

## Model scope in one paragraph

One leaf L at one WAL partition of three offsets. Its born activation `a1` and one
duplicate `a2`, which starts from the shared snapshot, replay, live-advance, capture into
the shared per-leaf snapshot, and append; the GC trims below the snapshot's coverage.
Every other author of an entry L owns is environment, constrained exactly by the
guarantee of the module that owns it: own stamped appends by the current activation only,
with a claim head equal to the allocator head at the raise (E); foreign appends only while
L's marker is open, with a close recording the allocator head (H); one hand-off window and
a hand-off that may deliver a foreign record to an activation; a 10.2.0 silo's unstamped
append, always admitted and raising the partition's unstamped watermark. The base adds
nothing else. The variants add one environment each: a WAL shard restart (`Restart`), own
unstamped appends by any running activation (`GateFlip`), L's tree purged and re-created
with a straddler of the purged incarnation and the re-created incarnation's first
initialisation (`Recreate`, R and H), and a bulk-born leaf (`Bulk`, B). The core owns
`Armed(a)`, `LiveAdvance`, `Replay`, `Capture`, `GcTrim`, the applied-prefix tracker
(`mine`, and the in-flight own appends), the unstamped watermark with its restart reset and
self-report, and the duplicate. The base also checks one liveness property,
`NewbornPinEventuallyUsable`, under `LiveSpec`'s explicit fairness; the variants check
safety only.

## Interface

This module is C in the assume-guarantee graph of the five modules that replace the
design model. It reads no state of the other modules: each assumption is an environment
step or guard written in the contract's vocabulary, and the assumption mutations below
weaken each one in turn and go red, so each is load-bearing.

- **Assumptions.** Every guarantee the contract lists for C, each modelled as stated:

  | Assumption | Guarantee | Discharged by | Here | Weakened by |
  |------------|-----------|---------------|------|-------------|
  | A claim makes one activation current, its head the allocator head at the raise | `ClaimHeadCoversPriorAppends` | E, `WalAppendEpoch` | `Claim(a)` sets `claimHead[a]` to `next` in the step that makes `a` current | `ReadPositionHonestAssumeClaimHeadBelowAllocator` |
  | A superseded activation never allocates again; no two activations share a raised epoch | `StaleEpochNeverAppends`, `RaisedEpochUnique` | E, `WalAppendEpoch` | `OwnAlloc(a)` requires `cur = a`; a `Claim` makes the previous current activation stale, and a stale one never claims | `ReadPositionHonestAssumeSupersededAllocates` |
  | A foreign append is allocated only while L's marker is open, or below the `closedAt` its close recorded | `HandOffBound` | H, `WalHandOffFence` | `ForeignAlloc` requires `hoOpen`; `Close` records any head up to `next` above every foreign offset | `ReadPositionHonestAssumeForeignAfterClose` (the allocation) and `ReadPositionHonestAssumeCloseHeadUnderstated` (the close) |
  | An absent or uninitialised marker holds L | `AbsentMarkerHolds` | H, `WalHandOffFence` | the purge deletes the marker: `mInit` is `FALSE` until the first initialisation | `ReadPositionHonestAssumeUninitialisedMarkerClosed` |
  | Every close, and the first initialisation, max-merges its heads into `closedAt`, so it never decreases within an incarnation of the marker | `CloseHeadMonotone` | H, `WalHandOffFence` | `Close` and `MarkInit` write `Max2(hoClosedAt, h)`; the purge discards the marker, so `Recreate` resets `hoClosedAt` | `ReadPositionHonestAssumeCloseReplacesHeads` |
  | A straddler is allocated only below the heads the re-created incarnation's first initialisation records | `StraddlerBelowInit` | R, `WalRoutingFloor` | `StraddlerAlloc` requires `~reinit`; `MarkInit` records any head up to `next` above every foreign and straddler offset | `ReadPositionHonestAssumeStraddlerAfterInit` (the allocation) and `ReadPositionHonestAssumeInitHeadsMissing` (the initialisation) |
  | A bulk-born leaf's birth position is at or below every record of its range it does not hold at birth | `BulkHintBound` | B, `WalBulkSwitch` | `Create` holds every record below the hint and reads from the hint | `ReadPositionHonestAssumeBulkHintAboveUnheld` |
  | Every unstamped append is reported | the stamp census | the core itself (the watermark) | `OldAlloc` and `OwnAllocUnstamped` raise `lastU`; a stamped append's response reports it; an own unstamped append reports itself | `ReadPositionHonestAssumeUnstampedUnreported` |

  Four readings of the contract are worth stating. `RaisedEpochUnique` and
  `StaleEpochNeverAppends` are modelled together as "only the current activation
  allocates a stamped append". `BulkHintBound`'s "birth read position" is the hint, the
  first offset the leaf reads, so its checkpoint starts at the hint minus one. And a write
  a purged incarnation's initiator allocated before the purge, in a window the purge left
  open, is covered as a straddler: `StraddlerBelowInit` bounds every append by an initiator
  of the purged incarnation by the first initialisation's heads, which is why
  `ReadPositionHonestAssumeInitHeadsMissing` and
  `ReadPositionHonestAssumeUninitialisedMarkerClosed` both find a counterexample through
  such a write as well as through a straddler. Finally, a close and the first
  initialisation record the weakest head their guarantee allows - any head up to `next`
  above the offsets it bounds - rather than `next` itself. Recording `next` would make
  `closedAt` monotone incidentally, as the allocator only grows, so the model would never
  depend on `CloseHeadMonotone` and could not show it load-bearing: with heads chosen this
  way and overwritten, a window opened before the re-created incarnation's first
  initialisation and closed after it lowers the initialisation's heads below the
  straddler (`ReadPositionHonestAssumeCloseReplacesHeads`, red in `Recreate`).
- **Guarantees.** Four, each a checked invariant of the base and every variant, consumed
  by no other module: `ReadPositionHonest`, `SnapshotHonest`, `NoTrimPastAcked` and
  `OldNeverRejected`; and one liveness property of the base,
  `NewbornPinEventuallyUsable`, the armed newborn's pin becoming usable without a
  replay from `-1`.
- **The graph.** E <- H, E <- R, and {E, H, R, B} <- C, where E is `WalAppendEpoch`,
  H `WalHandOffFence`, R `WalRoutingFloor` and B `WalBulkSwitch`; an arrow points from the
  module that assumes to the module that guarantees. C assumes every other module and is
  assumed by none, so it closes no cycle.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `author` | The WAL shard's allocator, and the author of each offset | `WalShardGrain._nextOffset`, assigned by `WalOffsetAllocationCore.Assign` under the shard's state lock. Only entries L owns are modelled; an author is whoever appended an entry L's replay applies. |
| `inflight`, `durable`, `acked` | Allocated and not yet durable; durable; acknowledged to the writer | The shard's in-flight flush chain (`WalShardGrain.StartFlush`), the entries the partition's `IWalStorageProvider` holds, and appends whose `WalShardGrain.AppendAsync` or `WalShardGrain.AppendStampedAsync` completed. A foreign entry counts as acknowledged once handed off to L. |
| `floor` | The GC trim floor | The prefix `LatticeWalGc.TrimShardAsync` trims, bounded by the leaf's pin at its snapshot's coverage (`WalDurability`'s `GcTrim`). |
| `up` | L's running activations | Activations of `BPlusLeafGrain`. |
| `cache` | Each activation's projection | `BPlusLeafGrain`'s entry cache (`LeafEntryCache`), as the set of owned offsets it holds. |
| `rp` | Each activation's read position | `BPlusLeafGrain.GetCurrentCheckpointForPartition`; the live advance moves it through `BPlusLeafGrain.StampCheckpointHintAsync`. |
| `mine` | The applied-prefix tracker | `BPlusLeafGrain._liveOwnHigh`, the highest own offset applied per partition, recorded by `BPlusLeafGrain.MarkLiveAppendApplied`. The in-flight own appends are `BPlusLeafGrain._liveAppendsUnapplied`, counted before every append by `BPlusLeafGrain.NoteLiveAppendIssued`. |
| `snapCov`, `snapRows` | The shared per-leaf snapshot | `LeafSnapshotBlob.SnapshotOffsetsByPartition` and its rows, kept by `ILeafSnapshotStorageGrain.SaveAsync`. |
| `cur`, `claimed`, `claimHead`, `stale` | E's guarantees as environment state: the current activation, each claim raised, its head, and the activations that never claim or append stamped again | `LeafAppendEpochFence.TryGetArmingView` returns the claim head under the current epoch; a superseded activation is `LeafAppendEpochFence._stale`. The epochs themselves are E's (`AppendEpochRefinement.md`). |
| `dupUsed`, `restarted`, `oldUsed`, `spUsed`, `windows` | Environment budgets | Modelling devices. |
| `mInit`, `hoOpen`, `hoClosedAt` | H's marker as environment state: initialised, open, and the maximum of the heads every close and the first initialisation recorded | `LeafHandOffState.Initialised` and the shard marker's flag (`ShardHandOffState.Initialised`), read with `HandOffMarkerCore.IsHeldOpen` and `HandOffMarkerCore.RequiredHead` through `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync` and `ShardRootGrain.GetLiveAdvanceViewAsync` (`LeafLiveAdvanceView`). |
| `lastU` | The partition's unstamped watermark | `WalShardGrain._unstampedWatermark`, raised under the state lock when `WalShardGrain.RaisesUnstampedWatermark` holds and returned in every stamped append's response. |
| `seenU` | The highest unstamped offset reported to each activation | `LeafAppendEpochFence.ReportedUnstamped`, raised by `LeafAppendEpochFence.OnStampedAppended` and, for an own unstamped append, `LeafAppendEpochFence.OnUnstampedAppended`. |
| `recreated`, `reinit` | The purge, and the re-created incarnation's first initialisation | Environment state for R and H: the tree purge, and `ShardRootGrain.FenceRecreatedIncarnationAsync`. |
| `phase`, `hint` | B's bulk switch, and the leaf's birth hint | `ShardRootGrain.CaptureBulkBirthHeadsAsync`, persisted with `ShardRootGrain.WriteBulkBirthHintAsync`. A bulk-born leaf is initialised without `SiblingInitialization.WalHeadsAtBirth`, so it seeds no birth pin; the hint reaches it after the relocation, through `ShardRootGrain.FinishBulkSwitchAsync` calling `BPlusLeafGrain.SetCheckpointOffsetHintsAsync`. For a bulk append the applied hint is capped per partition at the old leaf's replay frontier (`ShardRootGrain.CapAtReadFrontier`, #5752); that only lowers the read position, so it still satisfies `BulkHintBound` and maps to `Create`'s `hint - 1` in head terms. A bulk load caps its hint the same way, per partition at the lowest replay frontier of the leaves of the root it replaced, and applies none when it replaced no root or a replaced leaf reports no frontier (`ShardRootGrain.RetireReplacedRootAsync`, `BPlusLeafGrain.CopyRowsForBulkLoadAsync`, #5766). |
| `oldRejected` | Ghost for `OldNeverRejected` | Modelling device. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Create` | **Environment (B's `BulkHintBound`).** A bulk-born leaf's first activation, holding the rows relocated into it, reading from its birth hint under the switch's open marker | `ShardRootGrain.BeginBulkSwitchAsync` takes the hint (`ShardRootGrain.CaptureBulkBirthHeadsAsync`) with the open marker. The leaf is initialised without `SiblingInitialization.WalHeadsAtBirth` and seeds no birth pin; its read position arrives after the relocation, through `ShardRootGrain.FinishBulkSwitchAsync` calling `BPlusLeafGrain.SetCheckpointOffsetHintsAsync`. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_bulk_appended_leaf_arms_once_its_switch_has_closed` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_appended_leaves_start_from_the_birth_hint_and_the_runs_switches_close_by_fence`. |
| `Claim(a)` | **Environment (E's `ClaimHeadCoversPriorAppends`, `RaisedEpochUnique`).** A claim is raised: the activation becomes current, its claim head is the allocator head, and the one it supersedes never allocates stamped again | E's three-step claim (`AppendEpochRefinement.md`); the core reads its outcome through `LeafAppendEpochFence.TryGetArmingView`, which answers only for a claim under the current epoch of an activation not stale. **Over-approximation:** each activation claims once, and any running activation not stale may claim in any order. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `WalShardGrainTests.The_claim_head_is_the_allocator_next_offset`. |
| `OwnAlloc(a)` | **Environment (E's `StaleEpochNeverAppends`).** The current activation's stamped append is allocated, and its response reports the unstamped watermark | `WalFencedAppendDispatch.AppendPointAsync` and `WalFencedAppendDispatch.AppendBatchAsync` pass the watermark in each stamped response to `LeafAppendEpochFence.OnStampedAppended`. | Yes: `LeafAppendEpochFenceTests.Stamped_responses_raise_the_reported_watermark`, `WalShardGrainTests.Unstamped_appends_are_always_admitted_and_reported` and `BPlusLeafGrainTests.A_reported_unstamped_append_holds_the_advance_until_a_replay_reads_past_it`. |
| `OwnAllocUnstamped(a)` | An own append made unstamped - a closed gate view, a claim fault or exhausted rounds, an unreachable leaf - by any running activation, always admitted, raising the watermark and reporting itself | `WalFencedAppendDispatch.AppendPointAsync` and `WalFencedAppendDispatch.AppendBatchAsync` call `LeafAppendEpochFence.OnUnstampedAppended` with the offsets an unstamped append returned. | Yes: `LeafAppendEpochFenceTests.Own_unstamped_appends_report_themselves`, `BPlusLeafGrainTests.A_write_through_an_existing_leaf_method_reports_itself_and_holds_the_advance` and `BPlusLeafGrainTests.A_descent_unreachable_leaf_never_claims_and_appends_unstamped_and_reported`. |
| `Land(o)` | The append becomes durable; an own append is applied, recorded by the tracker and acknowledged | The append site applies, then `BPlusLeafGrain.MarkLiveAppendApplied` releases the count and records the offset in `BPlusLeafGrain._liveOwnHigh`; an apply that throws leaves the count held, which turns the advance off for the activation. | Yes: `BPlusLeafGrainTests.A_live_advance_waits_until_every_issued_append_is_applied` and `BPlusLeafGrainTests.A_failed_append_turns_the_live_advance_off_for_the_activation`. |
| `ForeignOpen` | **Environment (H).** A hand-off opens L's marker before its first admission. Its only mutation is a `TypeOK` bound (`TypeOkForeignWindowPastBound`): opening only enables `ForeignAlloc`, whose role `ReadPositionHonestAssumeForeignAfterClose` covers | `HandOffMarkerCore.Open`, on the leaf's marker or the shard's (`ShardRootGrain.OpenHandOffMarkerAsync`). | Yes: `HandOffMarkerCoreTests.An_open_is_idempotent_and_holds_only_overlapping_owners` and `BPlusLeafGrainTests.An_open_leaf_marker_holds_the_advance`. |
| `ForeignAlloc` | **Environment (H's `HandOffBound`).** A foreign append is allocated while L's marker is open | A hand-off initiator's stamped append, admitted at the WAL shard under its epoch or routing generation (`WalShardGrain.AppendStampedAsync`). | Yes: `BPlusLeafGrainTests.An_open_shard_marker_token_holds_only_the_leaves_it_overlaps` and `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump`. |
| `Close` | **Environment (H's `HandOffBound` and `CloseHeadMonotone`).** The close fences and records a head above every foreign record, max-merged into `closedAt` | `HandOffMarkerCore.Close` with the heads the initiator's re-claim returned (`ShardRootGrain.CloseHandOffMarkerAsync`); `HandOffMarkerCore.RequiredHead` takes the maximum over every retained close and the compacted heads. | Yes: `HandOffMarkerCoreTests.A_close_records_the_fence_heads_over_the_hand_off_range_and_never_reopens`, `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads` and `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it`. |
| `HandOff(o, a)` | **Environment.** A foreign record is delivered to an activation, which acknowledges it; it may never happen | The relocation of a hand-off's rows into the owner (a split's transfer, a reclaim's absorb, a bulk graft), which the core sees only as rows already in its projection. **Over-approximation:** any running activation may receive any durable foreign record, at any time. | Yes: `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump` and `LeafHandOffFenceIntegrationTests.Every_reclaim_opens_its_marker_on_the_survivor_and_closes_it_after_retiring_the_absorbed_leaf`. |
| `OldAlloc` | A 10.2.0 silo's unstamped append (or any path the census classes unstamped), always admitted, raising the unstamped watermark | `WalShardGrain.AppendAsync`, whose unstamped shapes have no rejection path (the fence never rejects one), raising `WalShardGrain._unstampedWatermark`. | Yes: `WalShardGrainTests.Unstamped_appends_are_always_admitted_and_reported`, `WalAppendFenceMixedVersionTests.The_existing_unstamped_methods_keep_working_on_the_older_build` and `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`. |
| `Replay(a)` | Read and apply the next durable entry, never a trimmed one | `BPlusLeafGrain.ReplayPartitionAsync`, the replay-based advance the core falls back to (`WalDurability`'s `ReadStep`). | Yes: `BPlusLeafGrainTests.A_leaf_that_has_not_read_to_its_claim_head_advances_only_by_replay` and `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`. |
| `LiveAdvance(a)` | An armed activation advances its read position to its highest own applied offset without reading | `BPlusLeafGrain.TryAdvanceLiveCheckpointAsync`, deciding with `LeafLiveAdvanceCore.TargetFor`, whose arming is `Armed(a)` conjunct for conjunct: `claimRaised` and `claimHead` from `LeafAppendEpochFence.TryGetArmingView`, `handOffClear` (`mInit` and `~hoOpen`) and `closeHead` from `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync`, `unstampedSeen`, and the tracker through `LeafLiveAdvanceCore.IsQuiescent`. Production holds the advance in more cases than `Armed(a)` (see the gaps). | Yes: `LeafLiveAdvanceCoreTests.The_advance_is_taken_exactly_when_the_leaf_is_armed`, `LeafLiveAdvanceCoreTests.An_armed_leaf_advances_to_its_highest_own_offset`, `BPlusLeafGrainTests.A_live_written_leaf_of_a_new_tree_advances_its_checkpoint_without_a_replay` and `BPlusLeafGrainTests.Each_partition_advances_to_its_own_highest_offset`. |
| `Capture(a)` | The shared per-leaf snapshot keeps the highest coverage | `BPlusLeafGrain.CaptureSnapshotCoreAsync`, after the coverage-lag tick banks a live advance (`BPlusLeafGrain.OnCoverageLagTimerTickAsync`); the store never lowers coverage (`LeafSnapshotStorageGrain.MergeMonotone`). | Yes: `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`, `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay` and `LeafSnapshotStorageGrainTests.SaveAsync_still_merges_a_regressing_capture_that_carries_every_stored_key`. |
| `GcTrim` | Trim the next durable entry below the snapshot's coverage | `LatticeWalGc.TrimShardAsync`, bounded by the leaf's pin (`WalDurability`'s `GcTrim`). **Over-approximation:** the model trims to the shared snapshot's coverage alone. | Yes: `LeafDurablePinCoreTests.A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage` and `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay`. |
| `Duplicate` | A second activation of L, or the next one, starts from the shared snapshot and must claim before it appends stamped | `BPlusLeafGrain.ExecuteActivationReplayAsync` rehydrates from the snapshot (`BPlusLeafGrain.TryRehydrateFromSnapshotAsync`); its own `LeafAppendEpochFence` claims afresh. **Environment:** once in the model. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `BPlusLeafGrainTests.Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint`. |
| `Restart` | A WAL shard restart, a reactivation or a move's cutover, resets the in-memory unstamped watermark to the recovered allocator head minus one | `WalShardGrain.OnActivateAsync` sets `WalShardGrain._unstampedWatermark` from the recovered `WalShardGrain._nextOffset`. **Environment:** once in the model. | Yes: `WalShardGrainTests.The_unstamped_watermark_restarts_at_the_recovered_head`. |
| `Recreate` | **Environment (H's `AbsentMarkerHolds`).** L's tree is purged and re-created: its marker is deleted and so not initialised, and every running activation belongs to the purged incarnation, whose epoch is retired | The leaf's clear deletes its marker row (`BPlusLeafGrain.ClearHandOffMarkerAsync`, `HandOffMarkerRows.ClearLeafAsync`), and the re-seeded shard root retires a predating marker (`ShardRootGrain.RetirePredatingHandOffMarkerAsync`); the epoch retirement is E's. | Yes: `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`, `TreeFenceRowPurgeIntegrationTests.A_tree_re_created_after_a_purge_that_retired_nothing_never_adopts_its_markers` and `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag`. |
| `StraddlerAlloc` | **Environment (R's `StraddlerBelowInit`).** A write a shard root of the purged incarnation admitted is allocated, only before the re-created incarnation's first initialisation | The WAL refuses a straddler at or below its initiator's routing floor (`WalShardGrain._generationFloors`), and its seam retry never re-stamps it (`ShardRootGrain.ThrowIfAdmittedUnderPurgedIncarnationAsync`). | Yes: `TreeFenceRowPurgeIntegrationTests.A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it` and `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`. |
| `MarkInit` | **Environment (R and H).** The re-created incarnation's first initialisation claims its seeded generation everywhere, records the heads as a close, max-merged into `closedAt` like any other, and initialises the marker state | `ShardRootGrain.FenceRecreatedIncarnationAsync`, then `ShardRootGrain.TryInitialiseHandOffMarkerAsync`; the leaf adopts the shard's flag in `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync`. | Yes: `TreeFenceRowPurgeIntegrationTests.A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it` and `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `ReadPositionHonest` | No running activation's checkpoint passes a durable entry it owns and does not hold, acknowledged or not, whoever appended it: a duplicate, a foreign author, a straddler, a 10.2.0 silo or itself. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it`, `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`, `LiveCheckpointAdvanceIntegrationTests.An_orphans_append_is_unstamped_so_the_reachable_owner_reads_it_before_it_advances`, `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it` and `LeafLiveAdvanceCoreTests.The_advance_is_taken_exactly_when_the_leaf_is_armed`. |
| `SnapshotHonest` | The shared snapshot never claims coverage of a durable entry it lacks: the live advance never passes an own append still in flight. | Yes: `BPlusLeafGrainTests.A_live_advance_waits_until_every_issued_append_is_applied`, `BPlusLeafGrainTests.A_failed_append_turns_the_live_advance_off_for_the_activation` and `LeafLiveAdvanceCoreTests.Each_quiescence_condition_holds_the_advance`. |
| `NoTrimPastAcked` | The GC never trims an acknowledged append the leaf's snapshot lacks, after a live advance as after a replay. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`, `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay`. |
| `NewbornPinEventuallyUsable` | A live-written leaf's own applied appends are eventually covered by its banked snapshot, so its WAL pin becomes usable without a replay from `-1`: the leaf arms, advances live and the coverage-lag tick captures. | Yes: `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay` and `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`. |
| `OldNeverRejected` | An unstamped append - a 10.2.0 silo's, or any while the gate is closed - is never rejected by the append fence. | Yes: `WalShardGrainTests.Unstamped_appends_are_always_admitted_and_reported` and `WalAppendFenceMixedVersionTests.The_existing_unstamped_methods_keep_working_on_the_older_build`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

`TypeOK` and the four invariants are state safety, reached and checked on the base and
re-checked in each variant. `NewbornPinEventuallyUsable` is liveness, checked on the base
and re-checked in `Bulk`, whose bulk-born leaf is a newborn too; the other variants leave
it out (below). The base carries the duplicate, one hand-off window and a
10.2.0 silo's append; each variant adds one environment (a restart, own unstamped appends,
the purge, a bulk birth), because the combinations are larger than the CI budget allows
for one case. The design model's combined configurations stay as local evidence in the
design note.

**The liveness property** (member 6b) is the design note's: under weak fairness of the
claim, replay, apply and live-advance steps, an armed activation's newborn pin eventually
becomes usable without a replay from `-1`. It is checked as `<>[]`: eventually, always,
the shared snapshot covers every durable own append. The base cfg's specification is
`LiveSpec`, `Spec` with one condition, `WF_vars(Progress)`:
- `Progress` is the disjunction of the leaf's own steps (`Claim`, `Land`, `LiveAdvance`,
  `Capture`), the environment steps that end a hold (`Close`, `Create`, `MarkInit`), and
  a replay **only while a bound the arming list names lies above the read position**
  (`ReplayNeeded`: the claim head, the close heads, a reported unstamped offset).
  This is a **deliberate under-approximation**, not a statement about production:
  production's starvation drive also replays from the checkpoint to the head, over the
  leaf's own appends. The property assumes no replay beyond the arming bounds, so it
  shows that the live advance alone makes the pin usable; the starvation drive is a
  further fallback it does not rely on. It is what keeps the property non-vacuous: with
  replay fair unconditionally, `NewbornPinEventuallyUsableAdvanceNeverFires` comes out
  clean (778,329 states, checked by the independent review). Weakening a fairness
  assumption only strengthens a liveness claim, so the restriction is sound.
- One condition over the disjunction forces the same quiescent end as one per step,
  and costs TLC one temporal branch instead of eleven. **Precondition:** every step but
  `Stutter` can be taken only finitely often in every configuration (`next` is bounded;
  each one-shot environment step fires once; `ForeignOpen` is bounded by `MaxWindows`;
  `Land` and `HandOff` consume their offsets; `rp`, `snapCov` and `floor` only rise,
  within bounds). A configuration with an unbounded step would need one condition per
  step.
- **Exclusion:** the property is discharged once the current activation has fallen off
  the log (`CurrentFellOffLog`: the GC trimmed an entry it must still replay). The state
  is reachable in production: the leaf's WAL pin is one per-leaf row merged
  monotonic-max, so a duplicate activation's capture can raise it past the current
  activation's checkpoint and let the GC trim past it. Production's response is the
  existing #3477 path: the partition latches stale and refuses hints and the live
  advance, and deactivation clears it, the reactivation rehydrating from the newer
  snapshot. The gap register below records the exclusion and the detectors that pin
  that path. Without the exclusion TLC reports exactly this behaviour; with the GC
  additionally bounded by every running activation's read position, the property holds
  without it (550,843 states, checked by the independent review).
- **Cost, measured** (tla2tools v1.7.4, two workers, liveness included): the base finds
  778,329 distinct states to depth 25 in 1 minute 27 seconds, and `Bulk` 875,431 in 2
  minutes 19 seconds. Stated as three leads-to formulas the base took 4 minutes, and with
  one fairness condition per step as well, 11.
- **Bounded out:** `Restart` holds the property too, but takes 7 minutes 18 seconds
  locally, too close to the 20-minute case budget on a CI runner, so it checks safety
  only. `GateFlip` and `Recreate` are outside the property's premise, a leaf born while
  the gate is open: `GateFlip`'s own appends are made under a closed gate view, and
  `Recreate` ends the newborn's incarnation.
- Its two experiments each break progress and go red in about five seconds:
  `NewbornPinEventuallyUsableAdvanceNeverFires` (the live advance never enabled, as
  before #5572) and `NewbornPinEventuallyUsableArmingNeverEnabled` (the claim-head
  conjunct asks for a read the driven replay never reaches).

Every experiment is an anchored perturbation. Seven edit the `Armed` predicate rather
than an action, so they name no perturbed action: `NewbornPinEventuallyUsableArmingNeverEnabled`,
`ReadPositionHonestNoClaimHeadReplay`,
`SnapshotHonestNoOwnTracker`, `ReadPositionHonestUnstampedNotReported`,
`ReadPositionHonestNoMarkerInit`, `ReadPositionHonestArmsUnderOpenMarker` and
`ReadPositionHonestArmsBelowCloseHead`. `NoTrimPastAckedLiveAdvance5578` edits both the
predicate and `OwnAlloc`. A clean coverage run (`-coverage`) of the base and each variant
confirms every action the configuration enables is taken, `LiveAdvance` included.

## Design mutations moved here

The design model's mutations the contract assigns to this module were moved, re-targeted
where needed and re-anchored on this module's text.

| Design mutation | Here |
|-----------------|------|
| `NoTrimPastAckedLiveAdvance5578` | Same name. It replaces `Armed` with #5578's rule and drops the current-activation guard from `OwnAlloc`, the environment's form of the epoch check it removed. |
| `ReadPositionHonestNoClaimHeadReplay` | Same name; edits `Armed`. |
| `SnapshotHonestNoOwnTracker` | Same name; edits `Armed`. |
| `ReadPositionHonestNoSelfReport` | Same name; anchored on `OwnAllocUnstamped`, with `WithGateFlip`. |
| `ReadPositionHonestUnstampedNotReported` | Same name; edits `Armed`. The base carries the 10.2.0 silo's append, so it needs no bound. |
| `ReadPositionHonestRestartForgetsUnstamped` | Same name; anchored on `Restart`, with `WithRestart`. |
| `OldNeverRejectedRejectedOnceFenced` | Same name; the edit is the admission in `OldAlloc`, and the partition is fenced once any claim of L has been raised there. |

Two of the design's redundancy checks bear on `Armed`. `ReadPositionHonestNoMarkerInit`,
clean in the design's legacy hand-off configuration, is red here: with the purge in the
environment, the initialised guard is what holds a re-created incarnation while a write of
the purged one is still allocated, so it is a mutation in this module.
`ReadPositionHonestArmsWithoutClaim` stays a redundancy check: run locally against this
module's base and all four variants it is clean, because an activation that has not claimed has only
unstamped own appends, each reporting itself, so it never arms past one. It is kept, as the
design keeps it, because arming without a claim is meaningless, and it is not in the
mutation directory, which holds only experiments that must go red.

## Deliberate abstraction gaps

- **The liveness property excludes a current activation that has fallen off the log.**
  `NewbornPinEventuallyUsable` is discharged by `CurrentFellOffLog`: the GC trimmed an
  entry the current activation must still replay. Production reaches that state through a
  duplicate activation. The leaf's WAL pin is one per-leaf row merged monotonic-max
  (`WalMaterialiserPinGrain`), so the duplicate's capture raises it past the current
  activation's checkpoint and the GC trims past it. Production's response is the
  existing #3477 path: the replay throws `LeafProjectionStaleException`, the warm rescue
  (`BPlusLeafGrain.TryRescueWarmStaleLeafAsync`) declines, and the partition is latched
  stale (`BPlusLeafGrain.LatchProjectionStaleDrive`). The latch
  (`BPlusLeafGrain.IsPartitionStaleLatched`) refuses checkpoint hints and the live advance
  until deactivation, and the reactivation rehydrates from the newer snapshot. No
  progress is owed meanwhile. The path is driven end to end, with the real WAL store, pin
  grain, snapshot storage and GC, by
  `BPlusLeafGrainTests.A_duplicate_activations_pin_trims_the_current_activation_off_the_log_and_it_latches_stale`
  (the latch, the refused hint and the refused live advance) and
  `BPlusLeafGrainTests.A_reactivation_after_the_duplicate_trim_rehydrates_the_newer_snapshot_and_loses_no_key`
  (every key of both activations served after the rehydrate). Disabling the latch turns
  both red. The latch's own detectors are
  `BPlusLeafGrainTests.A_stale_latched_partition_never_live_advances`,
  `BPlusLeafGrainTests.A_checkpoint_hint_does_not_advance_a_stale_latched_partition` and
  `BPlusLeafGrainTests.A_latched_stale_leaf_still_faults_a_direct_drive_without_replaying`;
  rehydrating from a newer snapshot is pinned by
  `BPlusLeafGrainTests.Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint`
  and `LeafSnapshotRehydrateIntegrationTests.Snapshot_capture_persists_blob_and_rehydrate_serves_keys_after_deactivation`.
  Production symbols: `WalMaterialiserPinGrain`, `LeafProjectionStaleException`, `BPlusLeafGrain.TryRescueWarmStaleLeafAsync`, `BPlusLeafGrain.LatchProjectionStaleDrive`, `BPlusLeafGrain.IsPartitionStaleLatched`.
- **Production holds the advance in more cases than `Armed`.** The model arms on the
  design's six conditions. Production also holds the advance while a saga prepare is
  pending, while another replay or a warm rescue is in flight, while the partition is
  latched stale, while the leaf may share keys with another live leaf (no shard index, a
  rangeless leaf with siblings, a suspected orphan), while the storage provider is not
  known to enforce ETags, and while one advance is already deciding. Each only holds the
  advance, so the model over-approximates production here.
  Production symbols: `LeafLiveAdvanceCore.IsQuiescent`, `BPlusLeafGrain.IsLiveAdvanceOverlapExcluded`, `BPlusLeafGrain.IsLiveAdvanceQuiescent`, `GrainStorageFencingCheck`.
- **The marker read is atomic with the advance.** Production reads the leaf's marker and
  its shard root's view fresh, after snapshotting its own appends, and then stamps the
  target; a hand-off that opens between the read and the stamp allocates only above every
  own append the snapshot holds. That holds because the snapshot
  (`BPlusLeafGrain.SnapshotLiveAdvanceCandidates`) is taken synchronously before the
  marker read is awaited, every target is capped at the snapshot's own high offset, and the
  arming view (`LeafAppendEpochFence.TryGetArmingView`) is read after the marker, so only
  fresher. The model reads the marker in the step that advances, so it cannot express that
  order. The detector
  `A_hand_off_opening_during_the_marker_read_cannot_carry_the_advance_past_its_foreign_record`
  (`BPlusLeafGrainTests.LiveCheckpointAdvance.cs`, #5768) pins it: a hand-off opens while
  the marker read is in flight, allocates a foreign record, and an own append lands above
  it, and the advance must stay below that record. Taking the snapshot after the read turns
  it red. The design's `FrozenView`, a superseded activation that keeps the marker
  state it last saw, is not carried: production reads
  fresh on every tick, and a superseded activation never arms at all, because
  `LeafAppendEpochFence.TryGetArmingView` answers false once it is stale. The model's
  `Armed` still lets a stale but claimed activation arm, a safe over-approximation.
  Production symbols: `BPlusLeafGrain.TryAdvanceLiveCheckpointAsync`, `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync`, `ShardRootGrain.GetLiveAdvanceViewAsync`, `BPlusLeafGrain.SnapshotLiveAdvanceCandidates`, `LeafAppendEpochFence.TryGetArmingView`.
- **Refusals that allocate nothing.** A definite refusal - a self-fenced append, a stale
  generation, a clock-floor refusal - leaves no own record, and production releases its
  tracker count. Here an own append is always allocated, so the release is not modelled;
  a refusal released wrongly would be an apply the tracker never saw.
  Production symbols: `BPlusLeafGrain.DrainRefusedUnallocatedAppends`, `LeafAppendEpochFence.TakeRefusedUnallocated`.
- **One partition, one leaf, two activations.** Production advances each partition over
  its own appends with its own claim head, close heads and watermark; the model has one.
  Other leaves of the partition appear only as authors of entries L does not own, which
  L's replay skips, and are not modelled. Each environment step happens at most once, and
  the variants do not combine.
  Production symbols: `BPlusLeafGrain._liveOwnHigh`.
- **Re-claims.** Each activation claims once. Production re-claims after a WAL shard
  restart unloads its entry, after a bump, and after its own hand-off close, each moving
  the claim head up and disarming the activation until it replays to it. The model keeps
  the first claim head across a restart, which arms more often, so the watermark's reset
  is load-bearing here (`ReadPositionHonestRestartForgetsUnstamped`).
  Production symbols: `LeafAppendEpochFence.OnUnloaded`, `LeafAppendEpochFence.BumpAndReclaimAsync`.
- **The duplicate's own snapshot and the purge's trim.** The re-created incarnation's
  activation starts from the shared snapshot the purged one left, and the purge trims no
  entry here; production clears the purged tree's snapshot and trims its WAL. Both only
  remove entries the model keeps for the re-created incarnation to read.
  Production symbols: `LeafSnapshotStorageGrain.MergeMonotone`, `LatticeWalGc.TrimShardAsync`.
- **The unreachable leaf's tick re-check.** A leaf whose tick finds it unreachable stops
  stamping for the activation (#5709); the model covers its appends as own unstamped appends
  (`OwnAllocUnstamped`) and does not model reachability.
  Production symbols: `LeafAppendEpochFence.StopStampingUnreachable`.
