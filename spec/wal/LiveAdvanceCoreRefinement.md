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

In plain terms, the module guarantees that a leaf's live advance never moves its read
position past a record of its range it does not hold, whoever wrote that record; that
the shared snapshot never claims coverage of a record it lacks; that the GC never trims
an acknowledged append the snapshot lacks; and that the append fence never rejects an
unstamped append. It also checks that a newborn leaf's WAL pin eventually becomes usable
through the live advance alone, without a replay from `-1`. It relies on the other four
modules only through the guarantees they check.

## Model scope in one paragraph

One leaf L at one WAL partition of three offsets. Its born activation `a1` and one
duplicate `a2`, which starts from the shared snapshot, replay, live-advance, capture into
the shared per-leaf snapshot, and append; the GC trims below the snapshot's coverage.
Every other author of an entry L owns is environment, constrained exactly by the
guarantee of the module that owns it: own stamped appends by the current activation only,
with a claim head equal to the allocator head at the raise (E); foreign appends only while
L's marker is open or not initialised, with a close recording a head above them once it is
(H); one hand-off window and
a hand-off that may deliver a foreign record to an activation; a 10.2.0 silo's unstamped
append, always admitted and raising the partition's unstamped watermark. The base adds
nothing else. The variants add one environment each: a WAL shard restart (`Restart`), own
unstamped appends by any running activation (`GateFlip`), L's tree purged and re-created
with a straddler of the purged incarnation and the re-created incarnation's first
initialisation (R and H, in three slices: `Recreate`, `RecreateSurvivor` and
`RecreateSurvivorMixed`), and a bulk-born leaf (`Bulk`, B). The core owns
`Armed(a)`, `LiveAdvance`, `Replay`, `Capture`, `GcTrim`, the applied-prefix tracker
(`mine`, and the in-flight own appends), the unstamped watermark with its restart reset and
self-report, and the duplicate. The base and the `Bulk` variant also check one liveness
property, `NewbornPinEventuallyUsable`, under `LiveSpec`'s explicit fairness; the other
variants check safety only.

## Interface

This module is C in the assume-guarantee graph of the five modules that replace the
design model. It reads no state of the other modules: each assumption is an environment
step or guard written in the contract's vocabulary, and the assumption mutations below
weaken each one in turn and go red, so each is load-bearing.

- **Assumptions.** Every guarantee the contract lists for C, each modelled as stated:

  | Assumption | Guarantee | Discharged by | Here | Weakened by |
  |------------|-----------|---------------|------|-------------|
  | A claim makes one activation current, its head the allocator head at the raise | `ClaimHeadCoversPriorAppends` | E, `WalAppendEpoch` | `Claim(a)` sets `claimHead[a]` to `next` in the step that makes `a` current | `ReadPositionHonestAssumeClaimHeadBelowAllocator` |
  | A superseded activation never allocates again; no two activations share a raised epoch | `StaleEpochNeverAppends`, `RaisedEpochUnique` | E, `WalAppendEpoch` | `OwnAlloc(a)` requires `cur = a`; a `Claim` makes the previous current activation stale, and a stale one never claims; the purge retires every raised epoch (`Recreate` leaves no activation current), but an activation that never claimed may claim afterwards, because E's clear finds no row of it | `ReadPositionHonestAssumeSupersededAllocates` |
  | A foreign append is allocated only while L's marker is open, or below the `closedAt` its close recorded | `HandOffBound` | H, `WalHandOffFence` | `ForeignAlloc` requires `hoOpen`, or the marker not initialised, where H's guarantee is conditioned away; `Close` records any head up to `next` above every foreign offset once the marker is initialised, and any head up to `next` before | `ReadPositionHonestAssumeForeignAfterClose` (the allocation) and `ReadPositionHonestAssumeCloseHeadUnderstated` (the close) |
  | An absent or uninitialised marker holds L | `AbsentMarkerHolds` | H, `WalHandOffFence` | the purge deletes the marker: `mInit` is `FALSE` until the first initialisation | `ReadPositionHonestAssumeUninitialisedMarkerClosed` |
  | Every close, and the first initialisation, max-merges its heads into `closedAt`, so it never decreases within an incarnation of the marker | `CloseHeadMonotone` | H, `WalHandOffFence` | `Close` and `MarkInit` write `Max2(hoClosedAt, h)`; the purge discards the marker, so `Recreate` resets `hoClosedAt` | `ReadPositionHonestAssumeCloseReplacesHeads` |
  | A straddler is allocated only below the heads the re-created incarnation's first initialisation records | `StraddlerBelowInit` | R, `WalRoutingFloor` | `StraddlerAlloc` requires `~reinit` | `ReadPositionHonestAssumeStraddlerAfterInit` (the allocation) and `ReadPositionHonestAssumeInitHeadsMissing` (the initialisation) |
  | The first initialisation records a head above every offset allocated before it, whoever allocated it | `InitHeadCoversPriorAllocations` | R, `WalRoutingFloor` | `MarkInit` records any head up to `next` above every foreign and straddler offset | `ReadPositionHonestAssumeInitHeadBelowForeign` |
  | A bulk-born leaf's birth position is at or below every record of its range it does not hold at birth | `BulkHintBound` | B, `WalBulkSwitch` | `Create` picks any read position up to the allocator head, and holds every record allocated at or below it: B's guarantee itself, not B's protocol, which starts at or below the hint minus one (#5752's cap, a timed-out or partial relocation) but does not state it | `ReadPositionHonestAssumeBulkHintAboveUnheld` |
  | Under the lazy arm (#5833), a bulk switch's close head is above every record allocated before it | `SwitchCloseCoversPriorAllocations` | B, `WalBulkSwitch` | `Close` with `WithPreArm` and `WithBulk` records a head above every foreign and pre-existing record (`CloseCovers`) | `ReadPositionHonestAssumeBulkCloseBelowPriorAllocations` |
  | A re-claim of a routing generation at the partition only raises the loaded value | the WAL's loaded entry is max-merged (R's cold load, C7; E's claim) | R, `WalRoutingFloor` | `OtherReceiverArm` and `ResolveDeferred` raise `gL` by max | `ReadPositionHonestAssumeLaterReclaimLowersGeneration` |
  | Every unstamped append is reported | the stamp census | the core itself (the watermark) | `OldAlloc` and `OwnAllocUnstamped` raise `lastU`; a stamped append's response reports it; an own unstamped append reports itself | `ReadPositionHonestAssumeUnstampedUnreported` |

  Four readings of the contract are worth stating. `RaisedEpochUnique` and
  `StaleEpochNeverAppends` are modelled together as "only the current activation
  allocates a stamped append". `BulkHintBound`'s "birth read position" is the first
  offset the leaf reads, one above its checkpoint; every record below it is held. And a write
  a purged incarnation's initiator allocated before the purge, in a window the purge left
  open, is covered as a straddler: `StraddlerBelowInit` bounds every append by an initiator
  of the purged incarnation by the first initialisation's heads, which is why
  `ReadPositionHonestAssumeInitHeadsMissing` and
  `ReadPositionHonestAssumeUninitialisedMarkerClosed` both find a counterexample through
  such a write as well as through a straddler. The environment is exactly each guarantee
  as its owning module checks it, not the protocol behind it: member 6c's composite check,
  which copies each producer's guarantees and each of these steps, found five edges where
  this module had assumed more, and each was widened or the producer's guarantee stated.
  A never-claimed activation may outlive the purge (E); a foreign append and a close are
  unconstrained while L's marker is not initialised (H, whose guarantee is conditioned on
  the marker reading closed); the first initialisation's head covers every earlier
  allocation (R, `InitHeadCoversPriorAllocations`, stated for this); and a bulk-born
  leaf may start at any read position that holds every record at or below it (B; a
  second composite pass found the hint bound is B's protocol, not its guarantee). Finally, a close and the first
  initialisation record the weakest head their guarantee allows - any head up to `next`
  above the offsets it bounds - rather than `next` itself. Recording `next` would make
  `closedAt` monotone incidentally, as the allocator only grows, so the model would never
  depend on `CloseHeadMonotone` and could not show it load-bearing: with heads chosen this
  way and overwritten, a window opened before the re-created incarnation's first
  initialisation and closed after it lowers the initialisation's heads below the
  straddler (`ReadPositionHonestAssumeCloseReplacesHeads`, red in `Recreate`).
- **Guarantees.** Four, each a checked invariant of the base and every variant, consumed
  by no other module: `ReadPositionHonest`, `SnapshotHonest`, `NoTrimPastAcked` and
  `OldNeverRejected`; two cost witnesses of the lazy arm (#5833), checked where their
  environment allows them, `PreArmCostWitness` and `ReclaimCostBound`; and one liveness property, checked on the base and in `Bulk`,
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
| `preArm` | The offsets appended under the `PreArm` class (#5833) | `WalAppendStamp.PreArm`, admitted by `WalShardGrain.AdmitStampLocked` with no entry check and no row. |
| `seenP` | The leaf-scoped watermark reported to each activation: its `foreignHigh` | The `ForeignHigh` the WAL shard folds into each stamped response's watermark (`WalShardGrain.ForeignHighLocked`), and the arming view it returns (`LeafAppendEpochFence.TryGetArmingView`). |
| `startHead`, `evStart`, `evFloor` | What the WAL shard's per-leaf tracking has lost: its start head, the last eviction of L's entry, and the eviction floor | `WalShardGrain.ResetLeafAuthors` on activation (the recovered head) and `WalShardGrain.FoldLeafAuthorsLocked` (the eviction floor, bounding memory at `MaxLeafAuthors`). |
| `gD`, `gL` | The shard root's durable routing generation, and the generation loaded at the partition | `ShardHandOffState.RoutingGeneration`, raised by the split or reclaim close; the WAL shard's loaded generation entry, raised only by a claim (`WalShardGrain.AdmitGenerationLocked`). |
| `pend`, `pendGen` | A deferred close head on L's marker at the partition, and the generation it re-claims | `HandOffMarkerCore.CloseDeferred`, read by `HandOffMarkerCore.TryGetPendingDeferred`. |
| `moved`, `otherMoved`, `dUsed` | Environment budgets: one key-moving close into L, one into another receiver, one stale donor write | Modelling devices. |
| `reclaims`, `strayReclaims` | Ghosts for `ReclaimCostBound` | Modelling devices. |
| `sHi`, `seenS` | The WAL shard's `staleHigh` for a pending key-moving close: the highest offset of the donor leaf from any token but the closing one, routed or epoch-only, under a generation below the one the close raises past; and the value reported to each activation (#5837, D2) | `WalShardGrain.StaleHighLocked`, kept beside the per-leaf tracking and reported on each response of the receiver while its deferred close is pending (`LeafAppendEpochFence.ReportedStaleHigh`). |
| `ft` | Each activation's fixed free-advance target | `LeafLiveAdvanceCore.FreeTarget`, the highest own applied offset, fixed before the marker view is read. |
| `sh`, `sibHint`, `sibRows` | A split's captured heads, the sibling's hint and the rows handed to it | `BPlusLeafGrain.CaptureSplitCutAsync`, one WAL-shard turn per partition returning the heads with the donor's bounds; the hint the sibling is seeded with (`SiblingInitialization.WalHeadsAtBirth`). |
| `tRecs` | The split transfer's records under the per-leaf class | `WalAppendStamp.Transfer`, appended by the receiver's `BPlusLeafGrain.MergeEntriesAsync`. |
| `dNarrowed`, `oblig`, `sDone`, `dKnown`, `dResumed`, `lRestart` | A split-born L's donor: its narrow and link obligation, its stragglers, the moved-key writes its closing activation knows, a resume after the heads and a restart after the close | `LeafNodeState.UnlinkedSplitSiblingId`, persisted with the narrow; the straggler sweep of `BPlusLeafGrain.CompleteSplitAsync`; `SplitInFlight` for a resume. |
| `lat`, `eagerG`, `repair`, `tried`, `birthAct` | The #5709 reachability latch on an unlinked split-born L: its decision, the gate's flip to eager, orphan repair on its range, and whether the running activation is the one its birth created | `LeafAppendEpochFence`'s reachability proof (`BPlusLeafGrain.ProveStampReachabilityCoreAsync`, `LeafAppendEpochFence.StopStampingUnreachable`); the birth proof in `BPlusLeafGrain.InitializeSiblingAsync`. |
| `nar`, `narWho`, `npend`, `dur`, `durWho`, `fObl`, `fTag`, `fBy`, `snapWho`, `fwdUsed` | The forward-exclusion relief (#5837): each activation's narrow in memory and whose it is, its pending persist, L's durable narrow and its obligation, each forward's narrow tag and the narrow that moved its key, and the span the snapshot reflects | `LeafNodeState`'s persisted span with `UnlinkedSplitSiblingId`; the narrow tag on `WalAppendStamp.ForwarderNarrow`; the reader's persisted tag on each stamped call (`LeafAppendEpochFence.PersistedNarrowTag`). |
| `svTaken`, `svOk`, `svHead` | The split's marker view (V3): taken, whether it read L's marker initialised and closed, and the close head it read | The marker read `BPlusLeafGrain.CompleteSplitAsync` makes after `BPlusLeafGrain.CaptureSplitCutAsync`, concurrently with the drain of the donor's own appends. |
| `chainUsed` | A forward chain through L's stale activation has been made (F2') | `WalAppendStamp.ForwarderChain`, the bounded list of (leaf, tag) forwarders the stamp carries. |
| `fInUsed`, `fInRecs`, `sfUsed` | The donor's pre-link forward into L, its offsets, and a stale D''s forward of L's key into another orphan | The forward tag on `WalAppendStamp.ForwarderNarrow`, read against the reader's split tag by `WalShardGrain`'s per-donor `staleHigh`. |
| `oWin`, `oHops` | An orphan sibling's marker and the forwards made into it | The orphan's `LeafHandOffState`; `BPlusLeafGrain.TryResolveSpanForwardTarget`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Create` | **Environment (B's `BulkHintBound`).** A bulk-born leaf's first activation under the switch's open marker, at any read position, holding every record at or below it | `ShardRootGrain.BeginBulkSwitchAsync` takes the hint (`ShardRootGrain.CaptureBulkBirthHeadsAsync`) with the open marker. The leaf is initialised without `SiblingInitialization.WalHeadsAtBirth` and seeds no birth pin; its read position arrives after the relocation, through `ShardRootGrain.FinishBulkSwitchAsync` calling `BPlusLeafGrain.SetCheckpointOffsetHintsAsync`. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_bulk_appended_leaf_arms_once_its_switch_has_closed` and `BulkAppendExistingRowsIntegrationTests.BulkAppend_appended_leaves_start_from_the_birth_hint_and_the_runs_switches_close_by_fence`. |
| `Claim(a)` | **Environment (E's `ClaimHeadCoversPriorAppends`, `RaisedEpochUnique`).** A claim is raised: the activation becomes current, its claim head is the allocator head, and the one it supersedes never allocates stamped again | E's three-step claim (`AppendEpochRefinement.md`); the core reads its outcome through `LeafAppendEpochFence.TryGetArmingView`, which answers only for a claim under the current epoch of an activation not stale. **Over-approximation:** each activation claims once, and any running activation not stale may claim in any order. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `WalShardGrainTests.The_claim_head_is_the_allocator_next_offset`. |
| `OwnAlloc(a)` | **Environment (E's `StaleEpochNeverAppends`).** The current activation's stamped append is allocated, and its response reports the unstamped watermark | `WalFencedAppendDispatch.AppendPointAsync` and `WalFencedAppendDispatch.AppendBatchAsync` pass the watermark in each stamped response to `LeafAppendEpochFence.OnStampedAppended`. | Yes: `LeafAppendEpochFenceTests.Stamped_responses_raise_the_reported_watermark`, `WalShardGrainTests.Unstamped_appends_are_always_admitted_and_reported` and `BPlusLeafGrainTests.A_reported_unstamped_append_holds_the_advance_until_a_replay_reads_past_it`. |
| `OwnAllocUnstamped(a)` | An own append made unstamped - a closed gate view, a claim fault or exhausted rounds, an unreachable leaf - by any running activation, always admitted, raising the watermark and reporting itself | `WalFencedAppendDispatch.AppendPointAsync` and `WalFencedAppendDispatch.AppendBatchAsync` call `LeafAppendEpochFence.OnUnstampedAppended` with the offsets an unstamped append returned. | Yes: `LeafAppendEpochFenceTests.Own_unstamped_appends_report_themselves`, `BPlusLeafGrainTests.A_write_through_an_existing_leaf_method_reports_itself_and_holds_the_advance` and `BPlusLeafGrainTests.A_descent_unreachable_leaf_never_claims_and_appends_unstamped_and_reported`. |
| `PreArmAlloc(a)` | An append of an activation that has not armed, under the `PreArm` class: no entry check and no row, admitted on its routing generation, raising only L's leaf-scoped tracking, and its response reporting the shared watermark | `WalShardGrain.AdmitStampLocked` (the `PreArm` branch to `WalShardGrain.AdmitGenerationLocked`); the tracking is `WalShardGrain.NoteLeafAuthorLocked`, run in the allocation step. | Yes: `WalShardGrainTests.A_pre_arm_append_is_admitted_with_no_entry_and_writes_no_fence_row` and `WalShardGrainTests.A_routed_pre_arm_append_never_raises_the_shared_watermark_and_an_unrouted_one_does`. |
| `Resolve(a)` | An armed activation whose marker gained a deferred close head re-arms: it re-claims the deferred generation, or takes the loaded one when a later raise is already re-claimed (C5), and records the allocator head as a close head | `WalShardGrain.ResolveDeferredGenerationAsync` from `WalShardGrain.ArmAppendEpochAsync`, then `HandOffMarkerCore.ResolveDeferred`. | Yes: `WalShardGrainTests.An_arm_resolves_a_deferred_generation_so_a_write_admitted_before_it_is_refused` and `WalShardGrainTests.An_arm_resolving_a_deferred_generation_below_a_loaded_one_takes_the_current_head`. |
| `Land(o)` | The append becomes durable; an own append is applied, recorded by the tracker and acknowledged | The append site applies, then `BPlusLeafGrain.MarkLiveAppendApplied` releases the count and records the offset in `BPlusLeafGrain._liveOwnHigh`; an apply that throws leaves the count held, which turns the advance off for the activation. | Yes: `BPlusLeafGrainTests.A_live_advance_waits_until_every_issued_append_is_applied` and `BPlusLeafGrainTests.A_failed_append_turns_the_live_advance_off_for_the_activation`. |
| `ForeignOpen` | **Environment (H).** A hand-off opens L's marker before its first admission. Its only mutation is a `TypeOK` bound (`TypeOkForeignWindowPastBound`): opening only enables `ForeignAlloc`, whose role `ReadPositionHonestAssumeForeignAfterClose` covers | `HandOffMarkerCore.Open`, on the leaf's marker or the shard's (`ShardRootGrain.OpenHandOffMarkerAsync`). | Yes: `HandOffMarkerCoreTests.An_open_is_idempotent_and_holds_only_overlapping_owners` and `BPlusLeafGrainTests.An_open_leaf_marker_holds_the_advance`. |
| `ForeignAlloc` | **Environment (H's `HandOffBound`).** A foreign append is allocated while L's marker is open, or while it is not initialised, where `HandOffBound` constrains nothing | A hand-off initiator's stamped append, admitted at the WAL shard under its epoch or routing generation (`WalShardGrain.AppendStampedAsync`). | Yes: `BPlusLeafGrainTests.An_open_shard_marker_token_holds_only_the_leaves_it_overlaps` and `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump`. |
| `Close` | **Environment (H's `HandOffBound` and `CloseHeadMonotone`).** The close fences and records a head, above every foreign record once the marker is initialised, max-merged into `closedAt` | `HandOffMarkerCore.Close` with the heads the initiator's re-claim returned (`ShardRootGrain.CloseHandOffMarkerAsync`); `HandOffMarkerCore.RequiredHead` takes the maximum over every retained close and the compacted heads. | Yes: `HandOffMarkerCoreTests.A_close_records_the_fence_heads_over_the_hand_off_range_and_never_reopens`, `HandOffMarkerCoreTests.A_later_close_with_lower_heads_never_lowers_the_required_heads` and `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it`. |
| `HandOff(o, a)` | **Environment.** A foreign record is delivered to an activation, which acknowledges it; it may never happen | The relocation of a hand-off's rows into the owner (a split's transfer, a reclaim's absorb, a bulk graft), which the core sees only as rows already in its projection. **Over-approximation:** any running activation may receive any durable foreign record, at any time. | Yes: `LeafHandOffFenceIntegrationTests.Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump` and `LeafHandOffFenceIntegrationTests.Every_reclaim_opens_its_marker_on_the_survivor_and_closes_it_after_retiring_the_absorbed_leaf`. |
| `OldAlloc` | A 10.2.0 silo's unstamped append (or any path the census classes unstamped), always admitted, raising the unstamped watermark | `WalShardGrain.AppendAsync`, whose unstamped shapes have no rejection path (the fence never rejects one), raising `WalShardGrain._unstampedWatermark`. | Yes: `WalShardGrainTests.Unstamped_appends_are_always_admitted_and_reported`, `WalAppendFenceMixedVersionTests.The_existing_unstamped_methods_keep_working_on_the_older_build` and `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`. |
| `Replay(a)` | Read and apply the next durable entry, never a trimmed one | `BPlusLeafGrain.ReplayPartitionAsync`, the replay-based advance the core falls back to (`WalDurability`'s `ReadStep`). | Yes: `BPlusLeafGrainTests.A_leaf_that_has_not_read_to_its_claim_head_advances_only_by_replay` and `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`. |
| `LiveAdvance(a)` | An armed activation advances its read position to its highest own applied offset without reading | `BPlusLeafGrain.TryAdvanceLiveCheckpointAsync`, deciding with `LeafLiveAdvanceCore.TargetFor`, whose arming is `Armed(a)` conjunct for conjunct: `claimRaised` and `claimHead` from `LeafAppendEpochFence.TryGetArmingView`, `handOffClear` (`mInit` and `~hoOpen`) and `closeHead` from `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync`, `unstampedSeen`, and the tracker through `LeafLiveAdvanceCore.IsQuiescent`. Production holds the advance in more cases than `Armed(a)` (see the gaps). | Yes: `LeafLiveAdvanceCoreTests.The_advance_is_taken_exactly_when_the_leaf_is_armed`, `LeafLiveAdvanceCoreTests.An_armed_leaf_advances_to_its_highest_own_offset`, `BPlusLeafGrainTests.A_live_written_leaf_of_a_new_tree_advances_its_checkpoint_without_a_replay` and `BPlusLeafGrainTests.Each_partition_advances_to_its_own_highest_offset`. |
| `Capture(a)` | The shared per-leaf snapshot keeps the highest coverage | `BPlusLeafGrain.CaptureSnapshotCoreAsync`, after the coverage-lag tick banks a live advance (`BPlusLeafGrain.OnCoverageLagTimerTickAsync`); the store never lowers coverage (`LeafSnapshotStorageGrain.MergeMonotone`). | Yes: `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`, `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay` and `LeafSnapshotStorageGrainTests.SaveAsync_still_merges_a_regressing_capture_that_carries_every_stored_key`. |
| `GcTrim` | Trim the next durable entry below the snapshot's coverage | `LatticeWalGc.TrimShardAsync`, bounded by the leaf's pin (`WalDurability`'s `GcTrim`). **Over-approximation:** the model trims to the shared snapshot's coverage alone. | Yes: `LeafDurablePinCoreTests.A_covered_partition_is_entitled_to_the_lower_of_its_persisted_checkpoint_and_coverage` and `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay`. |
| `Duplicate` | A second activation of L, or the next one, starts from the shared snapshot and must claim before it appends stamped | `BPlusLeafGrain.ExecuteActivationReplayAsync` rehydrates from the snapshot (`BPlusLeafGrain.TryRehydrateFromSnapshotAsync`); its own `LeafAppendEpochFence` claims afresh. **Environment:** once in the model. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `BPlusLeafGrainTests.Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint`. |
| `Restart` | A WAL shard restart, a reactivation or a move's cutover, resets the in-memory unstamped watermark to the recovered allocator head minus one | `WalShardGrain.OnActivateAsync` sets `WalShardGrain._unstampedWatermark` from the recovered `WalShardGrain._nextOffset`. **Environment:** once in the model. | Yes: `WalShardGrainTests.The_unstamped_watermark_restarts_at_the_recovered_head`. |
| `Evict` | **Environment.** The WAL shard evicts L's foreign-high entry to bound memory, raising its eviction floor to the entry's highest offset | `WalShardGrain.FoldLeafAuthorsLocked`. | Yes: `WalShardGrainTests.The_foreign_high_resets_below_the_recovered_head_and_never_drops_on_a_fold`. |
| `KeyMove` | A split or consolidation hands keys to L: the shard root, in its own turn as it links the split (C6), raises its routing generation with no WAL claim, and closes L's marker with a deferred head | `ShardRootGrain.CloseDeferredSplitHandOffsAsync` from `ShardRootGrain.LinkSplitAsync`, and the pre-arm branch of `ShardRootGrain.CloseReclaimHandOffAsync`, writing `HandOffMarkerCore.CloseDeferred`. | Yes: `HandOffMarkerCoreDeferredTests` and `WalShardGrainTests.An_arm_resolves_a_deferred_generation_so_a_write_admitted_before_it_is_refused`. |
| `OtherReceiverArm` | **Environment (C5).** A later key-moving close into another receiver, whose arm re-claims that later generation at the partition first | Another receiver's `WalShardGrain.ArmAppendEpochAsync`. | Yes: `WalShardGrainTests.An_arm_resolving_a_deferred_generation_below_a_loaded_one_takes_the_current_head`. |
| `StaleRouteAlloc` | **Environment.** A stale donor activation still routes L's keys to itself and appends one under the old generation as `PreArm`: admitted only while the loaded generation is not above it | `WalShardGrain.AdmitGenerationLocked`. | Yes: `WalShardGrainTests.An_arm_resolves_a_deferred_generation_so_a_write_admitted_before_it_is_refused`. |
| `UnroutedAlloc` | **Environment (C4).** The stale donor's write with no routing stamp, appended truly unstamped, raising the shared watermark | `WalFencedAppendDispatch` takes the unstamped method for a `PreArm` write with no routing (`WalShardGrain.RaisesUnstampedWatermark` defends it at the WAL). | Yes: `WalShardGrainTests.A_routed_pre_arm_append_never_raises_the_shared_watermark_and_an_unrouted_one_does`. |
| `Recreate` | **Environment (H's `AbsentMarkerHolds`).** L's tree is purged and re-created: its marker is deleted and so not initialised, and the purge retires every raised epoch, so no activation is current; one that never claimed may claim afterwards | The leaf's clear deletes its marker row (`BPlusLeafGrain.ClearHandOffMarkerAsync`, `HandOffMarkerRows.ClearLeafAsync`), and the re-seeded shard root retires a predating marker (`ShardRootGrain.RetirePredatingHandOffMarkerAsync`); the epoch retirement is E's. | Yes: `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`, `TreeFenceRowPurgeIntegrationTests.A_tree_re_created_after_a_purge_that_retired_nothing_never_adopts_its_markers` and `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag`. |
| `StraddlerAlloc` | **Environment (R's `StraddlerBelowInit`).** A write a shard root of the purged incarnation admitted is allocated, only before the re-created incarnation's first initialisation | The WAL refuses a straddler at or below its initiator's routing floor (`WalShardGrain._generationFloors`), and its seam retry never re-stamps it (`ShardRootGrain.ThrowIfAdmittedUnderPurgedIncarnationAsync`). | Yes: `TreeFenceRowPurgeIntegrationTests.A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it` and `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`. |
| `MarkInit` | **Environment (R's `InitHeadCoversPriorAllocations` and H).** The re-created incarnation's first initialisation claims its seeded generation everywhere, records heads above every earlier allocation as a close, max-merged into `closedAt` like any other, and initialises the marker state | `ShardRootGrain.FenceRecreatedIncarnationAsync`, then `ShardRootGrain.TryInitialiseHandOffMarkerAsync`; the leaf adopts the shard's flag in `BPlusLeafGrain.ReadLiveAdvanceHandOffAsync`. | Yes: `TreeFenceRowPurgeIntegrationTests.A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it` and `BPlusLeafGrainTests.An_uninitialised_leaf_marker_holds_until_the_leaf_adopts_its_shards_flag`. |
| `FreeTarget(a)` | The claimless live advance (#5837) fixes its target: the highest own applied append | `LeafLiveAdvanceCore.FreeTarget`. | Yes: `LeafLiveAdvanceCoreTests.FreeAdvanceViewBeforeHeads`. |
| `FreeCommit(a)` | After the target, the advance reads L's marker view and moves the read position to the target, with no claim, iff the marker is initialised with no window open, and the read position is at or above the close heads, the reported `foreignHigh`, the shared watermark and, while a deferred close is pending, the reported `staleHigh`, with every own append at or below the target applied | `LeafLiveAdvanceCore.TryFreeAdvance` from `BPlusLeafGrain.TryAdvanceLiveCheckpointAsync`. | Yes: `LeafLiveAdvanceCoreTests.FreeAdvanceIgnoresForeignHigh` and `LeafLiveAdvanceCoreTests.FreeAdvanceIgnoresCloseHeads` and `LeafLiveAdvanceCoreTests.FreeAdvancePassesPendingDeferredHead`. |
| `SplitHeads(a)` | A split captures each partition's allocator head with the donor's `foreignHigh`, shared watermark and `staleHigh`, in one WAL-shard turn (D4) | `BPlusLeafGrain.CaptureSplitCutAsync`. | Yes: `LeafLiveAdvanceCoreTests.SplitHintBoundsReadBeforeHeads`. |
| `SplitView(a)` | After the heads, the split reads L's marker view, possibly while the drain of its own appends is still applying them (V3); the commit evaluates the guard after the drain | The marker read in `BPlusLeafGrain.CompleteSplitAsync`, after `BPlusLeafGrain.CaptureSplitCutAsync`. | Yes: `LeafLiveAdvanceCoreTests.SplitViewBeforeHeads`. |
| `SplitCommit(a)` | After the cut, the donor reads its marker view and, under the advance's guard with the heads in place of the target, hands the sibling the heads minus one as its hint | `BPlusLeafGrain.CompleteSplitAsync`, advance before the cap (`ShardRootGrain.CapAtReadFrontier` otherwise). | Yes: `LeafLiveAdvanceCoreTests.SplitHintWithoutBound` and `LeafLiveAdvanceCoreTests.SplitHintAboveHeads`. |
| `DonorAlloc` | **Environment.** The live donor admits a write for L's keys after the split's heads and before its narrow, attributed to the donor | `BPlusLeafGrain.SetStampedAsync` on the donor. | Yes: `LeafLiveAdvanceCoreTests.DonorCloseHeadAtHeads`. |
| `DonorNarrow` | The donor narrows its span, persisting its link obligation in the same write (transfer condition (a)) | `BPlusLeafGrain.CompleteSplitAsync`, the narrow's persist with `LeafNodeState.UnlinkedSplitSiblingId`. | Yes: `LeafLiveAdvanceCoreTests.NarrowPersistedWithoutObligation`. |
| `DonorClose` | After the narrow and the drain, the donor closes L's birth window, in one conditional marker write, with the head one above its moved-key writes since the heads (all tokens', on a resumed split) and the deferred bound (rule S) | `BPlusLeafGrain.CloseSplitHandOffAsync`, writing `HandOffMarkerCore.CloseWithDeferredBound`. | Yes: `LeafLiveAdvanceCoreTests.DonorCloseBeforeDrain` and `LeafLiveAdvanceCoreTests.DonorCloseWithoutDeferredBound` and `LeafLiveAdvanceCoreTests.ResumedCloseUsesOnlyOwnHigh`. |
| `DonorRestart` | **Environment.** The donor restarts after taking the heads and a new activation resumes the split from `SplitInFlight` | `BPlusLeafGrain.CompleteRecoverySplitUnderGateAsync`. | Yes: `LeafLiveAdvanceCoreTests.ResumedCloseUsesOnlyOwnHigh`. |
| `LinkRestart` | **Environment.** The donor restarts after closing L and before the link; the link is re-driven from the durable obligation (transfer condition (b), L3) | `BPlusLeafGrain.CompleteRecoverySplitUnderGateAsync` from `LeafNodeState.UnlinkedSplitSiblingId`. | Yes: `LeafLiveAdvanceCoreTests.ObligationNotRedriven`. |
| `Straggler(a)` | The donor's writes since the heads reach L as an append of L's own activation, applied in the same call, under the per-leaf transfer class: in its tracker and its token's `foreignHigh`, never a claim, never the shared watermark (T1-T3) | `BPlusLeafGrain.MergeEntriesAsync` under the routing-mutation gate, checking its current span and stamping the routing generation it was established under. | Yes: `LeafLiveAdvanceCoreTests.TransferNotInForeignHigh` and `LeafLiveAdvanceCoreTests.TransferRaisesSharedWatermark`. |
| `OrphanForward` | **Environment.** A stale duplicate forwards an acknowledged write to an orphan sibling; the record is attributed to the first forwarder under the empty token while the orphan's marker holds the token open or the donor's close pending (condition 4, F1-F2) | `BPlusLeafGrain.TryResolveSpanForwardTarget` to `BPlusLeafGrain.SetStampedAsync` with `WalAppendStamp.Forwarder`; `WalShardGrain.NoteLeafAuthorLocked` under the forwarder and `Guid.Empty`. | Yes: `LeafLiveAdvanceCoreTests.ForwardNotAttributedToForwarder` and `LeafLiveAdvanceCoreTests.ReforwardAttributesImmediateForwarder` and `LeafLiveAdvanceCoreTests.ForwardScopeEndsAtWindowClose`. |
| `OrphanClose` | **Environment.** The orphan's donor closes its window, leaving its deferred close pending | `BPlusLeafGrain.CloseSplitHandOffAsync` on the stale duplicate. | Yes: `LeafLiveAdvanceCoreTests.ForwardScopeEndsAtWindowClose`. |
| `ProveReach` | An unlinked L's reachability proof after its donor's close: proven once linked; left undecided, and retried, while the donor's close is pending, the gate is in pre-arm and no orphan repair has begun; latched otherwise | `BPlusLeafGrain.ProveStampReachabilityCoreAsync`, deferring through `LeafAppendEpochFence.OnReachabilityUndecided`. | Yes: `LeafLiveAdvanceCoreTests.PendingDonorCloseMarksProven` and `LeafLiveAdvanceCoreTests.LatchUnreachableWithPendingDonorClose`. |
| `GateToEager` | **Environment.** The gate's view flips from pre-arm to eager | `ClusterManifestWalAppendFenceGate.IsPreArmOpen`. | Yes: `LeafLiveAdvanceCoreTests.PendingDonorCloseMarksProven`. |
| `StartRepair` | **Environment.** Orphan repair begins on L's range; L's next proof latches | `ShardRootGrain.OrphanRepairInProgress`. | Yes: `LeafLiveAdvanceCoreTests.PendingExemptionOverridesOrphanRepair`. |
| `Reactivate` | **Environment.** L's birth activation deactivates, and a later call - a re-init, a recovery re-call, a stale donor's late `InitializeSiblingAsync` - activates it again, unproven (P1, P3) | `BPlusLeafGrain.InitializeSiblingAsync`, proving only on the call that seeds the write-once slots. | Yes: `LeafLiveAdvanceCoreTests.BirthProofOnReinit`. |
| `NarrowMem(a)` | An activation of L narrows its span in memory for a split, stamping the narrow's identity (the sibling id) as its tag | `BPlusLeafGrain.CompleteSplitAsync`, the in-memory narrow. | Yes: `LeafLiveAdvanceCoreTests.ForwardExclusionByInMemorySpan`. |
| `PersistNarrow(a)` | It persists the narrow with its link obligation in one conditional write; the first to persist wins, the other keeps only its in-memory narrow | `BPlusLeafGrain.CompleteSplitAsync`, the narrow's persist with `LeafNodeState.UnlinkedSplitSiblingId`. | Yes: `LeafLiveAdvanceCoreTests.ForwardExcludedWithoutObligation` and `LeafLiveAdvanceCoreTests.ForwardExclusionTagCollision`. |
| `EvictTag` | **Environment.** A third narrow tag evicts one from the WAL shard's per-(leaf, partition) top-two forward table, and the evicted tag's forward high folds into the floor every reader counts (X6) | `WalShardGrain.ForwardHighLocked`, its fold into `WalShardGrain.FoldLeafAuthorsLocked`'s eviction floor. | Yes: `LeafLiveAdvanceCoreTests.ForwardTagEvictionDropsHigh`. |
| `ReforwardChain(h)` | L's stale activation forwards one of L's keys on, and the chain runs h hops: the stamp carries every forwarder up to k, each attributed under the empty token with its own hop's tag, and a chain past k goes unstamped (F2', C4) | `WalAppendStamp.ForwarderChain` built by `BPlusLeafGrain.TryResolveSpanForwardTarget`; `WalShardGrain`'s attribution to each forwarder. Production never excludes a chain hop from a reader's `foreignHigh` (S-b): the per-hop X1 exclusion is not taken, at a cost only. | Yes: `LeafLiveAdvanceCoreTests.ChainHopsAreNeverExcluded`, `LeafLiveAdvanceCoreTests.ReforwardOmitsImmediateForwarder`, `LeafLiveAdvanceCoreTests.ReforwardOmitsIntermediateForwarder` and `LeafLiveAdvanceCoreTests.ReforwardChainOverflowStamped`. |
| `ForwardIntoL(a)` | Before the link, L's donor forwards a new write of L's keys into L: L appends and applies it as its own, and the WAL attributes it to the donor under the empty token with the tag of the split that created L, which L's `staleHigh` excludes | `WalShardGrain`'s per-donor `staleHigh` read, excluding forwards tagged with the reader's own split. | Yes: `LeafLiveAdvanceCoreTests.StaleHighCountsReceiverTag`. |
| `StaleForwardOther` | A stale D' forwards one of L's keys into another orphan under that orphan's split tag; L's `staleHigh` counts it | The same read: only the reader's own split tag is excluded. | Yes: `LeafLiveAdvanceCoreTests.StaleHighExcludesOtherTag`. |
| `ForwardMoved(a)` | It forwards a new acknowledged write of a moved key, attributed to L under the empty token and its narrow's tag, before or after its persist; a reader excludes it only when its own persisted span is that narrow (X1-X4) | `BPlusLeafGrain.TryResolveSpanForwardTarget`; `WalShardGrain.ForwardHighLocked` per tag, excluded in `WalShardGrain.ForeignHighLocked`. | Yes: `LeafLiveAdvanceCoreTests.ForwardExclusionByForwarderSpan` and `LeafLiveAdvanceCoreTests.ForwardExclusionTagCollision`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `ReadPositionHonest` | No running activation's checkpoint passes a durable entry it owns and does not hold, acknowledged or not, whoever appended it: a duplicate, a foreign author, a straddler, a 10.2.0 silo or itself. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it`, `LiveCheckpointAdvanceIntegrationTests.A_foreign_unstamped_append_reported_after_arming_is_read_by_replay`, `LiveCheckpointAdvanceIntegrationTests.An_orphans_append_is_unstamped_so_the_reachable_owner_reads_it_before_it_advances`, `BPlusLeafGrainTests.A_close_head_above_the_checkpoint_holds_the_advance_until_a_replay_reaches_it` and `LeafLiveAdvanceCoreTests.The_advance_is_taken_exactly_when_the_leaf_is_armed`. |
| `SnapshotHonest` | The shared snapshot never claims coverage of a durable entry it lacks: the live advance never passes an own append still in flight. | Yes: `BPlusLeafGrainTests.A_live_advance_waits_until_every_issued_append_is_applied`, `BPlusLeafGrainTests.A_failed_append_turns_the_live_advance_off_for_the_activation` and `LeafLiveAdvanceCoreTests.Each_quiescence_condition_holds_the_advance`. |
| `NoTrimPastAcked` | The GC never trims an acknowledged append the leaf's snapshot lacks, after a live advance as after a replay. | Yes: `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`, `LiveCheckpointAdvanceIntegrationTests.A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it` and `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay`. |
| `NewbornPinEventuallyUsable` | A live-written leaf's own applied appends are eventually covered by its banked snapshot, so its WAL pin becomes usable without a replay from `-1`: the leaf arms, advances live and the coverage-lag tick captures. | Yes: `BPlusLeafGrainTests.The_coverage_lag_tick_makes_a_live_written_leafs_pin_usable_without_a_replay` and `LiveCheckpointAdvanceIntegrationTests.A_live_written_leaf_banks_its_snapshot_without_a_replay_drive`. |
| `PreArmCostWitness` | A `PreArm` append never raises the partition's shared unstamped watermark, so no activation is told to replay past its own `PreArm` appends (#5833's hard requirement). | Yes: `WalShardGrainTests.A_routed_pre_arm_append_never_raises_the_shared_watermark_and_an_unrouted_one_does`. |
| `ReclaimCostBound` | A key-moving close writes no WAL generation claim, an append never claims a higher generation it carries (C1), and an arm's deferred re-claim writes the generation row at most once per raise and partition. | Yes: `WalShardGrainTests.A_stamped_append_under_an_unloaded_generation_loads_it_and_is_admitted` and `WalAppendFenceFirstTouchCostIntegrationTests`. |
| `SiblingHintHonest` | A split's sibling is handed every durable record at or below its hint (#5752 for splits). | Yes: `LeafLiveAdvanceCoreTests.SplitHintWithoutBound`. |
| `TransferCostWitness` | A split transfer never raises the partition's shared watermark (#5837's 976 declines). | Yes: `LeafLiveAdvanceCoreTests.TransferRaisesSharedWatermark`. |
| `DeferredHeadCostWitness` | A split-born receiver's close heads never exceed the donor's moved-key high and the stale donors' `staleHigh`, plus one (#5837's 792 declines). | Yes: `LeafLiveAdvanceCoreTests.ResolveAtPartitionHead`. |
| `SoleActivationNeverFallsOff` | A sole running activation never falls off the log: the widened liveness excuse covers duplicates only. | Yes: `LeafLiveAdvanceCoreTests.GcTrimPastSolePin`. |
| `NarrowObligated` | A donor's narrow is durable only with its link obligation. | Yes: `LeafLiveAdvanceCoreTests.NarrowPersistedWithoutObligation`. |
| `UnlinkedNeverClaims` | An unlinked L takes no epoch, except its birth activation under the eager gate. | Yes: `LeafLiveAdvanceCoreTests.PendingDonorCloseMarksProven` and `LeafLiveAdvanceCoreTests.ExemptLeafArmsUnreachable` and `LeafLiveAdvanceCoreTests.BirthProofOnReinit`. |
| `RepairLatches` | Orphan repair always latches L. | Yes: `LeafLiveAdvanceCoreTests.PendingExemptionOverridesOrphanRepair`. |
| `PendingCloseNotLatched` | An unlinked L with its donor's close pending, under pre-arm and with no orphan repair, is never latched. | Yes: `LeafLiveAdvanceCoreTests.LatchUnreachableWithPendingDonorClose`. |
| `ForwardHonest` | No running activation passes a forward of a key L owns, or of a key its own span still holds, without holding it (#5837's forward-exclusion relief). | Yes: `LeafLiveAdvanceCoreTests.ForwardExclusionByForwarderSpan` and `LeafLiveAdvanceCoreTests.ForwardExclusionByInMemorySpan` and `LeafLiveAdvanceCoreTests.ForwardExclusionTagCollision` and `LeafLiveAdvanceCoreTests.ForwardExcludedWithoutObligation`. |
| `StaleHighExcludesReceiverTag` | A split-born receiver's `staleHigh` never counts its donor's forwards into itself, which it applied as its own, so a receiver that took a pre-link forward is not sent to replay past its own writes (cost, a5e2e2ef's ruling with F2'). | Yes: `LeafLiveAdvanceCoreTests.StaleHighCountsReceiverTag`. |
| `SnapshotForwardHonest` | The shared snapshot never covers such a forward without holding it. | Yes: `LeafLiveAdvanceCoreTests.ForwardExclusionByForwarderSpan`. |
| `SplitEventuallyLinked` | A split-born L is eventually linked. | Yes: `LeafLiveAdvanceCoreTests.ObligationNotRedriven`. |
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

**The lazy arm (#5833).** `WithPreArm` turns it on. A leaf claims at a partition only
when it arms there. Its appends before that are `PreArm` (`PreArmAlloc`), and the arm
replays to `foreignHigh` (`seenP`) rather than to the claim head. `foreignHigh` is the
highest offset any other activation of L allocated, stamped or `PreArm`, never below what
the WAL shard's memory lost (`startHead - 1`, the eviction floor). Key-moving closes defer
their fence to the receiver's arm (`WithKeyMove`).

The base, `KeyMove`, `PreArmRestart`, `PreArmBulk`, `PreArmRecreate` and `PreArmGateFlip`
check the lazy arm; the other six configurations check today's eager arm
(`WithPreArm = FALSE`, the path the gate keeps until every silo carries the capability).
Mutations that test an eager-only assumption are bound to it for the same reason.

**The claimless advance (#5837).** `WithFreeAdvance`, under the lazy arm, adds
`FreeTarget` and `FreeCommit`: the advance needs no claim, only the bounds the responses
to the leaf's own appends report. `WithSplitHint` adds the split's hint, `WithSplitBirth`
a split-born receiver with its live donor, `WithDonorRestart` the donor's restarts,
`WithLatch` the reachability latch on the unlinked receiver and `WithBirthProof` its
birth proof, and `WithOrphanForward` a stale duplicate's forward into an orphan.
`WithFwdExclusion` adds the relief that lets a donor exclude its own forwards once its
narrow is durable: `ReadPositionHonest`, `SnapshotHonest` and `NoTrimPastAcked` then skip
only the forwards of keys L no longer owns, which `ForwardHonest` and
`SnapshotForwardHonest` state. `WithDuplicate` and `WithEvict` exist to slice. Fifteen variants check it, each within the
case budget:

| Variant | Adds | States |
|---------|------|--------|
| `Free` | the advance on the base, safety only | 4,680,662 |
| `FreeKeyMove` | a pending key-moving close; liveness with no claim fair | 2,386,428 |
| `FreeSplitHint` | the split's hint | 3,562,343 |
| `FreeSplitBirth` | a split-born receiver and a duplicate, no link; liveness | 2,146,154 |
| `FreeSplitLink` | the link and both donor restarts, no duplicate; liveness and `SplitEventuallyLinked` | 428,890 |
| `FreeLatch` | the latch, the gate flip, orphan repair and the birth proof | 892,259 |
| `FreeOrphan` | the orphan's forward and re-forward | 707,726 |
| `FreeRestart` | a WAL shard restart and an eviction | 3,855,129 |
| `FreeRestartKeyMove` | a deferred close pending across a WAL shard restart | 477,300 |
| `FreeRecreate` | the purge, claimed slice | 1,715,417 |
| `FreeRecreateSurvivor` | the purge, unclaimed slice | 4,908,025 |
| `FreeBulk` | a bulk birth | 3,309,965 |
| `FreeGateFlip` | own unstamped appends | 1,904,304 |
| `FreeFwdExclusion` | the forward-exclusion relief: two divergent narrows, a forward before or after the persist | 4,217,142 |
| `FreeTagEvict` | the relief's WAL-side table: a tag's eviction (X6) and a WAL shard restart (X8) | 821,800 |
| `FreeReforward` | a forward chain at depths 2, 3 and 4 against k = 3 (F2') | PENDING_REFORWARD |
| `FreeReforwardRestart` | the chain with a WAL shard restart and an eviction: the folds of the orphan's and the chain's forwards (S-a) | PENDING_REFORWARDRESTART |
| `FreeForwardIn` | the donor's pre-link forward into L and a stale D''s into another orphan (the `staleHigh` tag exclusion) | PENDING_FORWARDIN |
| `FreeSplitView` | the split's view read concurrently with the drain (V3), with a hand-off window | PENDING_SPLITVIEW |

Under the claimless advance an activation learns its bounds only from the responses to
its own appends, which carry `foreignHigh` and the shared watermark together, so either
restart floor alone suffices: `ReadPositionHonestPreArmWatermarkNotReset` and
`ReadPositionHonestRestartForgetsUnstamped` are clean under it, and
`ReadPositionHonestFreeAdvanceRestartForgetsBothFloors` is red. With no claim there may be
no current activation, so `CurrentFellOffLog` excuses any running one, as production's
per-activation #3477 latch does; `SoleActivationNeverFallsOff` bounds that to duplicates.

The lazy arm makes two of the eager arm's guards redundant for `ReadPositionHonest`, and
their mutations stay clean under `WithPreArm` (they are red under `WithPreArm = FALSE`):
- `ReadPositionHonestAssumeSupersededAllocates`: `foreignHigh` counts a superseded
  activation's stamped appends, so E's "a superseded activation never allocates" is no
  longer what keeps them read.
- `ReadPositionHonestRestartForgetsUnstamped`: `foreignHigh`'s restart floor covers the
  offsets the shared watermark's reset covers.

The liveness property is unchanged in form. Under the lazy arm a claim's fairness is
conditioned on `ArmTrigger(a)`, an own durable append the shared snapshot does not cover,
which is the floor-holding pin that arms a leaf in production. So it now says the pin
becomes usable once the leaf arms on demand, not that it arms at its first write.
`NewbornPinEventuallyUsableDeferredHeadRefusedBelowLoaded` shows C5 is needed for it.

Building the lazy arm found that a bulk-born leaf's pre-switch records above its birth
read position were covered only by the claim-head replay: under the lazy arm they rest on
the switch's close head, now B's guarantee `SwitchCloseCoversPriorAllocations`.

**The purge, in three slices.** Since the environment admits a never-claimed activation
that outlives the purge, the purge alone (9,028,464 states) is over the 5M-state case
budget. Two constants select the purge's shape, decided at the step itself, so the slices
split the purged behaviours exactly: `WithClaimedPurge` for a purge that finds `a1`
claimed (with only the duplicate, if any, unclaimed), `WithUnclaimedSurvivor` for one
that finds `a1` unclaimed. The base and the other variants allow both.
- `Recreate` checks the claimed purge with the full environment: 3,693,715 states.
- `RecreateSurvivor` checks the unclaimed survivor without the 10.2.0 silo: 4,638,713.
- `RecreateSurvivorMixed` checks it with the silo and no hand-off window: 1,622,161.
- **Bounded out of CI:** the unclaimed survivor with both a hand-off window and the silo.
  It was checked locally and is clean at 8,913,193 distinct states (232 seconds), which is
  over the 5M-state case budget; the gap register below records it.

Measured with tla2tools v1.7.4 on two workers, the three slices take 2 minutes, 3
minutes and 1 minute.

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
  778,329 distinct states to depth 25 in 1 minute 27 seconds, and `Bulk` 1,114,185 to
  depth 25 in 2 minutes 38 seconds (re-measured with TLC 2.19). Stated as three
  leads-to formulas the base took 4 minutes, and with
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
module's base and its four variants as member 6a landed them it is clean, because an activation that has not claimed has only
unstamped own appends, each reporting itself, so it never arms past one. It is kept, as the
design keeps it, because arming without a claim is meaningless, and it is not in the
mutation directory, which holds only experiments that must go red.

`ReadPositionHonestAssumePurgeSparesClaimed` is a redundancy check too: with the purge
leaving the current activation's epoch raised (`Recreate` keeping `cur`), the `Recreate`
slice stays clean. The core's properties do not tell incarnations apart: the purged
activation's later appends are its own, applied by it and below every later claim head,
so retiring the purged epoch protects the re-created tree's identity, which E and R
check, not any read position here. It is kept out of the mutation directory for the same
reason as the one above.

## Deliberate abstraction gaps

- **The claimless advance's tracking is one entry per role (#5837).** One donor leaf, one
  key-moving close and one stale donor stand for the WAL shard's per-donor `staleHigh`
  table (top two tokens per generation, with the restart and eviction floors), and one
  orphan for every forward chain. The forwarder attribution needs a WAL-shard capability
  that honours the stamp's forwarder field; without it a pre-link forward falls back to
  unstamped (F3), which the model does not represent. The read of `staleHigh + 1` in the
  re-claim's own WAL turn is one step here.
  Production symbols: `WalShardGrain.StaleHighLocked`, `WalShardGrain.NoteLeafAuthorLocked`.
- **The forward-exclusion relief's table (#5837, X5-X8).** One tag eviction and one WAL
  shard restart stand for the top-two per-tag forward table's evictions and losses. Two
  rows are refinement obligations the model does not represent: the reader's tag comes
  only from a narrow whose persist succeeded (X7), and a WAL shard restart forgets the
  table into the start-head floor (X8), which the model checks only through `Restart`'s
  floors (`ForwardHonestForwardTableLostOnRestart`). The model found that evicting a
  leaf's foreign-high entry must fold its forwards' high, the empty token's, with its
  activations' (`ForwardHonestEvictionDropsForwardHigh`).
  Production symbols: `WalShardGrain.FoldLeafAuthorsLocked`, `WalShardGrain.ResetLeafAuthors`.
- **The forward relief's assumptions and limits (#5837; a5e2e2ef's S-b, N-a to N-d).**
  - *Chain hops are never excluded (S-b).* F2' allows each hop's reader to exclude the
    records of the narrow its persisted span is (X1, per hop). The model counts every
    chain record in L's `foreignHigh` (`ChainFwd`), and production does the same: a
    chain hop's attribution is never excluded, which costs at most a replay past a
    record the reader already applied. `LeafLiveAdvanceCoreTests.ChainHopsAreNeverExcluded`
    pins it, so a later exclusion must come back to the model first.
  - *Every tag folds on a tag eviction (N-c).* `EvictTag` folds every tag's forward high
    into the floor every reader counts, not only the evicted tag's; production's table
    folds only the evicted one, which is a subset of what the model folds, so the
    model's fold over-approximates the floor.
  - *The excluded forwards rest on the sibling pin (N-b).* `ReadPositionHonest`,
    `SnapshotHonest` and `NoTrimPastAcked` skip a donor's forward of a key L no longer
    owns. That is sound only because the record is applied by the sibling that owns
    the key, which pins the WAL until it does (condition (c) of the split's transfer);
    `ForwardHonest` and `SnapshotForwardHonest` check the forwards L does own.
  - *The receiver-tag exclusion does not survive a restart's fallback (N-a).*
    `StaleHighExcludesReceiverTag` holds while the WAL shard's per-tag table holds the
    forward. After a WAL shard restart the table is gone and `staleHigh` falls back to
    the start-head floor, which counts the receiver's own forwards too: the exclusion
    is a cost relief, and the fallback only over-counts.
  - *A reactivated reader's tag (N-d).* The reader excludes by the tag of its persisted
    narrow, so a reactivation must read that tag from durable state that survives the
    link (`LeafNodeState`); a reader with no durable tag counts every forward, which
    costs a replay and never passes a record.
  Production symbols: `WalShardGrain.StaleHighLocked`, `LeafAppendEpochFence.PersistedNarrowTag`.

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
  both red, and that red proof ran in CI: run 41693299334 failed all four
  parameterised cases of those two tests. The latch's own detectors are
  `BPlusLeafGrainTests.A_stale_latched_partition_never_live_advances`,
  `BPlusLeafGrainTests.A_checkpoint_hint_does_not_advance_a_stale_latched_partition` and
  `BPlusLeafGrainTests.A_latched_stale_leaf_still_faults_a_direct_drive_without_replaying`;
  rehydrating from a newer snapshot is pinned by
  `BPlusLeafGrainTests.Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint`
  and `LeafSnapshotRehydrateIntegrationTests.Snapshot_capture_persists_blob_and_rehydrate_serves_keys_after_deactivation`.
  Production symbols: `WalMaterialiserPinGrain`, `LeafProjectionStaleException`, `BPlusLeafGrain.TryRescueWarmStaleLeafAsync`, `BPlusLeafGrain.LatchProjectionStaleDrive`, `BPlusLeafGrain.IsPartitionStaleLatched`.
- **CI does not check a purge's unclaimed survivor with both a hand-off window and a 10.2.0 silo.**
  When the purge finds the born activation unclaimed, it may claim afterwards as the
  re-created incarnation's. That slice with every other environment of `Recreate` was
  checked locally and is clean at 8,913,193 distinct states (232 seconds), but it is over
  the 5M-state case budget, and cannot be cut further by
  scenario: splitting it by whether the window opens, or the silo appends, before or
  after the purge, or by whether the duplicate exists at the purge, leaves each half at
  6.5M to 7.5M, because those reachable states coincide whatever the order of the
  steps. It is checked in two halves instead: `RecreateSurvivor` (4,638,713 states) gives
  the survivor the hand-off window without the silo, and `RecreateSurvivorMixed`
  (1,622,161) gives it the silo's unstamped append without the window. What the
  survivor adds to the purge - its claim after it, its own appends, and the claim head,
  close heads and reported unstamped offsets it must read before it arms - meets each
  of those environments in one of them. Only an interleaving needing a foreign record
  and the silo's append at once, around the survivor's claim, is left to the local check;
  the claimed purge (`Recreate`) checks that interleaving in CI for every other activation.
  No production symbol: a model-budget scoping of which environments one configuration combines, not a production mechanism.
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
  `BPlusLeafGrainTests.A_hand_off_opening_during_the_marker_read_cannot_carry_the_advance_past_its_foreign_record`
  (#5768) pins it: a hand-off opens while
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
- **The lazy arm's tracking and partitions are abstracted (#5833).** The WAL shard keeps
  the top two distinct activations of a leaf by high offset, keyed by a per-activation
  token; the model reads `foreignHigh` off the authors of every offset since the shard
  last forgot, which is the value the top two return. One partition stands for each the
  leaf arms at: a batched arm across partitions is a production cost decision, and a
  partition the batch misses is an arm without a claim there
  (`ReadPositionHonestArmWithoutClaim`). The arm's epoch claim is E's; the core models its
  outcome. One stale donor write and one later receiver bound the key-moving environment.
  Production symbols: `WalShardGrain.NoteLeafAuthorLocked`, `WalShardGrain.ForeignHighLocked`, `LeafAppendEpochFence.ArmAsync`, `BPlusLeafGrain.ArmForLiveAdvanceAsync`.
