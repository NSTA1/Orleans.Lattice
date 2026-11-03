# Refinement note: the WAL routing floor to production

This note maps [`WalRoutingFloor.tla`](WalRoutingFloor.tla) - a tree's purge and
re-creation keeping a write the purged incarnation admitted out of the range a
re-created leaf advances over without reading, and keeping every live shard root's
routing generation above its own floor - to the Orleans.Lattice code that plays
each role. It is module R of the five modules promoted from the design model
[`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla) for epic #5605, and the
design authority is [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md),
section 2 ("Purging a tree's fence rows", "The WAL-side floor is load-bearing" and
"A purge that finishes on 10.2.0"). Like the other notes in this directory it is a
documented mapping, not a refinement proof: the gates check its names, detectors
and coverage, and its claims were checked by reading production.

## Model scope in one paragraph

One owner leaf L at one WAL partition of three offsets, and two shard roots of the
tree: s, L's own shard root, whose marker L reads, and s2 (`WithSecondInitiator`),
standing for every other shard root, whose keys the re-created tree may map to L.
Each has a routing generation (the purged incarnation's anywhere in 1..2, 1..3 in
the `WideGenerations` variant), a durable generation entry at L's partition and the
WAL shard's loaded copy of it, a floor in the tree's routing-floor row, and the WAL
shard's in-memory copy of that floor. Before the purge each shard root may admit
writes stamped with its generation; the WAL admits a stamped append on the loaded
entry alone, and an append meeting an unloaded entry re-presents a claim of its
stamp, which is where the floor is checked. The purge is either the new build's
(raise every initiator's floor over its own generation and entries before any
delete, retire the entries, unload them, and delete L's marker) or, with
`WithLegacyPurge`, one that finished on 10.2.0 and left every row; after it the
re-seed's catch-up runs, at most twice (a duplicate shard-root activation), and the
re-created incarnations of s and s2 initialise. A refused write's seam retry fails
as deleted when its stamp is at or below its initiator's floor. With `WithRestart`
the WAL shard reactivates once, unloading every entry and reloading its floors.
Leaf epochs, hand-off windows, bulk loads, replay and read positions belong to the
other modules (see the interface).

## Interface

This module is R in the assume-guarantee graph of the five modules that replace
the design model ([`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)).

- **Assumptions.** E's entry check for routing generations, and nothing else. It
  is modelled as an environment constraint, never by importing E's state, in its
  three parts: `Alloc(i)` allocates a stamp only from a loaded entry it is not
  below; a re-presented claim (`Represent(i)`) is raised only above `floor(i)` and
  at or above the durable entry; and every claim (`Represent(i)`, `FirstInit`,
  `S2Init`) raises the loaded entry in the same step that reads the allocator head
  it records. The purge unloads the entries, so a straddler meets an unloaded
  entry, re-claims and is refused there by the floor.
  `StraddlerBelowInitNoEntryCheck` drops the first part and
  `StraddlerBelowInitRoutingWalIgnoresFloor` the second; both go red, so the
  assumption is load-bearing here. The routing generation runs the same claim code
  as the leaf epoch (`WalShardGrain.ClaimCoreAsync`); `WalAppendEpoch`'s note gives
  that refinement argument.
- **Guarantees.** Two, each a checked invariant of the base and the variant:
  - `StraddlerBelowInit`: whenever L's marker reads initialised after the
    re-creation, every offset allocated to a shard root of the purged incarnation
    lies below the close the re-created incarnation's first initialisation
    recorded. A straddler is allocated before that close, or not at all.
  - `LiveGenerationAboveFloor`: every live shard root's current generation is
    above its own routing floor.
- **The shared vocabulary.**

  | Interface | Here |
  |-----------|------|
  | L, the partition, `next` | L, the one partition, `next` |
  | `Straddler(i)` | An offset `o` with `author[o] = i` once `recreated`: every allocation in the model is a write the purged incarnation's shard root `i` admitted, allocated before or after the purge. |
  | `Own(a)`, `Foreign(i)`, `Unstamped` | Not modelled: E, H and C own them. The re-created incarnation's own writes are `Foreign(i)` to L and belong to H. |
  | `entry(i)` | `loaded[i]`, the WAL shard's loaded entry, which an append is admitted on; `entryRow[i]` is its durable row. |
  | `floor(i)` | `walFloor[i]`, which a claim is checked against; `rowFloor[i]` is the routing-floor row it is loaded from and never exceeds. |
  | L's marker `initialised` | `MarkerReadsInit`: `mInit`, and after a 10.2.0 purge also the re-seed having run, because a re-created leaf exists only after it. |
  | L's marker `closedAt` | `closedAt`, the head the re-created incarnation's first initialisation recorded. |
  | `claimHead[a]`, `seenU[a]` | Not modelled: E and C own them. |

- **Who assumes what.** C assumes `StraddlerBelowInit` of every straddler append.
  `LiveGenerationAboveFloor` is what keeps that assumption from being met by
  refusing the live tree's own writes: no module assumes it, but its failure is a
  live tree whose every write #5705's guard fails as deleted.
- **The interface's correction.** Appends are admitted on the loaded entry alone
  (`WalAppendFenceCore.IsGenerationAdmitted`) and the floor is checked only at a
  claim; the model follows that, not an append-time floor check.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `gen` | Each shard root's routing generation, the one its live activation stamps with | `ShardHandOffState.RoutingGeneration` on the shard root's marker row, cached by the activation that stamps with it (`ShardRootGrain.EnsureRoutingGenerationAsync`). |
| `entryRow` | Each initiator's durable generation entry at L's partition | `WalRoutingGenerationState.Generation` in the row `WalAppendFenceRows.GenerationGrainType`. |
| `loaded` | The WAL shard's loaded entry for each initiator | The shard's in-memory generation table; `WalAppendFenceCore.Unloaded` when the activation has not loaded it. |
| `rowFloor` | Each initiator's floor in the tree's routing-floor row | `TreeRoutingFloorState.Floors` in the row `WalAppendFenceRows.RoutingFloorGrainType`, keyed by initiator tag. |
| `walFloor` | The WAL shard's in-memory floor for each initiator | `WalShardGrain.GenerationFloorLocked`, merged from the row by `WalShardGrain.MergeGenerationFloorsLocked`. |
| `live` | Each initiator's live shard-root activation stamps with `gen` | A `ShardRootGrain` activation of a live tree that has seeded its generation. |
| `pend`, `pstamp` | A write a shard root admitted, and the generation it was stamped with at admission | `ShardRootGrain.AdmissionRoutingStamp`, carried through the write's seam retries. |
| `next`, `author` | The WAL shard's allocator, and the initiator each offset went to | `WalShardGrain._nextOffset`, assigned under the shard's state lock. |
| `recreated`, `legacy` | The tree was purged and re-created; the purge finished on 10.2.0 | Modelling devices for the purge and its build. |
| `restarted` | The WAL shard reactivated | Modelling device. |
| `mInit`, `closedAt` | L's marker state is initialised, and the head its last close recorded | `ShardHandOffState.Initialised` and the close heads of the shard root's marker, read by a leaf through `ShardRootGrain.GetHandOffMarkerAsync`. |
| `markNew` | L's marker carries the re-created incarnation's identity | `ShardHandOffState.ReseedLineage` (#5716). |
| `caught`, `dup` | s has re-seeded the purged tombstone; a duplicate activation has too | Modelling devices for `ShardRootGrain.EnsureRootAsync` re-seeding a purged tombstone. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Admit(i)` | Before the purge, a live shard root admits a write stamped with its generation at admission; a deleted tree's shard root refuses operations first | `ShardRootGrain.AdmissionRoutingStamp` through `ShardRootGrain.EnsureRoutingGenerationAsync`; a deleted tree refuses in `ShardRootGrain.ThrowIfDeleted` before any routing-generation work. | Yes: `ShardRoutingGenerationIntegrationTests.Client_writes_are_stamped_with_the_shard_routing_generation_never_appended_unstamped` and `ShardRootGrainPurgeTests.An_unrouted_operation_on_an_unregistered_purged_copy_is_refused_as_purged`. |
| `Alloc(i)` | The WAL admits a stamped append on the initiator's loaded entry alone, the stamp not below it, and assigns an offset | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted`, under the lock that assigns `WalShardGrain._nextOffset`. | Yes: `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry` and `WalShardGrainTests.Routing_generations_are_fenced_and_durable`. |
| `Represent(i)` | An append met an unloaded entry; its leaf re-presents a claim of the stamp, refused at or below the WAL-side floor or below the durable entry, otherwise max-merged and loaded | `WalShardGrain.ClaimRoutingGenerationAsync` through `WalShardGrain.ClaimCoreAsync`, after `WalShardGrain.EnsureGenerationFloorLoadedAsync`. | Yes: `WalShardGrainTests.A_generation_entry_is_unloaded_until_re_presented_after_a_restart` and `WalShardGrainTests.A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation`. |
| `Refuse(i)` | A refused write's seam retry finds its stamp at or below its initiator's floor and surfaces the deleted tree, never re-stamping it (#5705) | `ShardRootGrain.NextRoutingRetryAsync` calls `ShardRootGrain.ThrowIfAdmittedUnderPurgedIncarnationAsync`, which throws `ShardRootGrain.PurgedIncarnation` before any reload or raise. | Yes: `ShardRootPurgedIncarnationTests.A_write_admitted_under_a_purged_generation_is_not_re_admitted_under_the_re_created_one` and `ShardRootPurgedIncarnationTests.A_write_refused_at_the_floor_never_re_creates_the_purged_marker_at_floor_plus_one`. |
| `Purge` | Raise every initiator's floor over its own generation and entries before any delete, then retire the entries (raising the WAL-side floors and unloading the loaded entries) and delete L's marker | `TreeRoutingFenceRetirement.RetireAsync` from the purge, with `WalAppendFenceRows.RaiseRoutingFloorsAsync`, `TreeRoutingFenceRetirement.RetireRowsAsync`, `TreeRoutingFenceRetirement.RetireMarkersAsync` and `WalShardGrain.RetireRoutingGenerationsAsync`. **Over-approximation:** the floor is raised over each initiator's own generation; production raises every initiator to the tree's highest, which is at least each one's own. | Yes: `TreeDeletionGrainTests.A_purge_retires_the_fence_rows_before_it_removes_the_registry_entry`, `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`, `WalShardGrainTests.Retiring_routing_generations_deletes_every_entry_under_a_floor_that_covers_them` and `WalShardGrainTests.The_floor_is_durable_before_any_generation_row_is_deleted_and_a_re_drive_finishes`. |
| `LegacyPurge` | A purge that finished on 10.2.0 retires no row and raises no floor | Not production code of this build: a 10.2.0 silo's purge. **Environment:** a cross-shard straddler spanning it is excluded (see the gaps). | Yes: `TreeFenceRowPurgeIntegrationTests.A_tree_re_created_after_a_purge_that_retired_nothing_never_adopts_its_markers`. |
| `CatchUp` | Re-seeding the purged tombstone finds a marker of another incarnation: raise only its own initiator's floor over the marker's generation and entries, and reset the marker to uninitialised | `ShardRootGrain.EnsureRootAsync` calls `ShardRootGrain.RetirePredatingHandOffMarkerAsync`, comparing `ShardHandOffState.ReseedLineage` with the re-seed's incarnation. | Yes: `ShardRootPurgedIncarnationTests.The_re_seed_catch_up_retires_a_marker_an_earlier_incarnation_left_under_its_own_floor` and `ShardRootPurgedIncarnationTests.A_later_shards_catch_up_never_covers_an_earlier_shards_live_generation`. |
| `CatchUpSkip` | A re-seed that finds the marker carrying this incarnation's identity leaves it | `ShardRootGrain.RetirePredatingHandOffMarkerAsync` returns without a write when `ShardHandOffState.ReseedLineage` is the re-seed's. | Yes: `ShardRootPurgedIncarnationTests.A_duplicate_activation_re_seeding_the_same_incarnation_leaves_its_siblings_live_marker_alone` and `ShardRootPurgedIncarnationTests.The_re_seed_catch_up_keeps_a_marker_the_new_incarnations_fence_opened`. |
| `FirstInit` | The re-created incarnation of L's shard root seeds its floor plus one, claims it, and records the allocator head as a close before the marker reads initialised | `ShardRootGrain.EnsureRoutingGenerationSlowAsync` and `ShardRootGrain.FenceRecreatedIncarnationAsync`, which also initialises the range-delete fence and records a close under `ShardRootGrain.RecreationFenceToken`, all before `ShardRootGrain.TryInitialiseHandOffMarkerAsync`. | Yes: `TreeFenceRowPurgeIntegrationTests.A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it` and `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`. |
| `S2Init` | Another shard root's re-created incarnation raises its own floor over anything a 10.2.0 purge left, seeds the floor plus one and claims it; L's initialisation does not claim its generation | The same path as `FirstInit` on that shard root: `ShardRootGrain.RetirePredatingHandOffMarkerAsync`, then `ShardRootGrain.EnsureRoutingGenerationSlowAsync`. | Yes: `ShardRootPurgedIncarnationTests.A_later_shards_catch_up_never_covers_an_earlier_shards_live_generation` and `WalShardGrainTests.A_generation_initialised_without_reading_the_floor_is_refused_at_its_first_claim`. |
| `Restart` | A WAL shard reactivation, or a move's cutover, unloads every entry; the next claim loads the floors from the row | The shard's generation table is in memory only; `WalShardGrain.EnsureGenerationFloorLoadedAsync` loads the row once per activation before any claim. **Environment:** once in the model. | Yes: `WalShardGrainTests.A_generation_entry_is_unloaded_until_re_presented_after_a_restart` and `WalShardGrainTests.A_generation_initialised_without_reading_the_floor_is_refused_at_its_first_claim`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `StraddlerBelowInit` | A write a purged incarnation admitted is never allocated at L's partition above the close the re-created incarnation's first initialisation recorded, so a re-created leaf that has replayed to that close never advances past it unread. | Yes: `WalShardGrainTests.A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation`, `ShardRootPurgedIncarnationTests.A_write_admitted_under_a_purged_generation_is_not_re_admitted_under_the_re_created_one` and `TreeFenceRowPurgeIntegrationTests.A_tree_re_created_after_a_purge_that_retired_nothing_never_adopts_its_markers`. |
| `LiveGenerationAboveFloor` | A live shard root's routing generation is always above its own routing floor, so the WAL never refuses its claims and #5705's guard never fails its writes as deleted. | Yes: `TreeFenceRowPurgeIntegrationTests.A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor`, `ShardRootPurgedIncarnationTests.A_later_shards_catch_up_never_covers_an_earlier_shards_live_generation` and `ShardRootPurgedIncarnationTests.A_duplicate_activation_re_seeding_the_same_incarnation_leaves_its_siblings_live_marker_alone`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

All three checked properties are state safety, reached and checked on the base
(10,704 distinct states, depth 15) and re-checked in the `WideGenerations` variant
(24,084 distinct states, depth 15). Every environment step is on in the base, so
nothing is bounded out; the variant widens the purged incarnation's generations so
the two shard roots' floors and seeds can differ by more than one. Liveness is not
checked here: that a re-created tree eventually initialises is the core module's
concern. Every experiment is an anchored action perturbation; none splices in an
action. A clean witness run confirms the steps the properties depend on are
reached: a straddler allocated after the purge and before the close, a refusal at
or below the floor, a re-presented claim admitted after the purge, a re-seed that
skips a marker its own incarnation seeded, both shard roots live after a 10.2.0
purge, a WAL shard restart after the catch-up, a straddler still pending once L's
marker reads initialised, a straddler of L's own shard root refused as stale after
a 10.2.0 purge's catch-up, and a cross-shard straddler pending after L has
initialised and before s2 has.

The design model's configurations that covered this module's scenarios were the
largest of the draft: `WalLiveAdvanceRecreateCrossShard.cfg` (15.25M distinct
states) and `WalLiveAdvanceRecreate.cfg` (6.47M). Factoring the routing floor out of
the leaf's epochs, hand-offs and replay leaves the base about 0.07% of the former,
so no configuration of this module is kept as local-only evidence.

## Design mutations moved here

The design model's mutations whose guard lives in this module were moved,
re-targeted at this module's properties and re-anchored on its text. The design
draft keeps its copies until every module has landed.

| Design mutation | Here |
|-----------------|------|
| `ReadPositionHonestRoutingAdoptsPredatingMarker` | `StraddlerBelowInitRoutingAdoptsPredatingMarker`; anchored on `CatchUp`. |
| `ReadPositionHonestRoutingFloorAfterDeletes` | `StraddlerBelowInitRoutingFloorAfterDeletes`; anchored on `Purge`. |
| `ReadPositionHonestRoutingInitWithoutClaim` | `StraddlerBelowInitRoutingInitWithoutClaim`; anchored on `FirstInit`. |
| `ReadPositionHonestRoutingPurgeKeepsMarkerInit` | `StraddlerBelowInitRoutingPurgeKeepsMarkerInit`; anchored on `Purge`. |
| `ReadPositionHonestRoutingWalIgnoresFloor` | `StraddlerBelowInitRoutingWalIgnoresFloor`; anchored on `Represent`, the claim, where the corrected interface puts the floor check. |
| `ReadPositionHonestStraddlerRaiseRecreatesMarker` | `StraddlerBelowInitStraddlerRaiseRecreatesMarker`; anchored on `Refuse`. |
| `LiveGenerationAboveFloorCatchUpRetiresLiveMarker` | Same name; anchored on `CatchUp`. |
| `LiveGenerationAboveFloorCatchUpTreeWide` | Same name; anchored on `CatchUp`. |
| `LiveGenerationAboveFloorRoutingRecreatedAtOne` | Same name; re-anchored from the purge onto `FirstInit`, which seeds the generation here. |

The module adds seven of its own, so every action has one: `StraddlerBelowInitAdmitAfterPurge`
(`Admit`), `StraddlerBelowInitNoEntryCheck` and `TypeOkAllocPastOffsetDomain`
(`Alloc`), `StraddlerBelowInitSkipWithoutIncarnation` (`CatchUpSkip`),
`StraddlerBelowInitLegacyPurgeCrossShard` (`LegacyPurge`, showing the documented
residue is real), `LiveGenerationAboveFloorSecondInitiatorKeepsGeneration`
(`S2Init`) and `StraddlerBelowInitRestartLosesFloor` (`Restart`).

## Redundancy checks

The design model kept no redundancy check whose guard is this module's. One guard
is redundant on one path only, and it is not a mutation of its own: after a new-build
purge the WAL-side floor refuses a straddler's re-presented claim whether or not the
first initialisation claims, so `StraddlerBelowInitRoutingInitWithoutClaim` goes red
only through a purge that finished on 10.2.0, where the claim is the only fence.

## Deliberate abstraction gaps

- **The range-delete fence is not a separate initiator.** The purge also retires the
  range-delete fence's generation and floor (tag `t0`), and the first initialisation
  drives it through its own claim; the model has the shard-root initiators only, and
  the range-delete fence's straddler is the same shape as a shard root's.
  Production symbols: `ShardRootGrain.FenceRecreatedIncarnationAsync`, `TreeRoutingFenceRetirement.RetireAsync`.
- **The mixed-version residue is excluded.** A cross-shard straddler spanning a purge
  that finished on 10.2.0, a re-seed run on a 10.2.0 silo, and the range-delete fence
  state such a purge leaves are outside the design's guarantee
  (`StraddlerBelowInitLegacyPurgeCrossShard` shows the first is real). Each needs a
  straddler's seam retry to outlast its deadline across a gate close, the purge, the
  re-creation and the gate reopening; operators should not purge trees during a
  rolling upgrade from 10.2.0.
  Production symbols: `ShardRootGrain.RetirePredatingHandOffMarkerAsync`.
- **One partition, one write per shard root at a time.** Each shard root has at most
  one admitted write pending; the partition has three offsets; the purge happens
  once; the catch-up runs at most twice; and only L's own shard root's marker is
  modelled. The re-created incarnation's own writes and hand-off windows are H's.
  Production symbols: `ShardRootGrain.AdmissionRoutingStamp`.
- **The purge is one step.** Production raises the floors, then retires the fence
  grain, the markers and each partition, re-driven while its registry removal is
  pending; the model takes them as one step, and `StraddlerBelowInitRoutingFloorAfterDeletes`
  stands for a crash between the deletes and a floor written after them.
  Production symbols: `TreeRoutingFenceRetirement.RetireAsync`, `TreeRoutingFenceRetirement.RetireRowsAsync`.
- **The marker-write guard is not modelled.** A purged incarnation's open or close
  that finds its marker absent under a covering floor never re-creates it (#5705).
  The model has no purged-incarnation marker write after the purge, so only the
  write guard is modelled.
  Production symbols: `ShardRootGrain.ThrowIfAdmittedUnderPurgedIncarnationAsync`.
- **A refused write above its floor is not re-stamped here.** Production's seam retry
  re-stamps a write refused above its initiator's floor; no straddler reaches that
  branch in the model (each is at or below its floor once refused), so `Refuse`
  models only the deleted outcome.
  Production symbols: `ShardRootGrain.NextRoutingRetryAsync`.
