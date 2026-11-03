# Refinement note: the live-advance interface check

This note maps [`WalLiveAdvanceComposite.tla`](WalLiveAdvanceComposite.tla) to the five
modules it composes. Those are the modules promoted from the design model
[`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla) for epic #5605:
`WalAppendEpoch` (E), `WalHandOffFence` (H), `WalRoutingFloor` (R), `WalBulkSwitch` (B) and
`WalLiveAdvanceCore` (C).

Each of those modules checks its own guarantees, and models every other module only as an
environment constraint in the form [`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)
states. Nothing in any one module shows that a guarantee meets the assumption that relies on
it. This module is that check.

It maps to production through the five modules' own notes. Three of its actions are the WAL
shard's admission and claim checks that the edges rest on, and they are mapped here as E, H
and R map them. The other actions are interface steps, non-behavioural here. Each edge's
detector is the test that pins its copied formulas to their sources.

## Model scope in one paragraph

The state is the interface vocabulary only:
- one WAL partition of two offsets, its allocator and the author of each offset;
- the claims of L's two activations and of two initiators, a foreign author `F` and the
  purged incarnation's shard root `S`;
- L's marker;
- a tree purge, on the new build or finished on 10.2.0, with the re-seed catch-up;
- the re-created incarnation's first initialisation;
- a bulk birth.

Each step is generated freely within the vocabulary. A step may claim any epoch or
generation, record any head, allocate under any stamp the author has held, or give the
bulk-born leaf any read position and any held records.

`Next` keeps a step only if every producer guarantee still holds after it. Each edge
invariant then holds unless a kept step of its kind fails the assumption its consumer makes.
A red edge is therefore a step the producers allow and the consumer does not model. The base
has about 0.55M distinct states and is checked in under a minute.

## Scope cuts

Three cuts keep the composite under its budget. For each, the combination it drops is either
covered by a module's own configuration or by no module at all, as stated.

- **One WAL shard restart.** The composite restarts the shard once. `WalAppendEpoch.cfg`,
  `WalHandOffFence.cfg`, `WalRoutingFloor.cfg` and `WalLiveAdvanceCore.Restart.cfg` check a
  restart against their own environments.
- **A purge never follows a bulk birth.** No module combines the two either: C's `Recreate`
  and `Bulk` variants are separate configurations, and B models no purge.
- **Two offsets, one hand-off window.** The modules check three or four offsets
  (`WalBulkSwitch.cfg`) and up to three windows (`WalHandOffFence.WideWindows.cfg`). The
  composite's edges each need one stamped append of each kind before or after one close or
  initialisation, so two offsets reach every edge. The witness mutations show it: each fires
  on the base.

## The edges

Each assumption edge of [`LiveAdvanceInterface.md`](LiveAdvanceInterface.md)'s graph has one
or more edge invariants. The columns are:
- **Copied guarantee:** the producer guarantee the edge relies on.
- **Copied assumption:** the consumer guard or effect the guarantee must meet.
- **Load-bearing mutation:** drops or weakens the guarantee, and must turn the edge red.
- **Witness:** replaces the edge's check by `FALSE`. It must fire, which shows a kept step of
  the edge's kind is reachable.

| Edge | Edge invariant | Copied guarantee | Copied assumption | Load-bearing mutation | Witness |
|------|----------------|------------------|-------------------|-----------------------|---------|
| E -> H | `EntryCheckMeetsHandOffAdmission` | E's entry check (`OwnAlloc`'s loaded-entry guard, `AppendAdmits`) | H's `ForeignAlloc` guard | `EntryCheckMeetsHandOffAdmissionUnloadedAdmitted` | `EntryCheckMeetsHandOffAdmissionWitness` |
| E -> H | `ClaimHeadMeetsHandOffClose` | `ClaimHeadCoversPriorAppends` | H's `CloseHeads` | `ClaimHeadMeetsHandOffCloseHeadUncovered` | `ClaimHeadMeetsHandOffCloseWitness` |
| E -> R | `EntryCheckMeetsRoutingAdmission` | E's entry check | R's `Alloc` guard | `EntryCheckMeetsRoutingAdmissionUnloadedAdmitted` | `EntryCheckMeetsRoutingAdmissionWitness` |
| E -> R | `ClaimWriteMeetsRoutingClaim` | E's claim write (`ClaimWrite`'s guards, `AlreadyDurable`) | R's `ClaimAdmits` | `ClaimWriteMeetsRoutingClaimBelowFloor` | `ClaimWriteMeetsRoutingClaimWitness` |
| E -> R | `ClaimHeadMeetsRoutingInit` | `ClaimHeadCoversPriorAppends` | R's `FirstInit` head | `ClaimHeadMeetsRoutingInitHeadUncovered` | `ClaimHeadMeetsRoutingInitWitness` |
| E -> C | `EpochMeetsCoreClaim` | `ClaimHeadCoversPriorAppends`, `RaisedEpochUnique` | C's `Claim` | `EpochMeetsCoreClaimHeadUncovered` | `EpochMeetsCoreClaimWitness` |
| E -> C | `EpochMeetsCoreOwnAlloc` | `StaleEpochNeverAppends`, `RaisedEpochUnique` | C's `OwnAlloc` guard | `EpochMeetsCoreOwnAllocSupersededAppends`, `EpochMeetsCoreOwnAllocEpochsShared` | `EpochMeetsCoreOwnAllocWitness` |
| H -> C | `HandOffMeetsCoreForeignAlloc` | `HandOffBound` | C's `ForeignAlloc` guard | `HandOffMeetsCoreForeignAllocUnbounded` | `HandOffMeetsCoreForeignAllocWitness` |
| H -> C | `HandOffMeetsCoreClose` | `HandOffBound`, `CloseHeadMonotone` | C's `Close` | `HandOffMeetsCoreCloseHeadUnbounded` | `HandOffMeetsCoreCloseWitness` |
| R -> C | `RoutingMeetsCoreStraddler` | `StraddlerBelowInit` | C's `StraddlerAlloc` guard | `RoutingMeetsCoreStraddlerUnbounded` | `RoutingMeetsCoreStraddlerWitness` |
| R -> C, H -> C | `RoutingMeetsCoreInit` | `StraddlerBelowInit`, `HandOffBound`, `CloseHeadMonotone` | C's `MarkInit` | `RoutingMeetsCoreInitStraddlerUncovered`, `RoutingMeetsCoreInitHeadsRegress` | `RoutingMeetsCoreInitWitness` |
| B -> C | `BulkMeetsCoreCreate` | `BulkHintBound` | C's `Create` | `BulkMeetsCoreCreateHintUnbounded` | `BulkMeetsCoreCreateWitness` |

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `author` | The partition's allocator, and the author of each offset: one of L's activations, the foreign author, the straddler, or a record the range held before a bulk birth | `WalShardGrain._nextOffset`, assigned under the shard's state lock; the modules' own `next` and `author`. |
| `up`, `dupUsed` | L's running activations, and the duplicate's budget | C's `up`. |
| `epoch`, `cstage`, `claimHead`, `prior`, `maxClaimed`, `lRetired`, `staleAccepted` | E's claims, over L's activations and the two initiators, with E's ghosts | E's variables of the same names, generalised from L's activations to every claimant as the interface's entry check states; `cstage` keeps only the raised stage. |
| `memFence`, `fence`, `retired` | E's loaded entry, durable entry and floor, per initiator | The shard's loaded generation entry (`WalShardGrain._generationEntries`), the durable row `WalAppendFenceRows.GenerationGrainType`, and the floor `WalShardGrain.GenerationFloorLocked`. These are E's `memFence`, `fence` and `retired`, H's `fFence`, and R's `loaded`, `entryRow` and `walFloor`. |
| `mInit`, `hoOpen`, `hoClosedAt`, `windows` | L's marker | `LeafHandOffState.Initialised` and `ShardHandOffState.Initialised`, read with `HandOffMarkerCore.IsHeldOpen` and `HandOffMarkerCore.RequiredHead`. These are H's variables of the same names, and R's `mInit` and `closedAt`. |
| `recreated`, `legacy`, `caught`, `reinit`, `purgedUp`, `pGen` | The purge, its kind, the catch-up, the first initialisation, the purged incarnation's activations, and the generation `S` held at the purge | R's `recreated`, `legacy` and `caught`; C's `recreated` and `reinit`. |
| `phase`, `hint`, `born`, `cache`, `rp` | The bulk birth: L's birth hint, and the bulk-born activation's records and read position | C's `phase`, `hint`, `cache` and `rp`; B's `nUp`, `heldN` and `rpN`. |
| `restarted` | The restart's budget | Modelling device. |
| `broken` | The edges a kept step has failed | Modelling device for the edge invariants. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Claim(a)` | One of L's activations raises a claim, with any epoch and any head | E's `ClaimRaise`; C's `Claim` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `OwnAlloc(a)` | A raised activation's stamped append is allocated | E's `OwnAlloc`; C's `OwnAlloc` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Duplicate` | A second activation of L | E's and C's `Duplicate` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `ForeignOpen` | The foreign author opens L's marker | H's and C's `ForeignOpen` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `ForeignAlloc` | A foreign stamped append, under any stamp the author has held, is allocated only if E's entry check admits it: a loaded entry and a stamp not below it | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted`, under the lock that assigns `WalShardGrain._nextOffset` (H's and C's `ForeignAlloc`). | Yes: `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry` and `WalShardGrainTests.Routing_generations_are_fenced_and_durable`. |
| `Close` | The foreign author re-claims any generation E's claim write admits, and L's marker closes recording any head | H's and C's `Close` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Restart` | The WAL shard restarts once and unloads every entry | The modules' `Restart` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Represent(i)` | An initiator re-presents a claim of a stamp it has held; E's claim write admits it only above the floor and above, or equal to, the durable entry | `WalShardGrain.ClaimRoutingGenerationAsync` through `WalShardGrain.ClaimCoreAsync`, after `WalShardGrain.EnsureGenerationFloorLoadedAsync` (E's `ClaimWrite`, R's `Represent`). | Yes: `WalShardGrainTests.A_generation_entry_is_unloaded_until_re_presented_after_a_restart` and `WalShardGrainTests.A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation`. |
| `Purge` | The new build purges the tree: `S`'s floor rises and its entry is retired, L's clear retires L's epochs, and L's marker is discarded | R's `Purge`; E's clear; C's `Recreate` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `LegacyPurge` | The purge finished on 10.2.0, which keeps every row and L's marker | R's `LegacyPurge` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `CatchUp` | After a 10.2.0 purge, `S` re-seeds the tombstone and resets L's marker | R's `CatchUp` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `StraddlerAlloc` | A write `S` admitted before the purge, under a stamp it held then, is allocated after it only if E's entry check admits it | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsGenerationAdmitted` (R's `Alloc`, C's `StraddlerAlloc`). | Yes: `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry` and `WalShardGrainTests.A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation`. |
| `FirstInit` | The re-created incarnation of `S` claims any generation E's claim write admits, and L's marker records any head and reads initialised | R's `FirstInit`; C's `MarkInit` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Birth` | The bulk switch creates L with any read position and any of the records allocated so far | B's `Finish`; C's `Create` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

Each edge invariant states that the modules' interface is consistent. Its production
counterpart is that the module checks compose. The detector for each one is the test that
pins every formula the composite copies to its source, so a module and its copy cannot drift
apart.

| Spec property | Production property | Detector |
|---------------|---------------------|----------|
| `EntryCheckMeetsHandOffAdmission` | A foreign author's stamped append that the WAL shard's entry check admits is one the hand-off fence's model admits. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `ClaimHeadMeetsHandOffClose` | A hand-off close's re-claim returns a head above every stamped foreign append, as the hand-off fence's model assumes. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `EntryCheckMeetsRoutingAdmission` | A straddler that the WAL shard's entry check admits is one the routing floor's model admits. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `ClaimWriteMeetsRoutingClaim` | A re-presented routing claim that the WAL shard's claim write admits is one the routing floor's model admits. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `ClaimHeadMeetsRoutingInit` | The re-created incarnation's first initialisation re-claims at the allocator head, as the routing floor's model assumes. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `EpochMeetsCoreClaim` | A claim the append-epoch fence allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `EpochMeetsCoreOwnAlloc` | An own stamped append the append-epoch fence admits comes from the activation the core takes as current. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `HandOffMeetsCoreForeignAlloc` | A foreign stamped append the hand-off fence allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `HandOffMeetsCoreClose` | A close the hand-off fence allows records heads the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `RoutingMeetsCoreStraddler` | A straddler the routing floor allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `RoutingMeetsCoreInit` | A first initialisation the routing floor and the hand-off fence allow records heads the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |
| `BulkMeetsCoreCreate` | A birth the bulk switch allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_copied_formula_matches_its_source` |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

## Redundancy checks

These record which guarantee carries an edge when another guarantee already implies it.

| Check | Result |
|-------|--------|
| `CloseHeadMonotone` on `HandOffMeetsCoreClose` | Not load-bearing. The composite has one hand-off window, so a foreign close never follows another close in the same incarnation. `CloseHeadMonotone` is load-bearing on `RoutingMeetsCoreInit` instead: a 10.2.0 purge keeps L's marker, so the first initialisation could lower the heads an earlier close recorded (`RoutingMeetsCoreInitHeadsRegress`). |
| `ClaimHeadCoversPriorAppends` on the close heads | The head a re-claim returns is the allocator head. So E's guarantee alone discharges H's `CloseHeads` and R's first-initialisation head. |

## Open findings

The first run found five interface gaps. Each is reported to the design owner, and its edge
stays red until the modules are fixed:
- **`EpochMeetsCoreClaim` (E -> C).** None of E's guarantees stops an activation that was
  running at the purge, and never claimed, from claiming afterwards. C's `Recreate` makes it
  stale.
- **`HandOffMeetsCoreForeignAlloc` and `HandOffMeetsCoreClose` (H -> C).** H's checked
  `HandOffBound` is conditional on `MarkerReadsClosed`, so while L's marker is uninitialised it
  bounds no foreign append or close. C requires the marker to be open, and the close's head to
  be above every foreign record.
- **`RoutingMeetsCoreInit` (R, H -> C).** No guarantee bounds the first initialisation's head
  above a foreign record of a window that is still open. C's `MarkInit` requires it.
- **`BulkMeetsCoreCreate` (B -> C).** B guarantees only `BulkHintBound`, and its read position
  may be capped below the hint (#5752). C's `Create` fixes the read position at the hint less
  one, holding every pre-existing record.
