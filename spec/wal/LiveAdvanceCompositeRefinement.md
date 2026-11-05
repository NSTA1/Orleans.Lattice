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
has 131,423 distinct states and is checked in seconds.

## Scope cuts

Three cuts keep the composite under its budget. For each, the combination it drops is either
covered by a module's own configuration or by no module at all, as stated.

- **One WAL shard restart.** The composite restarts the shard once. `WalAppendEpoch.cfg`,
  `WalHandOffFence.cfg`, `WalRoutingFloor.cfg` and `WalLiveAdvanceCore.Restart.cfg` check a
  restart against their own environments.
- **A purge never follows a bulk birth.** No module combines the two either: C's `Recreate`
  and `Bulk` variants are separate configurations, and B models no purge.
- **The bulk switch's own close is not a step.** The composite's `Close` is `F`'s, of a window
  `F` opened (`fWin`); while a bulk switch creates L the open window is the switch's, so `F`
  opens none. C's `Close` guard `phase = "live"` therefore holds of every kept close. B's
  protocol closes the switch's token only once its graft is no longer pending, after the
  birth, but no B guarantee states that, so no edge checks the switch's close.
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
| E -> C | `EpochMeetsCoreClaim` | `ClaimHeadCoversPriorAppends`, `RaisedEpochUnique`, the claim raise's guards (`ClaimRaise`) | C's `Claim` | `EpochMeetsCoreClaimHeadUncovered`, `EpochMeetsCoreClaimRetiredEpochRaised` | `EpochMeetsCoreClaimWitness` |
| E -> C | `EpochMeetsCoreOwnAlloc` | `StaleEpochNeverAppends`, `RaisedEpochUnique` | C's `OwnAlloc` guard | `EpochMeetsCoreOwnAllocSupersededAppends`, `EpochMeetsCoreOwnAllocEpochsShared` | `EpochMeetsCoreOwnAllocWitness` |
| H -> C | `HandOffMeetsCoreForeignAlloc` | `HandOffBound` | C's `ForeignAlloc` guard | `HandOffMeetsCoreForeignAllocUnbounded` | `HandOffMeetsCoreForeignAllocWitness` |
| H -> C | `HandOffMeetsCoreClose` | `HandOffBound`, `CloseHeadMonotone` | C's `Close` | `HandOffMeetsCoreCloseHeadUnbounded`, `HandOffMeetsCoreCloseHeadsRegress` | `HandOffMeetsCoreCloseWitness` |
| R -> C | `RoutingMeetsCoreStraddler` | `StraddlerBelowInit` | C's `StraddlerAlloc` guard | `RoutingMeetsCoreStraddlerUnbounded` | `RoutingMeetsCoreStraddlerWitness` |
| R -> C | `RoutingMeetsCoreInit` | `InitHeadCoversPriorAllocations` | C's `MarkInit` | `RoutingMeetsCoreInitHeadUncovered` | `RoutingMeetsCoreInitWitness` |
| B -> C | `BulkMeetsCoreCreate` | `BulkHintBound` | C's `Create` | `BulkMeetsCoreCreateHintUnbounded` | `BulkMeetsCoreCreateWitness` |
| H -> C | `DeferredCloseMeetsCoreResolve` | `DeferredHeadAtRaisedGeneration`, `HandOffBound` (a pending deferred head holds L), `ClaimHeadCoversPriorAppends` | C's `KeyMove` and `ResolveDeferred` | `DeferredCloseMeetsCoreResolveHeadUncovered`, `DeferredCloseMeetsCoreResolveDeferredBeforeRaise` | `DeferredCloseMeetsCoreResolveWitness` |

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `next`, `author` | The partition's allocator, and the author of each offset: one of L's activations, the foreign author, the straddler, or a record the range held before a bulk birth | `WalShardGrain._nextOffset`, assigned under the shard's state lock; the modules' own `next` and `author`. |
| `up`, `dupUsed` | L's running activations, and the duplicate's budget | C's `up`. |
| `epoch`, `cstage`, `claimHead`, `prior`, `maxClaimed`, `lRetired`, `staleAccepted` | E's claims, over L's activations and the two initiators, with E's ghosts | E's variables of the same names, generalised from L's activations to every claimant as the interface's entry check states; `cstage` keeps only the raised stage. |
| `memFence`, `fence`, `retired` | E's loaded entry, durable entry and floor, per initiator | The shard's loaded generation entry (`WalShardGrain._generationEntries`), the durable row `WalAppendFenceRows.GenerationGrainType`, and the floor `WalShardGrain.GenerationFloorLocked`. These are E's `memFence`, `fence` and `retired`, H's `fFence`, and R's `loaded`, `entryRow` and `walFloor`. |
| `mInit`, `hoOpen`, `hoClosedAt`, `windows`, `fWin`, `pend`, `pendGen` | L's marker, whether its open window is `F`'s rather than the bulk switch's, and a deferred close head pending on it with the generation it resolves at (#5839) | `LeafHandOffState.Initialised` and `ShardHandOffState.Initialised`, read with `HandOffMarkerCore.IsHeldOpen` and `HandOffMarkerCore.RequiredHead`. These are H's variables of the same names, and R's `mInit` and `closedAt`. |
| `recreated`, `legacy`, `caught`, `reinit`, `pGen` | The purge, its kind, the catch-up, the first initialisation, and the generation `S` held at the purge | R's `recreated`, `legacy` and `caught`; C's `recreated` and `reinit`. |
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
| `DeferredClose` | The foreign author's lazy key-moving close under pre-arm: it raises its routing generation with no WAL claim, and L's marker closes with a deferred head at the raised generation (#5839) | H's `CloseRaise` and `CloseDefer`; C's `KeyMove` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `ResolveDeferred(a)` | An activation of L arms and resolves the deferred head: the raised generation is re-claimed if E's claim write admits it, or the loaded entry is kept (C5), and L's marker records the claim's head | H's `ResolveDeferred`; C's `Resolve` and `Claim` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Restart` | The WAL shard restarts once and unloads every entry | The modules' `Restart` | Not applicable: an interface step, generated within the vocabulary. The modules that own it map it to production. |
| `Represent(i)` | An initiator re-presents a claim of a stamp it has held; E's claim write admits it only above the floor and above, or equal to, the durable entry | `WalShardGrain.ClaimRoutingGenerationAsync` through `WalShardGrain.ClaimCoreAsync`, after `WalShardGrain.EnsureGenerationFloorLoadedAsync` (E's `ClaimWrite`, R's `Represent`). Since #5831 the re-presentation also runs at the partition: a stamped append that meets the generation unloaded claims it itself (`WalShardGrain.LoadStampGenerationAsync`) and then runs a fresh admission (`WalShardGrain.AdmitStampLocked`), never admitting on the claim; the writer's path (`WalFencedAppendDispatch`) remains for an older partition. Claims of one initiator are serialised (`WalShardGrain.ClaimRoutingGenerationGatedAsync`), and one at or below a value the activation knows landed is decided from that value without a read, which is the decision the read would reach. These refine the one atomic `Represent` into finer steps whose interleavings the model already allows. | Yes: `WalShardGrainTests.A_generation_entry_is_unloaded_until_re_presented_after_a_restart`, `WalShardGrainTests.A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation`, `WalShardGrainTests.A_close_between_the_append_s_generation_load_and_its_admission_rejects_it_as_stale`, `WalShardGrainTests.Concurrent_re_presentations_of_one_generation_create_its_row_once` and `WalShardGrainTests.The_memory_decision_of_a_re_presented_generation_equals_the_io_decision`. |
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
| `EntryCheckMeetsHandOffAdmission` | A foreign author's stamped append that the WAL shard's entry check admits is one the hand-off fence's model admits. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `ClaimHeadMeetsHandOffClose` | A hand-off close's re-claim returns a head above every stamped foreign append, as the hand-off fence's model assumes. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `EntryCheckMeetsRoutingAdmission` | A straddler that the WAL shard's entry check admits is one the routing floor's model admits. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `ClaimWriteMeetsRoutingClaim` | A re-presented routing claim that the WAL shard's claim write admits is one the routing floor's model admits. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `ClaimHeadMeetsRoutingInit` | The re-created incarnation's first initialisation re-claims at the allocator head, as the routing floor's model assumes. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `EpochMeetsCoreClaim` | A claim the append-epoch fence allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `EpochMeetsCoreOwnAlloc` | An own stamped append the append-epoch fence admits comes from the activation the core takes as current. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `HandOffMeetsCoreForeignAlloc` | A foreign stamped append the hand-off fence allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `HandOffMeetsCoreClose` | A close the hand-off fence allows records heads the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `RoutingMeetsCoreStraddler` | A straddler the routing floor allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `RoutingMeetsCoreInit` | A first initialisation the routing floor and the hand-off fence allow records heads the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `BulkMeetsCoreCreate` | A birth the bulk switch allows is one the core's environment allows. | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |
| `DeferredCloseMeetsCoreResolve` | A lazy close the hand-off fence allows defers L's head at the raised generation, and its resolution records the allocator head, as the core's environment assumes (#5839). | Yes: `LiveAdvanceCompositeTests.Each_composite_copy_equals_its_source` |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

## Redundancy checks

These record which guarantee carries an edge when another guarantee already implies it.

| Check | Result |
|-------|--------|
| `StraddlerBelowInit` on `RoutingMeetsCoreInit` | Not load-bearing: `InitHeadCoversPriorAllocations` covers every prior allocation, straddlers included. It carries `RoutingMeetsCoreStraddler` instead (`RoutingMeetsCoreStraddlerUnbounded`). |

`ClaimHeadCoversPriorAppends` is not a redundancy: it is the guarantee both
`ClaimHeadMeetsHandOffClose` and `ClaimHeadMeetsRoutingInit` rely on, as the edge table shows.

## Findings

The first run found interface gaps F1 to F4 (F2 on two edges). #5776 closed them by
widening C's environment to the stated guarantees and adding R's
`InitHeadCoversPriorAllocations`:
- **F1, `EpochMeetsCoreClaim`.** C's `Recreate` no longer makes an activation that survived the
  purge unclaimed stale.
- **F2, `HandOffMeetsCoreForeignAlloc` and `HandOffMeetsCoreClose`.** C admits a foreign append,
  and a close at any head, while L's marker is uninitialised.
- **F3, `BulkMeetsCoreCreate`.** C's `Create` takes a read position below the hint with any
  records covering it, where it had fixed it at the hint less one.
- **F4, `RoutingMeetsCoreInit`.** R guarantees the first initialisation's head lies above every
  prior allocation.

The run against those fixes found one more, also closed in #5776:
- **F5, `BulkMeetsCoreCreate` (B -> C).** B's `BulkHintBound` allows a birth read position at
  or above the hint, over a foreign record of the switch's window that the leaf holds. C's
  `Create` now takes any read position up to the allocator head with any records covering it,
  which is `BulkHintBound` exactly.

Against the fixed modules every edge is clean.
