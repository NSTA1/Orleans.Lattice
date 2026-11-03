# Refinement note: the WAL append-epoch fence to production

This note maps [`WalAppendEpoch.tla`](WalAppendEpoch.tla) - one leaf's activations
claiming an append epoch at a WAL partition, and the WAL shard refusing an append
from a superseded or retired activation - to the Orleans.Lattice code that plays
each role. It is the first of the five modules promoted from the design model
[`design/WalLiveAdvance.tla`](design/WalLiveAdvance.tla) for epic #5605, and the
design authority is [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md),
section 1 ("The epoch fence: other activations of the leaf"). Like the other notes
in this directory it is a documented mapping, not a refinement proof: the gates
check its names, detectors and coverage, and its claims were checked by reading
production.

## Model scope in one paragraph

One leaf L at one WAL partition of three offsets, with epochs up to 3 (4 in the
`WideEpochs` variant). Its born activation `a1` and one duplicate `a2`, which also
stands for the next activation after a stop or a clear, each read L's epoch row
and claim in three steps: persist the epoch read plus one to the row as a strict
conditional write, max-merge it into the WAL shard's durable entry, and raise the
shard's in-memory entry under the offset lock, which returns the allocator head as
the claim head. A refusal while the row holds the activation's own write bumps
above the refusal and claims again; a moved row, or no room to bump, stops it. The
shard admits a stamped append only from a loaded entry and an epoch not below it,
rejects a lower one as stale (and the activation stops), and rejects an unloaded
entry, which the activation answers by re-presenting its claim. The environment
restarts the WAL shard once (the in-memory entry unloads), retires another leaf of
the partition once (the shared floor rises, possibly above every epoch L holds),
forces bumps from a second partition, and clears L once in production's three
steps: read the epoch row and its ETag, retire the entry under a floor of at least
the higher of the entry and the row value read (#5641), then delete the row
conditionally on the ETag read. The gate is open throughout; markers, foreign
authors, routing generations, bulk loads, replay and read positions belong to the
other modules (see the interface).

## Interface

This module is E in the assume-guarantee graph of the five modules that replace
the design model.

- **Assumptions.** None. The module reads no state of the other modules, and its
  environment steps are the WAL shard's own (restart), the partition's (another
  leaf's retirement) and the leaf's own lifecycle (a duplicate activation, a clear,
  a bump forced on another partition).
- **Guarantees.** Four, each a checked invariant of the base and the variant:
  - `StaleEpochNeverAppends`: no append is allocated from an epoch below the
    highest raised epoch of L, or at or below an epoch L's clear retired, across a
    WAL shard restart and across the purge straddle (a claim written before the
    clear and completed after it).
  - `ClaimHeadCoversPriorAppends`: when a claim is raised, its claim head is above
    every offset already allocated, so every append another activation made before
    the raise lies below the head the raised activation replays to.
  - `RaisedEpochUnique`: no two activations that can still append hold the same
    raised epoch, within an incarnation (the row's conditional write) or across
    incarnations (the retired floor and the clear's conditional delete).
  - `OnlySupersededRefused`: only a superseded activation is ever refused, at a
    claim or at an append.
- **The graph.** E <- H, E <- R, and {E, H, R, B} <- C, where H is
  `WalHandOffFence`, R `WalRoutingFloor`, B `WalBulkSwitch` and C
  `WalLiveAdvanceCore`; an arrow points from the module that assumes to the module
  that guarantees. Every module's assumptions are guarantees of modules that
  assume nothing of it, so the graph is acyclic.
- **The entry check for routing generations (refinement argument).** This module
  models leaf epochs only. The routing-generation path that H and R assume runs
  the same code, so the module's check covers it by refinement:
  - **One claim path.** `WalShardGrain.ClaimCoreAsync` is generic over the epoch
    and generation tables. It makes the same `WalAppendFenceCore.DecideClaimWrite`
    and `WalAppendFenceCore.IsRaiseAdmitted` decisions under the same lock that
    assigns offsets, returning the allocator head. The only difference is the
    floor: the partition's shared `_epochFloor` for a leaf epoch,
    `GenerationFloorLocked(initiator)` for a routing generation.
  - **One append rule.** `WalAppendFenceCore.IsGenerationAdmitted` admits a stamp
    only from a loaded entry it is not below, exactly as
    `WalAppendFenceCore.IsEpochAdmitted` does. Neither reads a floor.
  - **One retirement order.** `WalShardGrain.RetireRoutingGenerationsAsync` raises
    the floor in memory and unloads the entries before deleting a row, as a leaf
    retirement does.
  - **What does not carry over.** A generation has no epoch row and no conditional
    row write, so `RaisedEpochUnique` and `OnlySupersededRefused` are leaf-epoch
    guarantees only. H and R assume only the entry check.
  - **Detectors on the generation path:**
    - `WalAppendFenceCoreTests.A_routing_generation_is_admitted_only_from_a_loaded_current_entry`;
    - `WalShardGrainTests.Routing_generations_are_fenced_and_durable`;
    - `WalShardGrainTests.Retiring_routing_generations_deletes_every_entry_under_a_floor_that_covers_them`;
    - `WalShardGrainTests.The_floor_is_durable_before_any_generation_row_is_deleted_and_a_re_drive_finishes`;
    - `WalShardGrainTests.A_generation_initialised_without_reading_the_floor_is_refused_at_its_first_claim`.
- **Who assumes what.** H and R assume E's entry check (`StaleEpochNeverAppends`
  and `ClaimHeadCoversPriorAppends`); C assumes all four guarantees, and owns the
  read-position, snapshot, trim and liveness properties the design model checked
  over them.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `rowEpoch`, `rowInc` | L's epoch row: its value, and with it the row's ETag | `LeafAppendEpochState.Epoch` in the row `WalAppendFenceRows.LeafEpochGrainType`, written only by claims through `WalAppendFenceRows.WriteAsync` (conditional on the ETag). The model's ETag is the pair (incarnation, epoch): within an incarnation every row write raises the epoch, and the clear's delete (`WalAppendFenceRows.ClearAsync`) starts a new incarnation, so an epoch value that recurs never matches an old ETag. |
| `up` | L's running activations | Activations of `BPlusLeafGrain`; one stops through `BPlusLeafGrain.OnAppendEpochSuperseded`, which deactivates it. |
| `seen`, `seenInc` | The row and ETag each activation read | The read in `LeafAppendEpochFence.ClaimRowAsync` (`WalAppendFenceRows.ReadLeafEpochAsync`). The model takes it when the activation starts. |
| `epoch` | The epoch each activation persisted | `LeafAppendEpochFence.Epoch`; 0 before the first claim. |
| `cstage` | Each activation's claim at the partition | The partition's `LeafAppendEpochFence.PartitionClaim`: `ClaimedEpoch` set once `IWalShardGrain.ClaimAppendEpochAsync` accepted; reset by an unloaded rejection (`LeafAppendEpochFence.OnUnloaded`). |
| `claimHead` | The allocator head a raise returned | `WalAppendFenceClaimResult.Accept` with `WalShardGrain._nextOffset`, kept by the activation (`LeafAppendEpochFence.ClaimHead`). |
| `prior` | Ghost: the offsets already allocated at each raise | Modelling device for `ClaimHeadCoversPriorAppends`. |
| `dupUsed`, `restarted`, `otherRetired` | Environment budgets | Modelling devices. |
| `fence` | The WAL shard's durable entry for L | `WalAppendEpochEntryState.Epoch` in the row `WalAppendFenceRows.EntryGrainType`. |
| `memFence` | The WAL shard's in-memory entry for L | The loaded entry `WalShardGrain.FenceEntry` in the shard's epoch table; `WalAppendFenceCore.Unloaded` when the activation has not loaded it. |
| `retired` | The partition's retired-epoch floor | `WalAppendEpochFloorState.Floor` in the row `WalAppendFenceRows.FloorGrainType`, raised by `WalShardGrain.RaiseEpochFloorAsync` and loaded by `WalShardGrain.EnsureEpochFloorLoadedAsync`. |
| `next`, `author` | The WAL shard's allocator, and the activation each offset went to | `WalShardGrain._nextOffset`, assigned under the shard's state lock. |
| `clr`, `clrTag`, `clrRow` | L's clear: its step, the ETag it read, and the row value it passes on | `WalAppendFenceRetirement.RetireLeafAsync`: the row read, the `rowEpoch` argument of `IWalShardGrain.RetireAppendEpochAsync`, and the ETag the final conditional delete carries. |
| `lRetired`, `maxClaimed`, `staleAccepted`, `wrongRefusal` | Ghosts for the guarantees | Modelling devices. `lRetired` is taken from the ETag's epoch the clear read, not from the value it passes on, so a defect in what is passed cannot weaken the property. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `ClaimRow(a)` | Persist the epoch read plus one to L's epoch row, as a strict conditional write | `LeafAppendEpochFence.ClaimRowAsync` through `LeafAppendEpochFence.WriteRowOrSupersedeAsync`; once per activation (`LeafAppendEpochFence.EnsureEpochAsync`). It never adopts a winner's row (#5593). | Yes: `LeafAppendEpochFenceTests.The_first_append_persists_the_epoch_row_and_claims_eagerly_at_the_partition` and `LeafAppendEpochFenceTests.Two_activations_racing_to_create_the_epoch_row_leave_exactly_one_live`. |
| `ClaimConflict(a)` | The row moved since the read: another activation claimed, so this one is stale and stops | `LeafAppendEpochFence.WriteRowOrSupersedeAsync` turns the conflict into `LeafAppendEpochFence.MarkStale`, which calls `BPlusLeafGrain.OnAppendEpochSuperseded`. | Yes: `LeafAppendEpochFenceTests.A_conflict_on_the_epoch_row_supersedes_the_activation`, `LeafAppendEpochFenceTests.Two_activations_racing_to_create_the_epoch_row_leave_exactly_one_live` and `WalAppendFenceIntegrationTests.A_set_on_a_superseded_activation_is_retried_and_succeeds`. |
| `ClaimWrite(a)` | Max-merge the epoch into the durable entry; refuse it at or below the floor or below the entries; admit an equal re-presentation without a write | `WalShardGrain.ClaimAppendEpochAsync` through `WalShardGrain.ClaimCoreAsync`, deciding with `WalAppendFenceCore.DecideClaimWrite` and writing conditionally (an ETag conflict re-reads and re-decides). | Yes: `WalAppendFenceCoreTests.ClaimWrite_writes_only_strictly_above_the_loaded_and_durable_entries`, `WalAppendFenceCoreTests.ClaimWrite_is_refused_at_or_below_the_retired_floor`, `WalAppendFenceCoreTests.A_re_presented_claim_already_durable_proceeds_without_a_write` and `WalShardGrainTests.The_durable_entry_is_a_monotone_max_merge`. |
| `ClaimRaise(a)` | Under the offset lock, raise the in-memory entry and return the allocator head; refused if another claim or a retirement overtook the write | `WalShardGrain.ClaimCoreAsync` under the state lock, with `WalAppendFenceCore.IsRaiseAdmitted`, returning `WalAppendFenceClaimResult.Accept` with `WalShardGrain._nextOffset`; it first waits out any stamped batch of the leaf still assigning offsets (`WalShardGrain.BatchDrainLocked`). | Yes: `WalAppendFenceCoreTests.ClaimRaise_is_refused_when_another_claim_or_a_retirement_overtook_the_write`, `WalShardGrainTests.The_claim_head_is_the_allocator_next_offset` and `WalShardGrainTests.A_claim_raise_never_splits_a_stamped_batch`. |
| `ClaimBump(a)` | Any refusal while the row holds this activation's write bumps the row above the refusal, conditionally on its own ETag, and claims again | `LeafAppendEpochFence.RaiseEpochAboveAsync` and `LeafAppendEpochFence.BumpAsync`, with the threshold `WalAppendFenceCore.ClaimThreshold`; at most `LeafAppendEpochFence.MaxClaimRounds` rounds. **Over-approximation:** the model bounds the rounds by the epoch domain instead. | Yes: `LeafAppendEpochFenceTests.A_refusal_from_the_retired_floor_raises_the_epoch_above_it`, `LeafAppendEpochFenceTests.Refusals_bump_and_re_claim_for_at_most_the_round_cap_then_go_unstamped` and `WalAppendFenceCoreTests.ClaimThreshold_is_the_highest_of_the_entries_and_the_floor`. |
| `ClaimRefuse(a)` | A refusal whose row moved stops the activation; so does a bump with no room in the model | The bump's conditional write conflicts in `LeafAppendEpochFence.WriteRowOrSupersedeAsync`, then `LeafAppendEpochFence.MarkStale`. A bump past the round cap goes unstamped in production (`LeafAppendEpochFence.OnClaimsExhausted`), which this module does not model (see the gaps). | Yes: `LeafAppendEpochFenceTests.A_conflict_on_the_epoch_row_supersedes_the_activation` and `LeafAppendEpochFenceTests.Refusals_bump_and_re_claim_for_at_most_the_round_cap_then_go_unstamped`. |
| `OtherPartitionBump(a)` | A bump forced on another partition changes the activation's one epoch after it was raised here | `LeafAppendEpochFence.BumpAsync` from another partition's claim. **Over-approximation:** the model keeps stamping this partition with the bumped epoch, where `LeafAppendEpochFence.StampForAsync` first re-claims here under it. | Yes: `WalShardGrainTests.A_floor_refusal_bumps_the_activation_to_one_epoch_across_its_partitions`. |
| `OwnAlloc(a)` | The shard admits a stamped append from a loaded entry and an epoch not below it, never checking the floor, and assigns an offset | `WalShardGrain.AdmitStampLocked` with `WalAppendFenceCore.IsEpochAdmitted`, under the lock that assigns `WalShardGrain._nextOffset` (#5635). | Yes: `WalAppendFenceCoreTests.A_stamped_append_is_admitted_only_from_a_loaded_current_epoch`, `WalShardGrainTests.A_stale_epoch_append_is_rejected_and_never_allocated` and `WalShardGrainTests.Retiring_another_leaf_never_supersedes_a_live_leaf`. |
| `StaleAppend(a)` | The shard rejects a stamped append below its loaded entry, allocating nothing, and the activation stops | `WalShardGrain.AdmitStampLocked` returns `WalAppendFenceOutcome.StaleEpoch`; the leaf's `LeafAppendEpochFence.OnStale` marks it stale and its callers retry against the live activation. | Yes: `WalShardGrainTests.A_stale_epoch_append_is_rejected_and_never_allocated`, `LeafAppendEpochFenceTests.A_rejected_append_as_stale_supersedes_the_activation` and `LeafAppendEpochFenceTests.A_current_epoch_append_rejected_as_stale_still_supersedes`. |
| `Represent(a)` | An append meets an unloaded entry; the activation re-presents its claim with the same epoch, which loads the entry | `WalShardGrain.AdmitStampLocked` returns `WalAppendFenceOutcome.UnloadedEntry`; `LeafAppendEpochFence.OnUnloaded` resets the partition's claim, and the next `LeafAppendEpochFence.StampForAsync` claims again from the entry write. | Yes: `LeafAppendEpochFenceTests.An_unloaded_rejection_re_presents_the_same_epoch` and `WalShardGrainTests.An_unloaded_entry_rejects_until_the_claim_is_re_presented`. |
| `Duplicate` | A second activation, or the next after a stop or a clear, reads the epoch row and must claim before it appends | A duplicate `BPlusLeafGrain` activation's own `LeafAppendEpochFence`; the clearing activation that keeps serving forgets its claims (`LeafAppendEpochFence.ResetAfterClear`) and claims afresh like a new one. | Yes: `WalAppendFenceIntegrationTests.A_set_on_a_superseded_activation_is_retried_and_succeeds` and `LeafAppendEpochFenceTests.A_cleared_leaf_claims_afresh_without_being_superseded`. |
| `Restart` | A WAL shard reactivation, or a move's cutover, loses the in-memory entry; the entry and floor rows survive | The shard's epoch table is in memory only; the rows live in grain storage (`WalAppendFenceRows.EntryGrainType`, `WalAppendFenceRows.FloorGrainType`), not in the WAL stream. **Environment:** once in the model; production restarts at any time. | Yes: `WalShardGrainTests.An_unloaded_entry_rejects_until_the_claim_is_re_presented`, `WalShardGrainTests.The_durable_entry_is_a_monotone_max_merge` and `WalShardGrainTests.A_move_to_another_provider_carries_the_fence_table`. |
| `RetireOther` | Another leaf of the partition is retired: the shared floor rises, and L's entry is untouched | `WalShardGrain.RetireAppendEpochAsync` for another leaf: `WalShardGrain.RaiseEpochFloorAsync` max-merges the floor and `WalShardGrain.RemoveLoadedEntryAsync` removes only that leaf's entry. **Environment:** once, to any higher floor. | Yes: `WalShardGrainTests.Retiring_another_leaf_never_supersedes_a_live_leaf` and `WalShardGrainTests.A_floor_refusal_bumps_the_activation_to_one_epoch_across_its_partitions`. |
| `ClearRead` | L's clear reads its epoch row and ETag; a clear that finds no row retires nothing | `WalAppendFenceRetirement.RetireLeafAsync` reads the row (`WalAppendFenceRows.ReadLeafEpochAsync`) and passes its value to every partition; run by the leaf's clear (`BPlusLeafGrain.RetireAppendEpochAsync`) and re-run by the sweep (`WalAppendFenceSweeper`). The WAL GC's retirement passes `WalAppendFenceRows.RowEpochUnread` and the partition reads the row itself. | Yes: `WalAppendFenceIntegrationTests.Clearing_a_leaf_floors_every_partition_at_its_epoch_row_and_deletes_its_rows` and `WalShardGrainTests.An_unread_row_retirement_reads_the_epoch_row_at_the_partition`. |
| `ClearRetire` | Raise the floor durably to at least the higher of the entry and the row value, before deleting the entry; remove the loaded entry | `WalShardGrain.RetireUnderLeafGateAsync`: the floor first (`WalShardGrain.RaiseEpochFloorAsync`, #5641), then the entry row, then `WalShardGrain.RemoveLoadedEntryAsync`, under the leaf's gate (`WalShardGrain.EnterLeafGateAsync`). | Yes: `WalShardGrainTests.Retirement_raises_the_floor_before_deleting_the_entry`, `WalShardGrainTests.Retirement_removes_the_loaded_entry_even_with_appends_admitted` and `WalShardGrainTests.A_re_created_leaf_never_shares_an_epoch_with_a_pre_purge_duplicate`. |
| `ClearDelete` | Delete the epoch row conditionally on the ETag read; a row that moved since is kept | `WalAppendFenceRetirement.RetireLeafAsync` calls `WalAppendFenceRows.ClearAsync` with the row it read, once every partition has retired, and keeps the row on a conflict. | Yes: `WalAppendFenceIntegrationTests.Clearing_a_leaf_floors_every_partition_at_its_epoch_row_and_deletes_its_rows` (the delete after every retirement) and `LeafAppendEpochFenceTests.A_cleared_leaf_claims_afresh_without_being_superseded` (the re-created incarnation claims into the deleted row); the conflict branch is a gap, below. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor so TLC does not report a quiescent state as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `StaleEpochNeverAppends` | No stamped append is ever allocated from a superseded activation's epoch, or from an epoch L's clear retired, across a WAL shard restart or move. | Yes: `WalShardGrainTests.A_stale_epoch_append_is_rejected_and_never_allocated`, `WalShardGrainTests.Retirement_removes_the_loaded_entry_even_with_appends_admitted`, `WalShardGrainTests.An_unloaded_entry_rejects_until_the_claim_is_re_presented` and `WalShardGrainTests.The_durable_entry_is_a_monotone_max_merge`. |
| `ClaimHeadCoversPriorAppends` | An accepted claim's head is the allocator's next offset under the lock that raises the entry, so every append allocated before it, a stamped batch included, lies below it. | Yes: `WalShardGrainTests.The_claim_head_is_the_allocator_next_offset` and `WalShardGrainTests.A_claim_raise_never_splits_a_stamped_batch`. |
| `RaisedEpochUnique` | No two activations of a leaf that can still append ever hold the same raised epoch, before or after the leaf's clear. | Yes: `LeafAppendEpochFenceTests.Two_activations_racing_to_create_the_epoch_row_leave_exactly_one_live`, `WalShardGrainTests.A_re_created_leaf_never_shares_an_epoch_with_a_pre_purge_duplicate` and `WalAppendFenceIntegrationTests.Clearing_a_leaf_floors_every_partition_at_its_epoch_row_and_deletes_its_rows`. |
| `OnlySupersededRefused` | Only a superseded activation is deactivated by the fence: another leaf's retirement, a floor refusal, an unloaded entry or the leaf's own clear never deactivates a live one. | Yes: `WalShardGrainTests.Retiring_another_leaf_never_supersedes_a_live_leaf`, `LeafAppendEpochFenceTests.A_refusal_from_the_retired_floor_raises_the_epoch_above_it`, `LeafAppendEpochFenceTests.Unloaded_rejections_past_the_cap_go_unstamped_and_never_supersede` and `LeafAppendEpochFenceTests.A_cleared_leaf_claims_afresh_without_being_superseded`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

All five checked properties are state safety, reached and checked on the base and
re-checked in the `WideEpochs` variant. Each environment step is on in the base, so
nothing is bounded out; the variant adds one epoch of room for a further bump round.
Liveness is not checked here: that a claim eventually raises, under fair storage,
is the core module's concern. Every experiment is an anchored action perturbation
except `OnlySupersededRefusedFloorAtAppend`, which edits the admission predicate
both append steps read; none splices in an action. A clean witness run confirms the
steps the properties depend on are reached: a stale rejection, a re-presentation, a
claim admitted as `AlreadyDurable` after a restart, floor refusals that bump and
that stop, the clear's delete both deleting and keeping the row, a bump between the
clear's read and its delete, a raise and an append by the re-created incarnation, an
append at or below another leaf's raised floor, and an append above the loaded entry
after a bump forced on another partition.

## Design mutations moved here

The design model's mutations whose guard lives in this module were moved, re-targeted
at this module's properties and re-anchored on its text. The others stay with the
design model until the modules that own their guards are promoted.

| Design mutation | Here |
|-----------------|------|
| `StaleEpochNeverAppendsIgnoresRetiredFloor` | Same name; anchored on `ClaimRaise`. |
| `StaleEpochNeverAppendsRecreateKeepsLoadedEntry` | Same name; anchored on `ClearRetire`. |
| `StaleEpochNeverAppendsUnloadedEntryAccepted` | Same name; anchored on `OwnAlloc`. |
| `StaleEpochNeverAppendsClaimEntryOverwrite` | Same name; anchored on `ClaimWrite`. |
| `OnlySupersededRefusedFloorAtAppend` | Same name; it edits the admission predicate, so it names no perturbed action. |
| `OnlySupersededRefusedFloorInAppendGuards` | Same name; anchored on `OwnAlloc` and `StaleAppend`. |
| `OnlySupersededRefusedConflictWithoutMove` | Same name; anchored on `ClaimConflict`. |
| `NoTrimPastAckedNoEpochCheck` | `StaleEpochNeverAppendsNoEpochCheck`. |
| `NoTrimPastAckedLazyFence` | `StaleEpochNeverAppendsLazyFence`. |
| `ReadPositionHonestClaimReadableHead` | `ClaimHeadCoversPriorAppendsClaimReadableHead`; with no durability here, the readable head is any offset at or below the allocator head. |
| `ReadPositionHonestClaimAdoptsNoCas` | `RaisedEpochUniqueClaimAdoptsNoCas`. |
| `ReadPositionHonestFloorFromEntryOnly` | `RaisedEpochUniqueFloorFromEntryOnly`. |
| `ReadPositionHonestRecreateDropsFloor` | `StaleEpochNeverAppendsRecreateDropsFloor`; it breaks `RaisedEpochUnique` too. |

## Redundancy checks

The design model kept three guards its model implies, as redundancy checks that are
expected to stay clean. They are not mutations, so they are not in the mutation
directory, which holds only experiments that must go red; they were run locally
against this module, checking `TypeOK` with each of the four guarantees.

| Check | Here | Result |
|-------|------|--------|
| `OnlySupersededRefusedEntryRefusalStops` | Kept: only a floor refusal bumps (in `CanBump` and in `ClaimBump`'s guard), so an entry refusal stops the activation even while its row holds. | Clean against all four guarantees. Since #5641 the floor refuses the purge straddle first, and a witness run confirms no entry refusal ever meets a held row here. |
| `OnlySupersededRefusedEtagByValue` | Kept: the row compares ignore the incarnation. | Clean against all four guarantees: a pre-clear activation whose read matches a recurring value claims as the re-created incarnation would. |
| `ReadPositionHonestArmsWithoutClaim` | Dropped: arming and read positions are the core module's. | Not applicable here. |

## Redundancy checks: the bump's guards

`ClaimBump`'s semantic guards are redundant in the same sense (#5745 review, S3).
Each was removed alone, and every invariant stayed clean:

| Guard removed | Result | Why it is kept |
|---------------|--------|----------------|
| `RowHolds(a)`: the bump writes the row only while the activation's ETag holds | Clean | Without it, a superseded activation's bump displaces its successor. That is only another fresh claim, which the entry write and raise already make safe, so no invariant notices. Production's bump is a conditional write on the activation's last ETag (#5633), so a superseded activation stops instead of churning the row. It is kept for fidelity and to bound needless supersession. |
| The floor term of `BumpEpoch` (`retired`) | Clean | A bump at or below the floor is refused again at its next entry write, and the activation bumps again. The term makes one round enough. |
| The entry terms of `BumpEpoch` (`fence`, `memFence`) | Clean | The same argument at the entry: a bump at or below the entry is refused again at its write and raise. The terms make one round enough. |

The bump's bound (`BumpEpoch <= MaxEpoch`) is the only part whose removal is red
(`TypeOK`), because it keeps the epoch inside the model's domain.

## Deliberate abstraction gaps

- **The fence gate and the unstamped fallback.** The gate is open throughout, and a
  claim with no room to bump stops the activation. Production appends unstamped while
  an activation's view of the gate is closed, after a claim fault, or past the
  round cap, and reports the unstamped watermark; those appends, and what the
  watermark does to arming, belong to the core module.
  Production symbols: `LeafAppendEpochFence.OnClaimsExhausted`, `LeafAppendEpochFence.MaxClaimRounds`, `IWalAppendFenceGate`.
- **One leaf, one partition, two activations.** Other leaves appear only through
  the one retirement that raises the shared floor, and other partitions only through
  bumps forced on this activation's one epoch, which the model lets it stamp here
  without the re-claim production makes first. Each environment step happens at most
  once, and epochs stop at 3 (4 in the variant).
  Production symbols: `LeafAppendEpochFence.StampForAsync`.
- **The leaf's gate at the partition.** Production holds a per-leaf gate across a
  claim's entry write and raise, and the retirement takes the same gate, so L's own
  clear never retires between them. The model lets it, which is why the raise's floor
  check is load-bearing here (`StaleEpochNeverAppendsIgnoresRetiredFloor`); in
  production that check also covers another leaf's retirement in between.
  Production symbols: `WalShardGrain.EnterLeafGateAsync`, `WalShardGrain.ClaimAppendEpochAsync`, `WalShardGrain.RetireAppendEpochAsync`.
- **The clear's kept row has no detector.** The model's conditional delete keeps a
  row that moved between the clear's read and its delete, and
  `RaisedEpochUniqueClearDeleteUnconditional` shows that is load-bearing.
  Production keeps it by catching the conflict of the conditional delete, but no test
  drives a bump between the read and the delete; the detectors cited for the clear
  cover the delete itself and the re-created incarnation's claim.
  Production symbols: `WalAppendFenceRetirement.RetireLeafAsync`, `WalAppendFenceRows.ClearAsync`.
- **Retirements other than the leaf's clear.** The WAL GC's orphan-pin retirement
  and a reclaim retire the entry at one partition, flooring at the row the partition
  reads itself, and delete no row; the directory sweep re-runs the leaf's clear for
  a leaf whose clear left its rows (#5634). The model's clear covers their effect at
  this partition, the floor raised before the entry goes and the row kept, but not
  their scheduling or the sweep's directory.
  Production symbols: `LatticeWalGcScheduler.RetireAppendEpochEntryAsync`, `ShardRootGrain.RetireAbsorbedAndCaptureHeadsAsync`, `WalAppendFenceSweeper`, `WalAppendEpochDirectory`.
- **Stamped batches and the entry row's retries.** An append allocates one offset
  in one step. Production admits a multi-entry stamped batch whole under one lock
  section and the raise waits it out, and the claim's entry write retries a bounded
  number of ETag conflicts before refusing; both are atomic steps here.
  Production symbols: `WalShardGrain.BatchDrainLocked`, `WalShardGrain.RegisterStampedBatchLocked`, `WalShardGrain.ClaimCoreAsync`.
- **Self-fenced rejections.** A stale rejection of a stamp below the activation's own
  current epoch, taken before its own bump or a hand-off close, is retried rather
  than a supersession. Here every append carries the current epoch, so every stale
  rejection supersedes; the hand-off close's bump is the hand-off module's.
  Production symbols: `LeafAppendEpochFence.OnStale`, `LeafAppendEpochFence.BumpAndReclaimAsync`.
