# Refinement note: WAL GC starvation-drive scheduling (CI core)

## Model scope

A finite cohort of live, checkpointed leaves shares one sized silo replay gate.
Repeated GC passes consume a floor-nearest list in its frozen per-pass order.
Coverage timers supply a finite competing cohort with rearm opportunities.
Cold owners occupy physical permits but no starvation reservations. Every
delivered attempt receives the ordinary non-queueing policy outcome. Budget and
continuation opportunities return drive capacity even with parked read-only
storage. Positive pin progress additionally requires earned prefix banking.

This module is the CI-bound core of the original single starvation-drive model
(#4971). It drops the per-leaf stall clock and the ordinary foreground call
queue. Their obligation, that a timer never holds its turn across a drive, is
restated here as TurnReleased. The companion
[WalGcStarvationDriveWitness](WalGcStarvationDriveWitness.tla) keeps both and
checks, as TLC properties, that it refines this core; see its
[refinement note](WalGcStarvationDriveWitnessRefinement.md).

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `free`, `cold` | Circulating physical permits and ordinary holders. | `BPlusLeafGrain.CirculatingReplayPermits`, `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. |
| `leaf.drive`, `leaf.left` | Starvation reservation and remaining budget/continuation opportunities. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`, `BPlusLeafGrain.DriveStarvedCheckpointAsync`. |
| `hold` | Conditional single-slot timer-yield window after sweep refusal. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. |
| `leaf.pin`, `head`, `appended` | Inclusive consumer pin and finite exclusive-WAL-head growth, represented in one inclusive offset domain. | `LatticeWalGcScheduler.TryReactivateBlockedLeafAsync`, `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. |
| `leaf.attempts` | Settled per-consumer sweep attempt budget; admission refusals leave it unchanged. | `LatticeWalGcScheduler.ObserveAndHealBlockedTreeAsync`. |
| `leaf.touch`, `leaf.credit`, `leadDone` | Per-pass runner state, exact observed-sibling-completion predicate and first-touch barrier. | `ShareBoundedTouchRunner.RunAsync`. |
| `rank` | Frozen relative caller order; obsolete consumed slots are compacted. | `ShareBoundedTouchRunner.RunAsync`, `LatticeWalGcScheduler.ObserveAndHealBlockedTreeAsync`. |
| `leaf.retries` | Refused settlements in the finite retry witness, not the literal runtime ceiling. | `ShareBoundedTouchRunner.RunAsync`. |
| `leaf.refreshed`, `leaf.round` | Rearm opportunity and per-opportunity refresh witness. | `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. |
| `leaf.timer`, `leaf.turn` | Timer dispatch boundary and timer turn ownership. | `BPlusLeafGrain.DispatchTimerDriveAsync`. |
| `deleted` | Registry deletion prevents a new sweep reactivation. | `LatticeWalGcScheduler.TryReactivateBlockedLeafAsync`. |
| `timely` | Explicit earned-prefix-and-banking premise, fixed for the execution. | `BPlusLeafGrain.DriveStarvedCheckpointAsync`. |
| `admitted` | Ghost admission history projected onto the initial floor holder. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `GcPass` | Deliver the next eligible frozen-rank sweep attempt; policy may refuse. | `ShareBoundedTouchRunner.RunAsync`, `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. | Yes: `LatticeWalGcSchedulerCadenceTests.A_pass_with_one_free_replay_slot_spends_it_on_the_floor_holder_not_a_leaf_above_it`, `LatticeWalGcSchedulerCadenceTests.A_pass_re_drives_a_refused_floor_holder_once_a_sibling_frees_the_slot_instead_of_leaving_it_to_a_later_pass`. |
| `PassRearm` | Finish the drained pass and admit a later cadence/backoff episode; preserve non-exhausted budget debt. | `LatticeWalGcScheduler.ObserveAndHealBlockedTreeAsync`. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_refunds_admission_refusals_after_a_prior_completed_drive`. |
| `TimerTick` | Check a live frozen checkpoint against the WAL head and invalidate the refresh witness at each threshold. | `BPlusLeafGrain.OnCoverageLagTimerTickAsync`, `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. | Yes: `BPlusLeafGrainTests.Coverage_stall_timer_caught_up_leaf_never_requests_a_replay_permit`, `BPlusLeafGrainTests.Coverage_stall_timer_refreshes_its_phase_on_every_refused_or_deferred_opportunity`. |
| `TimerDispatchDrive` | Deliver the interleaving one-shot callback, not a drive awaited by the timer turn. | `BPlusLeafGrain.DispatchTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `DriveOpportunity` | Consume one budget opportunity, optionally bank one earned offset, and return capacity at the bound. | `BPlusLeafGrain.DriveStarvedCheckpointAsync`, `BPlusLeafGrain.ReleaseStarvationReplayPermit`. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked`, `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_advances_the_checkpoint_of_a_leaf_that_is_already_resident`. |
| `ReArm` | Refresh the phase for this opportunity, including refused/deferred work. | `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. | Yes: `BPlusLeafGrainTests.Coverage_stall_timer_refreshes_its_phase_on_every_refused_or_deferred_opportunity`. |
| `RefusalWindowTick` | An elapsed refusal window ends conditional timer yielding. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. | Yes: `StarvationReplayAdmissionTests.A_single_slot_share_stops_yielding_once_the_sweep_refusal_is_older_than_the_window`. |
| `ColdReturn` | Ordinary holder returns physical capacity without changing GC reservations. | `BPlusLeafGrain.TryAcquireStarvationReplayPermit`. | Yes: `WalGcStarvationDriveCoyoteTests.Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom`. |
| `WalAppend` | Later WAL work makes a previously caught-up checkpoint eligible again. | `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. | Yes: `BPlusLeafGrainTests.Coverage_stall_timer_caught_up_leaf_never_requests_a_replay_permit`. |
| `DeleteTree` | Deletion invalidates future reactivation, without rewinding durable pins. | `LatticeWalGcScheduler.TryReactivateBlockedLeafAsync`. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_does_not_reactivate_a_blocked_leaf_of_a_deleted_tree`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `PermitConservation` | Failed admission retains neither physical capacity nor reservation. | Yes: `StarvationReplayAdmissionTests.Failed_shared_acquisition_returns_the_gc_reservation`, `WalGcStarvationDriveCoyoteTests.Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom`. |
| `ShareRespected` | Starvation reservations, not ordinary holders, consume the GC share. | Yes: `StarvationReplayAdmissionTests.Acquisition_reserves_only_the_gc_share`, `WalGcStarvationDriveCoyoteTests.Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom`. |
| `NoAwaitedDrive` | Timer dispatch returns without awaiting the interleaving drive. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `RearmRefreshes` | Each opportunity invokes fresh phase mixing; later refreshes cannot hide behind the first one. | Yes: `BPlusLeafGrainTests.Coverage_stall_timer_refreshes_its_phase_on_every_refused_or_deferred_opportunity`. |
| `TimerReservedSlot` | Timer admission leaves conditional sweep headroom, including the single-slot refusal window. | Yes: `StarvationReplayAdmissionTests.A_single_slot_share_is_yielded_to_a_refused_sweep_drive_until_the_sweep_is_admitted`, `WalGcStarvationDriveCoyoteTests.Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom`. |
| `NoDeletedTreeReactivation` | A deleted tree cannot be reactivated by a new sweep touch. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_does_not_reactivate_a_blocked_leaf_of_a_deleted_tree`. |
| `NoAskWithoutWalAhead` | A timer needs new WAL work, not just an unchanged live checkpoint. | Yes: `BPlusLeafGrainTests.Coverage_stall_timer_caught_up_leaf_never_requests_a_replay_permit`. |
| `RefusalNotCharged` | Refusals after previous admissions remain outside the capped charged attempt budget. | Yes: `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_refunds_admission_refusals_after_a_prior_completed_drive`. |
| `NearestFirst` | Consume the selected pass's fixed floor-nearest order, including retries. | Yes: `LatticeWalGcSchedulerCadenceTests.A_pass_with_one_free_replay_slot_spends_it_on_the_floor_holder_not_a_leaf_above_it`, `LatticeWalGcSchedulerCadenceTests.A_pass_re_drives_a_refused_floor_holder_once_a_sibling_frees_the_slot_instead_of_leaving_it_to_a_later_pass`. |
| `RetryAfterSibling` | Only a later non-refused sibling completion credits an in-pass retry. | Yes: `LatticeWalGcSchedulerCadenceTests.A_pass_re_drives_a_refused_floor_holder_once_a_sibling_frees_the_slot_instead_of_leaving_it_to_a_later_pass`. |
| `FirstTouchAlone` | The first floor holder runs to settlement before sibling launch. | Yes: `LatticeWalGcSchedulerCadenceTests.A_pass_with_one_free_replay_slot_spends_it_on_the_floor_holder_not_a_leaf_above_it`. |
| `BudgetReleases` | The outer bounded drive frame releases capacity even while inner read-only storage is parked. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked`. |
| `PinMonotone` | Actual earned checkpoint progress does not rewind. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_advances_the_checkpoint_of_a_leaf_that_is_already_resident`. |
| `FloorHolderEventuallyDriven` | Returned capacity plus finite competition and real sweep opportunities eventually permit work on the initial floor holder. | Yes: `WalGcStarvationDriveCoyoteTests.Sweep_timer_and_cold_returns_preserve_reservations_and_conditional_headroom`. |
| `FloorAdvances` | With earned bounded-prefix banking and stable finite WAL work, repeated passes eventually catch up. | Yes: `BPlusLeafGrainTests.DriveStarvedCheckpointAsync_advances_the_checkpoint_of_a_leaf_that_is_already_resident`, `LatticeWalGcSchedulerCadenceTests.A_pass_re_drives_a_refused_floor_holder_once_a_sibling_frees_the_slot_instead_of_leaving_it_to_a_later_pass`. |
| `TurnReleased` | A timer that dispatched or was refused returns its exclusive turn rather than holding it across a drive. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain representation sanity, not a production representation. It retains its own paired experiments. |

## Property classification

TypeOK, PermitConservation, ShareRespected, NoAwaitedDrive and RearmRefreshes
are state safety. TimerReservedSlot, NoDeletedTreeReactivation,
NoAskWithoutWalAhead, RefusalNotCharged, NearestFirst, RetryAfterSibling,
FirstTouchAlone, BudgetReleases and PinMonotone are action safety.
FloorHolderEventuallyDriven, FloorAdvances and TurnReleased are temporal
liveness. The base and ShareOne configurations both retain the entire
checked-property set; ShareOne only withholds more of the floor so the
single-slot share and its refusal window are exercised. No temporal check is
omitted for budget.

### Mutation classification

Every mutation is an **action perturbation**: an anchored edit of the action
named in its `PERTURBS` header, or of a definition for the domain checks. None
is a transition splice. Each original obligation keeps its historical
experiment under its original name. TurnReleasedTimerRefusalKeepsTurn is new:
it makes a refused timer drive keep the timer turn, which only the temporal
TurnReleased property can reject.

## Detectors

The per-package Coyote fixture invokes the real admission and release methods.
Its choices interleave ordinary physical ownership, capacity returns and
sweep/timer requests; an explicit finite delivery tail tests returned capacity.
It is not a replica admission policy and makes no probabilistic fairness claim.
The timer detector invokes the actual activated grain's timer path repeatedly.
The refund detector runs the real hosted scheduler across a completed drive,
repeated refusals and later charged successes, preserving the initial debt.
The runner detectors enter through that same scheduler, not a substitute runner.
The detector methods are unchanged from the original model; the redesign moves
no production or Coyote obligation, so the production negative controls
recorded for the original model apply to the same mapped seams.

## Defect mutations whose fixes have landed

The timer dispatch/interleaving repair is integrated in
[PR #4950](https://github.com/NSTA1/Orleans.Lattice/pull/4950).
NoAwaitedDriveTimerAwaitsDrive and TurnReleasedTimerRefusalKeepsTurn reinstate
turn retention across a drive and are rejected by the mapped detector.

## Replaced draft assertions

The original model checked TurnNotHeldAcrossDrive over an explicit foreground
call queue and a per-leaf stall clock. Together with the timer cohort, that
state space did not complete within the CI budget. The core restates the
obligation as TurnReleased over the timer turn alone, and the witness module
re-checks the original TurnNotHeldAcrossDrive and its mutation, so the original
assertion is relocated rather than dropped. RearmRefreshesReArmNoJitter keeps
its historical name; it skips the phase refresh and does not model randomness.

## Deliberate abstraction gaps

Weak fairness is on delivered GcPass opportunities, including ordinary refusals,
never on a selected successful grant. Refused touches without sibling
completion drain to the later-pass boundary.

ColdReturn is not fair. Hung ordinary owners can retain every physical permit.
Liveness explicitly assumes their eventual return, a stable live tree, finite
timer competition and finite WAL append. A budget opportunity includes the
required runtime continuation; it is not wall time. The timely premise includes
durable banking before the budget is exhausted.

One in-pass retry witnesses the credit boundary; production
ShareBoundedTouchRunner.MaxInPassRetries is three. The cross-pass finite attempt
budget is two rather than the production budget. No exact cooldown, registry
fault, unsized gate, cross-silo bound or arbitrary infinite competitor-arrival
theorem is asserted.

The stall clock is abstracted: TimerTick may fire whenever the WAL head is
ahead of the pin, so the core admits every timer schedule the clocked witness
does, and possibly more. A safety property of the core therefore also holds
for the witness projection. The witness checks that projection only at its own
small bound.

A three-leaf instance of this core was measured outside CI. Its safety-only
check passed in several minutes, and its full liveness check did not finish
within the twenty-minute cap, so it is not enrolled and no three-leaf liveness
claim is made.

## Disposition of the original model

This section accounts for every obligation of the original single model
(#4971) against its machine-readable inventory: 17 properties, 13 `Next`
actions, 8 weak-fairness conditions, 24 mutations and 50 paired arms. Nothing
listed here is claimed as measured until the paired gate has run on the
committed sources.

### Properties

| Original property | Class | Checked in | Instances |
|-------------------|-------|-----------|-----------|
| PermitConservation | state safety | core | Base, TwoLeaf, ShareOne |
| ShareRespected | state safety | core | Base, TwoLeaf, ShareOne |
| NoAwaitedDrive | state safety | core | Base, TwoLeaf, ShareOne |
| RearmRefreshes | state safety | core | Base, TwoLeaf, ShareOne |
| TypeOK | state safety | core and witness | all |
| TimerReservedSlot | action safety | core | Base, TwoLeaf, ShareOne |
| NoDeletedTreeReactivation | action safety | core | Base, TwoLeaf, ShareOne |
| NoAskWithoutWalAhead | action safety | core | Base, TwoLeaf, ShareOne |
| RefusalNotCharged | action safety | core | Base, TwoLeaf, ShareOne |
| NearestFirst | action safety | core | Base, TwoLeaf, ShareOne |
| RetryAfterSibling | action safety | core | Base, TwoLeaf, ShareOne |
| FirstTouchAlone | action safety | core | Base, TwoLeaf, ShareOne |
| BudgetReleases | action safety | core | Base, TwoLeaf, ShareOne |
| PinMonotone | action safety | core | Base, TwoLeaf, ShareOne |
| FloorHolderEventuallyDriven | liveness | core | Base, TwoLeaf, ShareOne |
| FloorAdvances | liveness | core | Base, TwoLeaf, ShareOne |
| TurnNotHeldAcrossDrive | liveness | witness, with `TurnReleased` in the core | witness at one leaf; `TurnReleased` in every core instance |

No property is dropped or weakened, and no temporal property is replaced by a
safety monitor. `TurnNotHeldAcrossDrive` mentions the call queue, which only the
witness has, so it is checked there; the core's `TurnReleased` is the same
turn-return obligation stated over the turn and timer fields that both modules
share.

### Next actions

| Original action | Core | Witness |
|-----------------|------|---------|
| `GcPass`, `PassRearm`, `TimerDispatchDrive`, `DriveOpportunity`, `RefusalWindowTick`, `ColdReturn`, `WalAppend`, `DeleteTree`, `Stutter` | byte-identical body | byte-identical body |
| `TimerTick`, `ReArm` | stall clock removed | original body |
| `CallArrive`, `CallServe` | absent | original body; erased to a core stutter |

`ColdReturn`, `WalAppend` and `DeleteTree` remain environment actions without
fairness in both modules. Admission is still derived from the policy inside
`GcPass` and `TimerDispatchDrive`; no action assumes a fair grant.

### Weak fairness

`GcPass`, `PassRearm`, `RefusalWindowTick`, and the per-leaf `TimerTick`,
`TimerDispatchDrive`, `DriveOpportunity` and `ReArm` keep their original weak
fairness in both modules. `CallServe` keeps its weak fairness in the witness,
where the call queue exists. `CoreFairnessRefined` checks that the witness's
fairness implies the core's under the refinement mapping.

### Composition for any leaf count

The witness differs from the core only in the per-leaf stall clock, the call
queue and the `fresh` initial-state marker.
`WalGcStarvationDriveCompositionTests` checks this syntactically: every action
other than `TimerTick`, `ReArm`, `CallArrive` and `CallServe` is byte-identical
between the two modules, and the witness-only fields are read or written only
by those four actions. Those four actions change the state of their own leaf
`l` and no other. The refinement mapping is therefore a per-leaf projection, so
a witness step projects onto a core step, or a stutter, for any `LeafCount` if
it does so for every leaf state. That per-leaf lemma depends only on the state
of one leaf and on the shared fields, which a one-leaf instance ranges over
completely, and TLC checks it there as `CoreStepRefined`. The fairness transfer
is per-leaf in the same way: the stall clock grows only while the timer is
`"new"`, and `WF(CallServe)` discharges `TurnNotHeldAcrossDrive`. This argument
is a syntactic decomposition plus a checked per-leaf lemma, not a mechanised
proof at two leaves; a two-leaf witness run did not finish within the CI cap and
carries no claim.

`CoreInitRefined` is an invariant over the explicit `fresh` marker, which is
true only in initial states, rather than over TLC's search depth.
`CoreInitRefinedRefusalWindowOpen` is its paired bad-initial-state mutant.

### Leaf count and the larger-bound variants

The core base is one leaf, the fast gate. One leaf cannot exercise rank order
between leaves, sibling credit, or two leaves contending for the shared slot.
These are covered by:

- `TwoLeaf`: two leaves at the original BASE bounds, with every property.
- `ShareOne`: two leaves with three floor permits withheld, with every property.
- six mutants bounded to `LeafCount = 2`: `NearestFirstNoNearestFirst`,
  `NearestFirstLivePinResort`, `FirstTouchAloneConcurrentLead`,
  `ShareRespectedSweepIgnoresReservation` (also `Ceiling = 2`),
  `TimerReservedSlotTimerTakesLastSlot` and
  `RetryAfterSiblingRetryWithoutCredit`. Each gets a clean control at its own
  bounds as well as the base control.

`TwoLeaf` holds the original BASE constants and so carries every original
cross-leaf obligation at its original bound. The three-leaf bound of the
earlier model is not enrolled. Its liveness run did not finish within the CI
cap and carries no claim.

### Mutations and arms

All 24 original mutations keep their names and targets.

- 22 are in the core.
- `TurnNotHeldAcrossDriveCallNeverServed` and `TypeOKCallOutsideDomain` perturb
  the call queue, so they are in the witness.
- Every original mutation ran at `Leaves = {1, 2}`. Sixteen core mutants now run
  at the one-leaf base, or at `Ceiling = 2` for
  `TimerReservedSlotSingleSlotNoHold`. The six listed above keep two leaves.
- Each one-leaf mutant is expected to fire at one leaf; the gate fails the pair
  if any one does not.

The 50 original arms map as follows.

- **24 clean full-bound controls:** the core base and witness base controls.
- **24 mutants:** run under the same names.
- **2 extra matching controls** (`Ceiling = 2` for
  `ShareRespectedSweepIgnoresReservation` and
  `TimerReservedSlotSingleSlotNoHold`): each becomes the matching-bound control
  the gate runs for any mutant whose bounds differ from the base.

New experiments:

- `TurnReleasedTimerRefusalKeepsTurn`, in the core.
- 12 refinement mutants in the witness.

### Premises kept

The redesign keeps every premise of the original model:

- Admission is derived from policy.
- `ColdReturn` carries no fairness.
- "Timely" means an earned prefix plus banking.
- Appends and timer rounds are finite.
- `free` is an independent counter, not derived from other fields.
- No leaf symmetry is assumed.
- Rank is frozen per pass.
- There is no exact three-retry theorem.
- The original single-slot share is not monotone.

The original BASE run certifies only the original model at its own bounds. Its
results are predecessor evidence for this redesign, not a certification of it.
