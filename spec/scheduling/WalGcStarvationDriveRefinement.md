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