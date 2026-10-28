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
| TurnNotHeldAcrossDrive | liveness | witness, with `TurnReleased` in the core | TLC: witness at one leaf, `TurnReleased` in every core instance; argued at any constants via the CallServe transfer below |

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

The argument below is stated for arbitrary constants. It does not rest on a
finite TLC run covering the shared state. The one-leaf witness check
(`LeafCount = 1`, `Ceiling = 1`, `StallTicks = 1`) is supporting evidence only:
it does not range over two-leaf rank, running or credit states, and its
`Ceiling = 1` does not range over the core's `Ceiling = 4`.

**Witness versus the original model (checked by diff).** The witness module
differs from the accepted original model (inventory CC2E78FC, model
97327F94...) only in: the module name; `CONSTANTS Leaves` replaced by
`LeafCount` with `Leaves == 1..LeafCount`; the `fresh` variable with
`wvars == <<vars, fresh>>`, `MarkedInit` and `MarkedStep`; and the appended
`AbsLeaf`, `Core` instance and the three `CoreInitRefined`, `CoreStepRefined`
and `CoreFairnessRefined` definitions. Every action body, `Next`, every
property and every fairness conjunct is byte-identical to the original. A
statement about the witness at `Leaves = 1..n` is therefore a statement about
the original model at `Leaves = {1, ..., n}`.

**Refinement mapping.** `AbsLeaf` maps each leaf record to the same record
without `stall` and `call`; every other variable maps to itself and `fresh` is
dropped. `WalGcStarvationDriveCompositionTests` checks, for the committed
sources, that every action other than `TimerTick`, `ReArm`, `CallArrive` and
`CallServe` is byte-identical between the two modules and that `stall` and
`call` are read or written only by those four actions. The lemmas below rely
on those two syntactic facts and on nothing about the size of `Leaves`.

**Constant domain.** The argument is stated only for well-formed constants:
`LeafCount`, `Ceiling` and `DriveBudget` positive integers, and `Withheld`,
`RefusedSweepHold` and `StallTicks` natural numbers. Every committed
configuration lies in this domain. Neither module declares an `ASSUME` for it,
and nothing below is claimed for other integer values. "For any constants"
below means any constants in this domain.

1. **Init (argued; checked at the witness bound as `CoreInitRefined`).** The
   witness `Init` is the core `Init` with `stall` and `call` added to each leaf
   record. Applying `AbsLeaf` removes exactly those fields, so the projection
   of every witness initial state satisfies `Core!Init`, for any constants.
2. **The nine identical actions (argued).** `GcPass`, `PassRearm`,
   `TimerDispatchDrive(l)`, `DriveOpportunity(l)`, `RefusalWindowTick`,
   `ColdReturn`, `WalAppend`, `DeleteTree` and `Stutter` have the same text in
   both modules. Their guards, choices and next-state expressions read only
   projected fields, and each update of `leaf` is either an `EXCEPT` on
   projected fields or, in `DriveOpportunity`, a comprehension over `Leaves`
   whose body is such an `EXCEPT`. Projection commutes with those updates, so a
   witness step of the action projects to the core step of the same action with
   the same parameters, and the action is enabled in a witness state exactly
   when it is enabled in the projected core state. This holds for every
   `LeafCount` and `Ceiling`, including two-leaf rank, running and credit
   states, because it is a property of the expressions, not of any reachable
   set.
3. **`TimerTick(l)` (argued).** Its guard is the core guard. When
   `stall < StallTicks` it changes only `leaf[l].stall`, so it projects to a core
   stutter. Otherwise its effect on the projected fields is exactly the core
   `TimerTick(l)`.
4. **`ReArm(l)` (argued).** Its guard is the core guard. The choice
   `jitter \in 0..2` is non-empty, so it is enabled exactly when the core
   action is, and every choice has the core `ReArm(l)` effect on the projected
   fields.
5. **`CallArrive(l)` and `CallServe(l)` (argued).** They change only
   `leaf[l].call`, so they project to core stutters.

Lemmas 1 to 5 give `Init_w => Core!Init` and `[Next_w]_wvars => [Next_c]_vars`
under the mapping, for any constants. `CoreStepRefined` checks the step lemma
at the witness bound; it is a test of the lemma, not its proof.

**Fairness transfer (argued; checked at the witness bound as
`CoreFairnessRefined`).** Take a fair witness behaviour and its projection.

- `GcPass`, `PassRearm`, `RefusalWindowTick`, `TimerDispatchDrive(l)`,
  `DriveOpportunity(l)` and `ReArm(l)`: by lemmas 2 and 4 the core action is
  enabled exactly when the witness action is, and a witness step of the action
  that changes `vars` changes a projected field (none of these actions writes
  only `stall` or `call`). Witness weak fairness therefore gives core weak
  fairness.
- `TimerTick(l)`, finite stall progress: suppose the core `TimerTick(l)` is
  continuously enabled from some point, so `timer = "new"`, `turn = "idle"` and
  `~deleted` hold from then on. Then the witness `TimerTick(l)` is
  continuously enabled. While `timer = "new"`, the only action that writes
  `leaf[l].stall` is `TimerTick(l)` (`ReArm(l)` needs `timer = "rearm"`), and
  it only increments. `stall` lies in `-2..StallTicks` with `StallTicks` a
  natural number, so at most `StallTicks + 2` increments are possible, and
  witness weak fairness makes each of them, and then one further tick, occur.
  That further tick takes the full branch, which is a core `TimerTick(l)` step
  that changes `timer` (for example, from `stall = -2` with `StallTicks = 1`,
  three increments are followed by the full tick). Core weak fairness of
  `TimerTick(l)` follows for every natural `StallTicks`. This establishes that
  the opportunity eventually occurs; it gives no bound on scheduler latency,
  because fairness does not bound how many other steps interleave.
- `CallServe(l)` has no core counterpart; its fairness is used below.

**Properties of the original model.** Every original property except
`TurnNotHeldAcrossDrive` mentions only projected fields, and `TypeOK` also
constrains `stall` and `call`.

- State properties and the projected part of `TypeOK` hold on every projected
  state because they hold on every reachable core state.
- Each action property has the form `[][P]_vars` with `P` over projected
  fields, and each `P` holds for any pair of equal projected states (every
  antecedent requires a field to change, or the consequent is `>=` or
  `\subseteq` on an unchanged value). Witness steps that project to a core
  stutter, such as a stall tick or a call step, therefore satisfy `P`; every
  other step is a core step.
- `FloorHolderEventuallyDriven`, `FloorAdvances` and `TurnReleased` are
  stuttering-insensitive formulas over projected fields, and the projection is
  a fair core behaviour.
- The witness-only part of `TypeOK` (`stall \in -2..StallTicks`,
  `call \in {"new", "queued", "done"}`) is inductive for any constants:
  `TimerTick` increments only below `StallTicks`, `ReArm` writes `-2..0` or `0`,
  and only `CallArrive` and `CallServe` write `call`, each to a member.
- `TurnNotHeldAcrossDrive`, CallServe transfer: when `leaf[l].call = "queued"`,
  only `CallServe(l)` changes it. `TurnReleased` holds on the projection, so
  from some point `leaf[l].turn = "idle"` forever, `CallServe(l)` is then
  continuously enabled, and `WF_vars(CallServe(l))` serves the call.

So whenever the core satisfies its properties at given `LeafCount`, `Ceiling`,
`Withheld`, `DriveBudget` and `RefusedSweepHold`, the original model satisfies
all 17 of its properties at those constants with any natural `StallTicks`.

**Where each claim is checked.**

| Claim | Status |
|-------|--------|
| witness equals the original up to `LeafCount` and `fresh` | diff against 97327F94 |
| byte-identical actions; witness-only fields confined to four actions | `WalGcStarvationDriveCompositionTests` on committed sources |
| lemmas 1 to 5 and fairness transfer at any constants | argued above |
| the same lemmas at `LeafCount = 1`, `Ceiling = 1`, `StallTicks = 1` | TLC, witness cfg |
| core properties at one leaf, `TwoLeaf` and `ShareOne` | TLC, core cfgs |
| original properties at the original BASE constants | core `TwoLeaf` (TLC) plus the argument above |

The direction is one way: every witness behaviour projects to a core
behaviour. The converse is not claimed and is not needed, because the core
admits more timer schedules than the witness. No mechanised refinement proof
exists at two leaves; a two-leaf witness run did not finish within the CI cap
and carries no claim.

`CoreInitRefined` is a state invariant over the explicit `fresh` marker, which
is true only in initial states, rather than a property of TLC's search depth.
`CoreInitRefinedRefusalWindowOpen` is its paired bad-initial-state mutant.

### Leaf count and the larger-bound variants

The core base is one leaf, the fast gate. One leaf cannot exercise rank order
between leaves, sibling credit, or two leaves contending for the shared slot.
These are covered by:

- `TwoLeaf`: two leaves at the original BASE bounds (`Ceiling = 4`,
  `Withheld = 0`, `DriveBudget = 2`, `RefusedSweepHold = 2`), with every
  property. With the argument above this carries every original obligation at
  its original bound.
- `ShareOne`: two leaves with three floor permits withheld, with every
  property. It covers a single-slot share, which `TwoLeaf` does not reach; it
  is not a larger state space.
- seven mutants bounded to `LeafCount = 2`, listed in the table below. Each gets
  a clean control at its own bounds as well as the base control.

The three-leaf bound of the rejected `SingleSlotShare` variant is not enrolled.
Its liveness run did not finish within the CI cap and carries no claim.

### Mutations and arms

The 24 original mutations, with the intent, target and perturbed action
recorded in inventory CC2E78FC, and their new arms. Every original arm ran at
`Leaves = {1, 2}`, `Ceiling = 4`, `Withheld = 0`, `DriveBudget = 2`,
`StallTicks = 1`, `RefusedSweepHold = 2`, except rows 18 and 19, which ran at
`Ceiling = 2` with an extra same-bound control.

| # | Mutation | Target | Perturbs | Intent | New arm |
|---|----------|--------|----------|--------|---------|
| 1 | BudgetReleasesNoAbandon | BudgetReleases | DriveOpportunity | budget expiry fails to return drive ownership | core, one leaf |
| 2 | FirstTouchAloneConcurrentLead | FirstTouchAlone | GcPass | the next sweep touch starts before the floor lead finishes | core, two leaves |
| 3 | FloorAdvancesNeverBanks | FloorAdvances | DriveOpportunity | a drive with timely storage never banks any pin progress | core, one leaf |
| 4 | FloorHolderEventuallyDrivenSkipFloorHolder | FloorHolderEventuallyDriven | GcPass | every delivered sweep excludes the actual floor holder | core, one leaf |
| 5 | NearestFirstLivePinResort | NearestFirst | GcPass | pin changes re-sort an already frozen pass list | core, two leaves |
| 6 | NearestFirstNoNearestFirst | NearestFirst | GcPass | a sweep launches a higher-offset leaf before the floor holder | core, two leaves |
| 7 | NoAskWithoutWalAheadNoWalHeadTest | NoAskWithoutWalAhead | TimerTick | a caught-up checkpoint still dispatches a timer drive | core, one leaf |
| 8 | NoAwaitedDriveTimerAwaitsDrive | NoAwaitedDrive | TimerDispatchDrive | the timer retains its exclusive turn across replay | core, one leaf |
| 9 | NoDeletedTreeReactivationSweepIgnoresDeletion | NoDeletedTreeReactivation | GcPass | a delivered GC pass ignores tree deletion | core, one leaf |
| 10 | PermitConservationColdReturnLosesPermit | PermitConservation | ColdReturn | ordinary owner return loses physical capacity | core, one leaf |
| 11 | PinMonotoneAppendRewindsPin | PinMonotone | WalAppend | an environment append erases a banked durable pin | core, one leaf |
| 12 | RearmRefreshesReArmNoJitter | RearmRefreshes | ReArm | terminal rearm reuses the old phase without requesting a new offset | core, one leaf; skips the refresh, since the core has no jitter |
| 13 | RearmRefreshesSecondRefreshSkipped | RearmRefreshes | ReArm | only the first rearm refreshes its phase | core, one leaf |
| 14 | RefusalNotChargedChargeRefusal | RefusalNotCharged | GcPass | a refusal spends the consumer attempt budget | core, one leaf |
| 15 | RefusalNotChargedRearmChargesUndelivered | RefusalNotCharged | PassRearm | the next pass charges a touch that was never delivered | core, one leaf |
| 16 | RefusalNotChargedRefusalAfterSuccessCharged | RefusalNotCharged | GcPass | a prior successful drive disables the admission-refusal refund | core, two leaves |
| 17 | RetryAfterSiblingRetryWithoutCredit | RetryAfterSibling | GcPass | a refused touch retries without a non-refused sibling completion | core, two leaves |
| 18 | ShareRespectedSweepIgnoresReservation | ShareRespected | GcPass | a sweep takes physical capacity without checking its share | core, two leaves, `Ceiling = 2` (the original bound) |
| 19 | TimerReservedSlotSingleSlotNoHold | TimerReservedSlot | TimerDispatchDrive | a single-slot timer ignores an outstanding sweep refusal | core, one leaf, `Ceiling = 2` |
| 20 | TimerReservedSlotTimerTakesLastSlot | TimerReservedSlot | TimerDispatchDrive | timer admission uses the sweep limit | core, two leaves |
| 21 | TurnNotHeldAcrossDriveCallNeverServed | TurnNotHeldAcrossDrive | CallServe | a delivered foreground call never acknowledges completion | witness |
| 22 | TypeOKCallOutsideDomain | TypeOK | CallArrive | an arriving call installs an invalid lifecycle state | witness |
| 23 | TypeOKDeletionOutsideDomain | TypeOK | DeleteTree | environment deletion corrupts the bounded head witness | core, one leaf |
| 24 | TypeOKWindowOutsideDomain | TypeOK | RefusalWindowTick | clock advancement corrupts the bounded refusal window | core, one leaf |

That is 15 core mutants at one leaf, 7 at two leaves and 2 in the witness. No
mutation is renamed or dropped and every target is unchanged. Moving a mutant
to one leaf changes the detection experiment, not the guarantee: the property
it targets is still checked at the original bound in `TwoLeaf`. The gate fails
the pair if any mutant does not fire at its bound.

The 50 original arms map as follows.

- **24 clean full-bound controls:** the core base control (22 core rows), the
  witness base control (rows 21 and 22) and the `TwoLeaf` control, which runs
  the original BASE constant point.
- **24 mutants:** rows 1 to 24 above, under the same names.
- **2 extra matching controls** at `Ceiling = 2` for rows 18 and 19: row 18's
  is the exact original matching point (`LeafCount = 2`, `Ceiling = 2`); row
  19's runs at one leaf. Each is the matching-bound control the gate runs for
  any mutant whose bounds differ from the base.

New experiments, beyond the original 50 arms:

- `TurnReleasedTimerRefusalKeepsTurn`, in the core.
- 12 refinement mutants in the witness: `CoreFairnessRefinedReArmUnfair`,
  `CoreInitRefinedRefusalWindowOpen` and ten `CoreStepRefined` mutants
  (`AppendWithoutHead`, `ColdReturnDropsPermit`, `DeleteMarksAppend`,
  `PinWithoutTimelyStorage`, `RearmKeepsLead`, `ReArmSkipsRefresh`,
  `SweepAfterDeletion`, `TickKeepsRefresh`, `TimerRefusalAsSweep`,
  `WindowCollapses`).
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

### Retired experimental launcher

The twelve-hour experimental launcher that drove the original BASE run and the
rejected single-slot-share run is retired. The formal harness, CI workflows and
`tools/` have no dependency on it, and the redesign's measured runs go through
the ordinary `TlcModelCheckTests` route to `java`, so no adapter or import for
it remains. Its bounded supervisor fix (OWNER175, corrected candidate
`08BA6124`, regression `9278C7F8`) was accepted, and every receipt it produced
is kept as predecessor evidence, including the immutable single-slot-share
receipt `0BACFA0F`.

The successor gate's twenty-minute cap bounds the test process only. It does
not supply the old launcher's 43200-second whole-input budget or its
43100-second post-receipt end-to-end budget, and it is not a hard operating
system or filesystem deadline.
