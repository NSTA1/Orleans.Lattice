# Refinement note: WAL GC starvation-drive scheduling (refinement witness)

## Model scope

This module keeps the two mechanisms the CI core
[WalGcStarvationDrive](WalGcStarvationDrive.tla) abstracts: the per-leaf stall
clock that paces coverage timers, and the ordinary foreground call queue that
competes for the activation turn. It runs at a deliberately small bound so it
completes in seconds. It checks the original TurnNotHeldAcrossDrive obligation
directly, and checks that every behaviour projects to a behaviour of the core
by dropping the stall and call fields. See the core's
[refinement note](WalGcStarvationDriveRefinement.md) for the shared protocol.

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
| `leaf.stall`, `leaf.refreshed`, `leaf.round` | Bounded stall/rearm opportunity and per-opportunity refresh witness. | `BPlusLeafGrain.IsCheckpointStalledBehindLiveDataAsync`. |
| `leaf.timer`, `leaf.turn`, `leaf.call` | Timer dispatch boundary and foreground turn occupancy. | `BPlusLeafGrain.DispatchTimerDriveAsync`. |
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
| `CallArrive` | Queue a foreground call independently of parked background storage. | `BPlusLeafGrain.DispatchTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `CallServe` | Service the queued call when the timer turn has returned. | `BPlusLeafGrain.DispatchTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `TurnNotHeldAcrossDrive` | Pending storage does not prevent a foreground turn from completing. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain representation sanity, not a production representation. It retains its own paired experiments. |
| `CoreInitRefined` | Model-to-model refinement obligation onto WalGcStarvationDrive; no production counterpart. It retains its own paired experiments. |
| `CoreStepRefined` | Model-to-model refinement obligation onto WalGcStarvationDrive; no production counterpart. It retains its own paired experiments. |
| `CoreFairnessRefined` | Model-to-model refinement obligation onto WalGcStarvationDrive; no production counterpart. It retains its own paired experiments. |

## Property classification

TypeOK is state safety. CoreInitRefined and CoreStepRefined are action
safety: the first level maps to a core initial state, and every step maps to a
core step or a core stutter. CoreFairnessRefined checks the core's fairness
conditions on the projected behaviour, and TurnNotHeldAcrossDrive is temporal
liveness. CoreInitRefined is stated as an action property guarded on the first
level; it relies on Stutter always being enabled so that the first level has a
successor.

### Mutation classification

Every mutation is an **action perturbation**: an anchored edit of the action
named in its `PERTURBS` header, or of a definition for the domain check. None
is a transition splice. TurnNotHeldAcrossDriveNeverServed is the historical
experiment for the relocated obligation. The CoreStepRefined experiments each
break one action of this module so that it no longer projects to a core step;
CoreInitRefinedRefusalWindowOpen starts in a state the core does not admit;
CoreFairnessRefinedReArmUnfair removes a fairness condition the core requires.

## Detectors

TurnNotHeldAcrossDrive maps to the same timer detector as the core's
TurnReleased. The refinement properties are model-to-model obligations and have
no production detector.

## Deliberate abstraction gaps

The refinement is checked only at this module's small bound. A larger
refinement instance was measured outside CI and did not finish within the
twenty-minute cap, so no refinement claim is made beyond the enrolled bound.
The core's own guarantees at its larger bound rest on the core abstracting the
stall clock and call queue soundly, which this module witnesses at its bound.