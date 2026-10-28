# Refinement note: activation-local turn occupancy

<a id="model-scope"></a>

## Model scope in one paragraph

An activation has an exclusive turn, one queued foreground call, one finite
coverage-lag timer episode, and an external drive. This is the explicit turn
encoding used in SplitLinkTurns and SplitGateCycles, with a separate external
completion. It is not a general Orleans scheduler or an unbounded request stream.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `turn` | Exclusive activation-local owner. | The non-interleaving coverage callback in `BPlusLeafGrain.OnCoverageLagTimerTickAsync`. |
| `queued`, `served` | One outstanding foreground request and its acknowledgement. | `BPlusLeafGrain.GetVisibleRawEntriesAsync`. |
| `timer` | Finite timer admission/dispatch lifecycle. | `BPlusLeafGrain.DispatchTimerDriveAsync`. |
| `operation` | External replay completion, independent of foreground turns. | `BPlusLeafGrain.RunInterleavedTimerDriveAsync`. |
| `age` | Local admission/dispatch opportunities while queued. | The local work preceding `RegisterGrainTimer` in `BPlusLeafGrain.DispatchTimerDriveAsync`; not a runtime clock. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `CallQueued` | A foreground call arrives. | `BPlusLeafGrain.GetVisibleRawEntriesAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `TimerFire` | A coverage tick owns the exclusive turn. | `BPlusLeafGrain.OnCoverageLagTimerTickAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `AwaitInTurn` | Finite local admission work, not a storage await. | `BPlusLeafGrain.DispatchTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_choices_preserve_single_dispatch_and_foreground_service`. |
| `Dispatch` | Register an interleaving drive and release the exclusive turn. | `BPlusLeafGrain.DispatchTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `CallServe` | Serve foreground work without waiting for external replay. | `BPlusLeafGrain.GetVisibleRawEntriesAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `CallReturn` | Release foreground ownership. | `BPlusLeafGrain.GetVisibleRawEntriesAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_choices_preserve_single_dispatch_and_foreground_service`. |
| `Complete` | Environment completes the deferred drive. | `BPlusLeafGrain.RunInterleavedTimerDriveAsync`. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `Stutter` | No protocol progress. | No production step. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `ExclusiveTurn` | Foreground dequeue and turn ownership agree; a pending drive does not dispatch another. | Yes: `BPlusLeafGrainTests.Coverage_timer_choices_preserve_single_dispatch_and_foreground_service`. |
| `NoAwaitedDrive` | The exclusive tick returns after registering a one-shot Interleave timer. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `TurnServedWithinBound` | A finite timer episode consumes bounded local work, independent of replay completion. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |
| `EveryCallServed` | Once runtime opportunities continue, queued foreground work can complete even if the drive remains pending. | Yes: `BPlusLeafGrainTests.Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production state representation. Its own paired mutation still executes. |

<a id="property-classification"></a>

## Property classification (issue #2321)

ExclusiveTurn, NoAwaitedDrive and TurnServedWithinBound are state safety
predicates. TypeOK is domain safety. EveryCallServed is temporal liveness under
weak fairness of activation-local runtime opportunities. External completion
has no fairness assumption. ThreeGrains keeps every invariant and bounds out
only temporal liveness; the base checks it.

### Mutation classification

Every mutation below is an **action perturbation**: an anchored edit of its
`PERTURBS` action in `Next`. None is a transition splice.

- `EveryCallServedServeDoesNotAcknowledge`
- `ExclusiveTurnTimerAdmitsExclusiveCall`
- `NoAwaitedDriveAwaitDriveOnExclusiveTurn`
- `NoAwaitedDriveReturnReacquiresTimer`
- `TurnServedWithinBoundLocalWorkExceedsBound`
- `TypeOKCompletionOutsideDomain`
- `TypeOKQueuedAgeOutsideDomain`

## Detectors

The cooperative detector calls the real dispatcher, captures its real timer
registration and explores repeated tick/foreground/empty-completion choices.
The async detector independently starts the real callback against deliberately
pending WAL storage, serves foreground work, rejects duplicate dispatch, then
completes storage and verifies disposal. It does not block real async operations
inside the synchronous Coyote harness: that produces an uncontrolled-task hang,
not a scheduling counterexample. Neither detector claims to implement Orleans'
scheduler; the real Interleave registration is the runtime refinement boundary.

## Defect mutations whose fixes have landed

NoAwaitedDriveAwaitDriveOnExclusiveTurn reinstates the turn-retention mechanism
from [#4949](https://github.com/NSTA1/Orleans.Lattice/issues/4949), fixed by
[#4950](https://github.com/NSTA1/Orleans.Lattice/pull/4950). Its one-target
NoAwaitedDrive experiment fails when Dispatch leaves the timer turn held.
The production negative control changes the same timer registration's Interleave
option to false; the mapped real-dispatch detectors reject it.

## Replaced draft assertions

The draft's bounded foreground-service assertion cannot follow from fairness:
an arbitrarily long finite stutter is fair. TurnServedWithinBound therefore
states the concrete local opportunity bound, not a bound on arbitrary Next
steps, wall time, other grains, or delivery of storage replies. The explicit
TurnServedWithinBoundLocalWorkExceedsBound mutation violates that local bound.
NoAwaitedDriveAwaitDriveOnExclusiveTurn separately rejects holding the turn over
external work. EveryCallServed retains
the eventual-service obligation under its stated runtime premise; no temporal
guarantee was silently substituted for a latency bound.

## Deliberate abstraction gaps

One timer episode per activation; no continuously arriving exclusive work,
runtime scheduling policy, CLR thread schedules, deactivation or call timeout.
The two local timer steps are modelling opportunities, not production counters.
The model cannot guarantee a numerical service latency in an unfair runtime.
The no-runtime inline fallback is outside the deployed-runtime proof; the
existing fallback detector remains a separate production test.
