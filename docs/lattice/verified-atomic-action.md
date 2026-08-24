# Verified Atomic Action

The safety of the [atomic action](atomic-action.md) coordinator rests on three
load-bearing decisions: forward steps run strictly in order and never skip a
pending step, a forward fault compensates every committed step in strict reverse
order exactly once, and a crash-resume re-derives the unique resume point so the
saga neither re-runs a completed forward effect nor skips a pending compensation -
reaching a terminal state exactly once. Orleans.Lattice drives those decisions from
a single **verified core** - a pure, deterministic function that both the
production grain and a Coyote concurrency model execute - so the saga's sequencing
and crash-resume properties are machine-checked, not just asserted by prose and
integration tests.

This document describes the verification apparatus: the proven-core pattern, the
Coyote concurrency tier that model-checks the core under adversarial crash
interleavings, and the safety-and-liveness property catalogue. It is an assurance
document; the runtime behaviour it protects is documented in
[Atomic Action](atomic-action.md).

## The proven-core pattern

`AtomicActionPlanCore` is an `internal static` class holding the saga's entire
step-sequencing decision surface as pure functions over a caller-owned span of
per-step statuses (`Pending` / `ForwardDone` / `Compensated`) and the saga phase.
Each function is:

- **Deterministic and dependency-free** - it takes explicit inputs (the status
  vector as a `ReadOnlySpan<AtomicActionStepStatus>` and the `AtomicActionPhase`)
  and returns a verdict. No `Task`/`await`, no wall-clock read, no Orleans types, no
  storage, no allocation. Given the same inputs it always returns the same output.
- **The single source of the decision** - the production `AtomicActionGrain` calls
  the core to make the real decision (run the next forward step, commit, compensate
  the next step in reverse order, or settle a terminal) after every persisted step
  transition, including on a reminder-driven resume, and the Coyote model calls the
  *same* core to check it. There is no second, model-only reimplementation that
  could drift from production.

Because the decision logic is isolated behind pure functions, a model checker can
enumerate every ordering of the surrounding concurrent steps (here, crashes that
discard an un-persisted status mark) and assert a property holds at each one, while
production keeps the identical logic on its persisted path. The core is `internal`
and exposed to the test assembly through `InternalsVisibleTo`, so the model sees the
exact production types.

### The core's decisions

| Function | Decision it owns |
|---|---|
| `AtomicActionPlanCore.NextForwardIndex` | The next forward step is the lowest-indexed `Pending` step; forward progress is strictly ascending, so a resume re-derives the exact step a crash interrupted and never re-runs a `ForwardDone` effect. |
| `AtomicActionPlanCore.NextCompensationIndex` | The next step to compensate is the highest-indexed `ForwardDone` step; compensation is strictly descending, and because a compensated step is marked `Compensated` before re-deciding, each committed step is compensated exactly once even across a mid-compensation crash. |
| `AtomicActionPlanCore.Decide` | Reduces the status vector plus phase to the single next action: run a forward step, `Commit` only when every step is `ForwardDone` (never a partial set), compensate the next reverse step, settle `Compensated` when none remain, or `None` when already terminal. |

## The Coyote concurrency tier

`AtomicActionExecutionModel`
(`test/lattice/BPlusTree/Coyote/AtomicActionExecutionModel.cs`) implements
`ICoyoteModel` and drives the production `AtomicActionPlanCore` under
[Coyote](https://github.com/microsoft/coyote) systematic schedule exploration. It
runs a fixed plan whose last forward step faults - so the saga pivots to
compensation - and injects a nondeterministic **crash before the status mark is
persisted** at every forward and every compensating effect. Because the production
grain persists the per-step status vector and re-derives its next action purely from
that vector, a crash is modelled faithfully as "discard the un-persisted mark and
re-decide", an at-least-once effect whose safety the core must still guarantee.

After every step, on every explored order, the model asserts the safety properties
below with `Specification.Assert`.

The tier is tagged `[Category("Coyote")]` so the fast dev loop and the per-package
deterministic CI step skip it; a dedicated CI step runs the category. Run it locally
with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=Coyote"
```

### The model ships a non-vacuous guard test

A model that asserts nothing an interleaving can break is worthless, so the fixture
proves the model can fail. `AtomicActionExecutionModel` takes a
`useBrokenReverseOrder` flag: when set, it compensates the **lowest**-indexed
committed step first (forward order) instead of the highest.
`AtomicActionCoyoteTests` has two tests:

- `Compensation_runs_in_reverse_order_exactly_once_on_any_order` runs the proven
  core and calls `CoyoteModelHarness.AssertNoInterleavingViolation(...)` - no
  explored order trips an assertion.
- `Compensating_in_forward_order_is_caught` runs the broken core and calls
  `CoyoteModelHarness.AssertInterleavingViolationFound(...)` - Coyote *must* find an
  order whose compensation indices increase and trips the reverse-order assertion.

The guard test failing to find a violation fails the build, so the passing test is
meaningful rather than vacuous.

## The property catalogue

A model only checks what it asserts, so "verified" is bounded by the completeness of
the property set. The atomic action's correctness contract is:

Safety properties (checked by the pure model on every explored crash interleaving):

- **AllOrNothing-or-Compensated** - the saga commits only when every forward step
  is `ForwardDone`; a partial forward set can never commit, and a forward fault
  drives the saga to compensate every committed step. Owned by `Decide` /
  `NextForwardIndex`; asserted at every `Commit` and pinned by
  `AtomicActionPlanCoreTests`.
- **CompensateInReverseOnce** - on a forward fault, committed steps are compensated
  in strict reverse (highest-index-first) order, each exactly once, even when a
  crash re-attempts a step. Owned by `NextCompensationIndex`; asserted by the model
  after every compensation and pinned by
  `AtomicActionPlanCoreTests.Compensation_visits_every_forward_done_step_exactly_once_in_reverse`.
- **ResumeExactlyOnce** - the next action is a pure function of the persisted status
  vector and phase, so a reminder-driven resume re-derives the unique resume point;
  it neither re-runs a `ForwardDone` effect nor skips a pending compensation, and it
  reaches a terminal exactly once. Owned by the whole core; pinned by the resume
  cases in `AtomicActionPlanCoreTests` and the grain's
  `ExecuteAsync_resume_from_partial_forward_runs_only_the_pending_step`.
- **TerminalStability** - once the saga is `Committed`, `Compensated`, or
  `CompensationFailed`, `Decide` returns `None`; no further effect runs and the
  memoized outcome is stable across idempotent re-entry. Owned by `Decide`; pinned
  by `AtomicActionPlanCoreTests.Decide_terminal_phase_yields_none` and the grain's
  idempotent-re-entry test.

Liveness / operational properties (checked by the grain integration tier and unit
tests, not the pure model, because they concern durable machinery the grain owns):

- **Termination under bounded faults** - a saga interrupted by a crash resumes via
  the keepalive reminder and drives to a terminal outcome; a resume that cannot make
  progress because a handler changed underneath it (a version-tag mismatch) or is no
  longer registered parks loudly rather than replaying a changed effect. Checked by
  the grain's crash-resume and version-tag-parking unit tests.
- **CompensationContract / operator escalation** - when a compensating effect itself
  faults past its retry budget, the saga parks in `CompensationFailed` and surfaces
  `CompensationFailedException` rather than silently swallowing. Checked by
  `AtomicActionGrainTests.ExecuteAsync_compensation_fault_parks_in_compensation_failed_and_throws`
  and the integration tier.

The safety properties have a live model home in `AtomicActionExecutionModel`; the
operational properties have a home in `AtomicActionGrainTests` and
`AtomicActionGrainIntegrationTests`. The exhaustive truth-table for the pure core
lives in `AtomicActionPlanCoreTests`.

## What the model does not prove

The pure core proves the *sequencing* decision is safe. It deliberately does not
model the correctness of a caller's forward or compensating effect - a custom step's
compensation actually undoing its forward effect is the caller's contract (see
[Atomic Action, "Atomicity, precisely"](atomic-action.md)), outside the core's
reach. Nor does it re-prove the tree-write step's atomicity: a `TreeWrite` step
inherits that from the atomic-write machinery, whose own verified core is documented
in [Verified Atomic-Commit Protocol](verified-atomic-commit.md).

## Related

- [Atomic Action](atomic-action.md) - the user-facing guide to the coordinator.
- [Verified Atomic-Commit Protocol](verified-atomic-commit.md) - the proven-core +
  Coyote pattern applied to the multi-leaf atomic-write saga the tree-write step
  delegates to.
- [Verified Distributed Lock](verified-lock.md) - the same pattern applied to the
  distributed lock.
