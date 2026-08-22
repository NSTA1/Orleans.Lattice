# Verified Atomic-Commit Protocol

The all-or-nothing guarantees behind [atomic writes](atomic-writes.md) and
[online reshard](online-reshard.md) rest on one distributed protocol: a
multi-leaf prepare / commit / abort saga, a per-tree transaction-registry
decision, and a reader-visibility gate that resolves a pending key against a
single decision snapshot. Orleans.Lattice drives that protocol from a set of
**verified cores** - pure, deterministic functions that both the production
grains and an out-of-solution verification layer execute - so the protocol's
safety and liveness properties are machine-checked, not just asserted by prose
and integration tests.

This document describes the verification apparatus: the proven-core pattern, the
Coyote concurrency tier that model-checks the cores under adversarial
interleavings, the safety-and-liveness property catalogue, and the TLA+
specification that pins the protocol design above the code. It is an assurance
document; the runtime behaviour it protects is documented in
[Atomic Writes](atomic-writes.md) and [Online Reshard](online-reshard.md).

## The proven-core pattern

A verified core is a single pure function (or small pure type) that captures one
decision point of the protocol. Each core is:

- **Deterministic and dependency-free** - it takes explicit inputs and returns a
  verdict. No `Task`/`await`, no wall-clock or HLC read, no `RequestContext`, no
  Orleans types, no storage. Given the same inputs it always returns the same
  output.
- **The single source of the decision** - the production grain hot path calls
  the core to make the real decision, and the verification layer calls the *same*
  core to check it. There is no second, model-only reimplementation that could
  drift from production.

Because the decision logic is isolated behind a pure function, a model checker
can enumerate every ordering of the surrounding concurrent steps and assert a
property holds at each one, while production keeps the identical logic on its hot
path. The cores are `internal` and exposed to the test assembly through
`InternalsVisibleTo`, so the models see the exact production types.

### The extracted cores

| Core | Decision it owns | Introduced by |
|------|------------------|---------------|
| `AtomicVisibilityGate.ResolveKey` | How a read of a key carrying a pending mutation is answered against the recorded decision (surface the prepared value, hide the key, or fall through to the pre-saga value). | Level B (per-key read gate) |
| `SagaCoordinatorCore.Decide` | The coordinator verdict: commit iff every participant acked, abort on the first nack or unreachable leaf. | Phase 1 (#1589) |
| `TxRegistryDecisionCore` | The tree-wide commit / abort decision and its monotonic revision counter. | Phase 2 (#1590) |
| `ReaderStabilityGate` | Whether a snapshot read over N keys is stable against the current registry revision, generalised to arbitrary key counts. | Phase 2 (#1590) |
| `MigrationTerminalCore` | Whether a leaf that already applied a saga terminal is authoritative for a key, so a late shadow-forwarded prepared write falls through instead of shadowing the committed value. | Phase 3 (#1591) |
| `ShadowedMigrationReadGuard` | How a read resolves against a leaf mid-migration when a prepared bucket has been shadow-forwarded across a shard split. | Phase 3 (#1591) |
| `SplitBoundary` | Which post-split leaf owns a key, so migration routing is a pure function of the key and the split boundary. | Phase 3 (#1591) |
| `TerminalDecisionGuard.Classify` | The write-once classification of an incoming terminal (apply, idempotent duplicate, or rejected flip) at the serialized registry. | Phase 5 (#1594) |
| `TerminalArrivalTally` | The per-saga terminal arrival count that decides when a saga's registry entry may be tombstoned. | Phase 5 (#1594) |

The core files live under `src/lattice/BPlusTree/` next to the grains that call
them. The shape of a core is a pure verdict function, for example:

```text
// Illustrative shape (internal API):
AtomicVisibilityGate.ResolveKey(status, alreadyTerminal, preparedHiddenByTombstoneOrExpiry)
    -> PendingReadOutcome        // SurfacePrepared | Hidden | FallThroughToPreSaga

SagaCoordinatorCore.Decide(votes) -> SagaDecision   // Collecting | Commit | Abort
```

Because the production grain and the model both call `ResolveKey` and `Decide`,
a property proven of the core is a property of production.

## The Coyote concurrency tier

The cores are model-checked with [Microsoft Coyote](https://github.com/microsoft/coyote)
(the `Microsoft.Coyote.Test` package). Each model exercises one core (or a small
group of cooperating cores) under systematically explored interleavings of the
protocol's concurrent steps - the prepare fan-out, the registry decision, the
per-leaf terminal broadcast, duplicate terminal re-deliveries, and interleaved
reader probes - and asserts the safety and liveness properties at every step.

Concurrency in these models is **explicit cooperative interleaving**: a model
implements `ICoyoteModel` and yields decision points via the harness, and Coyote
drives controlled nondeterminism (`runtime.RandomBoolean()`) to explore the
schedule space. There is no `coyote rewrite` pass and real `Task`/`await` is not
controlled; the models encode the protocol's concurrency as data so it is fully
enumerable. The shared harness is `CoyoteModelHarness`
(`test/shared/Orleans.Lattice.Testing/Coyote/`), whose
`AssertNoInterleavingViolation` / `AssertInterleavingViolationFound` entry points
run a model to a bounded step count over many iterations.

The models live under `test/lattice/BPlusTree/Coyote/`:

| Model | Core(s) exercised | Phase |
|-------|-------------------|-------|
| `AtomicCommitVisibilityModel` | `AtomicVisibilityGate`, `TxDecisionView` | Level B |
| `SagaCoordinatorModel` | `SagaCoordinatorCore` | Phase 1 |
| `ReshardMigrationModel` | `MigrationTerminalCore`, `ShadowedMigrationReadGuard`, `SplitBoundary` | Phase 3 |
| `AtomicCommitLivenessModel` | The full saga under bounded fault injection | Phase 4 |
| `AtomicCommitInvariantModel` | The full single-saga lifecycle across every core | Phase 6 |

### Every model ships a non-vacuous guard test

A model that checks a property only has value if the property can actually fail.
Every model therefore ships a companion **guard test** that removes exactly the
one fix the property depends on and asserts Coyote *finds* the resulting
violation (`AssertInterleavingViolationFound`). A model with a green fix test and
a green guard test is proven load-bearing: the property holds with the fix in
place, and the check is not vacuously true because it catches the fix's removal.

For example, the reshard model's read-side guard removes the orphan fall-through
and asserts Coyote reproduces the split-view race of issue #1584; the liveness
model's guard removes the durable backstop and asserts the saga can then stall.

### Running the tier

The Coyote tier is opt-in and held out of the fast development loop and the
deterministic CI step. Every model and guard test is tagged `[Category("Coyote")]`.

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj -c Release --filter "Category=Coyote"
```

CI runs the tiers in the order normal -> coyote -> chaos; the Coyote tier is
excluded from the deterministic build step and from coverage. See the
"Coyote concurrency tier" section of
[`.github/instructions/testing.instructions.md`](../../.github/instructions/testing.instructions.md)
for the tier policy and the procedure for adding a new model.

## The property catalogue

A model only checks what it asserts, so "verified" is bounded by the
completeness of the property set. The protocol's full correctness contract is
enumerated as a catalogue, kept aligned name-for-name with the TLA+ spec below.

Safety properties:

- **AllOrNothing** - within one saga every key resolves identically for a
  snapshot reader; never a split view.
- **VisibilityMatchesDecision** - a key is observed post-saga exactly when the
  recorded decision is committed (the sharpest safety statement).
- **StrictIsolation** - an in-flight or aborted saga is never surfaced as
  committed.
- **CommitIntegrity** - commit implies every participant acked; abort implies at
  least one nack.
- **LinearizedTerminals** - no leaf applies a commit / abort terminal before the
  registry recorded that decision (decision-before-broadcast).
- **NoMixedTerminals** - a saga never applies a commit terminal on one leaf and
  an abort terminal on another.

Liveness and temporal properties:

- **DecisionDurability** - once terminal, the registry decision never flips.
- **MonotonicVisibility** - once a committed key is observed visible it stays
  visible, even across a reshard.
- **RevisionMonotonic** - the registry revision counter never decreases.
- **Termination** - every saga reaches a terminal decision under a bounded fault
  budget.
- **EveryCommittedKeyReadable** - every committed saga's keys eventually all
  become readable.

Each property has a live model home and a companion guard test. The full
catalogue table - property, plain-language meaning, owning core, encoding, guard
test, and whether it is net-new or cited from a sibling model - is maintained in
the "Property catalogue" section of
[`.github/instructions/testing.instructions.md`](../../.github/instructions/testing.instructions.md),
together with a gap analysis confirming every catalogued property has a home.

### Liveness under a cooperative harness

Because real `Task`/`await` is not controlled, there is no fair infinite
schedule for a temperature-style Coyote liveness monitor. Liveness is instead
encoded as **bounded progress**: a finite fault budget (drops, duplicates,
restarts) encodes the fairness assumption that faults do not happen forever; once
the budget is exhausted the transport is reliable, so a correct protocol must
converge, and the model asserts the good terminal state is reached within the
bounded step limit. The `Termination` and `EveryCommittedKeyReadable` properties
are checked this way.

## The TLA+ specification

Above the code cores sits a TLA+ specification of the protocol design, checked
exhaustively by TLC over a small bounded instance. It is deliberately abstract -
keys, participant leaves, and a transaction status, with no serialization,
timers, HLC, or WAL - so TLC can enumerate every interleaving of the decision and
broadcast steps.

The spec lives outside the compiled solution under [`spec/`](../../spec/):

| File | What it is |
|------|-----------|
| `AtomicCommit.tla` | The specification: state, actions, safety invariants, liveness properties. |
| `AtomicCommit.cfg` | The TLC model: the bounded instance and the invariant / property list. |
| `Refinement.md` | The refinement note mapping each spec variable and action to its protocol counterpart in the code cores. |
| `README.md` | How to run TLC and the last-checked result. |

The `AtomicCommit.cfg` instance fixes two concurrent sagas over three keys with
overlapping write sets and a bounded reshard orphan step, and checks all seven
safety invariants and all five temporal properties. A clean run enumerates a few
thousand distinct states with no invariant, temporal-property, or deadlock
violation. The spec's invariant names are the same names used by the property
catalogue above; the [refinement note](../../spec/Refinement.md) is the mapping
between the two levers.

TLC needs a Java runtime and the TLA+ tools, which the .NET build image does not
carry, so it is **not** a required per-PR check - the specification tracks the
protocol design rather than any single code change. Run it locally when the
protocol design changes; the procedure and the CI decision are in
[`spec/README.md`](../../spec/README.md).

## Why three levers

The three verification layers are deliberately complementary and cross-checked:

- **Cores + Coyote models** check the *code* under adversarial interleavings, so
  a proven property is a property of the exact production logic.
- **The property catalogue** bounds *what* is checked, enumerating the full
  safety and liveness contract so no property is silently unverified.
- **The TLA+ spec** checks the *design* independently of the implementation
  language, and its refinement note ties the abstract invariants back to the
  code cores name-for-name.

A gap in any one lever is visible from the others: a catalogued property with no
model home, or a spec invariant with no matching catalogue entry, is a tracked
discrepancy rather than a silent hole.

## Related

- [Atomic Writes](atomic-writes.md) - the runtime `SetManyAtomicAsync` surface
  and saga whose protocol this verifies.
- [Online Reshard](online-reshard.md) - the online shard-count migration whose
  shadow-forwarding safety Phase 3 verifies.
- [Consistency](consistency.md) - the public consistency guarantees these
  properties underpin.
- [Chaos Tests](chaos-tests.md) - the end-to-end integration contract that
  exercises the same guarantees against a live cluster.
- [Verified Atomic Commit sample](../../samples/VerifiedAtomicCommit/README.md) -
  a runnable demonstration of the all-or-nothing visibility these models prove.
