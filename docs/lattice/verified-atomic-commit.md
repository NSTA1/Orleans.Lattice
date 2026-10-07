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

## Scope: the single-cluster protocol and the replicated half, checked separately

Most of this document verifies the **single-cluster** protocol: one
coordinator, one tree's registry, and that cluster's own leaves. The
cross-cluster half - a saga's prepared writes and terminals replicated to a peer,
the receiver's per-source-shard terminal tally that holds the peer's view back
until every shard's terminal has arrived, the cross-tree receiver barrier, and a
receiver's bootstrap from a snapshot - is checked by a **separate** TLA+ module,
[`AtomicCommitCrossCluster.tla`](../../spec/atomic-commit/AtomicCommitCrossCluster.tla)
(mapped in its own
[refinement note](../../spec/atomic-commit/RefinementCrossCluster.md)), and by two
Coyote models of the receiver, described under
[The replicated half](#the-replicated-half).

Do not read coverage of either half as coverage of the other. The single-cluster
properties say nothing about a receiver, and the cross-cluster properties are all
claims about the receiver: its inputs (a tally, a barrier, a dial-back to that
barrier, and a transport that may reorder, drop a delivery and duplicate) and its
failure modes are its own. What the cross-cluster check covers, stated
precisely:

- It assumes a source shard's terminal reaches the receiver after that shard's
  prepares, which the built-in shipper's terminal hold provides (issue #4480); a
  bridge built on `IChangeFeed` relies on the feed's own ordering instead (issue
  #4511, fixed by #4519).
- It models every way production loses a record to a peer, each with its fix:
  a WAL retention trim past the shipper (issue #4534) or a batch it cannot
  encode (#4651) takes the peer off the log and withholds its saga records until
  a re-seed (#4577); a removed peer's shipper detaches from the log and re-marks
  its re-seed when it returns (#4652); and the receiver poisons a saga whose
  prepare it gave up on, withholding the saga's terminals until its own re-seed
  (issue #4591, fixed by #4633). The re-seed's drain discards a purged saga's
  leftover buckets and drains a decided one's (#4631), the replay that follows
  withholds a purged saga whole while purges are held (#4533, #4534-B), and no
  export drained while a silo predates the purge hold settles a re-seed (issue
  #4664, fixed by #4666). A tree that stops being replicated on the receiver is
  dropped from the undecided cross-tree barrier its terminal would have reached
  (#4698). A peer decommissioned for good (#4724) releases the origin's
  cross-tree purge holds. On a receiving cluster (#4740, issue #4736) it first
  abandons every undecided barrier from that peer whole, taking no decision, and
  only then settles each tree's pending buckets from the peer by that tree's
  registry, so the order the trees are walked in cannot split an operation
  (issue #4742). No import from the peer lifts its read fence while the peer
  stays decommissioned. Re-added, the peer's barriers are reset and each tree
  comes back as a fresh replica, unreadable until its fresh import (#4701). A peer that needs a re-seed of two trees at once
  re-seeds both. The module assumes an operator eventually re-adds or
  decommissions a detached peer; one left detached for ever keeps its trees'
  cross-tree decisions held at the origin for as long.
  The module checks each loss path in its own variant configurations, one loss
  per behaviour, and both trees off the log at one boundary in their own.
- It takes a cross-tree import's export in steps - open, close, drain - and
  every other import atomically. The export ships a decision row for a saga
  that decided while it ran (issue #4627) and completes from the source WAL any
  saga that decided between its passes (issue #4685, fixed by #4694), so its
  decision read and its rows are of one instant. The receiver's drain installs
  the rows one at a time, but behind a read fence for the whole drain (issue
  #4526, fixed by #4594), so no reader observes a partial import. A cross-tree import records the
  tree's arrival with the receiver barrier and keeps the tree read-fenced until
  the barrier decides (issue #4683, fixed by #4706); the purge hold, the export
  precondition and the boundary its fence waits on close the case where the
  origin purged the tree's half of the operation first (issue #4684, fixed by
  #4716 and #4721). A decided barrier keeps a tombstone of its verdict past its
  retention, so a terminal re-shipped later or an import of a stale export never
  reopens it, and a reader of a tree's barrier index settles each entry at the
  barrier (issue #4730, fixed by #4732); the tombstone drops once the origin's
  cross-tree purge frontier shows the operation purged on every participant
  (issue #4733, #4735).
- It replicates every key. On a peer with a `KeyFilter` or `KeyPrefixes`, the
  shipper drops the filtered prepares but ships every terminal, so all-or-nothing
  holds only over the keys that peer replicates.

The key filter is a scope limit, not a defect. The
bootstrap is modelled as production now ships it: the export carries the
recorded verdict of every saga the origin stores (issue #4481, fixed by #4501),
retained pre-cut records are shipped again and settled against it (issue #4482,
fixed by #4510), and the origin keeps that verdict while a prepare can still be
re-shipped (issue #4508, fixed by #4553). The receiver's integration tests and the cross-cluster chaos suites
in `test/lattice.replication/` remain the evidence for the deployed system
(issue #2324).

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
| `TerminalArrivalTally` | The completeness gate over a saga's per-source-shard terminal arrivals at a receiver: the expected count only grows (a max-merge of the stamped counts), and the per-tree decision mark flips once the distinct arrivals reach it. A terminal with no count is ungated (`IsUngated`) and marks on arrival. | Phase 5 (#1594), ungated test #4436 |
| `CrossTreeReceiverBarrier` | The receiver-side cross-tree barrier: complete only once every wait-set tree's terminal has arrived, one verdict (commit iff every arrival committed), and a wait set that cannot drift between terminals. | #4436 |

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
(the `Microsoft.Coyote.Test` package), except `ShadowedMigrationReadGuard` and
`TerminalArrivalTally`, which their own core unit-test suites cover instead. Each model exercises one core (or a small
group of cooperating cores) under systematically explored orderings of the
protocol's concurrent steps - the prepare fan-out, the registry decision, the
per-leaf terminal broadcast, duplicate terminal re-deliveries, and interleaved
reader probes - and asserts the safety and liveness properties at every step.

Concurrency in these models is **explicit cooperative step ordering**: a model
implements `ICoyoteModel` and advances the protocol's steps itself, and Coyote
drives controlled nondeterminism (`runtime.RandomBoolean()`) to explore the
resulting **choice** space. That is a choice space and not a thread schedule
space, and the distinction is load-bearing rather than pedantic: there is no
`coyote rewrite` pass, real `Task`/`await` is not controlled, and no model in
this repository creates a second controlled operation - no `Task.Run`, no
thread - so the concurrency degree Coyote observes is **zero** and there are no
thread interleavings for it to enumerate. What it does enumerate is every
resolution of the model's own choices, which is why the models encode the
protocol's concurrency as data in the first place: expressed that way it is
fully enumerable without threads. Raising the degree above zero, so that Coyote
also explores genuine operation interleavings, was considered under
[#2319](https://github.com/NSTA1/Orleans.Lattice/issues/2319) and deliberately
not done: it needs a `coyote rewrite` pass over the product assembly, and every
race these models target is already a choice point they explore. The honest fix
was to stop claiming the exploration, which the harness's member names and
remarks now do. The shared
harness is `CoyoteModelHarness`
(`test/shared/Orleans.Lattice.Testing/Coyote/`), whose
`AssertNoViolationInAnyExploredRun` / `AssertViolationFoundInSomeExploredRun` entry points
run a model to a bounded step count over many iterations. Both members are named
for explored **runs** for exactly this reason - an earlier pair named for
interleavings promised a search this tier does not perform.

The models live under `test/lattice/BPlusTree/Coyote/`:

| Model | Core(s) exercised | Phase |
|-------|-------------------|-------|
| `AtomicCommitVisibilityModel` | `AtomicVisibilityGate`, `TxDecisionView`, `TxRegistryDecisionCore`, `ReaderStabilityGate` - including registry call failures injected on the pre-fan-out snapshot, the revision probe, and the disambiguation snapshot (#3641) | Level B |
| `SagaCoordinatorModel` | `SagaCoordinatorCore` | Phase 1 |
| `ReshardMigrationModel` | `MigrationTerminalCore`, `AtomicVisibilityGate`, `TxRegistryDecisionCore` | Phase 3 |
| `AtomicCommitLivenessModel` | The full saga under bounded fault injection | Phase 4 |
| `AtomicCommitInvariantModel` | The full single-saga lifecycle: `SagaCoordinatorCore`, `TxRegistryDecisionCore`, `TerminalDecisionGuard`, `AtomicVisibilityGate` | Phase 6 |
| `ReshardForwardWindowModel` | `AtomicVisibilityGate`, `TxRegistryDecisionCore` - the reshard forward window, where a destination leaf holds a drain-migrated pre-saga value before it carries the concurrent saga's shadow marker | #3117 |
| `SplitPivotAdmissionModel` | `SplitBoundary` - a leaf may only be divided at a key strictly inside its own declared range | #3117 |
| `SpanAdmissionMigrationModel` | `SplitBoundary` - a cross-shard migration import is subject to the same declared-span admission as any other commit | #3117 |
| `MovedAwaySealInheritanceModel` | `SplitBoundary` - a leaf divided from a sealed leaf is born carrying the donor's moved-away seal | #3121 |

The same directory also holds the models of the other verified protocols - the write-ahead log, the distributed lock and the atomic action - which [Verified WAL](verified-wal.md), [Verified Distributed Lock](verified-lock.md) and [Verified Atomic Action](verified-atomic-action.md) document.

### The replicated half

Two further models drive the receiver of a replicated saga, and their properties
are the cross-cluster TLA+ module's, not the catalogue's below:

| Model | Cores driven | Properties asserted |
|-------|--------------|---------------------|
| `CrossClusterReceiverTallyModel` | `TerminalArrivalTally` (including the ungated path), `TerminalDecisionGuard`, `TxRegistryDecisionCore`, `MigrationTerminalCore`, `AtomicVisibilityGate` - a single-tree saga over several source shards, its prepares and terminals delivered in explored orders with lost acks re-delivered | `RAllOrNothing` and `RStrictIsolation` at every reader probe; `RCommittedEventuallyVisible` and `RNoStrandedPrepare` once the stream drains |
| `CrossTreeReceiverBarrierModel` | `CrossTreeReceiverBarrier`, `TerminalArrivalTally`, `TxRegistryDecisionCore`, `MigrationTerminalCore`, `AtomicVisibilityGate` - a cross-tree saga's hand-off (register the delegation, then notify the barrier), the per-tree finalise, and a dial to the barrier that can fail | The same four, across trees |

Their guards remove one fix each - the stamped count, the tally, the per-shard
delivery order, the late-prepare refusal, the whole-wait-set barrier, the
register-before-notify order, and the Indeterminate answer for an undiallable
delegation - and each must report a violation of one named property, checked by
its tag in Coyote's bug report rather than accepted as any violation. Two of
those guards reproduce production as it stood before a fix (issue #4480 before the
shipper's terminal hold, and issue #4448 before #4461), which is why they are
guards rather than fixed-design tests; they now stand as regression checks.

Neither model loses a record: they deliver every prepare and terminal. The loss
paths and their repair - the re-seed, the replay filter and the purge holds -
are grain-level mechanisms rather than pure cores, so the TLA+ module checks
them, in its variant configurations, and real-grain detectors cited in its
refinement note pin each fix.

### Every model ships a non-vacuous guard test

A model that checks a property only has value if the property can actually fail.
Every model therefore ships a companion **guard test** that removes exactly the
one fix the property depends on and asserts Coyote *finds* the resulting
violation (`AssertViolationFoundInSomeExploredRun`). A model with a green fix test and
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

In CI each tier is its own test run, and the runs are packed onto parallel
legs; a leg that carries several runs them deterministic, then Coyote, then
chaos. The Coyote tier is excluded from the deterministic tier and from
coverage. See the
"Coyote concurrency tier" section of
[`.github/instructions/testing.instructions.md`](../../.github/instructions/testing.instructions.md)
for the tier policy and the procedure for adding a new model.

## The property catalogue

A model only checks what it asserts, so "verified" is bounded by the
completeness of the property set. The protocol's full correctness contract is
enumerated as a catalogue, kept aligned name-for-name with the TLA+ spec below.

Safety properties:

- **AllOrNothing** - within one saga a snapshot reader never sees one key at its
  post-saga value and another at its pre-saga value; never a split view.
- **VisibilityMatchesDecision** - a key is observed post-saga only when the
  recorded decision is committed, and pre-saga only when it is not (the sharpest
  safety statement). A key the gate hides because the registry reports
  `Indeterminate` satisfies both: hiding asserts nothing about the saga.
- **StrictIsolation** - an in-flight or aborted saga is never surfaced as
  committed.
- **CommitIntegrity** - commit implies every participant acked; abort implies at
  least one nack.
- **LinearizedTerminals** - no leaf applies a commit / abort terminal before the
  registry recorded that decision (decision-before-broadcast).
- **NoMixedTerminals** - a saga never applies a commit terminal on one leaf and
  an abort terminal on another.

Liveness and temporal properties:

- **DecisionDurability** - once terminal, the registry decision never flips to the
  other terminal, and its row is never retired while a participant still holds an
  undrained prepared bucket (an unset hides a committed value just as a flip does).
- **MonotonicVisibility** - once a committed key has been observed visible it is
  never observed at its pre-saga value at any later point, even across a reshard
  or while the registry declines to report the decision. Being hidden in between
  does not excuse a reversion, so the TLA+ form is stated over the whole
  behaviour rather than over one step.
- **RevisionMonotonic** - the registry revision counter never decreases.
- **Termination** - every saga reaches a terminal decision under a bounded fault
  budget.
- **EveryCommittedKeyReadable** - every committed saga's keys are eventually all
  materialised at their post-saga value on their own leaves, and from then on
  every reader is served that value: they stay readable once the registry
  forgets the decision, and while it declines to report it, because a leaf that
  has applied the terminal defers to its projection (issue #4428).
- **NoStrandedPrepare** - every participant of a decided saga eventually applies
  the saga's terminal, so no prepared bucket is stranded.

In the TLA+ specification all three liveness properties fail on protocol
defects under the fairness the specification asserts, not only when that
fairness is removed. `Termination` alone catches a broadcast that never declares
a fully told saga done; `NoStrandedPrepare` alone catches a compensation fan-out
that skips participants whose prepare failed. `EveryCommittedKeyReadable`
catches a commit fan-out that stops early, which `Termination` misses, but for a
committed saga it coincides with `NoStrandedPrepare`, which catches that defect
too.

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
bounded step limit. The `Termination`, `EveryCommittedKeyReadable` and
`NoStrandedPrepare` properties are checked this way.

## The TLA+ specification

Above the code cores sits a TLA+ specification of the protocol design, checked
exhaustively by TLC over a small bounded instance. It is deliberately abstract -
keys, participant leaves, and a transaction status, with no serialization,
timers, HLC, or WAL - so TLC can enumerate every interleaving of the decision and
broadcast steps.

The spec lives outside the compiled solution, as the `atomic-commit` module under [`spec/`](../../spec/README.md), in [`spec/atomic-commit/`](../../spec/atomic-commit/README.md):

| File | What it is |
|------|-----------|
| `AtomicCommit.tla` | The specification: state, actions, safety invariants, liveness properties. |
| `AtomicCommit.cfg` | The TLC model: the bounded instance and the invariant / property list. |
| `mutations/` | Deliberate defects - at least one per checked property and at least one perturbing each protocol action - each of which must make its paired property fire (see [`spec/atomic-commit/mutations/README.md`](../../spec/atomic-commit/mutations/README.md)). |
| `Refinement.md` | The refinement note mapping each spec variable and action to its protocol counterpart in the code cores. |
| `AtomicCommit.manifest.json` | The counts the formal gates assert for this module, and where its mutations and refinement note live. |
| `README.md` | What is modelled, the last-checked result, and the module's counts table. |

The `AtomicCommit.cfg` instance fixes two concurrent sagas over three keys with
overlapping write sets and a bounded reshard orphan step, and checks all seven
invariants (the type invariant `TypeOK` plus the six safety invariants of the
catalogue above) and all six temporal properties. A clean run enumerates a few
tens of thousands of distinct states with no invariant, temporal-property, or
deadlock violation. The spec's invariant names are the same names used by the property
catalogue above; the [refinement note](../../spec/atomic-commit/Refinement.md) is the mapping
between the two levers. It maps every property the cfg checks to the core or
production seam that plays its protocol role, with the test that would detect a
regression there, and names `TypeOK` - a type-only well-formedness check with no
production counterpart - as its one reasoned exclusion.
`RefinementPropertyCoverageTests` fails the build when a checked property is
neither mapped nor excluded.

TLC **is** run per PR. `TlcModelCheckTests` (`test/lattice/Formal/`, tagged
`[Category("Tlc")]`) shells out to TLC from the ordinary deterministic test tier,
and CI provisions a Java runtime and a digest-pinned `tla2tools.jar` for it. The
fixture checks that the base specification holds and that each of the thirteen
checked properties of `AtomicCommit` (and every property of every other module) fires under its paired mutations in that module's mutation directory while
staying clean against the unmutated specification, so a property weakened until it
can no longer fail breaks the build instead of passing vacuously. Locally the
fixture skips when the toolchain is absent; under CI a missing toolchain fails it.
Run TLC by hand when iterating on the protocol design; the procedure and the CI
decision are in [`spec/README.md`](../../spec/README.md), which also describes the module layout the gates discover every specification by.

The same directory holds the cross-cluster module,
[`AtomicCommitCrossCluster.tla`](../../spec/atomic-commit/AtomicCommitCrossCluster.tla),
which instances `AtomicCommit` for the origin cluster and specifies the
receiver: replication of each prepare and terminal over a transport that may
reorder, drop a delivery and duplicate; every way production loses a record to
a peer, with its re-seed; the receiver's per-source-shard tally, including the
ungated legacy path; the cross-tree receiver barrier and the registry's
delegation to it, with an undiallable barrier answering Indeterminate; the two
delegation maps' disjointness; and a receiver's bootstrap from a snapshot. Its
properties - `RAllOrNothing`, `RStrictIsolation`, `RLinearizedTerminals`,
`DelegationsDisjoint`, `RMonotonicVisibility`, `RCommittedEventuallyVisible` and
`RNoStrandedPrepare` - are claims about the receiver only, each paired with its
own mutations under `spec/atomic-commit/mutations-cross-cluster/` and mapped in
[`RefinementCrossCluster.md`](../../spec/atomic-commit/RefinementCrossCluster.md),
whose abstraction gaps state what it does not cover.

## Why three levers

The three verification layers are deliberately complementary and cross-checked:

- **Cores + Coyote models** check the *code* under adversarial interleavings, so
  a proven property is a property of the exact production logic.
- **The property catalogue** bounds *what* is checked, enumerating the full
  safety and liveness contract so no property is silently unverified.
- **The TLA+ spec** checks the *design* independently of the implementation
  language, and its refinement note ties each checked property back, by name, to
  the core or production seam that implements it (`TypeOK` excepted, with its
  reason).

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
