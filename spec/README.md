# TLA+ specification of the Orleans.Lattice atomic-commit protocol

This directory holds a TLA+ specification of the distributed atomic-commit
protocol - the multi-leaf prepare / commit / abort saga, the per-tree
transaction-registry decision, and reader visibility - together with a TLC
model configuration that checks its safety and liveness properties
exhaustively over a small bounded instance.

It is the deliverable of level-C epic #1588, Phase 7 (#1596), lever (c): a
design-level specification above the code, checked by TLC, with a refinement
note ([`Refinement.md`](Refinement.md)) mapping it to the extracted Coyote
protocol cores.

## Why this lives here (and not in the solution)

The specification is intentionally **outside** the compiled solution
(`Orleans.Lattice.slnx`). It is not C#; it is checked by TLC, which needs a
Java runtime and the TLA+ tools. TLC **is** run per PR, but through an NUnit
fixture that shells out to it rather than by building anything here - see the
"CI decision" section below. This directory contains only `.tla`, `.cfg`,
`.mutation`, and `.md` files; nothing here is built by `dotnet`.

## Files

| File | What it is |
|------|-----------|
| [`AtomicCommit.tla`](AtomicCommit.tla) | The specification: state, actions, safety invariants, liveness properties. |
| [`AtomicCommit.cfg`](AtomicCommit.cfg) | The TLC model: the bounded instance and the invariant / property list to check. |
| [`mutations/`](mutations/) | One deliberate defect per checked property, each of which must make that property fire. See [`mutations/README.md`](mutations/README.md). |
| [`Refinement.md`](Refinement.md) | The refinement note: each spec variable, action and checked property mapped to its protocol counterpart in the code cores, or excluded with a reason. |
| `README.md` | This file. |

## What is modelled

The specification is deliberately abstract: keys, participant leaves, and a
transaction status. There is no serialization, no timers, no HLC, no WAL - the
issue scopes those out. The abstraction is chosen so TLC can enumerate every
interleaving of the protocol's decision and broadcast steps.

- **Coordinator** (`PrepareTx`, `DecideTx`, `BroadcastStep`) - the saga:
  prepare fan-out into hidden per-leaf pending buckets, a single terminal
  decision, then the per-leaf terminal broadcast one leaf at a time.
- **Transaction registry** (`decision`, `forgotten`, `revision`) - the single
  tree-wide commit / abort decision, whether its row has since been retired,
  and the monotonic revision. Recording the decision *before* the broadcast is
  the linearization point. `ForgetDecision` models the saga's post-fan-out
  cleanup: it retires the row (so `RegistryView` reverts to in-flight) without
  changing the outcome, and only once every participant has drained.
  `RegistryMask` models the registry declining to report a row it still holds
  (`masked`, production's `Indeterminate`), with no ordering against the
  participants at all.
- **Reader visibility** (`Observed`, `SurfaceViaGate`) - the per-key gate that
  resolves how a read of a key carrying a pending mutation is answered, resolved
  against one registry view so a saga is all-or-nothing visible. A read has
  three outcomes, not two: the post-saga value, the pre-saga value, or
  `"hidden"` when the registry has declined to report the decision - the gate's
  `Indeterminate` arm, which asserts nothing about the saga and so is never
  treated as a pre-saga read.
- **Reshard / migration** (`ShadowForwardOrphan`, `OrphanDrain`) - an abstract
  online shard-split step that shadow-forwards a stale prepared write onto a
  leaf that already applied the saga's terminal, and the leaf's discard of
  that late bucket. Until it is discarded, the gate's orphan guard
  (`AlreadyTerminal`, which `SurfaceViaGate` reads) makes the bucket fall
  through instead of shadowing the authoritative value (the #1584 class at
  design level).

## Properties checked

Safety invariants (checked at every reachable state):

| Invariant | Meaning |
|-----------|---------|
| `TypeOK` | State stays well-typed. |
| `AllOrNothing` | Atomicity: within one saga a snapshot reader never sees one key post-saga and another pre-saga - never a split view. A hidden key is compatible with either. |
| `VisibilityMatchesDecision` | A key is post-saga-visible only when the tree-wide decision is committed, and pre-saga-visible only when it is not (sharpest safety statement; implies `AllOrNothing` and `StrictIsolation`). |
| `StrictIsolation` | An in-flight or aborted saga is never surfaced as committed. |
| `CommitIntegrity` | Commit implies every participant acked; abort implies at least one nack. |
| `LinearizedTerminals` | No leaf applies a commit / abort terminal before the registry recorded that decision (decision-before-broadcast). |
| `NoMixedTerminals` | A saga never applies commit on one leaf and abort on another. |

Action / temporal properties:

| Property | Meaning |
|----------|---------|
| `DecisionDurability` | Once terminal, the registry decision never flips to the other terminal, and its row is never retired while a written key has not yet applied its terminal (its prepared bucket is still undrained). |
| `MonotonicVisibility` | Once a key is post-saga-visible it never reverts to its pre-saga value (even across a reshard, or while the registry declines to report the decision - going hidden is not a reversion). |
| `RevisionMonotonic` | The registry revision counter never decreases. |
| `Termination` | Every saga terminates (under weak fairness of saga progress). |
| `EveryCommittedKeyReadable` | Every committed saga's keys eventually read post-saga, or hidden while the registry declines to report the decision - never settling at the pre-saga value. |
| `NoStrandedPrepare` | Every participant of a decided saga eventually applies the saga's terminal. Unlike `Termination`, it fails on a protocol defect under the fairness the spec asserts. |

## The bounded instance

`AtomicCommit.cfg` fixes a concrete instance:

- 2 concurrent sagas (`t1`, `t2`),
- 3 keys (`k1`, `k2`, `k3`),
- `t1` writes `{k1, k2}`, `t2` writes `{k2, k3}` - 2 participants each,
  overlapping on `k2`,
- a bounded reshard orphan step per key (used-once budget).

**What the `k2` overlap does and does not buy.** The two sagas do share a key,
and TLC does interleave their two lifecycles. What the overlap does *not* do is
exercise any *cross-saga* claim, because every property above is stated
per-saga - bar `TypeOK` and `RevisionMonotonic`, which
constrain only the variables' domains and the shared revision counter: each
quantifies `\A t \in Txns` and then resolves that saga's keys against that
saga's own `decision[t]`, `terminal[t]` and `pend[t]`. No property relates
`t1`'s state to `t2`'s, and no protocol action's guard does either: every
variable but the shared revision counter is indexed by saga, so each saga's
properties are checked exactly as they would be for that saga alone. Read the
overlap as naming a shared key, not as evidence that concurrent sagas
contending for one key have been checked.

Such a property is *unexpressed here*, not inexpressible, and the price is
worth stating rather than hand-waving. The natural one is
`NoConcurrentPreparedWriters`: at most one saga holds a pending bucket on a key
at a time. It would strengthen the design rather than mirror the code: the
implementation takes no per-key admission lock, so two sagas writing one key
each stage their own per-transaction pending bucket on the leaf, and
overlapping sagas are resolved pairwise by last-writer-wins ("Ordering across
distinct sagas" in [atomic writes](../docs/lattice/atomic-writes.md)). As an
invariant alone it is false here for the same reason: `PrepareTx` has no
cross-saga precondition and both sagas may hold a bucket on `k2`. Making it
true costs one conjunct on `PrepareTx` requiring no other saga's bucket on any
key it writes.

That one conjunct is not free, and the reason is specific rather than general
caution: `ShadowForwardOrphan` may re-install a bucket on `k2` after `t1` is
done, `OrphanDrain` is **deliberately not fair** (see the fairness note in
`AtomicCommit.tla`), so a behaviour exists in which that orphan is never drained
and the gated `PrepareTx(t2)` is never enabled - which would break `Termination`.
Adding the property therefore also means deciding whether `OrphanDrain` becomes
fair, which weakens the "every safety property holds whether or not the orphan
fires" guarantee that its unfairness currently buys. Two coupled changes and a
re-run of TLC, not one conjunct.

The deeper limit is that the model abstracts values away entirely: even with the
conjunct in place, "which of two committed writers does a reader of `k2` observe" is
not a question this instance can ask, because `Observed` returns which side of
the saga a read lands on rather than a value. A cross-saga *visibility* property needs a value
domain, which is a larger change than that conjunct.

To widen the instance, declare the new model values on the `CONSTANTS` line of
`AtomicCommit.tla`, extend `TxWrites`, `Txns`, and `Keys` there, and add the
matching model-value assignments to `AtomicCommit.cfg`. The state space stays
small for the default instance (several thousand distinct states), but no
protocol action's guard refers to another saga, so the sagas' reachable states
combine as a product: every saga added multiplies the count by what one saga
alone can reach, and larger instances grow quickly.

## Claims in this directory that open issues own

One issue that is still **open** owns claims made in this directory.

- **#2319** owns raising the Coyote harness's concurrency degree above zero.
  #2325 corrected the two member names that promised schedule exploration the
  harness does not perform; making the exploration real is #2319's.

The three that used to appear here - **#2320** (the unordered decision-masking
action, now `RegistryMask`), **#2325** (documentation and API overclaims in the
atomicity surface, including the `k2` overlap discussed above) and **#2333**
(the `DecisionDurability` prose and its refinement seam) - are all resolved.

The boundary is recorded in full under
[territory owned by other open issues](Refinement.md#territory-owned-by-other-open-issues)
in the refinement note, which is where a census of that note's Detector column
meets it. Read it before filing any of these findings as new.

## How to run TLC

You need a Java runtime (JDK/JRE 11+) and `tla2tools.jar` from the
[TLA+ tools releases](https://github.com/tlaplus/tlaplus/releases).

```bash
# From this directory, with tla2tools.jar on hand:
java -cp /path/to/tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

On Windows PowerShell:

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

A clean run ends with `Model checking completed. No error has been found.`
and reports zero invariant or temporal-property violations and no deadlock.

TLC keeps its working files in a `states/` directory beside the specification
by default, and git does not ignore `spec/states/`: delete it after a run, or
pass `-metadir` with a directory outside the repository. The NUnit fixture
below avoids it by running every model in a scratch directory.

### How the NUnit fixture finds the toolchain

`TlcModelCheckTests` (see [CI decision](#ci-decision)) locates the same two
pieces itself. It reads `tla2tools.jar` from the `TLA_TOOLS_JAR` environment
variable (an absolute path), falling back to `tools/tla2tools.jar` at the
repository root (a gitignored path), and it runs `java` from `JAVA_HOME/bin`,
falling back to the first `java` on `PATH`. The CI workflows download the pinned
tla2tools v1.7.4 release to `tools/tla2tools.jar` and verify its SHA-256 digest
before the tests run.

### Confirming the model is non-vacuous

The invariants are load-bearing, not trivially true. To convince yourself,
temporarily weaken `BroadcastStep` so a leaf may apply a commit terminal while
the saga is still in `phase = "prepared"` (i.e. before `DecideTx` records the
decision): admit `"prepared"` to its phase guard and make the terminal kind
commit for every phase but `"aborting"`, which is what
[`mutations/LinearizedTerminalsBroadcastBeforeDecision.mutation`](mutations/LinearizedTerminalsBroadcastBeforeDecision.mutation)
does. Widening the guard alone is not enough: the unchanged kind rule then
applies an abort terminal, which TLC reports as
`Invariant LinearizedTerminals is violated` rather than as a split view. With
both changes, TLC reports `Invariant AllOrNothing is violated` with a
counterexample trace: a reader observes one key at its post-saga value while a
sibling key still shows pre-saga - exactly the split view the linearization
point exists to prevent. Revert the weakening to restore the clean run.

## The refinement note is gated for staleness, not for truth

[`Refinement.md`](Refinement.md) names production C# symbols in backticks. A
rename or deletion in `src/lattice/` would leave those references pointing at
code that no longer exists, while the note went on reading as authoritative.
`RefinementMappingStalenessTests` (in `test/lattice/Formal/`) closes that gap:
it parses the backticked `Type.Member` references out of the three mapping
tables and fails if any of them no longer resolves in `src/`. It is
toolchain-free, needs no JVM, and runs in the deterministic tier in
milliseconds. It skips the `Detector` column, whose test names
`RefinementDetectorMappingTests` resolves against `test/` instead.

Resolution handles three forms deliberately: an ordinary type member, a nested
type, and a **partial-class file suffix**. The mapping tables rely on the first
and the last - `ShardRootGrain.TxTerminal` is not a member at all but the file
`src/lattice/BPlusTree/Grains/ShardRootGrain.TxTerminal.cs`. A checker that
assumed `Type.Member` would report that (and `ShardRootGrain.Split` and
`BPlusLeafGrain.PendingTx`) as missing and be wrong. The gate reads source text
rather than using reflection, because several mapped symbols are `private` or
`internal` and the file-suffix form has no reflective existence at all.

**What a green run does and does not mean.** The gate checks that each named
symbol **exists**. It does not check that the row's claim about that symbol is
**true**. A row can name a dozen perfectly resolvable symbols and still assert
behaviour the code does not have; nothing here would notice. Verifying the
behavioural claims is separate work. Do not read a passing run as the note
having been validated, only as the note not naming code that has disappeared.

Bare backticked identifiers are not checked. In these tables they are
indistinguishable from TLA+ variables, spec-level string values, enum members
quoted without their type, and parameter names, so checking them would produce
false alarms; a staleness gate that cries wolf gets suppressed and is then
worse than no gate. The honest cost is that a rename of a symbol the note
mentions only in bare form is not caught.

A second toolchain-free gate, `RefinementPropertyCoverageTests`, checks the
note from the model's side: every property `AtomicCommit.cfg` checks must have
a row in the note's property-mapping table or, with a stated reason, in its
excluded-properties table, and a prose mention alone does not count. Like the
staleness gate, it checks coverage, not the truth of a mapping.

## Last checked

This specification was checked with **TLC 2.19** (tla2tools, rev 5a47802) on a
Temurin 21 JRE when it was added (#1597):

```
Model checking completed. No error has been found.
7649 states generated, 2809 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 17.
```

All seven invariants and all five temporal properties held; no deadlock. That run
predates #2612, which added the `forgotten` variable and the `ForgetDecision`
action and strengthened `DecisionDurability`, so these counts describe the earlier
model, not the current one. After #2320 added `masked` and `RegistryMask` and
#2321 added `NoStrandedPrepare`, a local run on tla2tools v1.7.4 checked all seven
invariants and all six temporal properties over 29,929 distinct states at depth 21
(124,625 generated), clean. The current specification is model-checked in CI by
`TlcModelCheckTests.The_base_specification_holds`, against the tla2tools v1.7.4
release the workflows pin (see [CI decision](#ci-decision)).

## CI decision

TLC **is** run per PR, as an ordinary NUnit fixture
(`test/lattice/Formal/TlcModelCheckTests.cs`) tagged `[Category("Tlc")]`. It
therefore rides the existing test fan-out with no change to the matrix planner:
the `deterministic` tier is the complement of `Chaos` and `Coyote`, so a new
category lands in it automatically, and `test/lattice`'s last shard is a
complement shard, so a new namespace is picked up without editing the shard
config. The workflow provisions a Temurin 17 JDK and a digest-pinned
`tla2tools.jar` before the leg runs.

Every lane that runs .NET tests has to do the same, because the `Tlc` category
is selected by any ordinary filter rather than opted into by name, and the
fixture fails closed when the toolchain is missing. That obligation was implicit
once, and the coverage lane duly ran without the toolchain and failed the whole
core suite at `OneTimeSetUp`. It is now explicit:
`CiTlaToolchainProvisioningTests` (in `test/lattice/Hygiene/`) requires each
workflow that runs tests either to provision the toolchain - pinned to the same
release and digest as the others, and digest-verified - or to carry a
`# tla-toolchain: not-required - <reason>` marker saying why it cannot select
the category.

This reverses an earlier decision recorded here, which is worth stating plainly
rather than quietly overwriting. That decision rested on two premises: that the
.NET build image carries no Java runtime, and that the specification tracks the
protocol *design* rather than any single code change, so gating a PR on it would
buy little marginal signal. The first premise was simply wrong - the GitHub
runner image ships several JDKs, and `actions/setup-java` selects one from the
image cache in a couple of seconds. The second was right about what TLC *was*
being asked to do, and is the part that changed: the fixture no longer only
checks that the specification holds. It checks that each paired **mutant** makes
its property fire - by name for an invariant or action property, and for the
liveness properties by way of a single-property configuration, because TLC
does not name the property in a temporal violation. That is a claim about the
specification's own diagnostic power, and unlike the design it tracks, it
regresses silently the moment somebody weakens a property - which is exactly
the failure the atomicity audit (epic #2299) found four times over.

Each of the thirteen properties is paired with at least one mutation, every
protocol action in `Next` is perturbed by at least one (issue #2322, gated by
`SpecActionMutationCoverageTests`), and each pairing runs as a two-arm
experiment: the generated single-property model must be **clean**
against the unmutated specification and **violated** against the mutant. The
control arm is what makes a red mutant evidence rather than merely a red run,
and it is the standing proof that the fixture is not vacuous. See
[`mutations/README.md`](mutations/README.md).

The local invocation documented above remains supported and is still the fast
path when iterating on the protocol design.

The dev loop does **not** run this category. The Tier 2 filter in
[`.github/instructions/testing.instructions.md`](../.github/instructions/testing.instructions.md)
excludes `Tlc` alongside `AzureStorageEmulator`, for the same reason: a
contributor without the external toolchain should not be blocked. Absence is
handled asymmetrically and deliberately - the fixture skips locally (a visible
`Skipped` count, not `Assert.Inconclusive`, which NUnit counts as neither passed
nor failed nor skipped and which has already produced a false green here) and
**fails** when `GITHUB_ACTIONS` is `true`, because in CI a missing toolchain is a
broken pipeline rather than a missing convenience.
