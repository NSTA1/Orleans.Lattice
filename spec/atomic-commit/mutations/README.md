# Specification mutations

Each `.mutation` file here describes a deliberate defect in
[`../AtomicCommit.tla`](../AtomicCommit.tla), paired with exactly one property
it must make TLC report as violated. Together they answer a question the base
specification cannot answer about itself: **does this property actually fire?**

Every property in [`../AtomicCommit.cfg`](../AtomicCommit.cfg) has at least
one, and `SpecMutationCatalogueTests` fails if that ever stops being true. Every
protocol action in the specification's `Next` relation is also perturbed by at
least one, which answers the question for the refinement note's action rows
rather than its property rows: **would the spec notice production deviating from
this step?** `SpecActionMutationCoverageTests` fails if an action goes unpaired
(issue #2322).

## Why

The atomicity audit (epic #2299) found verification artefacts that were green
because they could not reach the state they were named for. A Coyote model whose
harness never scheduled the interleaving it claimed to explore. A refinement row
asserting a correspondence its abstraction could not express. An invariant over a
variable no action ever set to the value that would violate it. None of them
failed. All of them read exactly like verification that works.

That is the whole problem: **an artefact that asserts a property it is
structurally unable to check is indistinguishable, from the outside, from one
that checks it.** Passing tells you nothing, because passing is what both cases
do. The only thing that separates them is a demonstration that the artefact
*can* go red, and the only honest form of that demonstration is a mutation that
makes it do so.

Issue #2323 is the recommendation. This directory is it.

## Each mutation is a controlled experiment, not a red run

A mutation is checked in **two arms** against the same generated
single-property cfg:

| Arm | Module | Required outcome |
| --- | --- | --- |
| Control | the unmutated base | clean |
| Treatment | the generated mutant | violates the named property |

Asserting only the second arm would repeat the mistake this directory exists to
catch. A mutant can go red for reasons that have nothing to do with the property
under test: a typo that makes the module unparseable, an accidental
out-of-domain variable, a cfg naming the wrong thing. A one-armed test cannot
distinguish "this property caught the defect" from "something went wrong". The
control arm is what turns a red mutant into evidence, and it is also the
standing proof that the harness is not vacuous, since the same property, the
same cfg and the same machinery produce green on one input and red on the other.

## Mutations are generated from the base, never checked in as copies

The obvious design is to check in a full mutated copy of the module beside the
base. That was the proof of concept, and it has a defect that only surfaces
months later: an edit to `AtomicCommit.tla` does not propagate to the copies,
nothing detects that it did not, and a mutant that has drifted far enough has
quietly stopped being evidence about the base while still passing. That is the
audit's own failure shape, reintroduced by the fix for it.

A test comparing mutant against base could *detect* that drift. Deriving the
mutant from the current base at run time makes it **unexpressible**, which is
strictly better, because there is no second copy to fall behind. So a
`.mutation` file stores only the difference, as anchored edits, and
`SpecMutation.Apply` requires each anchor to match the current base **exactly
once**:

- **zero matches** means the anchored region was edited, so the mutation has
  genuinely drifted and must be re-derived. The failure names the mutation, the
  edit and the anchor text, and arrives in milliseconds without running TLC.
- **more than one match** means the anchor is ambiguous and the edit could land
  somewhere unintended. Widen it with surrounding context.
- an edit to the base that does not touch an anchored region is absorbed
  silently and correctly, which is what stops this being a tax on every change
  to the specification.

The cfg is generated too, from the target property plus the base cfg's
`CONSTANTS` block. That removes a specific trap the audit hit: a cfg that
*prepended* a property to the base list instead of replacing it produced six
satisfiability branches rather than two, and reported two violations attributed
to the wrong properties. It read as a confident result.

## File format

Deliberately dull. `KEY: value` metadata, then one or more
`--- FIND` / `--- REPLACE` / `--- END` blocks holding verbatim TLA+ text.
Before the first block, a line starting with `#` is a comment and a blank line
is skipped; every mutation here opens with such a comment, naming the property
it pairs with and the defect it stands for. Anything cleverer would make a
mutation harder to review than the specification it mutates, and a mutation
nobody can review is not evidence.

```
MODULE: TerminationNoFairness
TARGET: Termination
CLASS: Temporal
SUMMARY: drops the weak-fairness conjunct from Spec, so a saga may stall forever

--- FIND
Spec == Init /\ [][Next]_vars /\ \A t \in Txns : WF_vars(TxProgress(t))
--- REPLACE
Spec == Init /\ [][Next]_vars
--- END
```

`MODULE` must be a valid TLA+ module name. The harness writes the generated
mutant to `<MODULE>.tla`, so the filename follows from it rather than the other
way round; keeping the two the same is a convention for readability, not a
constraint TLA+ imposes. `CLASS` is `Invariant`, `Action` or `Temporal`,
and selects which violation banner the harness expects.

`PERTURBS:` is optional: a comma-separated list of the actions in `Next` whose
definitions the mutation edits. It is a claim, so it is checked rather than
trusted - for each named action, at least one edit must anchor inside its
definition AND change something other than TLA+ comments there, so an edit that
only annotates the action while the real change lands elsewhere does not count -
and it is what the action-coverage gate counts. Leave it off a mutation that
edits a read definition, the fairness assumption, or that splices in an action
the protocol does not have.

`DEADLOCK: off` is optional, and `off` is its only accepted value. It runs both
arms of the experiment with TLC's `-deadlock` switch, which turns the deadlock
check off. A mutant can be left with no enabled action, and TLC then reports
`Deadlock reached` before it evaluates a temporal target (those are checked only
after the state search completes), so the run would be about the deadlock rather
than the property; with the check off TLC treats the stuck state as stuttering
forever, which is what the property then has to reject. It is a claim too: the harness runs a declaring mutant a
third time with the check left on - and with liveness checking deferred to the end of the search (`-lncheck final`), so a periodic mid-search liveness check on a slow machine cannot report the target first - and requires a deadlock, so the switch cannot
sit on a mutation that does not need it. Two do. `TerminationCompletionOverAllKeys`'s
defect IS the stuck state. `MonotonicVisibilityOrphanLosesTerminal`'s mutant has
always deadlocked, but while `MonotonicVisibility` was a single-step property
TLC reported the property during its state search, before it reached the
deadlock; the behaviour-level form is checked after the search completes, so
the deadlock now comes first unless the check is off.

`BOUNDS:` is optional: comma-separated `Name = value` assignments to bounds the
specification declares or defines (`BOUNDS: MaxFaults = 0, MaxHlc = 1`). They are
written into the mutant arm's cfg only, so the mutant runs on the smaller instance
while the control arm still checks the module's own bounds. A mutant has to show
one counterexample, not hold over the whole instance, and a temporal mutant pays
for the full state graph before TLC reports it, so the smallest instance that
still exhibits the violation is the cheapest honest one. The header cannot weaken
the experiment: the control arm, which decides that the property holds on the
base, never sees it; a bound below the violation leaves the mutant clean and fails
the pairing gate; and a name the specification does not have, which TLC would
silently ignore, is refused by `SpecMutationCatalogueTests`. Mutations that build
an identical control arm (same module, target and options) share one run of it
within a test process.

## `TypeOK` rides along in every cfg

Every generated cfg checks `TypeOK` alongside the target. A mutation that
accidentally puts a variable outside its declared domain would otherwise make
the target fire for a reason unrelated to the property, and the run would look
like a successful pairing. With `TypeOK` present, TLC reports `TypeOK` instead,
the banner assertion fails, and the mistake surfaces as a mistake.

`RevisionMonotonicRollback` is the mutation where this matters most: it
decrements the revision counter but deliberately stays inside
`0..(2 * Cardinality(Txns))`, so `TypeOK` still holds and monotonicity is the
only property that can catch it.

## TLC does not name the property for a liveness violation

Worth stating plainly, because it is a real gap rather than an oversight:

| Class | Banner | Names the property? |
| --- | --- | --- |
| Invariant | `Error: Invariant <Name> is violated.` | yes |
| Action property | `Error: Action property <Name> is violated.` | yes |
| Temporal | `Error: Temporal properties were violated.` | **no** |

So temporal targets - `MonotonicVisibility`, `Termination`,
`EveryCommittedKeyReadable` and `NoStrandedPrepare` - cannot have their banner
checked against the property name. `MonotonicVisibility` is a safety property,
but it is stated over whole behaviours with a nested `[]` (a single-step form is
too weak once an observation can be hidden), and TLC reports that form the same
way as liveness. For temporal targets the guard against a misattributed violation is
that the generated cfg names exactly one property, backed by the harness
asserting the violation *count* is one.

## Inventory

Every property is paired, and every protocol action is perturbed (the module's [counts table](../README.md#counts) states how many of each there are).
`Every_property_the_base_model_checks_has_a_mutation` reads the base cfg and
fails if a property is added without a mutation, and
`Every_protocol_action_is_perturbed_by_a_mutation` reads the specification's
`Next` and fails if an action is, so this table cannot silently fall behind.

| Mutation | Property it makes fire | Class | Perturbs | The defect it models |
| --- | --- | --- | --- | --- |
| `TypeOkRevisionRunaway` | `TypeOK` | Invariant | - | a revision bump with no decision behind it, leaving the declared domain |
| `AllOrNothingUngatedProjection` | `AllOrNothing` | Invariant | - | reads bypass the gate, so a mid-broadcast saga is seen as a split view |
| `VisibilityMatchesDecisionTerminalPresence` | `VisibilityMatchesDecision` | Invariant | - | any terminal counts as visible, so an aborted saga's writes surface |
| `VisibilityMatchesDecisionPrepareNotStaged` | `VisibilityMatchesDecision` | Invariant | `PrepareTx` | a prepare is acknowledged without staging its bucket, so a committed key reads pre-saga |
| `VisibilityMatchesDecisionDrainsLivePrepare` | `VisibilityMatchesDecision` | Invariant | `OrphanDrain` | the orphan discard drops a live prepare before its terminal lands |
| `StrictIsolationInflightSurfaces` | `StrictIsolation` | Invariant | - | absence of a decision reads as permission, so in-flight writes are readable |
| `CommitIntegrityIgnoresVotes` | `CommitIntegrity` | Invariant | `DecideTx` | the coordinator commits without consulting the prepare votes |
| `LinearizedTerminalsBroadcastBeforeDecision` | `LinearizedTerminals` | Invariant | `BroadcastStep` | a leaf applies a terminal before the registry records the decision |
| `NoMixedTerminalsPerKeyVote` | `NoMixedTerminals` | Invariant | `BroadcastStep` | each leaf takes its outcome from its own vote, so one saga does both |
| `DecisionDurabilityDecisionFlip` | `DecisionDurability` | Action | - | a recorded terminal decision is overwritten after the fact |
| `DecisionDurabilityEarlyForget` | `DecisionDurability` | Action | `ForgetDecision` | the registry row is retired before every participant has applied its terminal |
| `MonotonicVisibilityOrphanShadows` | `MonotonicVisibility` | Temporal | - | a late reshard orphan shadows the committed projection (the #1584 class) |
| `MonotonicVisibilityOrphanLosesTerminal` | `MonotonicVisibility` | Temporal (`DEADLOCK: off`) | `ShadowForwardOrphan` | an orphan lands on a leaf with no record of the terminal, then the row is retired (the #2318 class) |
| `MonotonicVisibilityMaskReportsAbsent` | `MonotonicVisibility` | Temporal | `RegistryMask` | a masked registry row is reported as absent rather than indeterminate (the #2320 experiment) |
| `MonotonicVisibilityPurgeAfterMask` | `MonotonicVisibility` | Temporal | - (adds an action) | a masked row is purged while a prepared bucket is still resident, so a key goes post, hidden, then pre |
| `RevisionMonotonicRollback` | `RevisionMonotonic` | Action | - | a stale registry write replays over a newer one |
| `TerminationNoFairness` | `Termination` | Temporal | - | no fairness, so a saga may stall forever |
| `TerminationCompletionOverAllKeys` | `Termination` | Temporal (`DEADLOCK: off`) | `BroadcastStep` | completion is tested over the whole keyspace, so a fully told saga is never declared done |
| `EveryCommittedKeyReadableIndeterminateFirst` | `EveryCommittedKeyReadable` | Temporal | - | the gate tests its Indeterminate arm ahead of the orphan guard, so a late orphan hides a committed key for as long as the registry is masked (#4428) |
| `EveryCommittedKeyReadableCommitFanOutStops` | `EveryCommittedKeyReadable` | Temporal | `BroadcastStep` | the commit fan-out stops after its first participant, so a committed key is never materialised |
| `NoStrandedPrepareCompensationSkipsNacked` | `NoStrandedPrepare` | Temporal | `BroadcastStep` | the compensation fan-out skips participants whose prepare failed, stranding their buckets |

### Some mutations break more than their target, and that is fine

The visibility properties are deliberately interrelated:
`VisibilityMatchesDecision` is the sharpened form of both `AllOrNothing` and
`StrictIsolation`, so a defect in the gate tends to trip several at once.
Because each generated cfg names only its target, that overlap cannot mislead
the harness. It does mean a mutation is evidence that its target *catches* the
defect, not that its target is the *only* property that would.

Where the overlap would hide something, the pairing is chosen to expose it
instead, and each claim below was measured with TLC rather than argued.

- `RevisionMonotonicRollback` stays inside the `TypeOK` domain, as above.
- Each liveness property has a protocol-level mutation, under the asserted
  fairness. `TerminationCompletionOverAllKeys` fires `Termination` alone among
  the three, `NoStrandedPrepareCompensationSkipsNacked` fires `NoStrandedPrepare`
  alone, and `EveryCommittedKeyReadableCommitFanOutStops` leaves `Termination`
  clean. That last one also fires `NoStrandedPrepare` and cannot avoid it: in
  this model a committed key materialises exactly when its terminal lands, so
  for a committed saga the two properties coincide, which the spec states
  rather than hides.
- `EveryCommittedKeyReadableCommitFanOutStops` leaves every invariant clean,
  which is what shows its property is not entailed by one:
  `([]VisibilityMatchesDecision /\ []AllOrNothing) => EveryCommittedKeyReadable`
  is violated on it. The pairing it replaced
  (`EveryCommittedKeyReadableStaleProjection`, a projection switched on an abort
  terminal) also broke `VisibilityMatchesDecision` and `AllOrNothing`, so that
  implication held on it, and an earlier revision of this section was wrong to
  say the pair avoided ambiguity. The property it paired with was also too weak
  to need the evidence: it asked for every key to be eventually observed
  post-saga or hidden, which `VisibilityMatchesDecision` forces in every
  committed state.
- `MonotonicVisibilityPurgeAfterMask` is the experiment that separates the
  current `MonotonicVisibility` from the single-step form it replaced: the old
  form model-checks clean on it, because no single step goes from post to pre.

### Protocol mutations and added-action mutations

This distinction matters, and reading past it would reproduce in miniature the
overclaim this whole directory exists to prevent.

Most mutations change something the protocol actually does: a guard, an
action's effect, a gate definition, a projection, or the fairness assumption.
For those, the pairing shows the property constrains the modelled protocol -
weaken the protocol and the property notices. Mutations that carry `PERTURBS:`
change a protocol action itself, which is the claim the refinement note's
action rows rest on.

`MonotonicVisibilityPurgeAfterMask` adds an action, `PruneExpired`, and for a
different reason from the added-action mutations below. It is a production
event the base deliberately leaves out: the lazy purge of an aged-out tombstone
with no ordering against the participants. Its property is not unfalsifiable -
protocol-level mutations already make it fire - so what this mutation
establishes is narrower: that the property, as now stated, would report that
hazard if the model reached it. It says nothing about whether the base reaches
it (it does not; see the retention-window gap in
[`../Refinement.md`](../Refinement.md)).

The remaining added-action mutations do not perturb the protocol at all.
`TypeOkRevisionRunaway`, `DecisionDurabilityDecisionFlip` and
`RevisionMonotonicRollback` splice a brand-new action into `Next`
(`RevisionRunaway`, `DecisionFlip`, `RevisionRollback`) that models no step of
the protocol. They do this because the properties they target are
**unfalsifiable by any behaviour of the base module**: `decision` is written by
exactly one action, `DecideTx`, under a guard of `phase[t] = "prepared"` that
the same action immediately leaves, and no action ever re-enters `"prepared"`,
so it is assigned at most once per saga; `revision` is only ever incremented,
once by `DecideTx` and at most once more by `ForgetDecision`, which sets the
`forgotten[t]` flag its own guard requires to be clear. So
`DecisionDurability`'s two no-flip conjuncts, `RevisionMonotonic` and `TypeOK`'s
`0..(2 * Cardinality(Txns))` revision conjunct cannot fail however the base is
scheduled.

`DecisionDurability`'s third conjunct is the exception. It forbids retiring a
decision's row while a written key has not yet applied its terminal, and
`ForgetDecision` is a protocol action whose drain guard is all that prevents
it. `DecisionDurabilityDecisionFlip` exercises only the flip half, so for a
while the retirement half was covered only at the implementation level, by
`AtomicCommitInvariantCoyoteTests.Forgetting_the_decision_before_every_leaf_drained_violates_decision_durability`
(see [`../Refinement.md`](../Refinement.md)). `DecisionDurabilityEarlyForget`
now covers it here too, by dropping that guard, and it is a protocol-level
perturbation rather than an added action.

For those added-action classifier mutations the two-arm experiment therefore
establishes something weaker than it does for protocol-level mutations. It
establishes that the property is
**well-formed**: that it is not a tautology, that it says what its name says,
and that TLC would report it if the state it forbids became reachable. It does
**not** establish that the property currently constrains the protocol, because
nothing in the protocol can violate it.

That is not a defect in the mutations, and it is not a reason to "fix" them by
hunting for a protocol-level perturbation instead - for the flip half and the
two revision properties there is none, which is precisely the point. It is a
limit on what may be concluded, and issue #2323's second supporting control
names it directly: such a result is a classifier,
never evidence of reachability. A future revision that lets a saga re-enter
`"prepared"` - a retry, a re-prepare, a recovery path - would make these classifier
properties load-bearing, and the mutations are the standing check that they
would be ready to fire on the day it does.

## Every generated cfg names exactly one target, in one block

Issue #2323 records an authoring mistake made during the audit itself: a mutation
cfg was written by **prepending** a property to the existing `PROPERTIES` block
instead of replacing it. The result was six conjuncts where one was intended, the
same property named in two blocks, and two separate violations misattributed to
one property - a confidently wrong headline that took four independent routes to
overturn. The tell was in the log the whole time: the satisfiability report showed
six branches where the intended cfg shows two.

The issue asks for two things in response, and the harness does both. Each cfg is
written **whole** by `SpecMutation.BuildConfig` rather than edited, so the fault
has no way in; and `Every_generated_config_names_its_target_once_in_a_single_block`
**asserts** it anyway, for every mutation, without a JVM.

The assertion is not redundant with the construction. "Unreachable by
construction" and "checked" are different states, and only the second survives
somebody refactoring the generator back into an edit. Distrusting exactly that
distinction is why this directory exists, so it would be incoherent to owe this
one to a code-reading. The gate checks the block headers separately from the
parsed names, because a repeated block header accumulates into a single list when
parsed and is invisible there.

## Running one by hand

The modules are generated, so there is nothing here to hand to TLC directly.
Run the fixture, which writes each arm into a scratch directory (TLC emits state
files beside the module it checks):

```
dotnet test test/lattice/Orleans.Lattice.Tests.csproj \
  --filter "FullyQualifiedName~TerminationNoFairness"
```

It needs a JVM and `tla2tools.jar`; see
[how the fixture finds them](../../README.md#how-the-nunit-fixture-finds-the-toolchain),
and note that it fails rather than skips under `GITHUB_ACTIONS`.
