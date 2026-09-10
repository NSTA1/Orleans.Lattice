# Specification mutations

Each `.mutation` file here describes a deliberate defect in
[`../AtomicCommit.tla`](../AtomicCommit.tla), paired with exactly one property
it must make TLC report as violated. Together they answer a question the base
specification cannot answer about itself: **does this property actually fire?**

Every property in [`../AtomicCommit.cfg`](../AtomicCommit.cfg) has one, and
`TlcModelCheckTests` fails if that ever stops being true.

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
Anything cleverer would make a mutation harder to review than the specification
it mutates, and a mutation nobody can review is not evidence.

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

## `TypeOK` rides along in every cfg

Every generated cfg checks `TypeOK` alongside the target. A mutation that
accidentally puts a variable outside its declared domain would otherwise make
the target fire for a reason unrelated to the property, and the run would look
like a successful pairing. With `TypeOK` present, TLC reports `TypeOK` instead,
the banner assertion fails, and the mistake surfaces as a mistake.

`RevisionMonotonicRollback` is the mutation where this matters most: it
decrements the revision counter but deliberately stays inside
`0..Cardinality(Txns)`, so `TypeOK` still holds and monotonicity is the only
property that can catch it.

## TLC does not name the property for a liveness violation

Worth stating plainly, because it is a real gap rather than an oversight:

| Class | Banner | Names the property? |
| --- | --- | --- |
| Invariant | `Error: Invariant <Name> is violated.` | yes |
| Action property | `Error: Action property <Name> is violated.` | yes |
| Liveness | `Error: Temporal properties were violated.` | **no** |

So `Termination` and `EveryCommittedKeyReadable` cannot have their banner
checked against the property name. For those two the guard against a
misattributed violation is that the generated cfg names exactly one property,
backed by the harness asserting the violation *count* is one.

## Inventory

All twelve properties are paired.
`Every_property_the_base_model_checks_has_a_mutation` reads the base cfg and
fails if a property is added without one, so this table cannot silently fall
behind.

| Mutation | Property it makes fire | Class | The defect it models |
| --- | --- | --- | --- |
| `TypeOkRevisionRunaway` | `TypeOK` | Invariant | a revision bump with no decision behind it, leaving the declared domain |
| `AllOrNothingUngatedProjection` | `AllOrNothing` | Invariant | reads bypass the gate, so a mid-broadcast saga is seen as a split view |
| `VisibilityMatchesDecisionTerminalPresence` | `VisibilityMatchesDecision` | Invariant | any terminal counts as visible, so an aborted saga's writes surface |
| `StrictIsolationInflightSurfaces` | `StrictIsolation` | Invariant | absence of a decision reads as permission, so in-flight writes are readable |
| `CommitIntegrityIgnoresVotes` | `CommitIntegrity` | Invariant | the coordinator commits without consulting the prepare votes |
| `LinearizedTerminalsBroadcastBeforeDecision` | `LinearizedTerminals` | Invariant | a leaf applies a terminal before the registry records the decision |
| `NoMixedTerminalsPerKeyVote` | `NoMixedTerminals` | Invariant | each leaf takes its outcome from its own vote, so one saga does both |
| `DecisionDurabilityDecisionFlip` | `DecisionDurability` | Action | a recorded terminal decision is overwritten after the fact |
| `MonotonicVisibilityOrphanShadows` | `MonotonicVisibility` | Action | a late reshard orphan shadows the committed projection (the #1584 class) |
| `RevisionMonotonicRollback` | `RevisionMonotonic` | Action | a stale registry write replays over a newer one |
| `TerminationNoFairness` | `Termination` | Temporal | no fairness, so a saga may stall forever |
| `EveryCommittedKeyReadableStaleProjection` | `EveryCommittedKeyReadable` | Temporal | the projection never switches to the post-saga value after a commit |

### Some mutations break more than their target, and that is fine

The visibility properties are deliberately interrelated:
`VisibilityMatchesDecision` is the sharpened form of both `AllOrNothing` and
`StrictIsolation`, so a defect in the gate tends to trip several at once.
Because each generated cfg names only its target, that overlap cannot mislead
the harness. It does mean a mutation is evidence that its target *catches* the
defect, not that its target is the *only* property that would.

Two pairs are chosen specifically to avoid that ambiguity where it would matter.
`RevisionMonotonicRollback` stays inside the `TypeOK` domain, as above.
`EveryCommittedKeyReadableStaleProjection` breaks the leads-to while leaving
every saga terminating normally, rather than taking the easy route of dropping
fairness, which would fire `Termination` as well and so prove nothing about the
property it is paired with.

### Nine mutations perturb the protocol; three add an action it does not have

This distinction matters, and reading past it would reproduce in miniature the
overclaim this whole directory exists to prevent.

Nine mutations change something the protocol actually does: a guard, a gate
definition, a projection, or the fairness assumption. For those, the pairing
shows the property constrains the modelled protocol - weaken the protocol and
the property notices.

Three do not. `TypeOkRevisionRunaway`, `DecisionDurabilityDecisionFlip` and
`RevisionMonotonicRollback` splice a brand-new action into `Next`
(`RevisionRunaway`, `DecisionFlip`, `RevisionRollback`) that models no step of
the protocol. They do this because the properties they target are
**unfalsifiable by any behaviour of the base module**: `decision` and `revision`
are each written by exactly one action, `DecideTx`, under a guard of
`phase[t] = "prepared"` that the same action immediately leaves, and no action
ever re-enters `"prepared"`. So each is assigned at most once per saga, and
`DecisionDurability`, `RevisionMonotonic` and `TypeOK`'s revision conjunct
cannot fail however the base is scheduled.

For those three the two-arm experiment therefore establishes something weaker
than it does for the other nine. It establishes that the property is
**well-formed**: that it is not a tautology, that it says what its name says,
and that TLC would report it if the state it forbids became reachable. It does
**not** establish that the property currently constrains the protocol, because
nothing in the protocol can violate it.

That is not a defect in the mutations, and it is not a reason to "fix" them by
hunting for a protocol-level perturbation instead - there is none, which is
precisely the point. It is a limit on what may be concluded, and issue #2323's
second supporting control names it directly: such a result is a classifier,
never evidence of reachability. A future revision that lets a saga re-enter
`"prepared"` - a retry, a re-prepare, a recovery path - would make these three
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

It needs a JVM and `tla2tools.jar`; see [`../README.md`](../README.md) for how
the fixture finds them, and note that it fails rather than skips under
`GITHUB_ACTIONS`.
