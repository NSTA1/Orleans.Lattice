# Specification mutations

Each file here is a deliberately broken copy of [`../AtomicCommit.tla`](../AtomicCommit.tla),
paired with a cfg that names exactly one property. Together they answer a
question the base specification cannot answer about itself: **does this property
actually fire?**

## Why

The atomicity audit (epic #2299) found four verification artefacts that were
green because they could not reach the state they were named for. A Coyote model
whose harness never scheduled the interleaving it claimed to explore. A
refinement row asserting a correspondence its abstraction could not express. An
invariant over a variable no action ever set to the value that would violate it.
None of them failed. All of them read exactly like verification that works.

That is the whole problem: **an artefact that asserts a property it is
structurally unable to check is indistinguishable, from the outside, from one
that checks it.** Passing tells you nothing, because it is what both cases do.
The only thing that separates them is a demonstration that the artefact *can*
go red, and the only honest form of that demonstration is a mutation that makes
it do so.

Issue #2323 is the recommendation: every property ships with a named mutation
under which it fires, checked in as a cfg, run in CI.

## Convention

A mutant is `<Base><MutationName>.tla` plus `<Base><MutationName>.cfg`.

1. **The module name must equal the filename.** TLA+ requires it, so a mutant
   cannot be named after the property alone.
2. **One property per cfg, written whole.** Not appended to the base cfg's
   property list. During the audit a cfg that *prepended* a property instead of
   replacing it produced six satisfiability branches rather than two, and
   reported two violations attributed to the wrong properties. It read as a
   confident result. `TlcModelCheckTests` asserts on both the property *name* in
   TLC's banner and the violation *count*, so that mistake fails rather than
   passes.
3. **Every mutant is paired with a test.** `TlcModelCheckTests` enumerates this
   directory and fails if a `.tla` here has no test asserting the specific
   property it makes fire. A mutant nobody runs is a file, not a gate.
4. **Head the module with the expected outcome** - which property, which TLC
   banner, and the shape of the counterexample - so a future reader can tell a
   mutation that stopped firing from one that never did.

## Mutants are standalone copies, and that is a known cost

A mutant cannot `EXTENDS` the base module. TLA+ does not permit redefining an
operator (`SurfaceViaGate`) or introducing a `VARIABLE` into an extended module,
and both are needed. So each mutant is a full copy.

The cost is drift: an edit to `AtomicCommit.tla` does not propagate here, and
nothing currently detects it. A mutant that has drifted far enough from the base
stops being evidence about the base. `The_base_specification_holds` bounds the
damage in one direction (a broken base is caught, so a mutant's red cannot be
laundered into a positive result), but it does not detect drift itself.
Structural comparison of mutant against base is a follow-up.

## Inventory

| Mutant | Property it must make fire | Issue |
| --- | --- | --- |
| `AtomicCommitDecisionExpiry` | `MonotonicVisibility` | #2320 |

Eleven of the twelve properties in [`../AtomicCommit.cfg`](../AtomicCommit.cfg)
are still unpaired. This directory is a proof of concept for the harness, not a
finished portfolio, and the table above is the honest statement of how much of
#2323 is done.

## Running one by hand

```
java -cp tools/tla2tools.jar tlc2.TLC \
  -config AtomicCommitDecisionExpiry.cfg \
  -workers auto -cleanup \
  AtomicCommitDecisionExpiry.tla
```

Run it from a scratch copy of the directory: TLC writes its state files beside
the module it checks. The fixture does this for you.
