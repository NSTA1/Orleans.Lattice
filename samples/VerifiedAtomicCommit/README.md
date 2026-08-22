# Verified Atomic Commit

## What it shows

The all-or-nothing visibility of `SetManyAtomicAsync` is not just asserted by
integration tests - it is driven by verified protocol cores that are
model-checked with Coyote and specified in TLA+. This sample makes that verified
property observable at runtime: a concurrent snapshot reader
(`GetManyAsync`, the N-key reader-stability path) races a saga that repeatedly
flips a whole key set between a PRE and a POST value, and every snapshot resolves
the saga all-or-nothing - never a torn, half-committed view. It then rejects a
guarded batch to show the abort path leaves no partial state either.

The same `AllOrNothing` / `VisibilityMatchesDecision` property this sample
observes is proven by the cores in `src/lattice/BPlusTree/`, the Coyote models in
`test/lattice/BPlusTree/Coyote/`, and the TLA+ spec in `spec/AtomicCommit.tla`.

## Run it

```
dotnet run --project samples/VerifiedAtomicCommit
```

## Expected output

The exact snapshot counts vary run-to-run (the reader and writer race), but the
torn-read count is always zero and the rejected batch always leaves the
pre-state intact.

```
== VerifiedAtomicCommit sample ==

Seeded 4 keys at their PRE value as one atomic batch.

1) Racing a concurrent snapshot reader against a flipping saga...
   Saga flips committed : 200
   Snapshots observed   : 1463
   ... all-PRE          : 729
   ... all-POST         : 734
   ... TORN (mixed)     : 0
   -> zero torn reads: every snapshot resolved the whole saga against one decision.

2) Seeded acct:a=120, acct:b=80; rejecting a guarded transfer...
   Guard 'Balance >= 100' outcome: PreconditionFailed
   acct:a = 120, acct:b = 80
   -> both accounts keep their ORIGINAL balances: no partial write leaked.

This all-or-nothing visibility is machine-checked, not just observed here:
  * cores    : src/lattice/BPlusTree/ (AtomicVisibilityGate, SagaCoordinatorCore, ...)
  * Coyote   : test/lattice/BPlusTree/Coyote/  (dotnet test --filter Category=Coyote)
  * TLA+     : spec/AtomicCommit.tla
  * docs     : docs/lattice/verified-atomic-commit.md

Done.
```

## When to use

- You want to see the runtime manifestation of the property the atomic-commit
  protocol cores, Coyote models, and TLA+ spec verify: a snapshot read observes a
  multi-key saga all-or-nothing, and a rejected guard leaves no partial state.
- You are extending the atomic-commit protocol and want a reproducible harness
  that races a reader against the saga through the public API.

## When not to use

- You only need the atomic-write API surface (successful batch, guarded batch,
  cross-tree overload) narrated step by step - see the
  [AtomicWrites](../AtomicWrites/README.md) sample for that.

## Feature doc

[docs/lattice/verified-atomic-commit.md](../../docs/lattice/verified-atomic-commit.md)
