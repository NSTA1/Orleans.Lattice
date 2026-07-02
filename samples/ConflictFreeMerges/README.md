# ConflictFreeMerges

## What it shows

Orleans.Lattice stores state as CRDTs, so two writers that mutate the same
logical value concurrently - with no coordination between them - always
converge to the **same** final state, regardless of the order their updates are
merged. This sample proves it at the primitive level with the public `PnCounter`
and `OrSet` CRDTs (merged in both orders to show order-independence), then lands
the converged value in a real Lattice tree.

## Run it

```
dotnet run --project samples/ConflictFreeMerges
```

## Expected output

```
Silo starting... ready.

== PN-Counter: two writers increment the same counter concurrently ==
  west (isolated) sees value = 2
  east (isolated) sees value = 5
  merge(west, east).Value = 7
  merge(east, west).Value = 7
  [OK] both orders converged deterministically to 7

== OR-Set: concurrent add vs add-then-remove of the same element ==
  west set contains 'green' = True
  east set contains 'green' = False
  merge(west, east) contains 'green' = True
  merge(east, west) contains 'green' = True
  [OK] add-wins converged deterministically (west's add survived)

== Persisting the converged counter value into a Lattice tree ==
  tree['counter/orders'] = 7

Done. Concurrent writers converged without locks, consensus, or a conflict prompt.
```

## When to use

- Multi-writer or multi-region data where you want availability over
  coordination: each writer proceeds locally and the state merges deterministically.
- Counters, sets, flags, and registers whose concurrent updates should combine
  by algebra (sum, add-wins, latest-wins) rather than by a lock or a manual
  conflict prompt.

## When not to use

- Invariants that require a global check before a write (e.g. "never oversell
  stock"). CRDTs converge, but they do not enforce cross-key transactional
  constraints - use `SetManyAtomicAsync` or an external guard for those.

## Feature docs

[docs/lattice/state-primitives.md](../../docs/lattice/state-primitives.md)
