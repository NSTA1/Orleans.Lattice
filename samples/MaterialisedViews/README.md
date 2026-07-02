# Materialised Views

## What it shows

A **materialised view** is an asynchronous, eventually-consistent projection of a
source tree, maintained by tailing that tree's write-ahead log. This sample
declares two views over a `people` tree at startup and shows each converging to
reflect source writes:

- A **filter / re-project** view `adults` that keeps only people with `Age >= 18`.
- An **aggregation** view `age-sum-by-city` that sums `Age` grouped by `City`.

It seeds five people, waits for the views to catch up, then mutates the source
(adds an adult and ages a child past 18) and shows both views re-converging - all
without ever writing to a view directly.

## Run it

```
dotnet run --project samples/MaterialisedViews
```

## Expected output

```
Silo starting... ready.

Writing source people:
  Alice (age 34, London)
  Bob (age 12, London)
  Carol (age 27, Paris)
  Dan (age 9, Paris)
  Eve (age 41, London)

After initial seed - views converged to source:
  'adults' view (3 keys, lag 0):
    Alice (age 34)
    Carol (age 27)
    Eve (age 41)
  'age-sum-by-city' aggregate:
    London: sum(age) = 87
    Paris: sum(age) = 36

Mutating source: add Frank(52, Paris); Bob turns 18 (London).

After mutation - views re-converged:
  'adults' view (5 keys, lag 0):
    Alice (age 34)
    Bob (age 18)
    Carol (age 27)
    Eve (age 41)
    Frank (age 52)
  'age-sum-by-city' aggregate:
    London: sum(age) = 93
    Paris: sum(age) = 88
```

## When to use

- You need a continuously-maintained secondary index, filtered subset, or rolled-up
  aggregate of a tree without recomputing it on every read.
- Consumers can tolerate eventual consistency (bounded apply lag behind the source).

## When not to use

- When you need the value the instant the write commits - a view lags the source.
  Read the source tree directly for read-your-writes semantics.
- Views are read-only and derived. Attempting to write to a `view-*` tree
  directly through the public `ILattice` surface is **rejected** with an
  `InvalidOperationException` - change the **source** and let the view converge.

## Notes

- The views are declared at startup (`AddLatticeViews(...)`) so their maintainers
  come online with the host before any writes. `WaitForSourceHeadAsync` makes the
  sample deterministic by blocking until the applied frontier reaches the source
  head; production readers typically just tolerate the small apply lag.

## Feature doc

- [Materialised views](../../docs/lattice/materialised-views.md)
