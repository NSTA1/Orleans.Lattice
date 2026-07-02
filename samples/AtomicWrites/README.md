# Atomic Writes

## What it shows

`ILattice.SetManyAtomicAsync` commits a batch of key-value pairs all-or-nothing:
either every key in the batch becomes visible together, or - on failure or a
failed guard - nothing is written and every key keeps its pre-batch value. This
sample commits a successful batch, then runs a guarded batch whose precondition
fails to prove no partial state leaks, and finally uses the
`IGrainFactory.SetManyAtomicAsync` overload to flip keys across two separate
trees as one unit.

## Run it

```
dotnet run --project samples/AtomicWrites
```

## Expected output

```
== AtomicWrites sample ==

1) Committing a 3-key shipment batch atomically...
   order:42/status = shipped
   order:42/tracking = 1Z999
   customer:alice/last-order = 42
   -> all three keys are visible together.

2) Seeded order:1=120, order:2=80.
   Guard 'current.Total >= 100' outcome: PreconditionFailed
   order:1 = 120, order:2 = 80
   -> both keys keep their ORIGINAL totals: no partial write leaked.

3) Guard 'current.Total > 0' outcome: Committed
   order:1 = 999, order:2 = 5
   -> both keys now hold the new totals: the batch committed as a unit.

4) Committing a batch spanning 'orders-east' and 'inventory'...
   Cross-tree outcome: Committed
   orders-east/order:42/status = fulfilled
   inventory/sku:99/reserved   = 0
   -> keys on both trees flipped together.

Done.
```

## When to use

- A multi-key change must be all-or-nothing (a shipment record, a re-key move,
  a guarded conditional update) and a reader must never see it half-applied.
- You need a precondition checked against the current stored values before the
  batch commits (the predicate overload), or a batch that spans two or more
  trees (the `IGrainFactory` overload).

## When not to use

- The hot write path where raw throughput matters. Atomic writes coordinate
  across keys via a saga, which is more expensive than a plain `SetAsync` or the
  non-atomic `SetManyAsync`. Reach for atomicity only where the all-or-nothing
  guarantee is actually required.

## Feature doc

[docs/lattice/atomic-writes.md](../../docs/lattice/atomic-writes.md)
