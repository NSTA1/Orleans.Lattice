# Atomic Action

## What it shows

`IAtomicActionGrain` is a generic, all-or-nothing saga / TCC coordinator. It runs
an ordered plan of steps - each a forward effect paired with a compensating effect
- and if a later step faults, every already-committed step is compensated in strict
reverse order, so the action leaves no partial effect behind.

This sample puts a **Lattice tree write in the same transaction as an external
effect**. The built-in `.TreeWrite` step delegates to the verified atomic-write
machinery (so it inherits the tree's atomicity) and its compensation is
library-synthesized from a captured pre-image; a custom `.Step` names a
pre-registered handler (here, one that reserves credit in a stand-in external
ledger). The saga plan is literally a Lattice tree write and a custom action
built together, then run all-or-nothing:

```csharp
var plan = new AtomicActionPlanBuilder()
    // Step 1: an atomic Lattice tree update - decrement on-hand stock.
    //         Auto-compensated from a captured pre-image on rollback.
    .TreeWrite("inventory", w => w.Upsert("sku-42/onhand", Encoding.UTF8.GetBytes("40")))
    // Step 2: a custom action - reserve credit in an external ledger.
    //         Its registered compensate effect releases the reservation.
    .Step("reserve-credit", Encoding.UTF8.GetBytes("alice:100"))
    .Build();

var outcome = await grainFactory
    .GetGrain<IAtomicActionGrain>("order-1001")
    .ExecuteAsync(plan);
```

Prefer the built-in `.TreeWrite` step over doing the tree write inside a custom
handler: a custom handler has no pre-image scratch, so it cannot cleanly restore
a tree key on compensation, whereas `.TreeWrite` captures and restores the
pre-image for you. Reach for a custom `.Step` for the *non-tree* effects.

To touch more than one tree in a single action, add one `.TreeWrite` step per tree:
the saga makes the whole plan all-or-nothing by compensation (each step's write is
individually atomic and, being a tree write, commits across every cluster the tree
replicates to). See the [feature doc](../../docs/lattice/atomic-action.md) for the
precise cross-tree and cross-cluster semantics.

The sample runs:

1. A **committing** plan - decrement stock in the `inventory` tree *and* reserve
   credit in the ledger, together.
2. A **rolling-back** plan - the same shape plus a third step that faults, proving
   the tree write is restored to its pre-saga value *and* the reservation is
   released.
3. An **idempotent retry** - re-issuing a terminal operation id returns the
   memoized outcome without re-running any effect.

## Run it

```
dotnet run --project samples/AtomicAction
```

## Expected output

```
== AtomicAction sample ==

1) Seeded inventory 'sku-42/onhand' = 41, ledger reservation = 0.
   Outcome: Committed
   inventory 'sku-42/onhand' = 40 (was 41)
   ledger reservation for order-1001 = 100
   -> one saga committed a Lattice tree write and a custom external action together.

2) Seeded inventory 'sku-99/onhand' = 5, ledger reservation = 0.
   Outcome: Compensated (faulted at step 2: carrier rejected the shipment)
   inventory 'sku-99/onhand' = 5
   ledger reservation for order-2002 = 0
   -> the tree write was restored and the reservation released: no partial effect.

3) Re-issuing operation 'order-1001' returns the memoized outcome: Committed
   -> a client retry after a timeout observes the original result, not a double-apply.

Done.
```

## When to use

- One logical operation must apply several effects - some to Lattice trees, some to
  external systems (a payment gateway, an email, another grain) - and a partial
  application is unacceptable.
- You want a durable, idempotent, crash-recoverable place to sequence those effects
  and roll them back on failure, keyed by an operation id.

## When not to use

- Your operation only writes keys, with no external effect. For one tree use
  [`SetManyAtomicAsync`](../../docs/lattice/atomic-writes.md) directly; for several
  trees under one isolated commit use the cross-tree atomic-write builder. Both are
  simpler than a saga and fully two-phase, and both commit atomically across every
  cluster the tree replicates to.
- Remember a custom step is best-effort eventually-consistent: between a forward
  effect committing and its compensation running, an external observer can see the
  intermediate effect. Make forward and compensating effects idempotent, and make
  compensation actually undo the forward effect - that is the caller's contract.

## Feature doc

[docs/lattice/atomic-action.md](../../docs/lattice/atomic-action.md)
