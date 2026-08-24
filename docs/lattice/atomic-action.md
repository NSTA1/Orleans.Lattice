# Atomic Action (saga / TCC coordinator)

`IAtomicActionGrain` is a public, generic, all-or-nothing **atomic-action
coordinator** - a [saga](https://microservices.io/patterns/data/saga.html) /
[TCC](https://en.wikipedia.org/wiki/Try-Confirm/Cancel) coordinator keyed by a
caller-supplied operation id. It runs an ordered plan of steps, each a forward
effect paired with a compensating effect, and commits all-or-nothing: if a forward
step faults, every already-committed step is compensated in strict reverse order,
so the action leaves no partial effect behind.

It generalizes the key-only [atomic write](atomic-writes.md) to arbitrary
caller-defined effects, and ships a built-in tree-write step that delegates to the
verified atomic-write machinery so a Lattice-tree mutation can be one step of a
larger business transaction without giving up the tree's atomicity guarantee.

The step-sequencing and crash-resume safety of the coordinator is machine-checked;
see [Verified Atomic Action](verified-atomic-action.md).

## When to use it

Use an atomic action when a single logical operation must apply several effects -
some to Lattice trees, some to external systems (a payment gateway, an email
service, another grain) - and a partial application is unacceptable. The
coordinator gives you a durable, idempotent, crash-recoverable place to sequence
those effects and roll them back on failure.

If your operation only writes keys in one or more Lattice trees, you do not need an
atomic action - use [`SetManyAtomicAsync`](atomic-writes.md) directly, which is
simpler and fully two-phase. Reach for an atomic action when you need to mix a tree
write with a non-tree effect.

## Registering handlers

A custom step never carries a delegate; it names a **handler** that you register
once at silo start. The handler id and an opaque, size-bounded args payload are all
that is persisted and replayed, so a plan is safe to persist (a persisted step can
only ever name an allow-listed, pre-registered handler - resolution
[fails closed](../../.github/instructions/security.instructions.md)).

Register handlers with `AddLatticeAtomicAction` alongside `AddLattice`:

```csharp verify
siloBuilder.AddLatticeAtomicAction(handlers => handlers
    .AddHandler(
        "charge-card",
        versionTag: "v1",
        forward: async ctx =>
        {
            // Forward effect: charge the card. Make it idempotent keyed on
            // ctx.OperationId so a crash-resume re-invocation does not double-charge.
            await Task.CompletedTask;
        },
        compensate: async ctx =>
        {
            // Compensating effect: refund the charge for the same ctx.OperationId.
            // Must fully and idempotently undo the forward effect.
            await Task.CompletedTask;
        }));
```

The `versionTag` is stamped into each step when a saga starts and re-checked on a
crash-resume: if a redeploy changes a handler's tag while a saga is in flight, the
saga parks rather than replaying a changed effect against a partially completed
plan. Bump the tag whenever the forward/compensate semantics change in a way that
is unsafe to replay.

## Building and running a plan

Build a plan with `AtomicActionPlanBuilder`, mixing built-in `TreeWrite` steps and
custom `Step` handlers in the order they should run. Resolve the coordinator by
operation id and call `ExecuteAsync`:

```csharp verify
var plan = new AtomicActionPlanBuilder()
    .TreeWrite("inventory", w => w
        .Upsert("sku-42/onhand", new byte[] { 0, 0, 0, 41 }))
    .Step("charge-card", Encoding.UTF8.GetBytes("order-4711:1999"))
    .Build();

IAtomicActionGrain saga = grainFactory.GetGrain<IAtomicActionGrain>("order-4711");
AtomicActionOutcome outcome = await saga.ExecuteAsync(plan);

switch (outcome.Status)
{
    case AtomicActionStatus.Committed:
        // Every forward step committed: stock decremented and card charged.
        break;
    case AtomicActionStatus.Compensated:
        // A forward step faulted; every completed step was rolled back in reverse.
        Console.WriteLine($"rolled back at step {outcome.FailedStepIndex}: {outcome.FailureMessage}");
        break;
}
```

The operation id (`"order-4711"`) is the **idempotency key**. Re-issuing the same
plan under the same id after the saga is terminal returns the memoized outcome
without re-running any effect, so a client that retries after a timeout observes the
original result rather than a duplicate action. Re-issuing a *different* plan under
a used id is rejected.

Poll a saga's outcome without starting or mutating it with `TryGetOutcomeAsync`,
which returns `null` until the saga is terminal:

```csharp verify
IAtomicActionGrain saga = grainFactory.GetGrain<IAtomicActionGrain>("order-4711");
AtomicActionOutcome? outcome = await saga.TryGetOutcomeAsync();
if (outcome is { Status: AtomicActionStatus.Committed })
{
    // The saga already committed under this operation id.
}
```

## The built-in tree-write step

`.TreeWrite(treeId, ...)` performs an atomic multi-key write to one Lattice tree as
a single saga step, with **library-synthesized** compensation - you supply no
compensating effect. Before the write, the coordinator captures each affected key's
pre-image; if a *later* step faults, it restores those pre-images with a fresh
write (the same pre-image / last-writer-wins technique the atomic-write coordinator
uses). The forward write itself delegates to `IAtomicWriteGrain`, so it inherits the
tree's verified atomicity:

- a single-tree write commits atomically, and
- a write that spans multiple trees routes through the cross-tree
  two-phase-commit coordinator,

exactly as a direct `SetManyAtomicAsync` would. The tree-write step never issues
independent per-tree writes that could partially commit across a cluster boundary.

## Atomicity, precisely

The saga does not make every step two-phase; it makes the *whole plan*
all-or-nothing by compensation. What that means concretely differs by step kind, and
this guide does not overclaim:

| Step kind | Forward atomicity | Rollback | Correctness rests on |
|---|---|---|---|
| `TreeWrite` | Inherited from the atomic-write machinery: single-tree atomic; cross-tree via 2PC. | Library-synthesized pre-image restore. | The verified atomic-write / cross-tree-2PC guarantee. |
| `Custom` (`Step`) | Whatever your forward effect provides. | Your registered compensating effect, best-effort and eventually consistent. | **Your** compensation contract. |

For a custom step, the saga is a best-effort, eventually-consistent compensating
transaction, not a distributed atomic commit: between a forward effect committing
and its compensation running (after a later fault) an external observer can see the
intermediate effect. Make forward and compensating effects idempotent, and make
compensation actually undo the forward effect - that is the caller's contract.

## When compensation itself fails

If a compensating effect faults (after the coordinator's retry budget), the saga
cannot guarantee it undid every committed step - the caller's compensation contract
was violated. The saga enters the terminal `CompensationFailed` state, and
`ExecuteAsync` throws `CompensationFailedException` rather than silently swallowing:
an operator must intervene.

```csharp verify
IAtomicActionGrain saga = grainFactory.GetGrain<IAtomicActionGrain>("order-4711");
AtomicActionPlan plan = new AtomicActionPlanBuilder().Step("charge-card").Build();
try
{
    await saga.ExecuteAsync(plan);
}
catch (CompensationFailedException ex)
{
    // A compensating effect itself faulted; the saga parked for operator
    // intervention. ex.StepIndex identifies the step whose compensation failed.
    Console.WriteLine($"manual intervention required at step {ex.StepIndex}");
}
```

## Durability and crash recovery

The saga persists its plan, per-step status vector, phase, and captured tree
pre-images after every step transition, so a reactivation resumes from the persisted
state and reaches its terminal outcome **exactly once** - a resume neither re-runs a
completed forward effect nor skips a pending compensation. Recovery is
reminder-driven: a keepalive reminder registered at saga start reactivates a
collected grain and drives the resume through the same pure decision core the Coyote
model checks.

After the saga is terminal, the grain arms a one-shot retention reminder
(`LatticeOptions.AtomicActionRetention`, default 48h). When it fires the grain
clears its persisted state and deactivates, so a re-issue within the window still
observes the memoized outcome while saga state does not leak forever.

## Limits

| Option | Default | Meaning |
|---|---|---|
| `LatticeOptions.MaxAtomicActionSteps` | 64 | Maximum number of steps in one plan. |
| `LatticeOptions.MaxAtomicActionArgsBytes` | 32 KiB | Maximum size of a custom step's args payload. |
| `LatticeOptions.AtomicActionRetention` | 48h | How long a terminal saga's memoized outcome is retained before its state is cleared. |

## Observability

The coordinator emits three instruments on the `orleans.lattice` meter, charted on
the Overview dashboard's "Atomic action (saga / TCC)" row and documented in
[Metrics](metrics.md):

- `orleans.lattice.atomic_action.completed` - terminal sagas by `outcome`
  (`committed` / `compensated` / `compensation_failed`).
- `orleans.lattice.atomic_action.step` - step effects by `phase` (`forward` /
  `compensate`) and `outcome` (`ok` / `fault`).
- `orleans.lattice.atomic_action.duration` - end-to-end saga duration by `outcome`.

## Related

- [Verified Atomic Action](verified-atomic-action.md) - the machine-checked safety
  properties of the coordinator's sequencing and crash-resume core.
- [Atomic Write](atomic-writes.md) - the key-only atomic multi-key write the
  tree-write step delegates to.
- [Distributed Lock](distributed-lock.md) - a sibling coordination primitive.
