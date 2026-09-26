# Idempotency Keys and Retry Policy

Orleans.Lattice exposes an **opt-in** retry surface for transient storage
faults on the public single-key and range-delete `ILattice` mutating
methods (see [Wiring a retry policy](#wiring-a-retry-policy) for the exact
list). The library never enables
retry on its own - retry is a property of the caller's environment (cloud
storage retry budgets, replication batch reissue, scheduled workers that
re-run a step) and the throw-and-revert contract of every mutating method
remains the default.

The surface is two cooperating pieces:

1. **`LatticeIdempotencyContext`** - an ambient scope that pins a single
   logical mutation identity (a `LatticeIdempotencyKey`) across one or
   more attempts. The key is stamped on the leaf write so that a retried
   write under the same key collapses to a no-op rather than a second
   mutation.
2. **`ILatticeRetryPolicy`** - an opt-in policy that re-invokes the
   caller's operation under the same ambient key when a transient
   exception escapes. The shipped default is
   `BoundedExponentialRetryPolicy`.

The two are independent. A caller may use the idempotency context
without a policy (rolling their own loop) or with the shipped policy.
A caller who never opens an idempotency scope and never wires a policy
pays nothing - the boundary helper short-circuits and the mutating
method runs with its original throw-and-revert semantics.

## When to use it

Use the retry surface when the caller knows the mutation is safe to
re-attempt under the same identity:

- A worker that consumes a queue message and writes one key per message.
  The message id is the natural idempotency key.
- A cross-cluster replication acceptor that already carries an authoring
  HLC and origin cluster id on the inbound batch.
- A scheduled job that recomputes a derived value and writes it back.

Do **not** use it for transactions that span multiple keys without going
through `SetManyAtomicAsync`. The retry policy does not coordinate
across multiple `ILattice` calls - it only re-invokes the operation
delegate you pass to it.

## The idempotency-key contract

`LatticeIdempotencyKey` is a small value carrying a single field:

- `Timestamp` - a `HybridLogicalClock` that pins the logical write time.
  Every retry under the same key re-stamps this exact HLC on the leaf,
  so LWW resolution treats the retry as a tie and the stored timestamp
  does not advance.

The authoring cluster identity is **not** part of the key. Origin is
infrastructure-resolved provenance, owned by the silo via
`LatticeOriginContext` and `ILatticeOriginClusterIdResolver`; the
mutation boundary stamps it independently of the caller's identity. A
caller-supplied origin would silently misroute loop-suppression,
per-origin merge resolution, and WAL / observer audit, so the slot is
deliberately not exposed.

The static factory `LatticeIdempotencyKey.Fresh()` mints a new key whose
timestamp is the current wall-clock tick, advanced past the last key the
same process minted so that no two keys share a timestamp. Use it for
ad-hoc callers that mint one key per logical operation:

```csharp verify
using (LatticeIdempotencyContext.NewScope())
{
    await lattice.SetAsync("orders/42", new byte[] { 1, 2, 3 }, cancellationToken);
}
```

`LatticeIdempotencyContext.NewScope()` is just shorthand for
`LatticeIdempotencyContext.With(LatticeIdempotencyKey.Fresh())`; use it
when the call site does not need to read the key back after the scope.

Construct the key explicitly only when you already have a stable upstream
identity to pin the HLC to (e.g. a queue message id mapped deterministically
to an HLC, so a crash-and-restart of the worker re-derives the same key):

```csharp verify
using Orleans.Lattice;

var key = new LatticeIdempotencyKey
{
    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
};

using (LatticeIdempotencyContext.With(key))
{
    await lattice.SetAsync("orders/42", new byte[] { 1, 2, 3 }, cancellationToken);
}
```

The key's lifetime is the `using` scope. Disposing the scope restores
the previous ambient key (or `null`). The scope flows across `await`
points on the same logical execution context, so chained `SetAsync`
calls within the scope all share the same key.

## Wiring a retry policy

The shipped `BoundedExponentialRetryPolicy` makes up to `MaxAttempts`
attempts in total (the first call counts as one), waiting
`min(MaxDelay, InitialDelay * 2^(attempt-1))` after failed attempt number
`attempt` before the next. Its defaults (on both
`BoundedExponentialRetryPolicyOptions` and the constructor) are
`MaxAttempts = 4`, `InitialDelay = 50 ms`, `MaxDelay = 2 s`, and a null
`RetryableExceptionClassifier`. Wire it via DI:

```csharp verify
siloBuilder.AddLatticeRetryPolicy(options =>
{
    options.MaxAttempts = 5;
    options.InitialDelay = TimeSpan.FromMilliseconds(100);
    options.MaxDelay = TimeSpan.FromSeconds(2);
    options.RetryableExceptionClassifier = ex =>
        ex is TimeoutException or System.Net.Sockets.SocketException;
});
```

`AddLatticeRetryPolicy` builds the policy when it is called - an invalid
value (`MaxAttempts` below 1, a negative `InitialDelay`, or a `MaxDelay`
below `InitialDelay`) throws `ArgumentOutOfRangeException` from that call -
and installs it as the per-tree `LatticeOptions.RetryPolicy` for every
tree. To pin a different policy on a single tree, set
`LatticeOptions.RetryPolicy` from
`ConfigureLattice("treeName", o => o.RetryPolicy = myPolicy)`, and register
that override after `AddLatticeRetryPolicy`: it assigns through
`ConfigureAll`, and options configure actions run in registration order, so
an `AddLatticeRetryPolicy` registered later replaces the tree's policy too.

The policy is only consulted when an idempotency scope is open. A
mutating call with no ambient key bypasses the policy entirely and
runs with the original throw-and-revert semantics - the library will
never silently retry a write that does not have a caller-supplied
identity.

The policy wraps the single-key mutators - `SetAsync` (both overloads),
`SetIfVersionAsync`, `GetOrSetAsync`, `ApplyCrdtDeltaAsync` (both
overloads, which the typed CRDT accessors write through), and
`DeleteAsync` - and the range deletes `DeleteRangeAsync` and
`DeleteRangeWherePredicateAsync`. The batch, atomic, and bulk-load entry
points never route through it (see
[What is explicitly out of scope](#what-is-explicitly-out-of-scope)).

## Caller-side retry without DI

If you prefer to keep the policy entirely at the caller (e.g. a
saga-style orchestrator that already runs retries), construct the
policy locally and call `ExecuteAsync` directly:

```csharp verify
var policy = new BoundedExponentialRetryPolicy(
    maxAttempts: 3,
    initialDelay: TimeSpan.FromMilliseconds(50),
    maxDelay: TimeSpan.FromMilliseconds(500));

using (LatticeIdempotencyContext.NewScope())
{
    await policy.ExecuteAsync(async ct =>
    {
        await lattice.SetAsync("orders/42", new byte[] { 1, 2, 3 }, ct);
    }, cancellationToken);
}
```

This pattern is the recommended shape for environments that do not run
inside the Orleans silo container, or that want to stack the lattice
policy under a broader resilience pipeline (e.g. Polly).

## How dedup is enforced on the storage side

Two storage paths participate:

- **Leaf LWW writes.** When the leaf grain sees an open
  `LatticeHlcOverrideContext` (which the idempotency-key boundary
  helper opens from `key.Timestamp`), it stamps the override verbatim
  rather than ticking its local HLC. A retry under the same key stamps
  the same HLC, so the receiver's LWW resolution treats the retry as a
  tie and the stored value does not advance.
- **PnCounter accessor.** `PnCounterAccessor.IncrementAsync` /
  `DecrementAsync` perform a single read plus one delta apply rather than
  a read-modify-write CAS loop - the leaf grain is the single writer
  authority per key. The delta they mint carries the replica's resulting
  per-replica total rather than the amount added, and a PN-counter folds
  deltas by pointwise maximum, so re-applying the same delta is a no-op.
  When the retry policy re-runs a failed apply it re-sends the same delta
  bytes under the same pinned HLC, so a retried apply cannot advance the
  counter twice. The accessor does not read the ambient key itself, so
  this covers retries of the apply inside one `IncrementAsync` call, not a
  second `IncrementAsync` call: calling the accessor again re-reads the
  counter and mints a fresh delta.

For replicated trees, the same identity flows through the
`Orleans.Lattice.Replication` push transport's WAL dedup, so a retry
that races a replication outbound batch collapses on the receiving
cluster as well.

## What is explicitly out of scope

- The library does **not** install a retry policy by default. The
  `LatticeOptions.RetryPolicy` slot is `null` unless the host wires
  one in.
- The retry policy does **not** wrap the batch, atomic, or bulk-load
  entry points: `SetManyAsync`, `SetManyWherePredicateAsync`,
  `ApplyCrdtDeltaManyAsync`, `SetManyAtomicAsync`,
  `SetManyAtomicWhereAsync`, `BulkLoadAsync`, and `BulkAppendChunkAsync`
  never route through it. An atomic batch carries its own recovery - its
  `operationId` overloads re-attach a retried call to the original saga -
  and a failed saga aborts as a unit rather than being re-driven by the
  policy.
- There is no per-method retry override on the `ILattice` interface.
  Retry is a property of the caller's environment, not a per-call
  parameter, and adding a parameter would couple every grain method
  signature to a concern that belongs at the boundary.
- The shipped `BoundedExponentialRetryPolicy` does not implement
  jitter. Hosts that need jitter or coordinated backoff across many
  callers should plug in their own `ILatticeRetryPolicy`.

## Operational guidance

- **Always pair the policy with an idempotency scope.** Without an
  ambient key, the policy is bypassed - and rightly so, because the
  storage path has no way to dedup a retry.
- **Keep `MaxAttempts` small.** The default of 4 attempts, a 50 ms
  initial delay and a 2 second cap is sized for transient storage faults;
  longer budgets risk amplifying load during a real outage.
- **Use the classifier to scope retries.** A null classifier (the
  default) retries on every exception, including programmer errors -
  the only exception the shipped policy never retries is a cancellation
  of the caller's own `CancellationToken`.
  In production, restrict the classifier to the storage-transient
  exception families your provider emits.
- **Treat the idempotency key as part of the application's data
  identity.** Persist it (e.g. on the queue message) so that a worker
  crash and restart can re-enter the same scope on retry.
