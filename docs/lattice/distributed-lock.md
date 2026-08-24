# Distributed Lock

`ILatticeLockGrain` is a cluster-wide, FIFO-fair distributed lock / lease keyed by
name. Because an Orleans grain activation is single-threaded and processes its
inbox in arrival order, a grain keyed by a lock name is a natural FIFO
mutual-exclusion point; this primitive packages that pattern and gets the
failure-mode details right - **bounded leases** so a crashed holder cannot wedge
the lock forever, and **monotonic fencing tokens** so a superseded holder is
detectable by the resource it guards.

The lock provides mutual exclusion only. It touches no tree, WAL, or atomic-write
saga, captures no pre-image, and offers no rollback. Use it to serialize a
cluster-wide critical section (a singleton job, a leader election, an
externally-visible resource that needs one writer at a time), not to make a batch
of key writes atomic - for that, use [atomic writes](atomic-writes.md).

## Resolving a lock

Resolve a lock by name through the grain factory. All callers naming the same
lock contend for the same activation and are serialized FIFO:

```csharp verify
ILatticeLockGrain theLock = grainFactory.GetGrain<ILatticeLockGrain>("inventory/sku-42");
LockStatus status = await theLock.GetStatusAsync();
Console.WriteLine($"held={status.IsHeld} queueDepth={status.QueueDepth}");
```

The lock name is any non-empty string; namespace it however you like
(`"inventory/sku-42"`, `"leader/report-generator"`). There is no registration
step - a lock name springs into existence the first time it is acquired and
costs nothing until then.

## Acquire, renew, release

`AcquireAsync` enqueues the caller FIFO and completes when the lock is granted to
them, or faults with a `TimeoutException` if the caller's `MaxWait` elapses first.
The call never blocks the grain's activation turn: a contended caller is enqueued
and its task completes from a later turn (a release, a lease expiry, or its own
wait-timeout).

```csharp verify
ILatticeLockGrain theLock = grainFactory.GetGrain<ILatticeLockGrain>("inventory/sku-42");

// Wait up to 10 seconds in the FIFO queue for a 30-second lease.
LockLease lease = await theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(10)));

long fencingToken = lease.Token.FencingToken;

// Do the guarded work here. Stamp fencingToken on every write to the resource
// this lock protects, so the resource can reject a write from a superseded
// holder (see "Fencing" below).

// Renew before the lease expires to keep holding the lock across long work.
lease = await theLock.RenewAsync(lease.Token, TimeSpan.FromSeconds(30));

// Release when done so the next FIFO waiter is granted immediately.
await theLock.ReleaseAsync(lease.Token);
```

`ReleaseAsync` is idempotent: a release with a stale token - one that is not the
current holder's - is a silent no-op and does not disturb the current holder. A
`RenewAsync` with a stale token, by contrast, throws
`LatticeLockConflictException`, because a holder that believes it still owns the
lock but does not needs to find out.

## Non-blocking try-acquire

`TryAcquireAsync` never queues: it returns the granted lease if the lock is free
(or its lease has expired and is reclaimable) at the moment of the call, or
`null` if it is currently held. Use it for a singleton job that should simply be
skipped when another worker already holds the lock:

```csharp verify
ILatticeLockGrain theLock = grainFactory.GetGrain<ILatticeLockGrain>("leader/report-generator");

LockLease? lease = await theLock.TryAcquireAsync(TimeSpan.FromMinutes(2));
if (lease is null)
{
    // Another worker holds the lock; skip this run.
    return;
}

try
{
    // ... run the singleton job, fenced by lease.Value.Token.FencingToken ...
}
finally
{
    await theLock.ReleaseAsync(lease.Value.Token);
}
```

A blocking acquire with `MaxWait` set to `TimeSpan.Zero` is equivalent to a
`TryAcquireAsync`, except it faults with a `TimeoutException` on contention rather
than returning `null`.

## Fencing

Every grant carries a strictly-increasing `LockToken.FencingToken` that is never
reused or decreased, even across activations and crashes. This is the
load-bearing correctness property of a distributed lock, per
[Martin Kleppmann's fencing-token argument](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
a lock alone cannot prevent a holder that was paused (a GC pause, an activation
move) past its lease expiry from acting on stale belief. The fix is to forward
the fencing token to the resource the lock guards and have that resource reject
any write bearing a token lower than the highest it has seen.

```csharp verify
ILatticeLockGrain theLock = grainFactory.GetGrain<ILatticeLockGrain>("orders/42");

LockLease lease = await theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(5)));

// Forward lease.Token.FencingToken to the guarded resource with every write.
// The resource records the highest token it has accepted and rejects any write
// carrying a lower token - so a holder whose lease expired and was reclaimed
// (and re-granted to someone else with a higher token) can no longer write,
// even if it never noticed it lost the lock.
long token = lease.Token.FencingToken;

await theLock.ReleaseAsync(lease.Token);
```

Because the fencing token is minted from a persisted, monotonic counter, it keeps
increasing across a grain reactivation or a silo crash - the counter is written
to durable state on every grant, so a recovered activation never re-issues a
token it already handed out.

## Leases and reclamation

A granted lease has a bounded duration. If the holder neither renews nor releases
before the lease expires, the lease is **reclaimed** and the next FIFO waiter is
granted - so a crashed holder cannot wedge the lock forever. Reclamation is
driven two ways:

- A fine-grained in-activation timer reclaims the lease at its exact expiry while
  the grain stays activated, so sub-second lease timing works.
- A minute-grained keepalive reminder is the durable backstop: if the grain was
  deactivated (or the silo crashed) with a held lease, the reminder reactivates
  it and reclaims the expired lease even with no live acquirer to drive it.

Choose a lease duration longer than the work you do under the lock, and renew
periodically for work that outlives a single lease. A non-positive
`LeaseDuration` defaults to
[`LatticeOptions.DefaultLockLeaseDuration`](configuration.md); every requested
duration is capped at `LatticeOptions.MaxLockLeaseDuration`.

## Handling timeout and observing status

A blocking acquire that never reaches the head of the queue within `MaxWait`
faults with `TimeoutException`, and the caller is removed from the queue so it
cannot wedge the FIFO order or be granted a lease nobody holds:

```csharp verify
ILatticeLockGrain theLock = grainFactory.GetGrain<ILatticeLockGrain>("orders/42");

try
{
    LockLease lease = await theLock.AcquireAsync(new LockAcquireRequest(
        LeaseDuration: TimeSpan.FromSeconds(30),
        MaxWait: TimeSpan.FromSeconds(5)));
    await theLock.ReleaseAsync(lease.Token);
}
catch (TimeoutException)
{
    // Could not reach the head of the FIFO queue within 5 seconds; back off.
}

// Point-in-time diagnostics only. Never use this to make an acquire decision -
// only the fencing token from an actual grant is authoritative.
LockStatus status = await theLock.GetStatusAsync();
Console.WriteLine(
    $"held={status.IsHeld} token={status.CurrentFencingToken} " +
    $"expires={status.LeaseExpiresAt:o} queueDepth={status.QueueDepth}");
```

`GetStatusAsync` is for observability and tests. `QueueDepth` reflects the
in-memory waiters on the current activation only, so treat it as a diagnostic
signal, not a coordination primitive.

## Durability and crash-safety

The fencing counter, the current holder token, and the lease expiry are persisted
after every transition, so a reactivation resumes a consistent view and the
fencing sequence never rewinds. The in-memory FIFO waiter queue is deliberately
transient: a queued acquirer's task cannot cross a process boundary, so on
deactivation the parked callers observe a clean cancellation (or their own
wait-timeout) and retry, rather than being granted a lease no live process holds.

## Observability

The lock publishes four instruments on the `orleans.lattice` meter, charted on
the Overview dashboard's "Distributed lock" row:

- `orleans.lattice.lock.acquired` (tagged `outcome=granted|timeout|unavailable`),
- `orleans.lattice.lock.released`,
- `orleans.lattice.lock.lease_reclaimed` - a sustained non-zero rate means holders
  are crashing or failing to renew before expiry,
- `orleans.lattice.lock.acquire.wait` (histogram, ms) - the FIFO queue wait before
  a grant, whose p95/p99 tail is the primary contention signal.

See [Metrics](metrics.md#distributed-lock-sourced-from-latticelockgrain) for the
full catalogue.

## Verification

The lock's fencing and admission decisions are extracted into a pure,
deterministic core (`LockAdmissionCore`) that both the production grain and a
Coyote concurrency model execute, so its safety properties - monotonic fencing,
stale-token rejection, mutual exclusion, and expired-lease reclamation - are
machine-checked against every adversarial interleaving, not just asserted by
integration tests. See [Verified Distributed Lock](verified-lock.md).

## Related

- [Verified Distributed Lock](verified-lock.md) - the proven-core and Coyote
  verification apparatus behind this lock.
- [Atomic Writes](atomic-writes.md) - for making a batch of key writes
  all-or-nothing, which is a different problem than mutual exclusion.
- [Configuration](configuration.md) - `DefaultLockLeaseDuration` and
  `MaxLockLeaseDuration`.
