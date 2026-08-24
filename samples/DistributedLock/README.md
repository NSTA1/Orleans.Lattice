# Distributed Lock

## What it shows

`ILatticeLockGrain` is a single-cluster, FIFO-fair distributed lock / lease keyed by
name. Because an Orleans grain activation is single-threaded and processes its inbox
in arrival order, a grain keyed by a lock name is a natural FIFO mutual-exclusion
point - and this primitive gets the failure modes right: **monotonic fencing
tokens** (so a superseded holder is detectable by the resource it guards) and
**bounded leases** (so a crashed holder cannot wedge the lock forever).

This sample demonstrates:

1. **Acquire, renew, release** - and the strictly-increasing fencing token stamped
   on every grant.
2. **Non-blocking try-acquire** - `null` while the lock is held, granted after it is
   released.
3. **FIFO blocking** - a second caller queued behind the holder is granted the
   instant the holder releases, with a strictly higher fencing token.

## Run it

```
dotnet run --project samples/DistributedLock
```

## Expected output

```
== DistributedLock sample ==

1) Acquiring the lock (30s lease, willing to wait 5s)...
   granted, fencing token = 1
   status: held=True queueDepth=0
   renewed the lease.
   released. The next caller can acquire immediately.

2) Holder acquired, fencing token = 2.
   TryAcquire while held -> null (skipped)
   TryAcquire after release -> granted, fencing token = 3

3) First holder acquired, fencing token = 4.
   a second caller is queued: queueDepth=1
   first holder releases...
   queued waiter granted, fencing token = 5 (strictly higher).

Done.
```

## When to use

- You need to serialize a cluster-wide critical section: a singleton job, a leader
  election, or an externally-visible resource that needs one writer at a time.
- You want a lock that survives a crashed holder (bounded lease reclamation) and
  gives you a fencing token to forward to the guarded resource, so a paused holder
  that lost the lock can no longer write.

## When not to use

- To coordinate mutual exclusion **across** clusters. The lock is single-cluster:
  for a given name each Orleans cluster keeps its own independent activation, so two
  clusters can grant the same lock at once and their fencing tokens are not
  comparable. When a mutation must be atomic across clusters, write it to a tree
  (see [atomic writes](../../docs/lattice/atomic-writes.md) or the
  [atomic action](../AtomicAction/README.md) coordinator), whose commit replicates
  to every cluster the tree replicates to.
- To make a batch of key writes atomic. That is a different problem than mutual
  exclusion - use [atomic writes](../../docs/lattice/atomic-writes.md) (or, to mix a
  tree write with other effects, the [atomic action](../AtomicAction/README.md)
  coordinator).
- As a general coordination signal off `GetStatusAsync`: that snapshot is for
  diagnostics only. Only the fencing token from an actual grant is authoritative.

## Feature doc

[docs/lattice/distributed-lock.md](../../docs/lattice/distributed-lock.md)
