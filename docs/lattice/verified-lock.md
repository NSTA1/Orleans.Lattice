# Verified Distributed Lock

The safety of the [distributed lock](distributed-lock.md) rests on three
load-bearing decisions: every grant mints a strictly-increasing fencing token, a
renew or release is honoured only for the current holder's token, and an expired
lease is reclaimed and handed to the head of the FIFO queue - never to anyone
else. Orleans.Lattice drives those decisions from a single **verified core** -
a pure, deterministic function that both the production grain and an
out-of-solution verification layer execute - so the lock's fencing and admission
properties are machine-checked, not just asserted by prose and integration tests.

This document describes the verification apparatus: the proven-core pattern, the
Coyote concurrency tier that model-checks the core under adversarial
interleavings, and the safety-and-liveness property catalogue. It is an assurance
document; the runtime behaviour it protects is documented in
[Distributed Lock](distributed-lock.md).

## The proven-core pattern

`LockAdmissionCore` is an `internal static` class holding the lock's entire
safety decision surface as pure functions over a caller-owned `LockCoreState`
(the fencing counter, the held flag, the holder token, and the lease expiry
tick). Each function is:

- **Deterministic and dependency-free** - it takes explicit inputs (including
  `now` as a tick value) and returns a verdict. No `Task`/`await`, no wall-clock
  read, no Orleans types, no storage, no allocation. Given the same inputs it
  always returns the same output.
- **The single source of the decision** - the production `LatticeLockGrain` hot
  path calls the core to make the real decision (mint a token, decide grant vs
  hold, validate a renew / release token, reclaim an expired lease), and the
  Coyote model calls the *same* core to check it. There is no second, model-only
  reimplementation that could drift from production.

Because the decision logic is isolated behind pure functions, a model checker can
enumerate every ordering of the surrounding concurrent steps and assert a
property holds at each one, while production keeps the identical logic on its hot
path. The core is `internal` and exposed to the test assembly through
`InternalsVisibleTo`, so the model sees the exact production types.

### The core's decisions

| Function | Decision it owns |
|---|---|
| `LockAdmissionCore.NextFencingToken` | The next fencing token is the strict successor of the last issued one; it overflows rather than wraps. |
| `LockAdmissionCore.Grant` | Mint the next fencing token, install the holder, and set the lease expiry - the only place a token is minted. |
| `LockAdmissionCore.Decide` | Grant iff the lock is free or its lease has expired; otherwise hold the current holder. |
| `LockAdmissionCore.IsCurrentHolder` | A presented token is valid iff it equals the current holder's token (and the lock is held). |
| `LockAdmissionCore.Renew` | Extend the lease iff the presented token is the current holder's; reject a stale token. |
| `LockAdmissionCore.Release` | Free the lock iff the presented token is the current holder's; a stale release is a no-op. |
| `LockAdmissionCore.ReclaimIfExpired` | Free an expired lease while preserving the fencing counter, so the next grant still strictly increases. |

## The Coyote concurrency tier

`LockAdmissionModel` (`test/lattice/BPlusTree/Coyote/LockAdmissionModel.cs`)
implements `ICoyoteModel` and drives the production `LockAdmissionCore` under
[Coyote](https://github.com/microsoft/coyote) systematic schedule exploration. It
reproduces the classic Kleppmann fencing race: holder `A` is granted the lock,
its lease expires (a GC pause or activation move that outlived the lease), and
then three events race in every order the runtime explores -

- the lock reclaims `A`'s expired lease and grants the next waiter `B` a
  strictly-greater fencing token (only when the admission gate says the lock is
  free, exactly as the production grain does);
- `A` wakes and issues a stale `Release` with its old token;
- `A` wakes and issues a stale `Renew` with its old token.

After every delivered event, on every explored order, the model asserts the
safety properties below with `Specification.Assert`.

The tier is tagged `[Category("Coyote")]` so the fast dev loop and the
per-package deterministic CI step skip it; a dedicated CI step runs the category.
Run it locally with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=Coyote"
```

### The model ships a non-vacuous guard test

A model that asserts nothing an interleaving can break is worthless, so the
fixture proves the model can fail. `LockAdmissionModel` takes a
`useBrokenTokenCheck` flag: when set, `Release` frees the lock **without**
checking the presented token matches the current holder. `LockAdmissionCoyoteTests`
has two tests:

- `Stale_token_never_dislodges_current_holder_on_any_order` runs the proven core
  and calls `CoyoteModelHarness.AssertNoInterleavingViolation(...)` - no explored
  order trips an assertion.
- `Release_ignoring_the_fencing_token_is_caught` runs the broken core and calls
  `CoyoteModelHarness.AssertInterleavingViolationFound(...)` - Coyote *must* find
  the order (reclaim-and-grant `B`, then deliver `A`'s stale release) that frees
  `B` and trips the assertion.

The guard test failing to find a violation fails the build, so the passing test
is meaningful rather than vacuous.

## The property catalogue

A model only checks what it asserts, so "verified" is bounded by the completeness
of the property set. The lock's correctness contract is:

Safety properties:

- **FencingMonotonic** - every grant's fencing token is strictly greater than
  every previously issued token, and tokens are never reused - even across
  reclamation, reactivation, and crashes. Owned by `NextFencingToken` / `Grant`;
  asserted by the model whenever `B` is granted, and pinned by
  `LockAdmissionCoreTests`.
- **MutualExclusion** - at most one holder at a time; a grant is possible only
  when `Decide` reports the lock free or its lease expired. Owned by `Decide` /
  `Grant`.
- **StaleTokenRejection** - once `B` holds the lock, no stale-token operation
  from a superseded holder `A` can dislodge it; `Renew` / `Release` honour only
  the current holder's token. Owned by `IsCurrentHolder` / `Renew` / `Release`;
  asserted by the model after every event once `B` is granted.
- **LeaseReclamationSafety** - an expired lease is reclaimed while the fencing
  counter is preserved, so the reclaiming grant still strictly increases and the
  reclaimed holder is fenced out. Owned by `ReclaimIfExpired`.

Liveness / fairness properties (checked by the grain integration tier, not the
pure model, because they concern the FIFO queue the grain owns):

- **FifoFairness** - waiters are granted the lock in strict enqueue order; no
  reordering. Checked by `AcquireAsync_grants_waiters_in_strict_fifo_order`.
- **NoStarvation under bounded faults** - a crashed or non-renewing holder's
  lease is reclaimed (by the in-activation timer, or the minute-grained keepalive
  reminder as the durable backstop) and the next waiter granted, so the queue
  always drains. Checked by `Lease_expiry_reclaims_and_grants_the_next_waiter`.

The safety properties have a live model home in `LockAdmissionModel`; the
fairness properties have a home in `LatticeLockGrainTests`. The exhaustive
truth-table for the pure core lives in `LockAdmissionCoreTests`.

## Related

- [Distributed Lock](distributed-lock.md) - the user-facing guide to the lock.
- [Verified Atomic-Commit Protocol](verified-atomic-commit.md) - the same
  proven-core + Coyote pattern applied to the multi-leaf atomic-write saga.
- [Verified WAL](verified-wal.md) - the pattern applied to the write-ahead log.
