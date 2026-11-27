# TLA+ specification of tenant policy freshness

This directory holds the formal specification of how a tenant registry write
reaches every silo's compiled policy snapshot, and when a silo may trust that
snapshot for an authorization decision. It follows the pattern the other modules
set: a TLA+ design checked by TLC in CI, every property and every action paired
with a mutation, and a refinement note mapping each construct to production and
to detector tests. Rate budgets and footprint quotas are out of scope.

## Modules

| Module | What it specifies |
|--------|-------------------|
| [`TenantIsolation.tla`](TenantIsolation.tla) | A registry write committing on one silo, the epoch advance published through the epoch grain's lease ledger, each silo's snapshot, lease and rebuild, publication failure and background re-publish, silo crash and membership recovery, membership falsely declaring a live silo dead, and epoch grain re-activation with the grace rule both epoch grains share. |

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `TenantIsolation` | 6 | 2 | 23 | 31 | 29 | 200,358 |

`Actions` counts the disjuncts of `Next`, including the non-behavioural `Stutter`.
`Behaviour rows` counts the action rows of the refinement note, excluding
non-behavioural actions, plus its property rows. `Distinct states` is TLC's count
for the base cfg (two silos, no faults).

### Variant configurations

Faults are checked in separate configurations so each stays tractable. Every
variant checks the same invariants and properties as the base cfg, except
`TenantIsolation.FalseDead.cfg`, which checks the safety properties only to bound
its cost; liveness under a false death declaration is checked in
`TenantIsolation.Faults.cfg`.

| Configuration | Instance | Distinct states |
|---------------|----------|-----------------|
| `TenantIsolation.cfg` | Two silos, no faults | 200,358 |
| `TenantIsolation.Crash.cfg` | Two silos, one silo crash | 1,361,654 |
| `TenantIsolation.Restart.cfg` | Two silos, one epoch grain re-activation | 672,536 |
| `TenantIsolation.Failure.cfg` | Two silos, one failed publication | 627,966 |
| `TenantIsolation.FalseDead.cfg` | Two silos, one epoch grain re-activation, one false death declaration | 2,680,898 |
| `TenantIsolation.Faults.cfg` | One silo, one of each fault interleaved, a false death declaration included | 108,547 |

## Files

| File | What it is |
|------|-----------|
| `TenantIsolation.tla` / `.cfg` / `.manifest.json` | The module, its base TLC model and its manifest. |
| `TenantIsolation.*.cfg` | The fault variant configurations. |
| [`mutations/`](mutations/) | One or more deliberate defects per property and per action. |
| [`TenantIsolationRefinement.md`](TenantIsolationRefinement.md) | The model mapped to production: variables, actions, properties, detectors and gaps. |

## Properties checked

| Property | Kind | Meaning |
|----------|------|---------|
| `TypeOK` | Invariant | State stays well-typed and within the fault budgets. |
| `NoStaleAuthority` | Invariant | Once a write has returned after its publication completed, no silo trusts a snapshot that misses it. |
| `WriterReadsOwnWrite` | Invariant | The committing silo never trusts a snapshot that misses its own write. |
| `PublicationWindowTracked` | Invariant | A write returns ahead of its coverage only through a failed publication still owed a re-publish, or a crashed writer. |
| `UnconfirmableDenies` | Invariant | A decision trusts only an authoritative snapshot, confirms only against a reachable registry, and otherwise denies. |
| `NoEarlyGraceRelease` | Invariant | A fresh epoch grain activation ends its grace only once no silo can still serve on a lease a previous activation granted: each has leased afresh, or has been seen dead by the activation for one lease plus the margin. A silo falsely declared dead is not assumed to have stopped. |
| `EpochMonotonic` | Action property | Epochs never repeat across advances or grain re-activations. |
| `EveryCommitEventuallyCovered` | Liveness | Every committed write is eventually published to every silo, or covered by its crashed writer being declared dead. |

## Defects this specification found

| Issue | Defect | Mutation |
|-------|--------|----------|
| #4806 | The epoch ledger captured only leases inside their recorded deadline, so an advance neither pushed to nor waited out a silo whose slower clock still trusted its lease inside the documented margin. Fixed: the ledger captures and waits out leases through deadline plus margin. | `StartAdvanceDropsMarginLease` |
| #5719 | The tenant-policy epoch grain ended its fresh-activation grace as soon as every member membership did not report dead had leased, so a silo falsely declared dead could keep deciding on a lease from the previous activation after a registry write returned. Fixed: a death counts only once the activation has seen it for one lease plus the margin. | `ReleaseGraceSkipsDead` |
| #6216 | Survivors invalidated their snapshots once, when membership declared a silo dead, so a registry write that silo committed after a false declaration and failed to publish before it stopped was never covered. Fixed: every newly-dead silo opens a false-death horizon during which survivors keep re-invalidating, and the registry refuses writes on a silo membership reports dead. | `HorizonClosesLeavesWriteUncovered` |

## What the assurance covers, and what it does not

The guarantee is bounded and conditional. No new authorization decision trusts a
snapshot older than a write whose publication completed before the write returned,
and a decision that cannot confirm currency denies. It does not revoke decisions
already taken. Two windows stay open by design and are tracked rather than assumed
away: a write whose publication fails returns while peers may trust their old
snapshot until the background re-publish lands, and a writer that crashes after
committing leaves peers unaware until membership declares it dead. A silo membership
falsely declares dead keeps serving on its snapshot and lease until it learns it is
dead, so a fresh epoch grain activation's grace counts a death only once it has seen
it for one lease plus the margin; the tenant-policy and app registry epoch grains both
implement that rule. Such a silo can also commit a registry write after its
declaration and stop before publishing it, so every survivor re-invalidates until a
false-death horizon has passed; this assumes a silo falsely declared dead stops within
the horizon. The guarantee
assumes no silo clock runs slower than the epoch grain's by more than the ledger
margin over one lease. Bounds are one write and two silos; see the refinement note
for the abstraction gaps.

## How to run TLC

```powershell
java -Xmx512m -cp C:\path\to\tla2tools.jar tlc2.TLC -workers 1 -config TenantIsolation.cfg TenantIsolation.tla
```

Pass `-metadir` with a directory outside the repository, or delete the `states/`
directory TLC leaves beside the module. The base cfg takes about a minute and the
largest variant a few minutes.