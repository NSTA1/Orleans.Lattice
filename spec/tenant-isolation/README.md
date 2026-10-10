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
| [`TenantIsolation.tla`](TenantIsolation.tla) | A registry write committing on one silo, the epoch advance published through the epoch grain's lease ledger, each silo's snapshot, lease and rebuild, publication failure and background re-publish, silo crash and membership recovery, and epoch grain re-activation with its grace. |

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `TenantIsolation` | 5 | 2 | 20 | 25 | 25 | 200,358 |

`Actions` counts the disjuncts of `Next`, including the non-behavioural `Stutter`.
`Behaviour rows` counts the action rows of the refinement note, excluding
non-behavioural actions, plus its property rows. `Distinct states` is TLC's count
for the base cfg (two silos, no faults).

### Variant configurations

Faults are checked in separate configurations so each stays tractable. Every
variant checks the same invariants and properties as the base cfg.

| Configuration | Instance | Distinct states |
|---------------|----------|-----------------|
| `TenantIsolation.cfg` | Two silos, no faults | 200,358 |
| `TenantIsolation.Crash.cfg` | Two silos, one silo crash | 919,142 |
| `TenantIsolation.Restart.cfg` | Two silos, one epoch grain re-activation | 672,536 |
| `TenantIsolation.Failure.cfg` | Two silos, one failed publication | 627,966 |
| `TenantIsolation.Faults.cfg` | One silo, one of each fault interleaved | 32,602 |

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
| `EpochMonotonic` | Action property | Epochs never repeat across advances or grain re-activations. |
| `EveryCommitEventuallyCovered` | Liveness | Every committed write is eventually published to every silo, or covered by its crashed writer being declared dead. |

## Defects this specification found

| Issue | Defect | Mutation |
|-------|--------|----------|
| #4806 | The epoch ledger captured only leases inside their recorded deadline, so an advance neither pushed to nor waited out a silo whose slower clock still trusted its lease inside the documented margin. Fixed: the ledger captures and waits out leases through deadline plus margin. | `StartAdvanceDropsMarginLease` |

## What the assurance covers, and what it does not

The guarantee is bounded and conditional. No new authorization decision trusts a
snapshot older than a write whose publication completed before the write returned,
and a decision that cannot confirm currency denies. It does not revoke decisions
already taken. Two windows stay open by design and are tracked rather than assumed
away: a write whose publication fails returns while peers may trust their old
snapshot until the background re-publish lands, and a writer that crashes after
committing leaves peers unaware until membership declares it dead. The guarantee
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