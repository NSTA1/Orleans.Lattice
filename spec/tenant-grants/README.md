# Cross-tenant grants

This module checks the design of the cross-tenant grant protocol (issue
#6050): the two-party agreement behind `ILatticeTenantGrantAdmin`, in which
the granting tenant offers a grant, the grantee approves or rejects it and
either party (or an operator) revokes it, and the consumer of that agreement,
the tenancy engine's cross-tenant read resolution at the auth gate. It is not
a machine-checked refinement proof of the C# implementation; the
[refinement note](Refinement.md) states which production seams its detectors
reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `TenantGrants` | 7 | 0 | 14 | 25 | 20 | 56,456 |

## TenantGrants

**Authorization is the F1 contract.** The module declares the abstract
operator `Allowed(caller, op, scope)` of `spec/auth-decision/` (contract
version 1) and its cfg binds it to `ContractAllowed`, the contract's minimal
grant-set definition: the platform operator holds `Admin` on the policy tree,
each tenant's delegated admin holds `Admin` on its own capability scope, and
two readers hold a `Read` on the granting tenant's tree. Nothing here depends
on how `Allowed` reaches its verdict beyond those grants, so the module holds
for any definition satisfying C1-C6.

**Freshness is the TenantIsolation contract.** The read side's snapshot
currency is the epoch and lease protocol of
[`spec/tenant-isolation/`](../tenant-isolation/README.md), in the form that
module guarantees: a registry write returns only once a silo that trusted its
snapshot has been told or had its lease waited out, and a failed publication
returns early and owes a re-publish.

**Actors and actions.** Tenant A grants tenant B one scope. An operator and
each tenant's admin call the facade: `Offer`, `Answer` (approve or reject)
and `Revoke`. The operator's approvals and revocations may be split, reading
the record in one step and committing it through the record join in a later
one (`CommitCall`), which is also how a replicated or restored copy of the
record is merged. Three readers acting as B read one of A's trees through the
gate on the facade silo, which confirms against the registry, or on a peer
silo, which trusts its compiled snapshot while it holds it authoritative
(`Read`), and the gate's existence probe answers for them (`Probe`). Each
registry write is published to the peer (`Deliver`, `Lapse`, `Complete`,
`Fail`, `Retry`), and the peer recompiles (`Rebuild`). Either tenant can be
deleted (`DeleteTenant`) and the grantee created again under its old id
(`Recreate`).

**Properties.**

- `TwoPartyActivation`: a grant is active only once the granting side offered,
  and the grantee side approved, that very agreement generation, the approval
  in the grantee's current incarnation.
- `AdminScopedToOwnSide`: an offer is written with the granting tenant's
  authority, an approval or rejection with the grantee's, a revocation with
  either's.
- `ReadRequiresActiveGrant`: a crossing read is admitted only under the
  reader's own authorization, its right to act as the grantee, and an active
  grant from a registered granting tenant whose scope covers the tree.
- `RevocationTakesEffect`: once a revocation has returned, a read admitted
  later sees the revoked agreement as active only while a failed publication
  is still owed its re-publish. That is `TenantIsolation`'s
  `PublicationWindowTracked` window; with the publication complete the
  permitted staleness is zero.
- `NonDisclosure`: an unauthorized caller is told only that it was denied and
  writes nothing, only an operator learns a tenant is not registered, an
  authorized answer cannot tell an unoffered grant from an unregistered
  granting tenant, a refused read is told only the stage that refused it and
  never whether a tenant is registered, and the existence probe never reveals
  a tree a read would refuse.
- `NoUnilateralTrap`: a revocation of an active grant by an operator or an
  admin of a registered party is never refused.

**Bounds.** Two tenants, one grant, three admins (the operator and one admin
per tenant), three readers, two trees, two silos, three grant writes, one
publication failure and one re-creation of the grantee. Every property is a
safety property over the last step or the registry, so these bounds reach
every counterexample the mutations target. There are no liveness properties.

## Refinement and mutations

- [Refinement note](Refinement.md), including the deliberate abstraction gaps.
- [Mutations](mutations/): every property and every action of `Next` has a
  falsifying mutation; no action is exempt.
