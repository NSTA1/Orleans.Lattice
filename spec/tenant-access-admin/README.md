# Delegated tenant-access administration

These modules check the design of delegated tenant-access administration
(epic #4154): tenant groups in the reserved `t/` namespace, a tenant's admin
and member sets, and the `tenant:` rules a tenant's own admins author beneath
the operator layer, all behind
`LatticeTenancyOptions.DelegatedAccessAdministrationEnabled`. They are not a
machine-checked refinement proof of the C# implementation; each refinement
note states which production seams its detectors reach.

The directory holds one module per half of the design, each with its own cfg,
manifest, mutation directory and refinement note. This README is shared.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `TenantAccessAuthorization` | 7 | 1 | 13 | 19 | 20 | 3,338 |

## TenantAccessAuthorization

The authorization half (#6049): who may write what, where a tenant group may
appear, which token claims survive, what the flag switches off, and the order
of checks in every facade verb.

**Authorization is the F1 contract.** The module declares the abstract
operator `Allowed(caller, op, scope)` of `spec/auth-decision/` (contract
version 1) and its cfg binds it to `ContractAllowed`, the contract's minimal
grant-set definition: the platform operator holds `Admin` on the policy tree,
and tenant A's delegated admin holds `Admin` on A's capability scope (tenant B
has none, so A's admin acting on B is the cross-tenant case). Nothing here depends on how `Allowed` reaches its verdict beyond those
two grants, so the module holds for any definition satisfying C1-C6.

**Actors and actions.** Three callers (a platform operator, an admin of
tenant A, and an outsider) act through the tenant policy facade (`FacadePutRule`, `Posture`), the tenant
directory facade (`FacadeUpsertGroup`, `FacadeAddEdge`, `AddMemberEntry`), the
admin-set facade (`AddAdminEntry`, which is not behind the flag) and the
cluster authorization facade (`OperatorPutRule`, `OperatorUpsertGroup`,
`ClusterAddEdge`). Policy records also arrive by replication or restore
(`ArriveRule`), written around the store. There are two silos, each with its
own flag: the facades are called on the first, and either silo resolves a token's claims
(`ResolveClaims`), decides a request (`Evaluate`), and flips its flag
(`FlipFlag`). Identity-directory validation is either required or not for the
whole behaviour.

**Properties.**

- `TenantRuleConfinement`: a `tenant:` rule is written only by the tenant
  policy facade after its admin check, is confined to its tenant's
  tenant-layer trees and tenant-wide scope with data-plane operations, and no
  rule in effect names a tenant group outside that group's tenant. A
  non-conforming tenant rule that arrives by replication or restore is
  dropped by the snapshot compiler.
- `ReservedGroupNamespace`: no cluster group anywhere in `t/` (malformed ids
  and `t/default/...` included), no tenant group for the default tenant, and
  no edge nesting a tenant group in a cluster group or another tenant's group.
- `ClaimsNeverConferTenantGroups`: an asserted `t/` group never survives claim
  filtering, whatever the resolving silo's flag says, unless the directory
  records the membership.
- `FeatureOffInert`: with a silo's flag off its delegated facades refuse every
  authorized call (the posture excepted) and its evaluator never uses a tenant
  rule. `FlipDeletesNothing`: a flip, either way, deletes nothing.
- `AuthorizeBeforeDisclose`: an unauthorized caller is told only that it was
  denied, and nothing is written.
- `DirectoryValidationOrder`: validation runs only after authorization, a
  refused validation writes nothing, and with validation required no stored
  reference names an unresolvable principal or a missing tenant group.

**Bounds.** Two tenants and the default tenant, one delegated admin (of tenant A), one group per tenant (A's exists at the start), one
user, one unresolvable id, one cluster group, two malformed `t/` ids, seven
rule scopes and one write per behaviour. Every property is a safety property
over the last step or the stores, so one write reaches every counterexample the
mutations target. There are no liveness properties.

## Refinement and mutations

- [Authorization refinement](RefinementAuthorization.md), including the
  deliberate abstraction gaps.
- [Authorization mutations](mutations-authorization/): every property and
  every action of `Next` has a falsifying mutation; no action is exempt.
