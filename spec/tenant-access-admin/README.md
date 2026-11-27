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
| `TenantAccessAuthorization` | 7 | 1 | 13 | 20 | 20 | 3,398 |
| `TenantAccessMembership` | 4 | 0 | 8 | 13 | 11 | 17,176 |

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
  non-conforming rule, tenant-tier or operator, that arrives by replication
  or restore is dropped by the snapshot compiler.
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

## TenantAccessMembership

The concurrency half (#6191): what concurrent removals, removal stamps and
arriving records can do to a tenant's admin set, member set and app role
bindings. Authorization is taken as decided: every modelled operation is one
an admin of the tenant has already been authorized for.

**Actors and actions.** Two concurrent callers each read the tenant record,
check, and stamp a removal (`BeginRemoveAdmin`, `BeginRemoveMember`,
`BeginDeleteGroup`, the last removing the tenant group's admin and member
entries), then commit it (`Commit`): one committing attempt of the registry's
compare-and-set loop, which re-merges the tombstones into the stored record
and re-applies the last-admin guard to the merged record before writing.
Removals run on a silo whose clock lags; adds (`AddEntry`) run on one whose
clock leads. Tenant records arrive by replication or restore around the
facades (`ArriveEntry`), stamped ahead of both silos and carrying another
tenant's group, and so do app records carrying role bindings
(`ArriveBinding`). A membership decision (`Count`) reports which entries
conferred anything.

**Properties.**

- `LastAdminGuard`: no interleaving of removals and group deletions leaves the
  tenant without an admin entry that counts for it, and a refused removal
  writes nothing. An entry that never counts (another tenant's group) does not
  hold the set open.
- `RemovalWinsUnderSkew`: a committed removal supersedes the slot it read,
  even when the remover's clock lags the silo or peer that wrote the slot.
- `NoCrossTenantMembership`: an admin-set, member-set or role-binding entry
  naming another tenant's group, or a malformed `t/` id, never confers
  anything, written or arrived, and a subject whose own id is shaped like such
  a group never matches it by exact id.

**Bounds.** One tenant with two users, a cluster group and its own group (two
admin entries and two member entries at the start), another tenant's group
and a malformed `t/` id, two concurrent callers, two silos, and three
operations (a removal's begin, an add or an arrival) per behaviour; commits
and decisions are free. Every property is a safety property, and three
operations reach every counterexample the mutations target, the deepest being
an arrival followed by two racing removals. There are no liveness properties.

## Refinement and mutations

- [Authorization refinement](RefinementAuthorization.md), including the
  deliberate abstraction gaps.
- [Authorization mutations](mutations-authorization/): every property and
  every action of `Next` has a falsifying mutation; no action is exempt.
- [Membership refinement](RefinementMembership.md), including the deliberate
  abstraction gaps.
- [Membership mutations](mutations-membership/): every property and every
  action of `Next` has a falsifying mutation; no action is exempt.
