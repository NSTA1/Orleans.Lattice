---
agent_spec: "docs/agents/api/auth.json"
---

# Architecture

The facade is a thin, administrator-gated layer over three seams the lower packages own: the membership directory, the authorization policy store and the access gate. It keeps no state of its own and adds no enforcement path beside the cluster's access gate. This page describes the request path by behaviour; the public surface is in the [Public API](api.md).

## Registration

`AddLatticeAuthApi` refuses to run unless `AddLatticeAuth(...)` has registered `ILatticeAuthorizationPolicyStore` on the same builder, and `AddLatticeAuth` in turn requires `AddLatticeMembership(...)`. It binds `LatticeApiAuthOptions` and registers one singleton implementation of `ILatticeAuthAdmin`. Nothing runs in the background; work happens only when a method is called. A transport binding such as [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/architecture.md) resolves `ILatticeAuthAdmin` from the same service provider.

## Request path

Every method follows the same shape:

1. **Argument validation.** Null or empty identifiers are rejected before anything else.
2. **Administrator check.** The facade asks the registered `ILatticeAccessGate` for a whole-tree `Admin` verdict on the reserved `sys-auth-policy` tree for the ambient caller, using the same enforcement primitive the core data path uses. The caller's subject comes from the core `ILatticeMembershipContext`, so it is whatever the [membership pipeline](../lattice.membership/architecture.md) resolves from the caller's credential. A bootstrap administrator passes through the gate's root-of-trust bypass; with `AccessAdministrationDelegationEnabled`, a subject holding the delegated grant passes too; every other caller, including an anonymous one, is denied with `LatticeAuthorizationDeniedException` and nothing is read or written.
3. **Tenant-tier checks.** A `t/` group id is refused on create and remove, a `tenant:` rule id and a tenant-wide sentinel scope are refused on rule authoring, and removal of a `tenant:` rule is the operator's break-glass path ([Tenant-tier alignment](README.md#tenant-tier-alignment)).
4. **Identity-directory validation.** On group creation and on adding a member, when `LatticeIdentityDirectoryOptions.ValidationRequired` is set and a real `ILatticeIdentityDirectory` is active, each referenced id (the group, and for an edge the member too) is resolved and the call fails with `LatticeDirectoryValidationException` when one does not resolve or resolves to the wrong kind. A `t/` id on an edge skips this step and is decided by the directory's nesting rule instead.
5. **The underlying operation.** Membership calls run against `ILatticeMembershipDirectory` under system origin, so the administrator check in step 2 is the single authorization point and the directory's own tree reads cannot re-enter the gate. Rule writes go to `ILatticeAuthorizationPolicyStore`, which runs its own tree operations under system origin and applies its rule-shape, app-owned and confinement guards.

The order of steps 2, 4 and 5 is fixed: authorize, then validate, then write.

## Introspection

`ExplainAsync` rebuilds the named subject from the membership directory - its transitive group closure, or for a group subject the group and its ancestors - and submits the request to the same `ILatticeAccessGate` the data plane uses, so the verdict is the gate's own. The evaluation runs inside a scope that keeps it off the decision meter and out of the audit sinks, because it is not the subject's own access attempt. Layered on that verdict, the facade asks the decision engine which layer and rule decided it, and collects the matching authored rules from the target tree and the `*` bucket, up to `MaxExplanationRules`.

`EffectivePermissionsAsync` resolves the same closure and lists every authored rule whose subject matches it, up to the same cap. It reports rules, not a verdict. Both results carry the live `AuthPolicyPosture`.

The directory reads `SearchDirectoryAsync` and `ResolveDirectoryPrincipalAsync` call the active `ILatticeIdentityDirectory`. With the default no-op provider they return `DirectorySearchResult.Unavailable` and `null` instead of calling it.

`GetAccessModelAsync` reports posture from the registrations it can see: the authentication mode from the registered credential authenticators, whether the gate enforces rules, the identity-directory provider and its guidance text, whether locally administered membership is effective under the group merge mode, the two authorization tier flags, and whether the tenant rule layer is active.

## Listings

`ListGroupsAsync`, `ListRulesAsync` and `ListRulesForTreeAsync` page with an exclusive continuation token over the directory's and the store's ordered enumerations. Filters - cluster groups only, or rules owned by the caller's validated active tenant - are applied before the page is cut. `ListRulesForTreeAsync` merges the tree's own rules with the `*` bucket and, for a tenant-layer tree, the owning tenant's tenant-wide rules, in catalogue order.

## Source map

- [Registration](../../src/lattice.api.auth/LatticeApiAuthServiceCollectionExtensions.cs)
- [Facade implementation](../../src/lattice.api.auth/LatticeAuthAdmin.cs)
- [Shared contract](../../src/lattice.api.abstractions/Auth/ILatticeAuthAdmin.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Auth` architecture](../lattice.auth/architecture.md) - the gate and policy store this facade drives.
- [`Orleans.Lattice.Api.Auth.Grpc` architecture](../lattice.api.auth.grpc/architecture.md) - the remote transport over this facade.
