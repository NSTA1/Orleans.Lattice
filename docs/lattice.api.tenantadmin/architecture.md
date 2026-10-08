# Architecture

The facade resolves caller authority and composes tenancy, membership, authorization and tree administration. It is transport-independent; gRPC and MCP adapt the same control contracts rather than independently implementing tenant lifecycle.

## Authorization before mutation

Registration requires the tenancy registry and supplies a monotonic write clock and tenant-tree cascade seam. Lifecycle and quota writes use operator authority. Tenant-specific operations consult the target tenant and the relevant operator or tenant-admin tier. Self-service derives visibility from the caller and returns no inaccessible record. Delegated directory/policy operations additionally require the tenancy opt-in and enforce tenant group/tree/rule confinement.

These boundaries are separate: an asserted tenant is not proof of authority, transport authorization is not a tenant-admin grant, and a tenant administrator cannot raise its own quotas or widen the operator-allowed region set. See [the authorization seams](README.md#authorization-seams) for the public contracts.

## Definition writes and side effects

Accepted registry changes carry monotonic last-writer-wins timestamps. Tenant deletion composes with the tree-cascade seam, while region-residency changes drive backfill and drain through resumable work. Hosted discovery re-finds owed backfill, and region-status listeners advance the lifecycle as work completes. A recorded target state is not proof that every tree has already moved or been purged.

Directory member/group operations use the membership-store seam. Tenant policy operations use the policy-store and policy-decision seams, filtering tenant-visible rules and usage rather than exposing unrestricted cluster policy administration.

## Namespace-confined tree facade

`ILatticeTenantScopedTreeAdmin` composes logical names into the selected tenant's tree namespace and delegates lifecycle operations to `ILatticeTreeAdmin`. Schema-policy changes are authorized by the tenant-scoped facade itself. This service does not expose a route for administering another tenant by passing an already-qualified or reserved tree id.

## Source map

- [Registration](../../src/lattice.api.tenantadmin/LatticeApiTenantAdminServiceCollectionExtensions.cs)
- [Lifecycle implementation](../../src/lattice.api.tenantadmin/LatticeTenantAdmin.cs)
- [Region administration](../../src/lattice.api.tenantadmin/LatticeTenantRegionAdmin.cs)
- [Backfill discovery](../../src/lattice.api.tenantadmin/TenantRegionBackfillDiscoveryService.cs)
- [Scoped tree contract](../../src/lattice.api.tenantadmin/ILatticeTenantScopedTreeAdmin.cs)
- [Directory registration](../../src/lattice.api.tenantadmin/Directory/LatticeApiTenantAdminServiceCollectionExtensions.Directory.cs)
- [Policy registration](../../src/lattice.api.tenantadmin/Policy/LatticeApiTenantAdminServiceCollectionExtensions.Policy.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Tenancy architecture](../lattice.tenancy/architecture.md)
