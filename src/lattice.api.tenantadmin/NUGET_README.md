# Orleans.Lattice.Api.TenantAdmin

Optional transport-agnostic **control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) multi-tenant
clusters. It presents one discoverable, fail-closed control surface for tenant
administration over the tenancy add-on's tenant registry, so a host can expose it
through a single seam that every transport binding (gRPC, MCP) adapts over.

## What it provides

`ILatticeTenantAdmin` covers the tenant lifecycle and quota authoring:

| Operation | Description |
|-----------|-------------|
| `CreateTenantAsync` | Registers a new active tenant and seeds the admin subjects that may see it (the calling subject when none are supplied). Fails if the tenant already exists. |
| `SuspendTenantAsync` | Transitions an existing tenant to the suspended status (idempotent). |
| `ResumeTenantAsync` | Transitions a suspended tenant back to active (idempotent). |
| `DeleteTenantAsync` | Removes a tenant, cascading the delete to the tenant's trees. |
| `SetTenantQuotasAsync` | Authors the tenant's resource quotas (keys, bytes, memory, trees, operations per second, and burst allowance). |

`ILatticeTenantRegionAdmin` administers per-tenant region residency:

| Operation | Description |
|-----------|-------------|
| `AuthorizeAllowedRegionsAsync` | Sets the regions a tenant is permitted to occupy. |
| `SetResidencyAsync` | Binds the tenant's data to a subset of its allowed regions. |
| `GetTenantRegionStatusAsync` | Reports the tenant's allowed, resident, and in-flight region state. |

`ILatticeTenantSelfService` is the read-only surface any authenticated caller may
invoke, scoped fail-closed to that caller: `GetCurrentTenantAsync` answers "which
tenant am I acting as", `ListAccessibleTenantsAsync` enumerates only the tenants
the caller may see, and `GetTenantAsync` reports one such tenant's status.

`ILatticeTenantScopedTreeAdmin` is an optional tenant-local tree-administration
surface. It takes an unqualified tree name, composes it into the active tenant's
namespace, and exposes tree create, existence check, delete, recover,
purge, deletion status, and schema-policy get / set / clear - so a tenant
administers its own trees without ever naming another tenant's.

`ILatticeTenantAccessAdmin` lists, adds, and removes a tenant's tenant-admin
subjects (a tenant can never be left without one), `ILatticeTenantGrantAdmin`
administers the two-step cross-tenant grant agreement (the granting tenant offers,
the grantee approves or rejects, and either party may revoke), and the read-only
`ILatticeTenantQuotaUsage` reports a tenant's per-dimension consumption against its
quota ceilings.

## Fail-closed by design

Every administrative operation authorizes the caller **before** it changes
anything (the tenant-tier gate reads only the target tenant's record to decide) - the tenant lifecycle and the
allowed-region set as a cluster-wide administrative operation, the tenant-tier
operations (residency, admin subjects, cross-tenant grants, usage) for the platform
operator or a live admin subject of the tenant - and an explicitly supplied admin
subject is validated against the identity directory when one is configured. An unauthenticated or unauthorized
caller is refused without learning whether a tenant exists. The reserved default
tenant can never be suspended, deleted, given quotas, have its admin subjects
changed, or be named in a cross-tenant grant offer, and a tenant id that shadows a
reserved namespace is rejected.

The add-on is **opt-in**: a cluster that does not register it exposes no tenant
administration and behaves exactly as before.

## Registration

```csharp
siloBuilder
    .AddLatticeTenancy(/* ... */)
    .AddLatticeTenantAdminApi();

// Optional: tenant-local tree administration, composing the tree-admin facade.
siloBuilder
    .AddLatticeSchemaEnforcement()
    .AddLatticeSchemaApi()
    .AddLatticeTreeAdminApi()
    .AddLatticeTenantScopedTreeAdminApi();
```

`AddLatticeTenantAdminApi()` must be called after `AddLatticeTenancy(...)`: the
facade operates on the tenancy engine's tenant registry, so that store must be
registered first. Calling it out of order fails fast with an actionable message.
`AddLatticeTenantScopedTreeAdminApi()` is an independent opt-in - add it only if
you want tenants to administer their own trees. It must be called after
`AddLatticeTreeAdminApi()` and `AddLatticeSchemaEnforcement(...)`, whose surfaces it
composes, and likewise fails fast when called out of order.

This package adds no transport behaviour of its own. Add the gRPC or MCP binding
package to expose the facade over the wire.

See the
[tenant-administration documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.tenantadmin/README.md)
for the full guide.
