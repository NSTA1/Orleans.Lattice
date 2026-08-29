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
surface. It takes an unqualified tree name, resolves it into the caller's own
tenant namespace, and exposes tree create, existence check, delete, recover,
purge, deletion status, and schema-policy get / set / clear - so a tenant
administers its own trees without ever naming another tenant's.

## Fail-closed by design

Every administrative operation authorizes the caller through the Lattice access
gate (the cluster-wide administrative operation) **before** it reads or writes
the registry, and an explicitly supplied admin subject is validated against the
identity directory when one is configured. An unauthenticated or unauthorized
caller is refused without learning whether a tenant exists. The reserved default
tenant can never be suspended or deleted, and a tenant id that shadows a reserved
namespace is rejected.

The add-on is **opt-in**: a cluster that does not register it exposes no tenant
administration and behaves exactly as before.

## Registration

```csharp
siloBuilder
    .AddLatticeTenancy(/* ... */)
    .AddLatticeTenantAdminApi()
    .AddLatticeTenantScopedTreeAdminApi();
```

`AddLatticeTenantAdminApi()` must be called after `AddLatticeTenancy(...)`: the
facade operates on the tenancy engine's tenant registry, so that store must be
registered first. Calling it out of order fails fast with an actionable message.
`AddLatticeTenantScopedTreeAdminApi()` is an independent opt-in - add it only if
you want tenants to administer their own trees.

This package adds no transport behaviour of its own. Add the gRPC or MCP binding
package to expose the facade over the wire.

See the
[tenant-administration documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.tenantadmin/README.md)
for the full guide.
