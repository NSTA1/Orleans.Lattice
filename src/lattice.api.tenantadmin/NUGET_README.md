# Orleans.Lattice.Api.TenantAdmin

Optional transport-agnostic **control facade** add-on for Orleans.Lattice
multi-tenant clusters. It presents one discoverable, fail-closed control surface
for the tenant lifecycle over the tenancy add-on's tenant registry, so a host can
expose tenant administration through a single seam that every transport binding
(gRPC, MCP) adapts over.

## What it provides

`ILatticeTenantAdmin` covers the tenant lifecycle:

| Operation | Description |
|-----------|-------------|
| `CreateTenantAsync` | Registers a new active tenant and seeds the admin subjects that may see it (the calling subject when none are supplied). Fails if the tenant already exists. |
| `SuspendTenantAsync` | Transitions an existing tenant to the suspended status (idempotent). |
| `ResumeTenantAsync` | Transitions a suspended tenant back to active (idempotent). |
| `DeleteTenantAsync` | Removes a tenant, cascading the delete to the tenant's trees. |

## Fail-closed by design

Every operation authorizes the caller through the Lattice access gate (the
cluster-wide administrative operation) **before** it reads or writes the
registry. An unauthenticated or unauthorized caller is refused without learning
whether a tenant exists. The reserved default tenant can never be suspended or
deleted.

The add-on is **opt-in**: a cluster that does not register it exposes no tenant
administration and behaves exactly as before.

## Registration

```csharp
siloBuilder
    .AddLatticeTenancy(/* ... */)
    .AddLatticeTenantAdminApi();
```

`AddLatticeTenantAdminApi()` must be called after `AddLatticeTenancy(...)`: the
facade operates on the tenancy engine's tenant registry, so that store must be
registered first. Calling it out of order fails fast with an actionable message.

This package adds no transport behaviour of its own. Add the gRPC or MCP binding
package to expose the facade over the wire.
