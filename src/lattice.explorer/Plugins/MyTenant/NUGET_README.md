# Orleans.Lattice.Explorer.Plugins.MyTenant

The **My Tenant** plugin for the [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice)
Explorer: the self-service area a *tenant administrator* manages their own
tenant from.

It is a self-contained plugin package - services, Razor components, and a scoped
stylesheet - that reaches the cluster only through the shared tenancy seam's
controlled domain model (`ITenancyDomain`). It references no gRPC binding and no
control-API contract of its own.

## Surfaces

| Surface | What it does |
|---------|--------------|
| Overview | The tenant's own descriptor and lifecycle status, and the tenants the caller may switch to when they administer more than one. |
| Members | The tenant's admin subjects: list, add, and remove. Removing the last one is refused, and the refusal is named rather than shown as a generic failure. |
| Quota | Consumption **against** each ceiling, captioned with the scope the figures were enforced under. |
| Regions | Residency management against the operator-authorized allowed set, with per-region lifecycle status. |
| Sharing | This tenant's side of the two-step cross-tenant grant agreement: outbound offers and revocations, and an inbound inbox with approve and reject. |
| Metrics | A placeholder seam the tenant-metrics issue fills. |

## Three things it refuses to flatten

1. **An unbounded quota dimension is not a ceiling of zero.** A `null` limit
   means no ceiling at all and renders as *No limit*, never as a full bar.
2. **An unmeasured dimension is not a measured zero.** A `null` usage means the
   reading carries no consumption figure, and renders as *Not measured*, never
   as an empty bar that reads "you are using none of your allowance".
3. **A pending grant authorizes nothing.** Only an `Active` grant is live, and
   every grant renders its state explicitly wherever it appears.

## Tenant isolation

The plugin scopes every read and every mutation to the caller's *active* tenant.
No tenant id is ever taken from the view, and every cross-tenant grant transition
is checked against the active tenant's role in that grant before a call is made,
so an admin of tenant A can neither approve a grant offered to B nor offer one
from B. The cluster re-enforces all of it: client gating here is advisory.

## Registration

```csharp
builder.Services.AddExplorerAccess();      // supplies the real platform-operator gate
builder.Services.AddExplorerTenantView();  // opts into tenant scoping
builder.Services.AddExplorerTenancy();     // the shared tenancy seam
builder.Services.AddExplorerMyTenant();
builder.Services.AddExplorerMyTenantPlugin();
```

`AddExplorerAccess()` must come **before** `AddExplorerTenantView()`: the
navigation core registers a fail-closed placeholder platform-operator gate with
`TryAdd`, so a head that calls them the other way round silently loses the real
one. This plugin detects that and says so rather than degrading in silence.

A head that registers none of it ships no My Tenant area, and a cluster without
the tenancy add-on reports the surface unavailable so it renders nothing.
