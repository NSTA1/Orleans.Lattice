# Orleans.Lattice.Explorer.Tenants

The **Tenants** plugin for the [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice)
Explorer: the platform-operator surface for managing every tenant on a cluster.

## What it gives you

- **Tenant list** - every tenant with its lifecycle state, the reserved-default
  flag, and headline usage.
- **Lifecycle** - create with seeded admin subjects, suspend, resume, and delete.
  Delete cascades to the tenant's trees, so the confirmation reports the tree
  count first.
- **Quotas** - view and author every dimension (`MaxBytes`, `MaxKeys`,
  `MaxMemoryBytes`, `MaxTreeCount`, `MaxOpsPerSecond`, and the burst percent).
  An unbounded dimension renders as unlimited and an unmeasured one as not
  measured; neither is ever flattened to zero, and the enforcement scope is
  captioned beside the figures.
- **Allowed regions** - authorize a tenant's allowed region set and read its
  per-region lifecycle status. Revoking a region the tenant is still resident in
  is refused by the cluster, and that refusal is reported for what it is.
- **Tenant access** - administer admin subjects, and offer, approve, reject, or
  revoke cross-tenant grants. Every grant carries its lifecycle state
  explicitly, because only an active grant authorizes anything.

## Gating

The area is **platform-operator only** and reports **unavailable** - rendering no
entry at all - on a cluster without the tenancy add-on. Gating on the client is
advisory; the cluster remains the sole enforcement point, so every operation
still renders a runtime refusal for what it is.

## Getting started

Registered for you by
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerPluginAdapters();
services.AddExplorerAccess();          // supplies the platform-operator gate
services.AddExplorerAccessPlugin();
services.AddExplorerTenantsPlugin();   // must follow the operator-gate provider
```

and link the plugin stylesheet from the head's document head:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.Tenants/lattice-tenants.css" />
```

Withholding `AddExplorerTenantsPlugin()` ships no Tenants area at all;
withholding the package reference removes the feature entirely.

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
