# Orleans.Lattice.Explorer.Core

Head-agnostic core of the **Orleans.Lattice Explorer** - the read-only operational
console for an [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice)
cluster. This package holds the shared, head-independent building blocks that
every explorer head (web, desktop) composes over.

## What it provides

- The read-only **state-API connection seam** - the explorer's only cluster
  dependency is the public read-only state-API gRPC client; it never references
  the cluster core or any grain interface.
- The **configuration store**, **session**, and **capability model**.
- The **catalog**, **metrics**, **topology**, **data**, **dead-letter**,
  **history**, and **navigation** services, each registered by its own
  `AddExplorer*` extension.
- A launcher-friendly **environment bootstrap** that seeds the first-run endpoint
  from process environment variables.

## Usage

This package is normally consumed transitively through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register the services directly when composing a custom head:

```csharp
services.AddExplorerConfiguration();
services.AddExplorerCatalog();
services.AddExplorerMetrics();
services.AddExplorerTopology();
```

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
