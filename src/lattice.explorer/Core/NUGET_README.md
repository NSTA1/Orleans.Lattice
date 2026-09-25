# Orleans.Lattice.Explorer.Core

Head-agnostic core of the **Orleans.Lattice Explorer** - the read-only operational
console for an [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice)
cluster. This package holds the shared, head-independent building blocks that
every explorer head (web, desktop) composes over.

## What it provides

- The read-only **state-API connection seam** - the explorer's only cluster
  dependency is the public read-only state-API gRPC client; it never references
  the cluster core or any grain interface.
- The **configuration store** (`AddExplorerConfiguration`, which also registers
  the state-API connection and the configuration session) and the
  **authentication session** (`AddExplorerAuth`): the credential store, the
  built-in Basic sign-in method, the auth-scheme discovery probe, and the
  re-authentication and federated sign-out seams a sign-in provider configures.
- The **catalog**, **metrics**, **topology**, **data**, **dead-letter**,
  **history**, **session** (UI preferences), **navigation** (the shell's route
  model), and **tenant view** (tenant scoping) services, each registered by its
  own `AddExplorer*` extension.
- A launcher-friendly **environment bootstrap** (`AddExplorerEnvironmentBootstrap`)
  that seeds the first-run endpoint, and optionally a sign-in credential, from
  process environment variables.

The four-state access model the Explorer's areas and surfaces are gated by lives
in `Orleans.Lattice.Explorer.Plugins.Abstractions`, not here.

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
