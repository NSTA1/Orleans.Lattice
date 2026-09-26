# Orleans.Lattice.Explorer.Core

Head-agnostic core of the **Orleans.Lattice Explorer** - the operational console
for an [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) cluster, which
browses cluster state read-only and administers the cluster through
capability-gated areas. This package holds the shared, head-independent building blocks that
every explorer head (web, desktop) composes over.

## What it provides

- The read-only **state-API connection seam** - this package reaches the cluster
  only through the public read-only state-API gRPC client: it calls no grain and
  never joins the cluster. It does restore the Orleans.Lattice core library
  transitively, through the shared API abstractions, and its public types use the
  core's `HybridLogicalClock`; the admin areas bring their own control-API
  clients in their plugin packages. The connection is configured with a
  `LatticeConnectionSettings` record: the endpoint `Address`, the
  `AllowUnencryptedHttp2` opt-in, the `Authentication` seam, non-secret
  `TransportHeaders`, and the `DegradeAfter`, `HealthCheckInterval`,
  `TransientRetryBackoff`, and `MaxTransientRetries` timings. The Explorer
  documentation's configuration page lists every member with its type and
  default.
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
