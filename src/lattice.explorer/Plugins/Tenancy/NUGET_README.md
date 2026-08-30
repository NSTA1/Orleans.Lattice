# Orleans.Lattice.Explorer.Plugins.Tenancy

The shared **tenancy seam** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). It is not a
rendered surface of its own: it is the single place the Explorer's tenancy
plugins reach the cluster's tenant-administration facades, so each plugin
operates against a controlled domain model rather than the raw connection.

## What it provides

- **A client wrapper** over both tenant-administration gRPC clients - the
  administrative one and the read-only self-service one - wired to the same
  endpoint and sign-in as the Explorer's state connection, with the channel
  rebuilt lazily when either changes.
- **An Explorer-term domain model.** Every tenant, region, quota, admin subject,
  and cross-tenant grant the plugins read is projected onto a type owned by this
  package, so no control-API wire type reaches a plugin.
- **A fault mapping** that gives each documented facade refusal its own result
  status - an already-registered tenant, an unknown tenant, a reserved-tenant
  refusal, a region outside the allowed set, the last resident region, the last
  admin subject, an unknown grant, and an illegal grant transition - rather than
  collapsing them into one generic failure.
- **Availability detection.** On a cluster without the tenancy add-on the
  surface reports unavailable, so a tenancy plugin's gate resolves to the
  four-state model's unavailable state and renders nothing at all.
- **Quota figures that do not lie.** An absent ceiling stays absent rather than
  becoming a limit of zero, and an unmeasured dimension stays unmeasured rather
  than becoming a measured zero, so an unlimited tenant never renders as a full
  bar and an unsampled rate never renders as idle.
- **Grant state carried explicitly**, because only an active grant authorizes
  anything and a pending offer must never be presented as live access.

## Usage

```csharp
services.AddExplorerTenancy();
```

Call it after `AddExplorerConfiguration()` and `AddExplorerAuth()`, whose session
and sign-in the client reads, and after `AddExplorerTenantView()`, whose tenant
context, switcher, and platform-operator gate this seam reuses rather than
duplicating.
