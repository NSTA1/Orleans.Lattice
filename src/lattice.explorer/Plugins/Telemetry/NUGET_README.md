# Orleans.Lattice.Explorer.Plugins.Telemetry

The shared **telemetry seam** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice), plus the
two surfaces built on it. The seam is the single place the Explorer's telemetry
panels reach the cluster's telemetry facade, so each panel operates against a
controlled domain model rather than the raw connection. The operator-facing
Telemetry area (`AddExplorerTelemetryPlugin()`) and the tenant-metrics section of
the My tenant area (`AddExplorerTelemetryMyTenantSection()`) are independent
opt-ins; both need `AddExplorerTelemetry()`.

## What it provides

- **A client wrapper** over the telemetry gRPC binding, wired to the same
  endpoint and sign-in as the Explorer's state connection, with the channel
  rebuilt lazily when either changes.
- **Server-authored catalogue discovery.** A panel is driven by the queries the
  cluster actually offers - their titles, units, measurement semantics, and
  declared bounds - rather than by ids hard-coded in the client, so a panel
  title cannot drift from the instrument behind it.
- **An Explorer-term domain model.** Every query descriptor, bound, window,
  series, and scope a panel reads is projected onto a type owned by this
  package, so no control-API wire type reaches a panel.
- **A fault mapping that keeps three different failures apart.** An unknown or
  unoffered query, a window the entry's bounds refuse, and a metrics backend
  that could not answer are three distinct result statuses - so a backend outage
  never presents as an invalid query and a user never retries a bad query
  forever.
- **Tenant scoping left where it belongs.** The seam forwards the visibility the
  caller *requests* and reports the scope the facade *pinned*, including whether
  the request was degraded. It performs no local tenant filtering: that is the
  bypassable path a routable facade exists to prevent.
- **Availability detection.** On a cluster that serves no telemetry facade, or
  that offers the caller no queries at all, the surface reports unavailable, so
  a telemetry plugin's gate resolves to the four-state model's unavailable state
  and renders nothing.

## Usage

```csharp
services.AddExplorerTelemetry();
services.AddExplorerTelemetryPlugin();          // the Telemetry area
services.AddExplorerTelemetryMyTenantSection(); // the My tenant metrics section
```

The area plugin also needs the shell's plugin adapters (`AddExplorerPluginAdapters()`),
and the metrics section appears only when the My tenant area is registered too. A
head that renders either surface links the plugin stylesheet:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.Plugins.Telemetry/lattice-telemetry.css" />
```

Call it after `AddExplorerConfiguration()` and `AddExplorerAuth()`, whose session
and sign-in the client reads, and after `AddExplorerTenantView()` when the head
has the tenancy add-on, whose requested visibility this seam forwards rather
than deciding one of its own.
