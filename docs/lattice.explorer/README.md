# Orleans.Lattice.Explorer

A web console for a running [Orleans.Lattice](../../README.md) cluster. It browses
data, runs Lattice Apps, and administers access, schema, tenancy, replication,
backups, telemetry and the cluster itself, entirely over the cluster's gRPC APIs.

## What is it?

`Orleans.Lattice.Explorer.Web` is the embeddable hosting library for the Explorer.
The console reaches a cluster only through the cluster's public facades, so it
never joins Orleans membership. Out of the box it gives you:

- **Nine native areas.** Data, Apps, Access, Schema, Tenancy, Replication,
  Backups, Telemetry and Cluster are compiled into the console. Each one probes
  the facade it needs and shows itself only to a caller who can use it.
- **The address is the navigation.** Every page has one canonical, lower-case
  address. The address line shows it as a chain of nodes you can type into, with
  completions from every area and a command palette. A directory spine on the
  left lists the areas you can reach.
- **Lattice Apps as first-class citizens.** Browse app sources, review consent,
  and install, upgrade, disable and uninstall apps as native pages. An app's own
  UI runs in a sandboxed, credential-free frame that reaches the cluster only
  through the cluster's app bridge.
- **The documentation site's visual world.** The console is drawn in the same
  order-diagram language as the documentation site: the Paper and Board themes,
  a separate contrast axis, two densities, and the marker node for "you are
  here".
- **Phone-first-class.** Reading and simple actions work at 360px wide: tables
  become two-line rows with detail sheets, the spine becomes a slide-in sheet, and
  the header folds into one menu.
- **Auth-aware sign-in.** A pluggable `IExplorerAuthMethod` model (Basic,
  [Entra](connecting-to-an-auth-enabled-state-api.md) through the companion
  packages, or your own) attaches its credential to every call the console makes.
- **Two hosting shapes from one code path.** Run the standalone
  `Orleans.Lattice.Explorer.WebHost` process, or embed the console in your own
  ASP.NET Core application. Both use the same `AddLatticeExplorerWeb` and
  `MapLatticeExplorer` pair, so they cannot drift.

`Orleans.Lattice.Explorer.Web` is the single package a consumer references. The
libraries it composes, `Orleans.Lattice.Explorer.Core`,
`Orleans.Lattice.Explorer.UI` and `Orleans.Lattice.Explorer.AppKit`, restore
transitively.

## Core properties

- **Out-of-cluster by construction.** The console reaches a cluster only over
  its gRPC endpoint, so it can be deployed and scaled on its own and costs the
  silos nothing but the calls it makes.
- **Every change is a facade operation.** The console has no private path into a
  tree. Anything that changes state, such as a restore, a tenant deletion, a
  schema remediation, a reshard or an app writing through the bridge, is that
  facade's own authorised operation, and the cluster authorises every call.
- **Fail-closed areas.** An area that cannot prove you may see it is hidden, and
  its address renders the not-found page. Probes are time-boxed, so one slow
  facade never stalls the console. See [Area availability](area-availability.md).
- **One credential per circuit.** Every facade rides one channel for the browser
  circuit, built from the configured endpoint and the signed-in credential. It is
  rebuilt when the connection settings or the sign-in change.
- **No extension points but apps.** There is no plugin model and no public API to
  register an area. The only way a third party puts UI into the console is a
  [Lattice App](lattice-apps.md), and an app's UI never sees a credential.
- **Embeddable without wiring.** The UI ships its static web assets at
  `_content/Orleans.Lattice.Explorer.UI/`, served automatically. A host mounts
  the whole console with two extension calls under a configurable base path.

## Features

| Feature | Surface | Summary |
|---|---|---|
| Data | [State API](../lattice.api.state/README.md), [tree administration](../lattice.api.treeadmin/README.md) | Trees and views, key scans (live or snapshot), entries, per-key history and point-in-time reads, metrics, strict-mode dead letters, tag indexes and materialised views. See [The Explorer areas](areas.md#data). |
| Apps | [App control, catalogue, workspace and bridge](../lattice.api.apps/README.md) | Your apps, the source catalogue, consent review, lifecycle, each app's own pages, and its sandboxed UI. See [Lattice Apps in the Explorer](lattice-apps.md). |
| Access | [Auth control API](../lattice.api.auth/README.md) | Rules, groups and decision explanation. See [Managing access](managing-access.md). |
| Schema | [Schema control API](../lattice.api.schema/README.md) | Policy, version configuration, compliance scans, remediation and schema dead letters. See [Managing schema](managing-schema.md). |
| Tenancy | [Tenant administration API](../lattice.api.tenantadmin/README.md) | The operator's tenant directory and each tenant's members, quota, regions and sharing. See [Tenant scope](tenant-scope.md). |
| Replication | [Replication API](../lattice.api.replication/README.md) | The estate map, peer link health, enrolled trees, and enabling or disabling replication for a tree. See [The Explorer areas](areas.md#replication). |
| Backups | [Backup control API](../lattice.api.backup/README.md) | The backup catalogue, capture, restore, schedules, health and catalogue maintenance. See [Managing backups](managing-backups.md). |
| Telemetry | [Telemetry API](../lattice.api.telemetry/README.md) | Boards of charts and tables built from the queries the cluster offers the caller. See [The Explorer areas](areas.md#telemetry). |
| Cluster | [Tree administration API](../lattice.api.treeadmin/README.md) | The estate, regions, every tree's configuration, shards, storage and lifecycle, reshard, resize, snapshot, WAL placement and orphaned-leaf repair. See [The Explorer areas](areas.md#cluster). |
| Navigation | Address line, palette and spine | Canonical addresses, completions, commands, tenancy re-rooting. See [The Explorer navigation model](navigation-model.md). |
| Sign-in | `IExplorerAuthMethod` | Basic, Entra or custom sign-in that attaches its credential to every call. See [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md). |
| Hosting | `AddLatticeExplorerWeb` / `MapLatticeExplorer` | One code path for the standalone head and for embedding, under a configurable base path. See [Running and hosting the Explorer](running-the-explorer.md). |

## Quick Start

Run the bundled standalone head: the `Orleans.Lattice.Explorer.WebHost` process is
built on the two extension calls below, plus the standard exception-handler,
HSTS, HTTPS-redirection and antiforgery middleware.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeExplorerWeb();

var app = builder.Build();
app.UseAntiforgery();
app.MapLatticeExplorer();
app.Run();
```

To embed the console in an existing ASP.NET Core application, mount it under a
subpath and seed it with a configuration document, so there is no interactive
first-run step:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeExplorerWeb(options =>
{
    options.BasePath = "/explorer";
    options.ConfigFilePath = "explorer-config.json";
});

var app = builder.Build();
app.UseAntiforgery();
app.MapLatticeExplorer();
app.Run();
```

There is nothing to register per area: every area is compiled in and decides for
itself whether you may see it. The [Explorer sample](../../samples/Explorer/README.md)
co-hosts a single-silo cluster with the facades most areas need, and the
[task-board sample app](../../samples/Explorer/Apps/TaskBoard/README.md).

See [Running and hosting the Explorer](running-the-explorer.md) for the full
hosting, deployment and subpath guidance, and
[Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
for wiring sign-in.

## Reference

### Using the console

- [The Explorer navigation model](navigation-model.md) - the address grammar, the address line, completions, the command palette, the directory spine and tenancy re-rooting.
- [Area availability](area-availability.md) - how each area decides whether you may see it, and why hidden areas are left out rather than demoted.
- [The Explorer areas](areas.md) - every area's pages, addresses, query keys, actions and commands.
- [Lattice Apps in the Explorer](lattice-apps.md) - the Apps area, the sandboxed app frame, and writing an app UI.
- [Managing access](managing-access.md), [Managing schema](managing-schema.md) and [Managing backups](managing-backups.md) - the Access, Schema and Backups areas in depth.
- [Tenant scope](tenant-scope.md) - tenancy in the console and the Tenancy area.
- [What the Explorer remembers](what-the-explorer-remembers.md) - the preference contract: what is remembered, where, and how to reset it.
- [Theming and density](theming-and-density.md) - the themes, the contrast axis, density, and how a choice is applied at first paint.
- [Accessibility conformance](accessibility-conformance.md) - what the console targets, how that is verified, and the known limitations.

### Hosting and administration

- [Running and hosting the Explorer](running-the-explorer.md) - standalone and embedded hosting, package shape, the app frame route and security headers.
- [Configuration](configuration.md) - every public options property, its type and its default, plus the launcher environment variables, the persisted configuration document and the connection settings.
- [Multi-replica and failover hosting](multi-replica-hosting.md) - durable auth state and graceful re-authentication for a multi-replica deployment.
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md) - selecting a sign-in method and attaching its credential.
- [Adding a custom auth method](adding-a-custom-auth-method.md) - implementing `IExplorerAuthMethod` for a bespoke sign-in.

## See also

- [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md) - the optional Microsoft Entra ID interactive (desktop or device-code) sign-in provider.
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md) - the optional hosted-web (OpenID Connect) Entra sign-in provider.
- [Installable apps](../lattice.apps/README.md) - the Lattice Apps model the Apps area administers.
