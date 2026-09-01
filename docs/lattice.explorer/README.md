# Orleans.Lattice.Explorer

An opt-in, auth-aware web console for a running [Orleans.Lattice](../../README.md) cluster - browse trees and materialised views, and administer backups and access control, entirely over the cluster's gRPC APIs.

## What is it?

`Orleans.Lattice.Explorer.Web` is the embeddable hosting library for the Explorer console. It talks to a cluster only through the read-only state API and the auth, backup, and schema control gRPC bindings, so it never joins the cluster's Orleans membership and never holds a mutation path into the data plane beyond what those control facades already expose. It adds:

- **A read-only tree browser** - the catalog of trees and materialised views, each tree's shard-root structure, key-ordered snapshot-isolated entry scans and single-key record inspection, live change observation, and per-tree metrics, all over the [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) gRPC surface.
- **Capability-gated admin areas** - a Backups area over the backup control API and an Access (membership and access-control) area over the auth control API, each surfaced only after a capability probe confirms the connected endpoint offers it and the signed-in principal is allowed to use it.
- **A Schema plugin** - management over the schema control API. It ships as its own package but is withheld by default because its versioning UI cannot yet express what differs between schema versions: a head surfaces it by calling `AddExplorerSchemaPlugin()`, and renders no Schema tab by not calling it.
- **Auth-aware sign-in** - a pluggable `IExplorerAuthMethod` model (Basic, [Entra](connecting-to-an-auth-enabled-state-api.md) via the optional companion package, or a custom method) that acquires and attaches a bearer token to an auth-enabled State API.
- **Two hosting shapes from one code path** - run the bundled standalone `Orleans.Lattice.Explorer.WebHost` process, or embed the console in your own ASP.NET application; both are built on the same `AddLatticeExplorerWeb` / `MapLatticeExplorer` pair, so they cannot drift.

`Orleans.Lattice.Explorer.Web` is the single package a consumer references; the shared explorer libraries it composes restore transitively.

## Core properties

- **Read-only over the data plane.** The console observes cluster state and drives only the backup, access, and schema *control* facades. There is no direct write, delete, split, or reconfigure path into a tree's data.
- **Out-of-cluster by construction.** The Explorer reaches a cluster purely over its gRPC endpoints, so it can be deployed and scaled independently and never taxes Orleans membership or the silo's activation budget.
- **Fail-closed and capability-gated.** Each plugin declares an access gate resolving one of four states. `Allowed` renders normally; `AuthenticationRequired` stays prominent and clickable, inviting sign-in; `Denied` renders **visible but demoted**, grouped below a divider and stating the permission it needs and who to ask; `Unavailable` renders no entry, with the absence explained in a capabilities affordance. The gating is advisory and the server remains the sole enforcement point, which is exactly why a denied area is shown rather than hidden - see [Navigation visibility policy](navigation-visibility-policy.md). A plugin is absent from the rail entirely only when the head did not register it - registration is the whole of the opt-in, and there is no per-area option flag.
- **Head-agnostic core.** The connection, configuration, session, capability, and navigation services live in `Orleans.Lattice.Explorer.Core` and depend only on the public read-only state-API gRPC client, so every head renders the same behaviour.
- **Embeddable without wiring.** The shared UI ships its static web assets at `_content/Orleans.Lattice.Explorer.UI/`, served automatically; a host mounts the whole console with two extension calls under a configurable base path.

## Features

| Feature | Surface | Summary |
|---|---|---|
| Tree browser | State-API gRPC surface | Catalog, shard structure, snapshot-isolated entry scans, single-key inspection, change feed, and per-tree metrics. |
| Backups area | Backup control API | Capability-gated capture, restore, catalog listing, chain describe, and retention over the [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) binding. |
| Access area | Auth control API | Capability-gated membership and policy administration and decision explanation over the [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) binding. |
| Schema plugin | Schema control API | Schema policy, dead letters, versioning, and remediation. Ships withheld; surface it with `AddExplorerSchemaPlugin()`. |
| Auth-aware sign-in | `IExplorerAuthMethod` | Pluggable login (Basic, Entra, or custom) that attaches a bearer token to an auth-enabled State API. |
| Standalone or embedded hosting | `AddLatticeExplorerWeb` / `MapLatticeExplorer` | One code path for the bundled `WebHost` process and for embedding in an existing ASP.NET app, under a configurable base path. |

## Quick Start

Run the bundled standalone head - the `Orleans.Lattice.Explorer.WebHost` process is just the two extension calls below:

```csharp
using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddLatticeExplorerWeb();

var app = builder.Build();
app.MapLatticeExplorer();
app.Run();
```

Embed the console in an existing ASP.NET application, mounted under a subpath and seeded with a target endpoint so there is no interactive first-run step:

```csharp
using Orleans.Lattice.Explorer.Web;

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.BasePath = "/explorer";
    options.ConfigFilePath = "explorer-config.json";
});

// The Schema area is an opt-in plugin: registering it is the whole of the
// opt-in, and a head that does not register it renders no Schema area.
builder.Services.AddExplorerSchemaPlugin();

// ...after building the app:
app.MapLatticeExplorer();
```

See [Running and hosting the Explorer](running-the-explorer.md) for the full hosting, deployment, and subpath-mounting guidance, and [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md) for wiring sign-in.

## Reference

### Using the console

- [The Explorer navigation model](navigation-model.md) - the four navigation tiers, why primary navigation is a left rail, and how a URL addresses a view.
- [Navigation visibility policy](navigation-visibility-policy.md) - why areas you cannot open are shown and demoted rather than hidden, and why that is a usability policy and not a security control.
- [What the Explorer remembers](what-the-explorer-remembers.md) - the preference contract: what is remembered, at what scope, and how to reset it.
- [Tenant scope](tenant-scope.md) - the tenant lens, the tenant administration area, and the self-service area, and how the picker adapts to what you can reach.
- [Theming and density](theming-and-density.md) - the available themes, the separate contrast axis, density, and how a choice is applied at first paint.
- [Accessibility conformance](accessibility-conformance.md) - what the console targets, how that is verified, and the known limitations.

### Hosting and administration

- [Running and hosting the Explorer](running-the-explorer.md) - standalone and embedded hosting, package shape, and deployment without taxing cluster scaling.
- [Multi-replica and failover hosting](multi-replica-hosting.md) - opt-in durable auth state (shared Data Protection key ring, estate-global token cache) and graceful re-authentication for a multi-replica deployment.
- [Configuration](configuration.md) - the primary hosting and configuration options properties, their types, and their defaults.
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md) - selecting a login method and attaching a bearer token.
- [Adding a custom auth method](adding-a-custom-auth-method.md) - implementing `IExplorerAuthMethod` for a bespoke sign-in.
- [Managing backups from the Explorer](managing-backups.md) - the Backups area and its capability gating.
- [Managing access control from the Explorer](managing-access.md) - the Access area and its capability gating.
- [Managing schema from the Explorer](managing-schema.md) - the Schema plugin, withheld by default.
- [Writing an Explorer plugin](writing-a-plugin.md) - the extension model: adding a tab as its own package, with its own domain contract, access gate, and styling.

## See also

- [`Orleans.Lattice.Explorer.Entra`](connecting-to-an-auth-enabled-state-api.md) - the optional Microsoft Entra ID interactive login provider for the console.
- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) - the read-only state API the tree browser reads from.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) and [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the control facades the Backups and Access areas drive.
