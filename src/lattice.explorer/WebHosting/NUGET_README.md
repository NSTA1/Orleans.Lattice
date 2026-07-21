# Orleans.Lattice.Explorer.Web

Opt-in **embeddable hosting library** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice) Blazor Server
web head. Add and run the read-only explorer console **inside your own ASP.NET
application** (or a thin dedicated host) and point it at a cluster's state-API
endpoint, behind a single `AddLatticeExplorerWeb` + `MapLatticeExplorer` opt-in.

The standalone `Orleans.Lattice.Explorer.WebHost` process is built on these same
two extensions, so the embeddable library and the standalone head share one code
path and cannot drift.

## Package shape

This package ships as one of a small family of explorer packages. It depends on
the shared explorer libraries, which restore automatically for a consumer:

- `Orleans.Lattice.Explorer.UI` - the shared Razor component class library (its
  static web assets are served automatically at
  `_content/Orleans.Lattice.Explorer.UI/`).
- `Orleans.Lattice.Explorer.Core` - the head-agnostic connection, configuration,
  session, capability, and navigation services.
- `Orleans.Lattice.Explorer.Backup` - the Backups management area.
- `Orleans.Lattice.Explorer.Access` - the Access (membership and access-control)
  management area.
- `Orleans.Lattice.Explorer.Schema` - the Schema (schema-policy management) area.

## Usage

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.BasePath = "/explorer";           // mount point (default "/")
    // options.ConfigFilePath = "/etc/lattice/explorer.json"; // optional
});

var app = builder.Build();

app.UseHttpsRedirection();
app.UseAntiforgery();

app.MapLatticeExplorer();

app.Run();
```

`AddLatticeExplorerWeb` registers everything the standalone head wires up: Razor
components with interactive server components, the shared explorer UI, the
state-API connection seam, the configuration backing store plus environment
bootstrap, the catalog / metrics / topology / data / dead-letter / history /
session services, the capability store, the Backups, Access, and Schema areas,
and the auth / cookie / data-protection plumbing.

`MapLatticeExplorer` maps the Razor components (interactive server render mode
with the UI additional assembly), the static assets, and the server-side
`/auth/login` and `/auth/logout` endpoints under the configured base path.

## Interactivity and static assets

The explorer's interactive Blazor components live in this hosting library, not in
your host project. So that the framework's `_framework/blazor.web.js` script is
composed into your app (without it the console renders but never becomes
interactive), the package's `build/Orleans.Lattice.Explorer.Web.props`
automatically sets `RequiresAspNetWebAssets` for you; you do not need to set
anything. If you have already set that property yourself, your value is kept.

The UI's stylesheet and scripts ship as static web assets of the referenced
`Orleans.Lattice.Explorer.UI` package and are served automatically under the
Development environment. When you run under a non-Development environment, call
`builder.WebHost.UseStaticWebAssets()` so those assets are mapped and the console
is styled.

> **Containerized isolated hosts:** the web-asset contribution is resolved during
> restore, so a Dockerfile that restores from the `.csproj` alone and then runs
> `dotnet publish --no-restore` can silently drop `blazor.web.js` - the console
> then renders but its circuit never starts (Sign in does nothing). Let publish
> restore (drop `--no-restore`), and optionally add an empty `_Imports.razor` to
> the host as a belt-and-braces Blazor-host trigger. See the deployment guide.

## Deployment note: session affinity

The web head is Blazor Server, so scaling it beyond a single instance requires
**session affinity**. Scope that affinity to the explorer's own route or host by
pinning the Blazor circuit path (the `/_blazor` endpoint under your chosen base
path); never apply it cluster-wide, so co-hosting the explorer does not impose a
sticky-session tax on the cluster's data-plane ingress.

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
