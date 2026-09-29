# Running and hosting the Explorer

The Orleans.Lattice Explorer is an auth-aware Blazor Server console for a
running cluster. It connects to the cluster over the Lattice gRPC facades rather
than joining Orleans membership, so it can run as its own head or be embedded in
another ASP.NET Core app. The native areas are compiled into the UI and decide
whether to show themselves by probing the facades available at the configured
endpoint.

## Two ways to run it

The supported web head is the embeddable hosting library
`Orleans.Lattice.Explorer.Web`. Both the standalone process and an embedded host
use the same two extension methods:

- `AddLatticeExplorerWeb(...)` registers interactive Razor components, the
  Explorer UI, the connection and configuration services, the cookie-backed
  credential store, the app frame host, the native areas, and the launcher
  environment bootstrap.
- `MapLatticeExplorer()` maps the Explorer under the configured base path: static
  assets, the `auth/login` and `auth/logout` server form-post endpoints, the
  Lattice App frame route, and the interactive Razor components.

The standalone `Orleans.Lattice.Explorer.WebHost` program is those calls plus the
standard ASP.NET exception, HSTS, HTTPS redirection and antiforgery middleware.
To embed the console, reference `Orleans.Lattice.Explorer.Web`, call
`AddLatticeExplorerWeb` during service registration, call `UseAntiforgery`, and
then call `MapLatticeExplorer` on the application.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.BasePath = "/explorer";
});

var app = builder.Build();
app.UseAntiforgery();
app.MapLatticeExplorer();
```

## Package shape

The shipped Explorer rewrite has five Explorer packages:

- `Orleans.Lattice.Explorer.Web` - the ASP.NET Core hosting library with
  `AddLatticeExplorerWeb` and `MapLatticeExplorer`.
- `Orleans.Lattice.Explorer.WebHost` - the standalone executable head.
- `Orleans.Lattice.Explorer.Core` - connection, configuration, authentication,
  tenant and session services shared by heads.
- `Orleans.Lattice.Explorer.UI` - the Razor UI package. Its static web assets are
  served from `_content/Orleans.Lattice.Explorer.UI/`.
- `Orleans.Lattice.Explorer.AppKit` - the static app-frame kit served by the app
  frame route for Lattice Apps.

The native areas live in the UI package. There is no public area registration
API; third-party user interfaces are added as Lattice Apps.

## Configuration

`AddLatticeExplorerWeb` accepts `LatticeExplorerWebOptions`. The options most
commonly set by a host are:

- `BasePath` - the mount point for the console. It defaults to `/` and is
  normalised to a single leading slash with no trailing slash.
- `ConfigFilePath` - an explicit JSON configuration document path. When unset,
  the web head uses `LATTICE_EXPLORER_CONFIG`, then the per-user local app-data
  default.
- `UseEnvironmentBootstrap` - when `true` (the default), the head seeds the
  first-run endpoint from `LATTICE_EXPLORER_ENDPOINT` and related environment
  variables when no configuration has been persisted.
- `AllowEnvironmentCredentialSeed` - when `false` (the default), the web head
  refuses to apply `LATTICE_EXPLORER_USERNAME` and `LATTICE_EXPLORER_PASSWORD` to
  browser circuits. Enable it only for a single-operator deployment.
- `AllowInteractiveEndpointConfiguration` - when `false` (the default), the
  browser cannot write the process-wide endpoint configuration. Pre-provision the
  JSON document or use the environment bootstrap instead.
- The `DataProtection*` properties - optional shared ASP.NET Data Protection key
  ring configuration for multi-replica hosted-web sign-in.

See [Configuration](configuration.md) for the complete option table, launcher
environment variables, persisted document schema, and connection settings.

## Mounting under a subpath

Set `BasePath` to mount the console under a subpath such as `/explorer`. The web
head branches the ASP.NET pipeline at that prefix. Inside the branch the prefix
is moved into `PathBase`, so the Razor components keep their root-relative page
routes and the Explorer endpoints do not collide with routes owned by the host
application.

The proxy in front of the app must preserve the prefix for `BasePath` to apply.
If the proxy strips the prefix before forwarding, leave `BasePath` at `/` and let
the proxy own the public path.

## Lattice App frame route

Lattice Apps run inside the Explorer's app frame. The frame bootstrap document is
served by the Explorer head at:

```text
{BasePath}/_apps/frame/v1/frame.html
```

The route serves the AppKit static assets from
`_content/Orleans.Lattice.Explorer.AppKit/appkit/v1`. The bootstrap document has
its own sandboxed Content-Security-Policy with `frame-ancestors 'self'`; the web
head exempts only this route from its global `X-Frame-Options: DENY` header. All
other Explorer pages, assets and SignalR endpoints keep the anti-framing header.

## Deployment: prefer an isolated head

The Explorer web head is a Blazor Server application. A connected browser owns a
stateful SignalR circuit, so a multi-instance deployment needs session affinity
for Explorer traffic.

Run the Explorer as an isolated head where possible: its own process or
deployment, pointed at the cluster's gRPC endpoint. That scopes sticky routing to
the low-traffic admin console and avoids imposing affinity on the cluster's own
front door. If you co-host the Explorer in another app, scope any affinity rule
to the Explorer `BasePath` rather than to unrelated traffic.

### Static web assets in a thin host

An isolated head can be a thin project with no Razor files of its own. The
Explorer packages provide the Razor components and static assets. In a published
ASP.NET Core app those assets are composed by the static web assets system. The
sample host calls `UseStaticWebAssets()` so `dotnet run` serves the packaged
assets even outside the Development environment.

If a container build restores and publishes in separate stages, make sure the
publish stage restores with the full host source available, or do not use
`--no-restore`. To check the Blazor client asset, request:

```text
{BasePath}/_framework/blazor.web.js
```

A `200` response with a non-empty body means the interactive circuit script was
published.

## SignalR receive size

The app frame bridge sends each frame request to .NET as one JavaScript interop
message. The web head raises Blazor Server's `HubOptions.MaximumReceiveMessageSize`
to at least 160 KiB so those messages are not refused by the default 32 KiB
limit.

## Security response headers

`MapLatticeExplorer` installs response-header middleware for every Explorer
response. At the root mount the middleware is added to the application pipeline;
under a subpath it is the first middleware inside the isolated branch.

| Header | Value | Notes |
|---|---|---|
| `Content-Security-Policy` | `default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'` | Providers can add extra `form-action` sources through `ExplorerContentSecurityPolicyOptions`. |
| `X-Frame-Options` | `DENY` | Emitted on every Explorer response except the app-frame bootstrap route. |
| `X-Content-Type-Options` | `nosniff` | Prevents MIME sniffing. |
| `Referrer-Policy` | `no-referrer` | Avoids leaking tree, key, tenant or subject context in a referrer. |
| `Permissions-Policy` | `camera=(), microphone=(), geolocation=(), interest-cohort=()` | Disables browser features the console does not use. |

The middleware sets a header only when it is absent already. A host that maps the
Explorer on an endpoint builder that is not also the ASP.NET middleware pipeline
is refused at startup, because the console would otherwise be served without its
security headers.

The app-frame route has its own headers. Files on `/_apps/frame/v1/` carry
`X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`,
`Cross-Origin-Resource-Policy: cross-origin`, `Access-Control-Allow-Origin: *`,
and an immutable cache lifetime. `frame.html` also carries the sandboxed app
frame CSP.

## Sign-in endpoints

The web head maps two local form-post endpoints below the Explorer mount:

- `POST auth/login` signs in with the submitted Basic username and password.
- `POST auth/logout` clears the local State API credential.

Both endpoints validate antiforgery tokens and redirect back to the Explorer base
href. A federated provider such as the hosted-web Entra package can publish a
separate sign-out path so the identity menu posts there instead and ends the
browser identity-provider session as well.

## See also

- [Explorer overview](README.md)
- [Configuration](configuration.md)
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Multi-replica and failover hosting](multi-replica-hosting.md)
- [Lattice Apps](lattice-apps.md)
- [Area availability](area-availability.md)
