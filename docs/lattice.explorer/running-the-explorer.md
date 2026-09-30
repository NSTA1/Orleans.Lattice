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
  credential store, the app frame host, the native areas, the launcher
  environment bootstrap, and, in Development, the circuit-fault logging rule
  described under [Diagnosing a console that stops responding](#diagnosing-a-console-that-stops-responding).
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
  browser cannot edit, test or save the process-wide endpoint configuration.
  Pre-provision the JSON document or use the environment bootstrap instead. See
  [The connection dialog](#the-connection-dialog).
- The `DataProtection*` properties - optional shared ASP.NET Data Protection key
  ring configuration for multi-replica hosted-web sign-in.

See [Configuration](configuration.md) for the complete option table, launcher
environment variables, persisted document schema, and connection settings.

### The connection dialog

`AllowInteractiveEndpointConfiguration` decides what the browser may do with the
cluster endpoint, because the connection dialog's **Test connection** dials, from
the head, whatever address the visitor types. On a head anyone can reach, that
would be a host and port probe into the head's own network.

- **Not opted in (the default).** The header's connection indicator has no
  **Connection settings** entry, and the connection dialog is read-only, titled
  **Cluster connection**: it shows the configured endpoint and says it is set by
  the deployment and cannot be changed from the browser. With no endpoint
  configured, including on first run, it explains that the deployment sets one
  through `LATTICE_EXPLORER_ENDPOINT` or a pre-provisioned configuration
  document. There is no form, no test and no save, and the head refuses a
  connection test even if one is asked for.
- **Opted in.** **Connection settings** opens the editable **Connect to a
  cluster** dialog: the endpoint, **Insecure loopback development mode**,
  **Allow unencrypted HTTP/2 (h2c)**, **Test connection** and **Save and
  connect**. On first run it has no Cancel or Close.

A connection test reports one of three outcomes, in fixed words, never the
endpoint's own status text or an exception message, which would describe
whatever answered at an address the visitor chose:

| Outcome | Hint |
| --- | --- |
| Reachable | None. |
| Reachable - sign-in required | The endpoint answered and asks for a sign-in, which you can do after saving. |
| Unreachable | No Lattice API answered at this address. Check the endpoint and its transport settings. |

The probe is always anonymous: the circuit's credential and the configuration's
metadata headers are never sent to an unconfirmed address, and an authentication
refusal still counts as reachable. The configuration's transport headers, such as
an origin-lock header for a fronting proxy, are sent only when the test targets
the endpoint the head is configured for, and are dropped for any other. A probe
that does not answer within 15 seconds is Unreachable.

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
its own sandboxed Content-Security-Policy with `frame-ancestors 'self'`. The web
head sends `X-Frame-Options: DENY` on every response, this route included, and
only the route's own endpoint lifts it, for a file it actually serves. The
exemption is not a path match, so a co-hosted route or fallback that answers
under the same path keeps the anti-framing header, as do every Explorer page,
asset and SignalR endpoint.

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
Explorer packages provide the Razor components and static assets, which are
served automatically by a published host and under the Development environment.
When you run from build output (for example with `dotnet run`) under a
non-Development environment, call `builder.WebHost.UseStaticWebAssets()` so those
assets are mapped and the console is styled. The Explorer sample host does
exactly that.

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
| `Content-Security-Policy` | `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'` | `script-src` is `'self'` alone: the console serves no inline script, so an injected inline script or `on*` handler does not run. `style-src` keeps `'unsafe-inline'` for the inline `style` attributes interactive components emit. Providers can add extra `form-action` sources through `ExplorerContentSecurityPolicyOptions`. |
| `X-Frame-Options` | `DENY` | Emitted on every Explorer response. Only the app-frame route's endpoint removes it, and only for a file it serves; see [Lattice App frame route](#lattice-app-frame-route). |
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

## Diagnosing a console that stops responding

When Blazor Server meets an unhandled exception in a circuit, it terminates the
circuit: the console stays on screen but no longer answers, and the browser shows
only that an unhandled exception occurred on the current circuit. The framework
logs that termination at `Error` under the
`Microsoft.AspNetCore.Components.Server.Circuits` category, with the exception
and its stack trace.

A host whose logging filters silence that category (for example
`"Microsoft": "None"`) would lose the record, so in Development the web head
keeps it: `AddLatticeExplorerWeb` adds a logging filter rule that shows the
`Microsoft.AspNetCore.Components.Server.Circuits` category at `Error` or above.
A host that already names that category at `Error` or below keeps its own rule,
and outside Development nothing is added, so production logging stays the host's
decision. The rule logs nothing new: the record is the framework's own, carrying
the exception and the circuit id, and no user input or values.

To also send the fault's detail to the browser while developing, set Blazor
Server's `CircuitOptions.DetailedErrors` to `true` in Development; the web head
does not change it.

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
