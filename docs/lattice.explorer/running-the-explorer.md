# Running and hosting the Explorer

The Orleans.Lattice Explorer is an auth-aware web console for a running cluster:
a read-only browser of the cluster's trees and state, plus capability-gated
administration areas that change the cluster through its control APIs -
capturing, restoring, and deleting backups, editing access control, managing
tenants, and, when a head surfaces it, managing schema. It talks to a cluster
only through the cluster's gRPC APIs (state, and the auth, backup, schema,
tenant-administration, and telemetry bindings), so it never needs to be part of
the cluster's Orleans membership. This page covers how
to run it: as a standalone head, or embedded in your own ASP.NET application, and
how to deploy it without taxing your cluster's scaling behaviour.

## Two ways to run it

The Explorer web head ships as an opt-in **embeddable hosting library**,
`Orleans.Lattice.Explorer.Web`. Both
supported ways to run it are built on the same two extension methods, so they
share one code path and cannot drift:

- **`AddLatticeExplorerWeb(...)`** on the service collection registers the Razor
  interactive-server components, the shared explorer UI, the state-API connection
  seam, the configuration backing store, and the Backups, Access, Tenants, My
  Tenant, and Telemetry areas (and the Schema area, hidden by default) with their
  capability probes and auth plumbing.
- **`MapLatticeExplorer(...)`** on the endpoint route builder maps the Razor
  components, the UI static assets, and the sign-in/sign-out endpoints, under the
  configured base path.

To run a **standalone head**, use the bundled `Orleans.Lattice.Explorer.WebHost`
process, whose `Program` is those two calls plus the standard exception-handler,
HSTS, HTTPS-redirection, and antiforgery middleware. To **embed** the console
in an existing ASP.NET app, reference the `Orleans.Lattice.Explorer.Web` package
and make the same two calls in your own host.

### Package shape

`Orleans.Lattice.Explorer.Web` is the one package a consumer references; the
shared explorer libraries it builds on restore transitively:

- `Orleans.Lattice.Explorer.UI` - the shared Razor component class library. Its
  static web assets are served automatically at
  `_content/Orleans.Lattice.Explorer.UI/`, with no extra wiring.
- `Orleans.Lattice.Explorer.Core` - the head-agnostic connection, configuration,
  session, authentication, tenant-scoping, and navigation services, and the
  catalog, data, dead-letter, history, metrics, and topology readers.
- `Orleans.Lattice.Explorer.DesignSystem` - the design tokens, the named
  breakpoints, and the adaptive layout primitives every surface is styled with.
- `Orleans.Lattice.Explorer.Plugins.Abstractions` - the plugin contract, including
  the four-state access model every area and surface is gated by.
- `Orleans.Lattice.Explorer.Plugins.Selection` - the shared kernel of the
  per-selection surfaces, which ship as `Orleans.Lattice.Explorer.Plugins.Data`,
  `Orleans.Lattice.Explorer.Plugins.Topology`,
  `Orleans.Lattice.Explorer.Plugins.Metrics`,
  `Orleans.Lattice.Explorer.Plugins.DeadLetter`,
  `Orleans.Lattice.Explorer.Plugins.History`, and
  `Orleans.Lattice.Explorer.Plugins.TagIndex`.
- `Orleans.Lattice.Explorer.Backup` - the Backups management area.
- `Orleans.Lattice.Explorer.Access` - the Access (membership and access-control)
  management area.
- `Orleans.Lattice.Explorer.Plugins.Tenancy` - the shared tenant-administration
  seam behind `Orleans.Lattice.Explorer.Plugins.Tenants` (the Tenant
  administration area) and `Orleans.Lattice.Explorer.Plugins.MyTenant` (the My
  tenant area).
- `Orleans.Lattice.Explorer.Plugins.Telemetry` - the Telemetry area and the My
  tenant metrics section.
- `Orleans.Lattice.Explorer.Schema` - the Schema management area. It ships but is
  withheld by default (see the Schema plugin note below).

## Configuration

`AddLatticeExplorerWeb` takes an optional configure callback over
`LatticeExplorerWebOptions`:

- **`BasePath`** - the mount point for the whole console, default `/`. Set it to
  a subpath such as `/explorer` to host the console alongside other routes. The
  value is normalized into the route prefix and the client base href.
- **`ConfigFilePath`** - an optional path to the JSON configuration document the
  console reads its connection configuration from (the state-API endpoint and
  related settings), so a deployment can ship its target without an interactive
  first-run step. When unset, the `LATTICE_EXPLORER_CONFIG` environment variable
  and then the per-user app-data default are used.
- **`UseEnvironmentBootstrap`** - default `true`; when set, the console seeds the
  connection endpoint from `LATTICE_EXPLORER_ENDPOINT` (and related variables)
  whenever no configuration is persisted. The seed is held in memory and never
  written back to the configuration document.
- **`AllowEnvironmentCredentialSeed`** - default `false`. The environment
  bootstrap can also read a sign-in credential, but the web head's credential
  store is per browser, so a seeded credential would sign every anonymous
  visitor in as the operator. It is therefore withheld unless you opt in here,
  which is only appropriate on a host reachable by exactly one trusted operator.
- **`AllowInteractiveEndpointConfiguration`** - default `false`. The connection
  configuration is a singleton shared by every circuit, so an unauthenticated
  visitor who could change it would be able to repoint the deployment at a host
  they control. Interactive changes are refused by the configuration store and
  the connection-settings affordance is withheld; configure the endpoint with
  `ConfigFilePath` or the environment bootstrap instead. Opt in only for a web
  head that is genuinely configured through its own UI by a trusted operator.
- **`DataProtectionKeyRingBlobUri`, `DataProtectionKeyRingCredential`,
  `DataProtectionApplicationName`, and `ConfigureDataProtection`** - opt-in
  persistence for the ASP.NET Data Protection key ring, so every replica of a
  multi-replica deployment can decrypt the session cookie another replica issued.
  Unset, the framework's per-instance ephemeral ring is used. See
  [Multi-replica and failover hosting](multi-replica-hosting.md).
- **The Schema plugin** - withheld by default, and no longer an option flag. The
  schema-management area ships as its own plugin package, and registration is the
  whole of the opt-in: call `builder.Services.AddExplorerSchemaPlugin()` to surface
  it, or omit the call to render no Schema tab. It stays withheld by default because
  its versioning UI cannot yet express what differs between schema versions. The
  schema control services are registered either way, so a head can surface the area
  without any other wiring change. See
  [Managing schema from the Explorer](managing-schema.md).

### Mounting under a subpath

Setting `BasePath` is the whole of it. The head applies the prefix itself, as a
`PathBase` at the front of its pipeline, and maps its components, static assets
and sign-in endpoints at the root behind it - so the pages, the Blazor Server
circuit and the assets all resolve under the mount, and the host document is
emitted with a matching `<base href>`.

Under a subpath the console is mapped inside an isolated branch pipeline rooted
at the prefix. The branch strips the prefix before its own routing runs, so every
component keeps its declared root-relative `@page` template, and it keeps the
console's endpoints out of the host application's endpoint table so they cannot
collide with the host's own routes. Grouping the endpoints under the prefix
instead leaves every declared template unresolvable and serves no page at all
([dotnet/aspnetcore#64965](https://github.com/dotnet/aspnetcore/issues/64965)).

The app has to see the prefix for `BasePath` to apply to it. A front end that
strips the prefix before forwarding leaves the head serving at the root: either
have it preserve the prefix, or leave `BasePath` at `/` and let the front end
own the public path entirely.

## Deployment: prefer an isolated head

The Explorer web head is a **Blazor Server** application. Blazor Server keeps a
stateful SignalR **circuit** per connected browser, so every request from a given
session must reach the **same** server instance. In a multi-instance deployment
that means the front end must be configured for **session affinity** (sticky
sessions).

That requirement is cheap for a small admin tool on its own, but it is not free
if you **co-host the Explorer inside a cluster silo**. Forcing session affinity
on the endpoint that also fronts cluster traffic imposes an affinity constraint
on the whole front end for the sake of a low-traffic admin console, which works
against how the cluster otherwise wants to scale and shed load.

**Recommendation: run the Explorer as an isolated, dedicated head** - its own
process or deployment, separate from the cluster silos, pointed at the cluster's
gRPC endpoint. Because the Explorer joins the cluster only as a gRPC *client*,
nothing about it needs to live inside a silo. Running it separately scopes the
session-affinity requirement to the small admin deployment, where a single
instance or a stickily-routed pair is entirely adequate, and leaves the cluster's
own scaling behaviour untouched.

If you do choose to co-host the Explorer in a cluster, scope the session affinity
to the Explorer's `BasePath` alone rather than applying it cluster-wide, so the
affinity constraint does not spread to unrelated cluster traffic.

### Shipping the Blazor client asset from an isolated host

An isolated head is typically a thin project that references
`Orleans.Lattice.Explorer.Web` and calls `AddLatticeExplorerWeb` /
`MapLatticeExplorer`, with **no Razor content of its own** - every component ships
inside the referenced razor class library (RCL). That is the intended shape. The
`Orleans.Lattice.Explorer.Web` package contributes an MSBuild props file that sets
`RequiresAspNetWebAssets` for you, so a normal `dotnet publish` composes the
framework's `_framework/blazor.web.js` client script into the head even though the
host has no Razor content itself.

The sharp edge is a **container build that restores and publishes in separate
steps**. Blazor-host detection and the web-asset contribution are resolved during
**restore**, so a Dockerfile that restores from the `.csproj` alone and then runs
`dotnet publish --no-restore` can silently drop `blazor.web.js`. The console then
renders server-side but its interactive circuit never starts: **Sign in does
nothing, area entries stay inert, and the console reports "Access to the state API was
denied"** even though the endpoint is reachable.

To keep the asset in a containerized isolated host:

- **Do not publish with `--no-restore`.** Let `dotnet publish` restore, or copy
  the full host source into the build stage and restore with it present, so the
  web-asset contribution is resolved before publish.
- **Add a trivial piece of Razor content as a belt-and-braces trigger** - an
  empty `_Imports.razor` next to `Program.cs` is enough. It contributes nothing at
  runtime but makes the SDK's Blazor-host detection robust to the build shape.

To confirm the asset shipped, request `<BasePath>/_framework/blazor.web.js` from
the running head (or through your front end) and check for a `200` with a
non-trivial body; a `404` means the circuit script was not published.

## Security posture of the web head

### Per-circuit credential isolation

The Explorer is a Blazor Server app, so each connected browser gets its own
stateful **circuit**. The per-operator auth session and the cluster (state-API)
connection are registered **scoped**, so each circuit signs in and drives its
own connection independently, keyed on its own credential cookie. One operator's
credential is never shared with another circuit: the console can serve multiple
operators from the same process without one operator's sign-in flipping the
connection another operator sees. The credential-bearing gRPC control-plane
clients (the Access, Backup, and Schema areas, the tenant-administration client
behind the Tenant administration and My tenant areas, and the telemetry client)
are likewise scoped per circuit and act under the calling circuit's own
authentication.

### Security response headers

`MapLatticeExplorer` installs a middleware that emits a baseline set of security
response headers on **every** explorer response - HTML pages, the `_framework`
assets, and the SignalR negotiate / hub endpoints alike:

| Header | Value | Purpose |
|---|---|---|
| `Content-Security-Policy` | includes `frame-ancestors 'none'` | Anti-clickjacking (CWE-1021): the authenticated admin console cannot be framed by a foreign origin. |
| `X-Frame-Options` | `DENY` | Denies framing on older browsers that predate CSP `frame-ancestors`. |
| `X-Content-Type-Options` | `nosniff` | Stops the browser MIME-sniffing a response away from its declared content type. |
| `Referrer-Policy` | `no-referrer` | Keeps a request URL (which can carry tree, key, or subject context in its path or query) out of the `Referer` header on any outbound navigation to a foreign origin. |
| `Permissions-Policy` | `camera=(), microphone=(), geolocation=(), interest-cohort=()` | Disables browser features the console does not use (empty allow-list for every origin) and opts out of Topics/FLoC cohort computation. |

The middleware is attached by `MapLatticeExplorer` - directly on the
application pipeline for a root mount, and as the first middleware inside the
isolated branch pipeline for a subpath mount - so both the standalone head and
any host that mounts the console under a subpath inherit it, while a subpath host
keeps its own unrelated routes free of the explorer's policy. Each header is set
only when it is not already present, so a value legitimately set elsewhere in the
pipeline is
preserved rather than clobbered; where the interactive Blazor runtime contributes
its own `frame-ancestors 'self'` Content-Security-Policy, the browser enforces
the intersection of the policies in force. The header values are cached, so this
per-response path allocates nothing.

The baseline `Content-Security-Policy` also carries `form-action 'self'`, so the
console's forms may only post back to their own origin. A federated sign-out
provider extends this: when the Entra hosted-web provider is registered it
contributes its identity-provider authority origin as an additional `form-action`
source through the core `ExplorerContentSecurityPolicyOptions` contract, which the
middleware composes into the policy once at startup. Without it the browser would
block the antiforgery-guarded sign-out `POST`, because that request redirects
cross-origin (HTTP 302) to the identity provider's end-session endpoint and
browsers enforce `form-action` across the whole redirect chain. A provider that
contributes no origin leaves the baseline policy byte-for-byte unchanged.

## See also

- [Explorer overview](README.md)
- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Managing backups from the Explorer](managing-backups.md)
- [Managing access control from the Explorer](managing-access.md)
- [Managing schema from the Explorer](managing-schema.md) (area hidden by default)
