# Running and hosting the Explorer

The Orleans.Lattice Explorer is a read-only, auth-aware web console for a running
cluster. It talks to a cluster only through the cluster's gRPC APIs (state, and
the auth, backup, and schema control bindings), so it never needs to be part of
the cluster's Orleans membership. This page covers how to run it: as a standalone
head, or embedded in your own ASP.NET application, and how to deploy it without
taxing your cluster's scaling behaviour.

## Two ways to run it

The Explorer web head ships as an opt-in **embeddable hosting library**,
`Orleans.Lattice.Explorer.Web`. Both
supported ways to run it are built on the same two extension methods, so they
share one code path and cannot drift:

- **`AddLatticeExplorerWeb(...)`** on the service collection registers the Razor
  interactive-server components, the shared explorer UI, the state-API connection
  seam, the configuration backing store, and the Backups and Access areas (and
  the Schema area, hidden by default) with their capability probes and auth
  plumbing.
- **`MapLatticeExplorer(...)`** on the endpoint route builder maps the Razor
  components, the UI static assets, and the sign-in/sign-out endpoints, under the
  configured base path.

To run a **standalone head**, use the bundled `Orleans.Lattice.Explorer.WebHost`
process, whose whole `Program` is just those two calls. To **embed** the console
in an existing ASP.NET app, reference the `Orleans.Lattice.Explorer.Web` package
and make the same two calls in your own host.

### Package shape

`Orleans.Lattice.Explorer.Web` is the one package a consumer references; the
shared explorer libraries it builds on restore transitively:

- `Orleans.Lattice.Explorer.UI` - the shared Razor component class library. Its
  static web assets are served automatically at
  `_content/Orleans.Lattice.Explorer.UI/`, with no extra wiring.
- `Orleans.Lattice.Explorer.Core` - the head-agnostic connection, configuration,
  session, capability, and navigation services.
- `Orleans.Lattice.Explorer.Backup` - the Backups management area.
- `Orleans.Lattice.Explorer.Access` - the Access (membership and access-control)
  management area.
- `Orleans.Lattice.Explorer.Schema` - the Schema management area. It ships but is
  hidden from the switcher by default (see `EnableSchemaArea` below).

## Configuration

`AddLatticeExplorerWeb` takes an optional configure callback over
`LatticeExplorerWebOptions`:

- **`BasePath`** - the mount point for the whole console, default `/`. Set it to
  a subpath such as `/explorer` to host the console alongside other routes. The
  value is normalized into the route prefix and the client base href.
- **`ConfigFilePath`** - an optional path to a JSON file that seeds the
  connection configuration (the state-API endpoint and related settings), so a
  deployment can ship its target without an interactive first-run step.
- **`UseEnvironmentBootstrap`** - default `true`; when set, the host also reads
  connection configuration from the environment at startup.
- **`EnableSchemaArea`** - default `false`. The schema-management area is withheld
  from the switcher for now because its versioning UI cannot yet express what
  differs between schema versions. Set it to `true` to surface the area; the
  schema control services are registered either way, so this only toggles
  visibility. See [Managing schema from the Explorer](managing-schema.md).

### Mounting under a subpath

When you set `BasePath` to a subpath, the Blazor Server circuit and the static
assets are served under that prefix. A reverse proxy or host that strips the
prefix before the request reaches the app must re-establish it with
`UsePathBase` so the circuit and the sign-in endpoints resolve; hosting the app
directly at the subpath needs no extra step.

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

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Managing backups from the Explorer](managing-backups.md)
- [Managing access control from the Explorer](managing-access.md)
- [Managing schema from the Explorer](managing-schema.md) (area hidden by default)
