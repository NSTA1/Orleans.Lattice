# Explorer sample

A one-command, self-contained demo of the opt-in `Orleans.Lattice.Explorer.Web`
hosting library. It co-hosts, in a single process:

1. a single-silo Orleans cluster with the state-API, auth-admin, and schema-admin
   gRPC surfaces, and
2. the embeddable **Explorer web console**, pointed at that gRPC endpoint,

so you can open the console in a browser and browse a live tree end to end.

The console is registered and mounted with the exact two calls a consumer makes
to embed it in their own ASP.NET app:

- `AddLatticeExplorerWeb()` registers the Razor components, the shared explorer
  UI, the state-API connection seam, and the Backups, Access, and Schema areas.
- `MapLatticeExplorer()` maps the interactive-server components, static assets,
  and sign-in / sign-out endpoints.

This is the same code path as the standalone web head, so the standalone head and
any co-hosted console cannot drift.

## Run it

```
dotnet run --project samples/Explorer/Explorer.csproj
```

Then open `http://localhost:5080/` in a browser. The sample seeds a demo tree
(`factory-floor`, 12 entries) and stays running until you press Ctrl+C.

The console is seeded to connect to the co-hosted gRPC endpoint through the
launcher-friendly bootstrap environment variables (`LATTICE_EXPLORER_ENDPOINT`
and `LATTICE_EXPLORER_INSECURE_DEV`), so it connects with no first-run setup. It
also auto-signs-in as a demo administrator (`LATTICE_EXPLORER_USERNAME` /
`LATTICE_EXPLORER_PASSWORD`), which is what unlocks the admin areas below. The
gRPC surface listens on `http://localhost:5199` over HTTP/2 without TLS (h2c) to
stay dependency-free; a real deployment would terminate TLS and register real
authorizers instead of disabling authorization.

## The admin areas

The console's top-level areas are capability-gated and fail closed. This sample
co-hosts the auth and schema gRPC admin APIs and auto-signs-in as a bootstrap
administrator (`explorer-admin`), so the **Explore**, **Access**, and **Schema**
areas are all enabled out of the box. The **Backups** area stays disabled because
this sample does not co-host the backup gRPC API.

How the admin sign-in works, so you can adapt it:

- The silo registers membership and authorization (`AddLatticeMembership`,
  `AddLatticeAuth`) with `explorer-admin` as a bootstrap administrator, plus
  schema enforcement (`AddLatticeSchemaEnforcement`) and the auth and schema
  control facades (`AddLatticeAuthApi`, `AddLatticeSchemaApi`).
- The auth and schema gRPC bindings (`AddLatticeAuthApiGrpc`,
  `AddLatticeSchemaApiGrpc`) are configured with the `Basic` credential scheme so
  the console's `authorization: Basic base64(user:pass)` header is understood.
- `DemoBasicAuthenticator` (a trivial trusted-token authenticator) decodes that
  header to recover the `explorer-admin` subject; because it is a bootstrap
  administrator, the fail-closed capability probes accept it and the areas light
  up. A real deployment resolves the subject from a validated JWT / Entra token
  instead, and leaves transport authorization enabled.

The data-plane default is kept permissive (`DefaultEffect = Allow`) so the
read-only Explore area works without a sign-in; the reserved control plane
(membership and policy) is always governed and only the bootstrap administrator
can manage it. See:

- [Running the Explorer](../../docs/lattice.explorer/running-the-explorer.md) -
  hosting, options, subpath mounting, and the isolated-head deployment note.
- [Managing access control](../../docs/lattice.explorer/managing-access.md).
- [Managing schema](../../docs/lattice.explorer/managing-schema.md).
- [Managing backups](../../docs/lattice.explorer/managing-backups.md).

## What to look at

- `Program.cs` - the silo host wiring (state + auth + schema gRPC surfaces and
  the bootstrap-administrator authorization setup), the console registration
  (`AddLatticeExplorerWeb` / `MapLatticeExplorer`), and the bootstrap seeding that
  points the console at the local endpoint and auto-signs it in.
- `DemoBasicAuthenticator.cs` - the demo trusted-token authenticator that maps the
  console's Basic sign-in to the `explorer-admin` subject.
