# Explorer sample

A one-command, self-contained demo of the opt-in `Orleans.Lattice.Explorer.Web`
hosting library. It co-hosts, in a single process:

1. a single-silo Orleans cluster with the read-only state-API gRPC surface, and
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
and `LATTICE_EXPLORER_INSECURE_DEV`), so it connects with no first-run setup. The
gRPC surface listens on `http://localhost:5199` over HTTP/2 without TLS (h2c) to
stay dependency-free; a real deployment would terminate TLS and register an
`ILatticeStateApiAuthorizer` instead of disabling authorization.

## The admin areas

The console's top-level areas are capability-gated and fail closed. This sample
enables only the read-only state API, so the **Explore** area shows live data
while the **Access**, **Backups**, and **Schema** admin areas render disabled
until their gRPC admin APIs are enabled on the endpoint and an authorized
operator signs in. To light them up, additionally register the auth, backup, and
schema gRPC APIs (`AddLatticeAuthApiGrpc`, `AddLatticeBackupApiGrpc`,
`AddLatticeSchemaApiGrpc`) with a real authorizer, and sign in with an operator
holding the matching admin grants. See:

- [Running the Explorer](../../docs/lattice.explorer/running-the-explorer.md) -
  hosting, options, subpath mounting, and the isolated-head deployment note.
- [Managing access control](../../docs/lattice.explorer/managing-access.md).
- [Managing schema](../../docs/lattice.explorer/managing-schema.md).
- [Managing backups](../../docs/lattice.explorer/managing-backups.md).

## What to look at

- `Program.cs` - the silo + state-API gRPC host wiring, the console registration
  (`AddLatticeExplorerWeb` / `MapLatticeExplorer`), and the bootstrap seeding that
  points the console at the local endpoint.
