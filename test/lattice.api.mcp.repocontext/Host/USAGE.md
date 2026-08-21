# RepoContext MCP host tests

These tests exercise the container host app that lives at
`apps/repocontext/` (project `Orleans.Lattice.Api.Mcp.RepoContext.Host`,
`IsPackable=false`, `OutputType=Exe`).

## Why the host tests live here

The host is a non-packable executable under `apps/`, so it is deliberately not
discoverable by CI's `src/<pkg>` + `test/<pkg>` package globbing. Creating a
`src/`-discovered package purely to host its tests would violate that boundary.
Instead the host is added to this existing test project via a `ProjectReference`,
and the host exposes its internals to it through
`[assembly: InternalsVisibleTo("Orleans.Lattice.Api.Mcp.RepoContext.Tests")]`.
All host tests are grouped under the `Host/` folder here.

## Test tiers

- **Unit** (`Host/*Tests.cs`, no category): profile selection / fail-fast
  validation, readiness-state transitions, health-check reporting, data-path
  guard, compaction constants, trusted-access constants, SQLite schema
  round-trip, durability-selector factory registration, startup-service seeding.
- **Integration** (`RepoContextHostIntegrationTests`, `[Category("Integration")]`):
  brings up the real host over a `TestServer` and asserts restart durability
  (WAL replay across a rebuilt host on the same data root), the health-probe
  lifecycle, and that the scaling endpoint is served only in the azure profile.
- **Container** (`RepoContextContainerSmokeTests`, `[Category("Container")]` +
  `[Explicit]`): builds the image from `apps/repocontext/Dockerfile` and runs it,
  asserting the distroless container reaches its readiness probe. Requires a
  Docker daemon; excluded from the unit and integration tiers.

## Running

```powershell
# Unit tier
dotnet test test/lattice.api.mcp.repocontext -c Release `
  --filter "TestCategory!=Integration&TestCategory!=Chaos&TestCategory!=AzureStorageEmulator&TestCategory!=Container"

# Integration tier
dotnet test test/lattice.api.mcp.repocontext -c Release --filter "TestCategory=Integration"

# Container tier (needs Docker)
dotnet test test/lattice.api.mcp.repocontext -c Release --filter "TestCategory=Container" -- NUnit.Explicit=false
```
