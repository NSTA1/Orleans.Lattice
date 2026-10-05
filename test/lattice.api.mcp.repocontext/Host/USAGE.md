# RepoContext MCP host tests

These tests exercise the container host app that lives at
`apps/repocontext/` (project `Orleans.Lattice.Api.Mcp.RepoContext.Host`,
`IsPackable=false`, `OutputType=Exe`).

## Why the host tests live here

The host is a non-packable executable under `apps/`, so it is deliberately not
discoverable by CI's `src/<pkg>` + `test/<pkg>` package globbing. Creating a
`src/`-discovered package purely to host its tests would violate that boundary.
Instead the host is added to this existing test project via a `ProjectReference`,
and the host exposes its internals to it through an
`<InternalsVisibleTo Include="Orleans.Lattice.Api.Mcp.RepoContext.Tests" />` item in
its project file.
All host tests are grouped under the `Host/` folder here.

## Test tiers

- **Unit** (`Host/*Tests.cs`, no category): among them profile selection / fail-fast
  validation, readiness-state transitions, health-check reporting, data-path
  guard, compaction constants, trusted-access constants, SQLite schema
  round-trip and incremental auto-vacuum, durability-selector factory registration,
  startup-service seeding, and the opt-in startup sweep that classifies and deletes
  stranded leaf-snapshot rows in the SQLite grain store.
- **Integration** (`[Category("Integration")]`): `RepoContextHostIntegrationTests`
  brings up the real host over a `TestServer` and asserts restart durability
  (WAL replay across a rebuilt host on the same data root), the health-probe
  lifecycle, that the scaling endpoint is served only in the azure profile, that
  the local host opts past the default-deny MCP gate and runs the transport
  stateless, that an indexing run writes under the local agent with no ambient
  credential, and that the metrics endpoint is mapped and serves a recorded
  instrument value.
  The other fixtures in this tier cover the ambient-configuration entry point
  `Program.cs` calls (`RepoContextHostBuilderAmbientConfigurationTests`), the
  process-exit-code wiring into the drain signal (`RepoContextExitCodeWiringTests`),
  memory surviving the destruction of the store (`RepoContextMemoryBackupRecoveryTests`),
  the orphaned-leaf repair being invocable rather than merely advertised
  (`RepoContextOrphanedLeafRepairReachabilityTests`), and the Orleans instrument
  name the activation census reads (`RepoContextActivationCensusInstrumentNameTests`).
- **Container** (`[Category("Container")]` + `[Explicit]`): `RepoContextContainerSmokeTests`
  builds the image from `apps/repocontext/Dockerfile` and runs it, asserting the
  distroless container reaches its readiness probe, and
  `RepoContextComposeShutdownBehaviourTests` brings the sample compose stack up and
  asserts a plain `docker compose stop` returns without a kill and with a normal
  exit code. Both require a Docker daemon; excluded from the unit and integration
  tiers.

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
