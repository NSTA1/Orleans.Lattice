# Reference-architecture host projects

Three minimal, production-shaped reference host projects for the active-active,
cross-region Orleans.Lattice estate described in the root `reference-architecture.md`.
Each project references the **published Orleans.Lattice v8.0.0 NuGet packages**
(from nuget.org), not project references into `src/`, so it consumes the released
library exactly as a real deployment would.

| Host | Project | Role |
|------|---------|------|
| Silo | `Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj` | The always-on Orleans silo: Azure Table clustering + durable Azure Table WAL, cross-region replication (shipper + receiver), the Azure Blob backup sink (primary/standby), the read-only State API + auth-admin control plane over gRPC, the `lattice.scaling` compute-axis signal, OpenTelemetry `/metrics`, and Entra auth. |
| Mcp | `Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj` | A stateless remote MCP server (`AddLatticeMcpRemote` over gRPC) fronting the silo, with the telemetry tool module and Entra auth. |
| Explorer | `Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj` | A standalone Explorer web console (Blazor Server) that connects, as a gRPC/gRPC-web client, to the silo's State + Auth gRPC endpoint, with an interactive Entra sign-in. |

All external inputs (connection targets, tenant / client ids, the replication key,
the peer list, merge modes, the backup-primary flag) come from environment
variables / `IConfiguration`. No secret is hardcoded: the only secret, the
per-cluster replication key, is read from the environment
(`LATTICE_REPLICATION_SECRET`, injected from Key Vault at deploy time). Managed
identity is the first-class Azure storage auth mode (`DefaultAzureCredential` +
service URIs); a connection string is accepted only as the local / emulator
fallback.

For a genuinely runnable local stack (Azurite + all three hosts + Prometheus +
Grafana, no Azure), see `../local/`.

## Configuration surface

Configuration uses the standard .NET `IConfiguration` binding: a key like
`Silo:HttpPort` is set by the environment variable `Silo__HttpPort` (double
underscore separator, case-insensitive).

### Silo

| Key | Default | Meaning |
|-----|---------|---------|
| `Cluster:Id` / `Cluster:ServiceId` | `lattice` | Orleans cluster / service id. |
| `Silo:HttpPort` / `Silo:GrpcPort` | `8080` / `8081` | HTTP port (health, scaling, `/metrics`) and HTTP/2 gRPC port (state, auth, replication). |
| `Silo:SiloPort` / `Silo:GatewayPort` | `11111` / `30000` | Orleans silo-to-silo and gateway ports. |
| `Silo:AdvertisedIp` | (auto) | Advertised IP for Orleans endpoints when the default NIC probe is not appropriate. |
| `Storage:ConnectionString` | - | Emulator / dev storage connection string (Azurite). Mutually exclusive with the service URIs. |
| `Storage:TableServiceUri` / `Storage:BlobServiceUri` | - | Managed-identity storage endpoints (production). |
| `Wal:TableName` / `Clustering:TableName` / `Reminders:TableName` / `GrainStorage:TableName` | `OrleansLatticeWal` / `...Clustering` / `...Reminders` / `...Grains` | Azure Table names. |
| `Replication:ClusterId` | `Cluster:Id` | This region's replication cluster id. |
| `Replication:Peers` | - | Enrolled peers as `clusterId=endpoint,clusterId=endpoint` (receiver-enrollment gate; must be reciprocal per region). |
| `Replication:Trees` | - | Per-tree wire merge mode as `treeName=MergeMode,...` (for example `orders=LwwRegister`; must match on both ends). |
| `Replication:AllowPlaintext` | `false` | Allow `http://` peer endpoints (local dev only; Azure uses server TLS). |
| `Backup:Primary` | `false` | `true` on the single designated backup-primary region (scheduler on); `false` on DR standbys (scheduler off). |
| `Backup:ContainerName` | `orleans-lattice-backup` | Blob container for the backup sink. |
| `Backup:FullIntervalHours` / `Backup:IncrementalIntervalMinutes` / `Backup:RetentionKeepLast` | `24` / `60` / `7` | Schedule tuning (primary only). |
| `Scaling:MinReplicas` | `1` | Floor for the compute-axis scaling signal. |
| `StateApi:RequireAuthorization` | `false` | When `true`, the state gRPC surface is gated by the turnkey env-var credential authorizer and the auth surface requires authorization. Local dev leaves it `false` (a documented bypass); a deployment sets it `true` behind the Entra front door. |
| `Auth:DefaultEffect` | `Deny` | `Deny` (secure default) or `Allow` (fully-open local dev cluster). |
| `Auth:BootstrapAdministrators` | - | Comma-separated subject ids seeded as administrators. |
| `Entra:Enabled` | `false` | Enable Entra-backed authentication for the exposed facades. |
| `Entra:TenantId` / `Entra:ClientId` / `Entra:Authority` / `Entra:Audiences` | - | Entra authenticator configuration. |
| `Entra:Graph:ClientSecret` | - | Enables the app-only Microsoft Graph group resolver (injected from Key Vault). |

### Mcp

| Key | Default | Meaning |
|-----|---------|---------|
| `Mcp:StateEndpoint` | (required) | The silo's State gRPC endpoint. |
| `Mcp:AuthEndpoint` | `Mcp:StateEndpoint` | The silo's Auth gRPC endpoint (needed for permission-scoped discovery). |
| `Mcp:DataEndpoint` / `Mcp:BackupEndpoint` | - | Optional data / backup gRPC endpoints (only if the silo exposes them). |
| `Mcp:RequireAuthorization` | `Entra:Enabled` | Fail-closed toggle on the MCP HTTP endpoint. |
| `Mcp:EnableDataWrites` / `Mcp:EnableBackupControl` / `Mcp:EnableAuthAdministration` | `false` | Advertise the mutating tool verbs of each group. |
| `Mcp:AdministratorToken` / `Mcp:AdministratorScheme` | - / `Bearer` | Service credential for discovery-time permission introspection of non-administrator callers. |
| `Mcp:Telemetry:BackendAddress` | - | PromQL backend for the telemetry tool module (only wired when set). |
| `Entra:Enabled` / `Entra:TenantId` / `Entra:Authority` / `Entra:Audience` / `Entra:ClientId` | - | Entra JWT validation on the front door; the token is forwarded to and re-validated by the silo. |

### Explorer

| Key | Default | Meaning |
|-----|---------|---------|
| `Explorer:ConfigFilePath` | `%TEMP%/lattice-explorer/config.json` | Writable JSON config backing store (the chiseled non-root image has no writable app-data dir). |
| `Explorer:EnableSchemaArea` | `false` | Surface the schema-management area. |
| `Entra:Enabled` / `Entra:TenantId` / `Entra:ClientId` / `Entra:Authority` / `Entra:UseDeviceCode` | - | Interactive Entra sign-in provider. |
| `LATTICE_EXPLORER_ENDPOINT` | - | The remote State/Auth gRPC endpoint the console connects to (read by the explorer's own environment bootstrap). |
| `LATTICE_EXPLORER_INSECURE_DEV` | - | `true` to allow the local h2c dev transport. |
| `LATTICE_EXPLORER_USERNAME` / `LATTICE_EXPLORER_PASSWORD` | - | Optional first-run auto-sign-in (local dev). |

## Container images

Each host has a multi-stage Dockerfile:

- **Build stage** `mcr.microsoft.com/dotnet/sdk:10.0`.
- **Final stage** `mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled` -
  framework-dependent, distroless, shell-less, and **non-root by default** (the
  chiseled base runs as the `app` user, UID 1654).

Because the final image has no shell, health checks are **HTTP/TCP only** (the
`/health` endpoint on the HTTP port); there is no shell to exec. TLS is
terminated at the platform ingress, so the containers serve plain HTTP
internally. NativeAOT and aggressive trimming are **out of scope** - Orleans
(and Blazor Server, for the Explorer) do not support them.

### InvariantGlobalization audit

All three images set `InvariantGlobalization=true` (in the `.csproj` and
reinforced by `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1` in the Dockerfile),
dropping the ICU dependency to shrink the image and reduce surface. This is only
safe if no correctness-relevant comparison depends on culture-aware (ICU-backed)
collation or casing.

**Method.** Enumerate every string-comparison and case/format site across the
Lattice surface these hosts consume and classify each as ordinal (ICU-independent)
or culture-sensitive.

**Result: PASS.**

- The core library performs **487** ordinal / `OrdinalIgnoreCase` comparison
  sites - the overwhelming default for keys, tree names, header names, and
  identifiers.
- Every `ToLowerInvariant()` / `ToUpperInvariant()` site across the whole
  consumed surface operates on a **guaranteed-ASCII** input:
  - lowercased gRPC header names in the five `Header*CredentialBridge` bridges;
  - lowercased enum names (`ViewMaintainerGrain` WAL-saturation state,
    `LatticeApiMcpGroupCapabilityMap` group name);
  - a boolean-ish config token in `EnvironmentExplorerBootstrap`
    (`"true"` / `"false"`);
  - lowercased hex digest strings in `RestoreSagaDispatcher`.
  ASCII invariant casing is code-point based and does **not** consult ICU, so it
  behaves identically with or without ICU.
- Every remaining culture reference pins `CultureInfo.InvariantCulture`
  explicitly for **number formatting / parsing** (offsets, shard indices,
  counter values). Invariant number formatting is available and unchanged under
  `InvariantGlobalization=true`.

There are **no** culture-sensitive (ICU-backed) linguistic comparisons, casings,
or collations on any correctness-relevant path. `InvariantGlobalization=true` is
therefore correctness-safe for all three hosts.

For the **Explorer** head specifically, the only residual culture effect is
display-side formatting (numbers, dates) and UI list ordering rendering in the
invariant culture rather than the operator's locale - a cosmetic change that is
acceptable for an operator console, since tree keys and access decisions are
compared ordinally underneath.

**Decision: enable `InvariantGlobalization` in all three hosts.**
