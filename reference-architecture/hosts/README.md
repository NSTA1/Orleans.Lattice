# Reference-architecture host projects

Three minimal, production-shaped reference host projects for the active-active,
cross-region Orleans.Lattice estate described in the root `reference-architecture.md`.
Each project references the **published Orleans.Lattice NuGet packages** (from
nuget.org): the Silo and MCP heads pin the 9.8.x line (every package at
9.8.0), and the Explorer head pins the 9.4.x line of the
`Orleans.Lattice.Explorer.*` libraries - including
`Orleans.Lattice.Explorer.Entra.Web` for hosted-web OIDC sign-in - plus
`Orleans.Lattice.Caching.AzureBlob` 9.8.0 for its distributed token cache. Each
`.csproj` holds the exact pins. They are not project references into `src/`, so
each head consumes the released library exactly as a real deployment would.

All three heads also reference `Common/Orleans.Lattice.ReferenceArchitecture.Hosting.csproj`,
a shared hosting library that is not a published package (it references only the
ASP.NET Core shared framework). It supplies the Front Door origin lock described
below, the guard that confines the silo's `/metrics` and `/lattice/scale`
endpoints to its internal HTTP port (404 on any other port), and the filter that
drops informational request logs for successful requests on high-frequency probe
paths. Its tests
live in `Common.Tests/`.

| Host | Project | Role |
|------|---------|------|
| Silo | `Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj` | The always-on Orleans silo: Azure Table clustering + durable Azure Table WAL, cross-region replication (shipper + receiver), the Azure Blob backup sink (primary/standby), the read-only State API, the read-write Data API, and the auth-admin, backup, schema, and tree-administration control planes over gRPC (plus the replication control plane when enabled), the `lattice.scaling` compute-axis signal, OpenTelemetry `/metrics`, and Entra auth. |
| Mcp | `Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj` | A stateless remote MCP server (`AddLatticeMcpRemote` over gRPC) fronting the silo, with the telemetry tool module and Entra auth. |
| Explorer | `Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj` | A standalone Explorer web console (Blazor Server) that connects, as a gRPC/gRPC-web client, to the silo's State + Auth gRPC endpoint, with a hosted-web Entra (OpenID Connect) sign-in and a distributed token cache over the region storage account. |

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
| `Cluster:Id` / `Cluster:ServiceId` | `lattice` / `Cluster:Id` | Orleans cluster / service id. |
| `Silo:HttpPort` / `Silo:GrpcPort` | `8080` / `8081` | HTTP port (health, scaling, `/metrics`) and HTTP/2 gRPC port (state, auth, replication). |
| `Silo:SiloPort` / `Silo:GatewayPort` | `11111` / `30000` | Orleans silo-to-silo and gateway ports. |
| `Silo:AdvertisedIp` | (auto) | Advertised IP for Orleans endpoints when the default NIC probe is not appropriate. |
| `Storage:ConnectionString` | - | Emulator / dev storage connection string (Azurite). Mutually exclusive with the service URIs. |
| `Storage:TableServiceUri` / `Storage:BlobServiceUri` | - | Managed-identity storage endpoints (production). |
| `Wal:TableName` / `Clustering:TableName` / `Reminders:TableName` / `GrainStorage:TableName` | `OrleansLatticeWal` / `...Clustering` / `...Reminders` / `...Grains` | Azure Table names. |
| `Replication:ClusterId` | `Cluster:Id` | This region's replication cluster id. |
| `Replication:Peers` | - | Enrolled peers as `clusterId=endpoint,clusterId=endpoint`: the clusters this region ships to (and whose cross-cluster saga calls it accepts). Must be reciprocal across regions. |
| `Replication:Trees` | - | Per-tree wire merge mode as `treeName=MergeMode,...` (for example `orders=LwwRegister`; must match on both ends). |
| `Replication:AllowPlaintext` | `false` | Allow `http://` peer endpoints (local dev only; Azure uses server TLS). |
| `Replication:EnableRuntimeConfig` | `false` | Runtime per-tree replication control plane: enrols the `sys-replication-config` tree and co-hosts the replication control facade and its gRPC binding, fail-closed behind an authored Replication grant. Compute binds it to the deployer's replication-control switch. |
| `Replication:EnableDigestAntiEntropy` / `Replication:DigestProbeIntervalSeconds` | `false` / `0` | Cross-cluster anti-entropy (digest probe, Merkle-walk drift localisation, bounded automatic repair), set symmetrically per region; a positive interval overrides the package's probe cadence. |
| `Backup:Primary` | `false` | `true` on the single designated backup-primary region (scheduler on); `false` on DR standbys (scheduler off). |
| `Backup:ContainerName` | `orleans-lattice-backup` | Blob container for the backup sink. |
| `Backup:BlobConnectionString` / `Backup:BlobServiceUri` | - | Optional dedicated storage for the backup sink (an emulator connection string, or a managed-identity blob endpoint). When neither is set the sink uses the `Storage:*` identity. |
| `Backup:FullIntervalHours` / `Backup:IncrementalIntervalMinutes` / `Backup:RetentionKeepLast` | `24` / `60` / `7` | Schedule tuning (primary only). |
| `Scaling:MinReplicas` | `1` | Floor for the compute-axis scaling signal. |
| `StateApi:RequireAuthorization` | `false` | When `true`, the silo's gRPC facades require authorization. With Entra on, their coarse transport gates are opened and the deny-by-default per-subject access gate, keyed on the caller's Entra identity, is the real enforcement; with Entra off, the state surface is gated by the turnkey env-var credential authorizer (a shared username / password). Local dev leaves it `false` (a documented bypass); a deployment sets it `true` behind the Entra front door. |
| `DataApi:Enabled` | `true` | Exposes the read-write Data API gRPC binding, co-hosted on the silo gRPC port (same origin as the State API). Enabled by default; every mutation is still subject-checked by the deny-by-default access gate. Set `false` to withhold the write surface entirely. |
| `Auth:DefaultEffect` | `Deny` | `Deny` (secure default) or `Allow` (fully-open local dev cluster). |
| `Auth:BootstrapAdministrators` | - | Comma-separated subject ids seeded as administrators. |
| `Auth:DevAuthenticateForwardedSubject` | `false` | Local dev bypass, honoured only when Entra is off: trust a forwarded bearer token as its named subject when that id is a configured bootstrap administrator. |
| `Entra:Enabled` | `false` | Enable Entra-backed authentication for the exposed facades. |
| `Entra:TenantId` / `Entra:ClientId` / `Entra:Authority` / `Entra:Audiences` | - / - / `https://login.microsoftonline.com/{TenantId}/v2.0` / `ClientId` and `api://{ClientId}` | Entra authenticator configuration. `TenantId` and `ClientId` are required when Entra is on; `Audiences` is comma-separated. |
| `Entra:Algorithms` | `RS256` | Comma-separated allow-list of accepted JWT signature algorithms (the header `alg`), pinned as defense-in-depth against algorithm-confusion attacks (CWE-347). Defaults to `RS256`, the algorithm Entra issues v2.0 tokens with; a token advertising any other algorithm is refused. |
| `Entra:Graph:UseManagedIdentity` | `false` | Enables the app-only Microsoft Graph group resolver via a secret-less managed identity (`DefaultAzureCredential`). Compute sets this `true` on the silo when Entra is on. Ignored when `Entra:Graph:ClientSecret` is supplied. |
| `Entra:Graph:ClientSecret` | - | Dev / back-compat override: enables the app-only Microsoft Graph group resolver with a client secret (injected from Key Vault). Takes precedence over managed identity when set. |

### Mcp

| Key | Default | Meaning |
|-----|---------|---------|
| `Mcp:StateEndpoint` | (required) | The silo's State gRPC endpoint. |
| `Mcp:AuthEndpoint` | `Mcp:StateEndpoint` | The silo's Auth gRPC endpoint (needed for permission-scoped discovery). |
| `Mcp:DataEndpoint` / `Mcp:BackupEndpoint` | - | Data / backup gRPC endpoints. Compute sets `Mcp:DataEndpoint` to the silo gRPC FQDN when the Data API is enabled (the default; the write facade rides that endpoint), and `Mcp:BackupEndpoint` to the same FQDN when the deployer's backup-control switch is on (also the default). The silo always co-hosts the backup facade. |
| `Mcp:ReplicationEndpoint` | - | Replication control gRPC endpoint. Compute sets it to the silo gRPC FQDN when replication control is enabled. |
| `Mcp:TreeAdminEndpoint` | `Mcp:StateEndpoint` | Tree-administration and schema-control gRPC endpoint (co-hosted on the silo gRPC port). |
| `Mcp:RequireAuthorization` | `Entra:Enabled` | Fail-closed toggle on the MCP HTTP endpoint. |
| `Mcp:EnableDataWrites` / `Mcp:EnableBackupControl` / `Mcp:EnableReplicationControl` / `Mcp:EnableAuthAdministration` | `false` | Advertise the mutating tool verbs of each group. Compute binds the first three to the deployer's Data API, backup-control, and replication-control switches (all on by default); `EnableAuthAdministration` stays `false`. |
| `Mcp:EnableTreeAdminLifecycle` / `Mcp:EnableTreeAdminSchemaControl` | `true` / `true` | Advertise the mutating tree-lifecycle tools (create, alias, per-tree config, delete / recover / purge, bulk load, restore, reshard, resize, snapshot, WAL moves, view and tag-index maintenance, compaction, retention, orphaned-leaf repair) and the schema-mutation tools. The read-only tree-administration tools are advertised either way, and every call is still gated at the silo. |
| `Mcp:Stateless` | `true` | Stateless streamable-HTTP transport (no per-session server state), so a follow-up request can land on any region or replica behind Front Door. |
| `Mcp:AdministratorToken` / `Mcp:AdministratorScheme` | - / `Bearer` | Service credential for discovery-time permission introspection of non-administrator callers. |
| `Mcp:Telemetry:BackendAddress` | - | PromQL backend for the telemetry tool module (only wired when set). |
| `Mcp:Telemetry:AuthMode` | `None` | Backend auth mode. `None` for an unauthenticated backend (local compose Prometheus). `DynamicBearer` makes the head mint a rotating managed-identity Entra token per query for an Azure Monitor managed-Prometheus endpoint (no static secret); the workload identity needs Monitoring Data Reader on the workspace. |
| `Mcp:Telemetry:Scope` | `https://prometheus.monitor.azure.com/.default` | Access-token scope for `DynamicBearer` mode; override only for a non-default Azure Monitor audience. |
| `Mcp:RegionId` / `Mcp:ClusterId` | `current` / - | This head's own (default) region id and cluster id, surfaced by `lattice_list_regions` and targeted when a tool call supplies no `region`. Compute sets them to the region code and Orleans cluster id. |
| `Mcp:VerifyRegionIdentity` | `false` | Probe each peer region's state facade once and reject a peer whose endpoint does not reach its advertised cluster (an anycast/Front Door misconfiguration). Compute sets it `true` whenever peer regions are wired. |
| `Mcp:Regions:{n}:RegionId` / `:ClusterId` / `:StateEndpoint` / `:AuthEndpoint` / `:DataEndpoint` / `:BackupEndpoint` / `:ReplicationEndpoint` / `:TreeAdminEndpoint` | - | The peer regions a caller may target via the optional per-call `region` selector. Each peer is dialed at its DIRECT region-pinned silo gRPC FQDN (the same endpoint replication uses), which serves every facade group. Compute populates these on pass 2 from the sibling regions' silo FQDNs. |
| `Entra:Enabled` / `Entra:TenantId` / `Entra:Authority` / `Entra:Audience` / `Entra:ClientId` | - | Entra JWT validation on the front door; the token is forwarded to and re-validated by the silo. |
| `Mcp:PublicUrl` / `Mcp:Oauth:Scopes` | - | With Entra on, the head's public URL enables OAuth 2.0 Protected Resource Metadata (RFC 9728) discovery at `/.well-known/oauth-protected-resource`, advertising the given scopes. Compute sets both from the Front Door MCP endpoint and the silo `user_impersonation` scope. |
| `Mcp:DevAuthenticateAll` / `Mcp:DevSubjectId` | `false` / `local-dev-admin` | Local dev bypass, forced off when Entra is on: authenticate every request as one synthetic subject. |

### Explorer

| Key | Default | Meaning |
|-----|---------|---------|
| `Explorer:ConfigFilePath` | `%TEMP%/lattice-explorer/config.json` | Writable JSON config backing store (the chiseled non-root image has no writable app-data dir). |
| `Entra:Enabled` | `false` | Enable the hosted-web Microsoft Entra (OpenID Connect, auth-code + PKCE) sign-in provider. |
| `Entra:TenantId` | - | Directory (tenant) the console signs operators in against. |
| `Entra:WebClientId` | - | The Explorer console's OWN confidential web-app registration (holds the OIDC redirect URIs); NOT the silo facade audience. |
| `Entra:Scopes` | - | Comma-separated downstream State API scope requested on-behalf-of the operator (for example `api://{tenantId}/{base}-silo/user_impersonation`). Empty resolves the scope from the advertised audience. |
| `Entra:ClientSecret` | - | Optional confidential-client secret. Left unset in Azure: the container authenticates secret-lessly via a federated managed-identity assertion (`AZURE_CLIENT_ID`). |
| `Entra:TokenCache:BlobServiceUri` | - | Blob endpoint of the per-region account backing the Microsoft.Identity.Web distributed token cache, so tokens are shared across warm replicas and survive restart. Empty falls back to an in-memory cache. Consumed via the `AZURE_CLIENT_ID` managed identity. |
| `Entra:TokenCache:ContainerName` | `explorer-token-cache` | Container (on the per-region account) that stores the token cache. |
| `Explorer:PublicOrigin` | - | The public origin (scheme + host) operators reach the console at - the Front Door Explorer endpoint - so OpenID Connect builds its sign-in redirect URIs against that host rather than the internal Container Apps origin. Empty leaves requests untouched (local / compose). |
| `LATTICE_EXPLORER_ENDPOINT` | - | The remote State/Auth gRPC endpoint the console connects to (read by the explorer's own environment bootstrap). |
| `LATTICE_EXPLORER_INSECURE_DEV` | - | `true` to allow the local h2c dev transport. |
| `LATTICE_EXPLORER_TRANSPORT_HEADERS` | - | Semicolon-separated `Name=Value` non-secret headers sent on every call to the silo. Compute sets `X-Azure-FDID=<frontDoorId>` so the console, which dials the silo origin directly, passes its origin lock. |
| `LATTICE_EXPLORER_USERNAME` / `LATTICE_EXPLORER_PASSWORD` | - | Inert in this host. The web Explorer honours this first-run sign-in seed only when a host opts in with `LatticeExplorerWebOptions.AllowEnvironmentCredentialSeed`, and this host does not, so sign-in is always interactive (Entra OIDC when enabled, otherwise the console's sign-in dialog). |

### Front Door origin lock (all hosts)

| Key | Default | Meaning |
|-----|---------|---------|
| `LATTICE_FRONT_DOOR_ID` | - | The Azure Front Door profile id (a GUID). When set, every host rejects (HTTP 403) any request whose `X-Azure-FDID` header is absent, duplicated, or does not match this id (compared case-insensitively) - so only traffic that actually traversed the estate's Front Door instance is served. When empty or unset the lock is disabled (local dev / docker-compose, and deploy pass 1 before the Front Door exists). |

The lock always exempts the platform health probe path (`/health`), which ACA
calls on the container directly, bypassing Front Door. The **Silo** host also
exempts `/metrics` (the OpenTelemetry scrape) and `/lattice/scale` (the KEDA
compute-axis signal), which are served on the internal-only HTTP port and are
likewise probed directly, and the replication engine's silo-to-silo gRPC services
(`/orleans.lattice.replication.LatticeReplication`, `.LatticeRemoteSnapshot`, and
`.LatticeSaga`), which peer regions dial directly and which authenticate every
call with the shared replication key. Exemptions match on whole path segments, so
a lookalike such as `/healthz` remains locked.

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

- The core library's string comparisons are overwhelmingly ordinal /
  `OrdinalIgnoreCase` (**487** sites when this audit was taken) - the default for
  keys, tree names, header names, and identifiers.
- Every `ToLowerInvariant()` / `ToUpperInvariant()` site across the whole
  consumed surface operates on a **guaranteed-ASCII** input:
  - lowercased gRPC header names - the credential-header lookup in each gRPC
    facade's header credential bridge, and the active-tenant header lookups;
  - lowercased enum names (the view maintainer's WAL-saturation metric tag, the
    MCP head's tool-group name, and the Explorer UI's display and CSS-class
    labels);
  - a boolean-ish config token in `EnvironmentExplorerBootstrap`
    (`"1"` / `"true"` / `"yes"` / `"on"`);
  - lowercased hex digest strings (the replication restore saga's deterministic
    ids).
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
