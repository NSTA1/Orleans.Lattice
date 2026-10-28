---
agent_spec: "docs/agents/api/mcp.json"
---

# Configuration

Every configuration knob of `Orleans.Lattice.Api.Mcp`: the front-door options `AddLatticeMcp` binds, the OAuth protected-resource metadata block, the remote-host options `AddLatticeMcpRemote` binds, and the managed-identity administrator options. The tool-module opt-in flags are parameters of the `AddXTools(...)` methods; they are listed in [Public API](api.md#registration) and [Tools](tools.md#opting-in). For the walkthrough of registering and mapping the server see [Setup](setup.md); for the remote topology see [Remote hosting](remote.md).

The companion tool modules carry their own configuration: [`Orleans.Lattice.Api.Mcp.Telemetry`](../lattice.api.mcp.telemetry/configuration.md) (the metrics backend, its credential, guardrails and allow-list) and [`Orleans.Lattice.Api.Mcp.RepoContext`](../lattice.api.mcp.repocontext/configuration.md) (the repository-context options and environment variables).

## `LatticeApiMcpOptions`

Populated through the optional `AddLatticeMcp(Action<LatticeApiMcpOptions>? configure = null)` delegate, which is applied with `services.Configure`, so repeated calls accumulate. No options validator is registered. `MapLatticeMcp` reads the options once, when it maps the endpoint, so the values in force at that moment are the ones the endpoint uses.

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | When `true`, `MapLatticeMcp` applies ASP.NET Core `RequireAuthorization()` to the transport so an anonymous session is refused. Set `false` only behind an outer authentication boundary. |
| `TransportPattern` | `string` | `""` | The route the streamable-HTTP transport is mapped at. Empty maps it at the route builder's root; the origin check also covers the whole application when it is empty. |
| `Stateless` | `bool` | `false` | Copied onto the SDK's `HttpServerTransportOptions.Stateless`. The per-session, permission-scoped tool collections rely on the stateful default. |
| `RequireAllowedOrigin` | `bool` | `false` | When `true`, a request on the transport path whose `Origin` header is not allowed is answered `403` before authentication, routing or any tool runs (the DNS-rebinding defence). A request without an `Origin` header is unaffected. |
| `AllowedOrigins` | `IList<string>` | empty | The `scheme://host[:port]` origins allowed when `RequireAllowedOrigin` is `true`, matched case-insensitively against the whole `Origin` value. Get-only; add to it. |
| `AllowLoopbackOrigins` | `bool` | `true` | When `RequireAllowedOrigin` is `true`, also allow any `http`/`https` origin on `localhost` or a loopback address, on any port. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound header the credential bridge reads the caller's token from. |
| `CredentialScheme` | `string` | `Bearer` | The scheme stamped on the bridged credential; a case-insensitive scheme prefix is stripped from the header value before the token is used. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` | The inbound header carrying the caller's asserted active tenant. Empty disables header-based tenant selection. The assertion is re-validated downstream; an absent, blank or invalid value asserts no tenant. |
| `EnableStateTools` / `EnableDataTools` / `EnableBackupTools` / `EnableAuthTools` / `EnableReplicationTools` / `EnableTenantAdminTools` | `bool` | `false` | Informational module markers. `AddStateTools`, `AddBackupTools`, `AddAuthTools`, `AddReplicationTools` and `AddTenantAdminTools` set their marker; nothing sets `EnableDataTools`. No code reads any of them: a module is registered only by its `AddXTools(...)` call, so setting a marker adds nothing. |
| `EnableBackupControlTools` / `EnableAuthAdministration` / `EnableReplicationControlTools` / `EnableTreeAdminSchemaControlTools` / `EnableTreeAdminLifecycleTools` / `EnableTenantAdminControlTools` | `bool` | `false` | Destructive-verb opt-ins, read by the registered module when it builds its tool list. Each is set by the matching module flag (`enableControl`, `enableAdministration`, `enableSchemaControl` or `enableLifecycle`), and setting one directly is equivalent; none has any effect unless its module is registered. Data writes have no option: only `AddDataTools(enableWrites: true)` opts them in. |
| `ProtectedResourceMetadata` | `LatticeApiMcpProtectedResourceMetadata?` | `null` | Opts into OAuth 2.0 Protected Resource Metadata (RFC 9728): `MapLatticeMcp` serves the anonymous document and the `401` bearer challenge gains a `resource_metadata` hint. `null` leaves both off. |

## `LatticeApiMcpProtectedResourceMetadata`

Assigned to `LatticeApiMcpOptions.ProtectedResourceMetadata`. `MapLatticeMcp` validates it when it maps the document and throws `InvalidOperationException` if `Resource` is unset or `WellKnownPath` is not a root-absolute path. The document is serialized once and served with `AllowAnonymous()`; empty collections are omitted from it. See [OAuth discovery](setup.md#oauth-discovery-rfc-9728).

| Property | Type | Default | Purpose |
|---|---|---|---|
| `Resource` | `Uri?` | `null` (required) | The server's public, canonical base URL, emitted as `resource` and used to build the absolute `resource_metadata` hint. |
| `AuthorizationServers` | `IList<Uri>` | empty | Emitted as `authorization_servers`. Get-only. |
| `ScopesSupported` | `IList<string>` | empty | Emitted as `scopes_supported`. Get-only. |
| `BearerMethodsSupported` | `IList<string>` | `["header"]` | Emitted as `bearer_methods_supported`. Get-only; clear it to omit the field. |
| `WellKnownPath` | `string` | `/.well-known/oauth-protected-resource` (`DefaultWellKnownPath`) | The path the document is served at. Must be non-blank and start with `/`. |

## `LatticeApiMcpRemoteOptions`

Populated through `AddLatticeMcpRemote(Action<LatticeApiMcpRemoteOptions> configure)`; `configure` must not be `null`. The delegate is invoked once at registration to decide which groups to wire - a group whose endpoint is `null` at that moment gets no gRPC adapter and no tool module - and is also registered with `services.Configure` so the runtime collaborators (the credential source, the endpoint report) resolve the same values. A later `Configure<LatticeApiMcpRemoteOptions>` call therefore cannot add a group. `AddLatticeMcpRemote` calls `AddLatticeMcp()` without a delegate, so the front-door options above are set with a separate `AddLatticeMcp(...)` call. No options validator is registered. The narrative is in [Remote hosting](remote.md#options).

| Option | Type | Default | Purpose |
|---|---|---|---|
| `State` / `Data` / `Auth` / `Backup` / `Replication` / `TreeAdmin` / `TenantAdmin` / `Telemetry` | `LatticeApiMcpRemoteEndpoint?` | `null` | The current region's endpoint for each facade group; `null` does not serve the group. `TreeAdmin` backs every tree-administration tool, schema tools included. `TenantAdmin` backs the tenant self-awareness tools and, with `EnableTenantControl`, the tenant-admin tools. `Telemetry` wires no adapter or tool module: it only advertises where the region's telemetry facade is served, for discovery and region routing. `Auth` is required for remote discovery to grant any group. |
| `RegionId` | `string` | `current` (`DefaultRegionId`) | The id of the current (default) region a call targets when it supplies no `region` argument. |
| `ClusterId` | `string?` | `null` | The current region's Orleans cluster id, reported by `lattice_list_regions`; when unset it is resolved from the state facade at read time. |
| `Regions` | `IList<LatticeApiMcpRemoteRegionOptions>` | empty | Additional peer regions a caller may target per call. Get-only. Keep each `RegionId` unique: duplicates are not rejected. |
| `VerifyRegionIdentity` | `bool` | `false` | When `true`, each peer's reported cluster id is checked against its advertised `ClusterId` before a call is routed there; a mismatched region is omitted from discovery and refused. |
| `CredentialHeaderName` | `string` | `authorization` | The outbound gRPC metadata header the forwarded caller credential is stamped on. |
| `CredentialScheme` | `string` | `Bearer` | The scheme prefix of the outbound credential (`"{scheme} {token}"`); empty sends the bare token. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` | The outbound header the caller's ambient active tenant is forwarded on; empty disables forwarding. |
| `AdministratorCredential` | `LatticeCredential?` | `null` | A static administrator credential used only for trusted permission introspection during discovery. Superseded by `AddLatticeMcpManagedIdentityAdministrator` when that is registered. |
| `EnableDataWrites` / `EnableBackupControl` / `EnableAuthAdministration` / `EnableReplicationControl` / `EnableSchemaControl` / `EnableLifecycleControl` / `EnableTenantControl` | `bool` | `false` | Forwarded to the matching tool module's destructive-verb flag. Ignored when the group's endpoint is unset; `EnableSchemaControl` and `EnableLifecycleControl` both depend on `TreeAdmin`, `EnableTenantControl` on `TenantAdmin`. |

## `LatticeApiMcpRemoteRegionOptions`

One peer region in `LatticeApiMcpRemoteOptions.Regions`. A region serves a group only when its own per-group endpoint is set; it never inherits the top-level endpoints. See [Multi-region routing](remote.md#multi-region-routing).

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RegionId` | `string` | (required) | The id a caller passes as the `region` argument. |
| `ClusterId` | `string?` | `null` | The region's advertised Orleans cluster id; `VerifyRegionIdentity` checks it. A peer without one cannot be verified and stays routable. |
| `State` / `Data` / `Auth` / `Backup` / `Replication` / `TreeAdmin` / `TenantAdmin` / `Telemetry` | `LatticeApiMcpRemoteEndpoint?` | `null` | The peer's per-group endpoints. Address each peer by its own region-pinned endpoint, never a shared or anycast one. |

## `LatticeApiMcpRemoteEndpoint`

| Property | Type | Default | Purpose |
|---|---|---|---|
| `Endpoint` | `string` | (required, `init`) | The gRPC address of the group's binding, also reported verbatim by `lattice_capabilities`. |
| `CallInvoker` | `CallInvoker?` | `null` (`init`) | A pre-built gRPC invoker to reuse (custom TLS, retries, deadlines, an origin-lock header) instead of the channel derived from `Endpoint`. |

## `LatticeApiMcpManagedIdentityAdministratorOptions`

Populated through `AddLatticeMcpManagedIdentityAdministrator(Action<LatticeApiMcpManagedIdentityAdministratorOptions> configure)`; `configure` must not be `null`. The registration adds an options validator, which runs when the options are first resolved. See [Refreshing the administrator token](remote.md#refreshing-the-administrator-token).

| Option | Type | Default | Validation |
|---|---|---|---|
| `Credential` | `TokenCredential?` (`Azure.Core`) | `null` | Required. Only the bearer token it produces is forwarded. |
| `Scope` | `string` | `""` | Required: must be non-blank. The remote silo's audience, for example `api://<silo-app-id>/.default`. |
| `RefreshSkew` | `TimeSpan` | 5 minutes | Must not be negative. How long before expiry a cached token is proactively replaced. |

## Source map

- [Front-door options](../../src/lattice.api.mcp/LatticeApiMcpOptions.cs)
- [Protected-resource metadata](../../src/lattice.api.mcp/LatticeApiMcpProtectedResourceMetadata.cs) and its [mapping](../../src/lattice.api.mcp/LatticeMcpServiceCollectionExtensions.ProtectedResourceMetadata.cs)
- [Front-door registration](../../src/lattice.api.mcp/LatticeMcpServiceCollectionExtensions.cs)
- [Remote options](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteOptions.cs), [peer regions](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteRegionOptions.cs), [endpoints](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteEndpoint.cs) and [registration](../../src/lattice.api.mcp/Remote/LatticeMcpRemoteServiceCollectionExtensions.cs)
- [Managed-identity administrator options](../../src/lattice.api.mcp/Remote/LatticeApiMcpManagedIdentityAdministratorOptions.cs) and [registration](../../src/lattice.api.mcp/Remote/LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Setup](setup.md), [Security](security.md), [Remote hosting](remote.md)
