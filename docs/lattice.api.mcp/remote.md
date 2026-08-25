# Remote hosting

The MCP server can run **in the silo** (co-hosted with the facades it binds, resolving them in-process) or **out of the silo** as a standalone host that reaches the cluster over the network. `AddLatticeMcpRemote(...)` wires the out-of-silo topology: the same built-in tool modules, bound over the `Orleans.Lattice.Api.*.Grpc` clients instead of the in-process facades.

## When to use it

Use remote hosting when the MCP endpoint cannot live on a cluster silo - for example a dedicated agent-gateway process, a host in a different trust zone, or a single MCP front door fronting a cluster it is not a member of. When the MCP server can co-host on a silo, prefer the in-process topology ([Setup](setup.md)): it avoids a network hop and the extra credential-forwarding configuration below.

## Wiring

`AddLatticeMcpRemote(...)` registers the MCP infrastructure (it calls `AddLatticeMcp` internally), the gRPC-backed facade adapters, the credential-forwarding interceptor, and, for each configured group, the matching tool module. Configure an endpoint only for the groups you want to serve:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeMcpRemote(o =>
{
    o.State = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster-a.internal:5001" };
    o.Data = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster-a.internal:5001" };
    o.EnableDataWrites = false;

    // Required for non-administrator callers' tools to be discovered remotely.
    o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster-a.internal:5001" };

    // Runtime per-tree replication control (inspect always; enable/disable gated).
    o.Replication = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster-a.internal:5001" };
    o.EnableReplicationControl = true;
});

var app = builder.Build();
app.MapLatticeMcp();
```

Each `LatticeApiMcpRemoteEndpoint` names the served `Endpoint` (surfaced verbatim in the `lattice_capabilities` report) and optionally supplies a pre-built `CallInvoker` so a host that already owns a tuned gRPC channel (custom TLS, retries, deadlines) can reuse it instead of the address-derived default.

## Options

| Option | Purpose |
|---|---|
| `State` / `Data` / `Auth` / `Backup` / `Replication` / `TreeAdmin` / `TenantAdmin` | The per-group remote endpoint, or `null` to not serve that group. The tree-administration endpoint also backs the read-only tree-administration diagnostics tools (`lattice_treeadmin_*`), the tree-administration lifecycle tools (`lattice_treeadmin_tree_*`), and the tree-administration schema tools (`lattice_treeadmin_schema_*`), since the tree-administration-API, schema-API, and tree-administration gRPC services are co-hosted on the same silo address. The `TenantAdmin` endpoint backs both the read-only tenant self-awareness tools (`lattice_tenant_current` / `lattice_tenant_list` / `lattice_tenant_get`) and, when `EnableTenantControl` is set, the mutating tenant-admin tools (`lattice_tenant_create` / `lattice_tenant_suspend` / `lattice_tenant_resume` / `lattice_tenant_delete`), since the self-service read RPCs are co-hosted on the tenant-administration gRPC service address. |
| `CredentialHeaderName` | Header the resolved caller credential is stamped onto for the outbound call. Defaults to `authorization`. |
| `CredentialScheme` | Scheme prefix prepended to the outbound token (`"{scheme} {token}"`). Defaults to `Bearer`; empty sends the bare token. |
| `AdministratorCredential` | The **static** admin service credential used for trusted, read-only permission introspection of each caller. See [discovery](#discovery-requires-the-auth-endpoint) below. For a long-lived server prefer a self-refreshing managed-identity token (see [Refreshing administrator token](#refreshing-the-administrator-token)). |
| `EnableDataWrites` / `EnableBackupControl` / `EnableAuthAdministration` / `EnableReplicationControl` / `EnableSchemaControl` / `EnableLifecycleControl` / `EnableTenantControl` | Forward the destructive-verb opt-in to the corresponding tool module. Ignored when that group's endpoint is unset. `EnableSchemaControl` gates the mutating `lattice_treeadmin_schema_*` tools and `EnableLifecycleControl` gates the tree-administration lifecycle/control mutation tools; both are ignored when `TreeAdmin` is unset. `EnableTenantControl` gates the mutating `lattice_tenant_*` admin tools and is ignored when `TenantAdmin` is unset; the read-only tenant self-awareness tools are served whenever `TenantAdmin` is set, with no flag. |
| `RegionId` | The id of the current (default) region a call targets when no `region` selector is supplied. Defaults to `current`. |
| `ClusterId` | The Orleans cluster id of the current region, surfaced in `lattice_list_regions`. Optional advertisement metadata. |
| `Regions` | Additional peer regions a caller may target with the optional per-call `region` argument. Each is a `LatticeApiMcpRemoteRegionOptions` with its own `RegionId`, optional `ClusterId`, and per-group endpoints. |
| `VerifyRegionIdentity` | When `true`, each peer region's endpoint is probed once and its reported cluster id checked against the region's advertised `ClusterId` before any call is routed there; a region that reaches the wrong cluster is omitted from `lattice_list_regions` and rejected fail-closed. Defaults to `false`. See [Region targeting behind a global load balancer](#region-targeting-behind-a-global-load-balancer). |

## Multi-region routing

By default the endpoints above define a single region - the current cluster. To let one MCP head front more than one region, add a `LatticeApiMcpRemoteRegionOptions` per peer to `Regions`. The top-level endpoints stay the default (current) region, so an existing single-region configuration is unchanged; the peers are additive and opt-in per call.

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeMcpRemote(o =>
{
    // The current (default) region, targeted when no `region` is supplied.
    o.RegionId = "us-east";
    o.ClusterId = "cluster-us-east";
    o.Data = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://us-east.internal:5001" };
    o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://us-east.internal:5001" };

    // A reachable peer region a caller may target with `"region": "eu-west"`.
    // Each peer endpoint is that region's OWN, region-pinned silo FQDN - never a
    // shared/anycast endpoint - so the `region` selector is deterministic.
    o.Regions.Add(new LatticeApiMcpRemoteRegionOptions
    {
        RegionId = "eu-west",
        ClusterId = "cluster-eu-west",
        Data = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://eu-west.internal:5001" },
        Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://eu-west.internal:5001" },
    });

    // Prove each peer reaches the cluster it advertises before routing to it.
    o.VerifyRegionIdentity = true;
});

var app = builder.Build();
app.MapLatticeMcp();
```

The region list `lattice_list_regions` reports, and the routing a `region` argument drives, are both built once at startup from this single configuration, so discovery and routing can never disagree. A region serves a facade group only when that group's per-region endpoint is set; an unset group is reported unavailable for the region and is not routable there (fail-closed discovery). A cross-region call forwards the same caller credential to the target region's gRPC binding via the same interceptor described below, so the target authorizes it independently. See [Tools](tools.md#region-targeting) for the caller-facing surface.

## Region targeting behind a global load balancer

Region targeting is **deterministic** - a `region` selector must reach that exact region. That is fundamentally at odds with a global anycast load balancer (for example Azure Front Door), whose job is to *hide* which region serves a request by latency-routing to the nearest healthy origin. So a peer region must be addressed by its **own, region-pinned endpoint** (its silo's direct gRPC FQDN), never a shared Front Door endpoint. Point a region at an anycast endpoint and a call targeting it lands on whichever region the load balancer picks, and the served-region annotation becomes untrustworthy.

Two consequences when the deployment fronts its regions with a load balancer that enforces an origin lock (a required header such as `X-Azure-FDID`):

- **Stamp the origin-lock header on the direct dial yourself.** Reach the peer's internal gRPC ingress directly and supply a pre-built `CallInvoker` (via `LatticeApiMcpRemoteEndpoint.CallInvoker`) that adds the required header - one global Front Door id typically validates every regional origin, so the same invoker works for every peer. Do **not** route gRPC *through* the load balancer: it would inject the header itself (a duplicate the origin lock rejects) and gRPC-through-Front-Door is not generally supported.
- **Turn on `VerifyRegionIdentity`.** It probes each peer's state facade once and checks the reported cluster id against the region's advertised `ClusterId`. A region whose endpoint reaches the wrong cluster - the exact symptom of a mis-pointed or anycast endpoint - is omitted from `lattice_list_regions` and rejected fail-closed when targeted, so a misconfiguration surfaces as a clean discovery gap rather than a call silently answered by the wrong region.

The reference architecture already builds one origin-lock `CallInvoker` per silo FQDN that stamps `X-Azure-FDID` on every outbound gRPC call (its MCP head dials the silo directly, not through Front Door). That same per-endpoint invoker is the hook a multi-region deployment hands to each peer region's `LatticeApiMcpRemoteEndpoint.CallInvoker`, alongside `VerifyRegionIdentity`, to target regions deterministically behind a Front Door estate.

## Credential flow over the wire

The remote binding's credential-forwarding interceptor stamps the resolved caller credential onto each outbound gRPC call as `{scheme} {token}`. It resolves the credential in order: an administrator service credential for a system-origin introspection call, then the ambient `LatticeCredentialContext` stamped by the tool, then the `HttpContext` credential bridge, then anonymous. The remote cluster's gRPC binding then authenticates that credential and applies its own per-tree / per-key access gate - so enforcement still happens at the data owner, not at the MCP host.

## Discovery requires the auth endpoint

The in-silo permission-scoped discovery relies on a **system-origin bypass** to introspect a caller's effective permissions. That bypass does not cross the wire. Remotely, the discovery core must authenticate as an administrator to introspect a non-administrator caller, so serving any group's tools to non-administrator callers requires both the `Auth` endpoint and an `AdministratorCredential` to be configured. Without them, only an administrator caller can enumerate tools remotely.

## Refreshing the administrator token

`AdministratorCredential` is a **static** token. When acquired from Entra it typically carries a ~1h lifetime, so a long-lived remote MCP head silently loses its introspection capability once it expires (discovery then advertises no tools to non-administrator callers until the process is restarted or the value is rotated by hand). For an always-on server, register the managed-identity administrator source instead: it acquires the silo-audience token from an `Azure.Core` `TokenCredential`, caches it, and refreshes it a configurable skew before expiry.

```csharp
using Azure.Identity;

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeMcpRemote(o =>
{
    o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster.internal:5001" };
    // No static o.AdministratorCredential needed.
});

builder.Services.AddLatticeMcpManagedIdentityAdministrator(o =>
{
    o.Credential = new ManagedIdentityCredential();      // or DefaultAzureCredential()
    o.Scope = "api://<silo-app-id>/.default";            // the remote silo audience
    o.RefreshSkew = TimeSpan.FromMinutes(5);             // optional; defaults to 5 minutes
});
```

The managed-identity source takes precedence over `AdministratorCredential` regardless of registration order. It is **fail-closed**: if token acquisition fails it forwards no administrator credential (the introspection call is anonymous and the remote cluster denies it), self-healing on the next successful acquisition rather than forwarding a stale token.

## OAuth discovery (RFC 9728)

A remote head is the common place to advertise OAuth discovery, because a client that connects to it over the internet has no pre-shared token. `AddLatticeMcpRemote` wires the base MCP binding (including the discovery endpoint and challenge hint), so opt in by layering the `ProtectedResourceMetadata` option onto the shared `LatticeApiMcpOptions` with an additive `AddLatticeMcp` call. See [Setup](setup.md#oauth-discovery-rfc-9728) for what each field means and [Security](security.md#oauth-discovery-is-anonymous-by-design) for why the metadata endpoint is anonymous.

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeMcpRemote(o =>
{
    o.State = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster.internal:5001" };
    o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster.internal:5001" };
});

// Additive: layer discovery onto the same options the remote binding registered.
builder.Services.AddLatticeMcp(o =>
{
    o.ProtectedResourceMetadata = new LatticeApiMcpProtectedResourceMetadata
    {
        Resource = new Uri("https://mcp.example.com"),
        AuthorizationServers = { new Uri("https://login.microsoftonline.com/<tenant>/v2.0") },
        ScopesSupported = { "api://<server-app-id>/.default" },
    };
});

var app = builder.Build();
app.MapLatticeMcp();
```

## Deferred tools

A few tools back facade operations that have no gRPC method yet, so they cannot be served remotely. The remote host defers them - they are simply omitted from the remote tool list rather than advertised and then failing. Currently deferred: `lattice_state_get_tree_summary`, `lattice_state_get_shard_summaries`, `lattice_state_get_physical_shard_count`, and `lattice_backup_inventory`. They remain fully available in the in-silo topology; each becomes discoverable remotely with no other change once its gRPC method is bound.

## Next

- [Setup](setup.md) - the in-silo topology.
- [Security](security.md) - the fail-closed posture the remote host preserves.
