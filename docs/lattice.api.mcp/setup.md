# Setup

Registering the `Orleans.Lattice.Api.Mcp` server on a silo host, configuring the options, and mapping the endpoint.

## Prerequisites

The MCP server binds the `Orleans.Lattice.Api.*` facades, so the facades a tool module needs must be registered on the same silo:

- State tools need `AddLatticeStateApi()`.
- Data tools need `AddLatticeDataApi()`.
- Backup tools need `AddLatticeBackupApi()` (which itself follows `AddLatticeBackup(...)`).
- Auth tools need `AddLatticeAuthApi()` (which itself follows `AddLatticeAuth(...)`).
- Replication tools need `AddLatticeReplicationApi()` (which itself follows `AddLatticeReplication(..., enableRuntimeConfig: true)`).

Only register the facades whose tool modules you intend to expose.

## Register the front door

`AddLatticeMcp(...)` wires the MCP server, the streamable-HTTP transport, the `AddHttpContextAccessor` the credential bridge reads, the default-deny authorizer, and the credential bridge. It is idempotent. `MapLatticeMcp()` maps the transport and applies `RequireAuthorization()` when enforcement is on.

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi();
});

builder.Services.AddLatticeMcp(o =>
{
    o.RequireAuthorization = true;   // fail closed (the default)
    o.TransportPattern = "/mcp";     // mount the transport under /mcp
    o.CredentialHeaderName = "authorization";
    o.CredentialScheme = "Bearer";
});
builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>();
builder.Services.AddStateTools();

var app = builder.Build();
app.MapLatticeMcp();
```

## Options

`LatticeApiMcpOptions` (populated through the `AddLatticeMcp` delegate):

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | When `true`, `MapLatticeMcp` applies `RequireAuthorization()` so an unauthenticated session is default-denied. Set `false` only behind an outer authentication boundary. |
| `TransportPattern` | `string` | `""` | The route the streamable-HTTP transport mounts at. Empty mounts at the route builder's root; set a sub-path (for example `/mcp`) to co-host alongside other endpoints. |
| `Stateless` | `bool` | `false` | Whether the HTTP transport runs stateless. The permission-scoped per-session tool collections rely on the stateful (default) mode; enable only for a horizontally-scaled deployment with a fixed tool set. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound header carrying the caller's credential token, bridged onto the ambient Lattice credential. |
| `CredentialScheme` | `string` | `Bearer` | The scheme stamped on the bridged credential; a case-insensitive scheme prefix (for example `"Bearer "`) is stripped from the header value before the remaining token is used. |
| `EnableStateTools` / `EnableDataTools` / `EnableBackupTools` / `EnableBackupControlTools` / `EnableAuthTools` / `EnableAuthAdministration` / `EnableReplicationTools` / `EnableReplicationControlTools` / `EnableTreeAdminSchemaControlTools` / `EnableTreeAdminLifecycleTools` / `EnableTenantAdminTools` / `EnableTenantAdminControlTools` | `bool` | `false` | Per-module enable flags. Set by the `AddXTools(...)` extensions; a host normally toggles them through those calls rather than directly. The tree-administration group is registered by `AddTreeAdminTools(...)`; these two flags opt in its mutating schema-management and lifecycle/control tools. The tenant-administration group is registered by `AddTenantAdminTools(...)`; `EnableTenantAdminTools` advertises the `tenantadmin` capability and `EnableTenantAdminControlTools` opts in its mutating tenant-lifecycle tools. |
| `ProtectedResourceMetadata` | `LatticeApiMcpProtectedResourceMetadata?` | `null` | Opt into OAuth 2.0 Protected Resource Metadata (RFC 9728). When set, an anonymous metadata document is served at `/.well-known/oauth-protected-resource` and the `401` bearer challenge carries a `resource_metadata` hint. See [OAuth discovery](#oauth-discovery-rfc-9728). |

## Add the tool modules

The server exposes no tools until a module is added. See [Tools](tools.md) for the module opt-in flags and the full catalogue. A minimal read-only server adds just `AddStateTools()`; a full-control server adds every module with its destructive flag set.

## OAuth discovery (RFC 9728)

By default the server is a plain bearer-token resource: a caller must already hold a token. Set `ProtectedResourceMetadata` to opt into OAuth 2.0 Protected Resource Metadata ([RFC 9728](https://www.rfc-editor.org/rfc/rfc9728)) so a spec-compliant MCP client can discover the authorization server and run the sign-in flow itself. `MapLatticeMcp` then serves an anonymous metadata document at `/.well-known/oauth-protected-resource`, and the binding appends a `resource_metadata` hint to the `401` bearer challenge on the transport path. The feature is scheme-agnostic - it augments whatever bearer challenge the host's authentication handler emits - so it needs no dependency on a specific auth library. `Resource` is this server's public, canonical URL as clients reach it (for example the CDN or ingress edge).

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeMcp(o =>
{
    o.RequireAuthorization = true;
    o.TransportPattern = "/mcp";
    o.ProtectedResourceMetadata = new LatticeApiMcpProtectedResourceMetadata
    {
        Resource = new Uri("https://mcp.example.com"),
        AuthorizationServers = { new Uri("https://login.microsoftonline.com/<tenant>/v2.0") },
        ScopesSupported = { "api://<server-app-id>/.default" },
    };
});
```

## Next

- [Tools](tools.md) - the built-in modules and every tool they expose.
- [Security](security.md) - the fail-closed posture and the credential bridge.
- [Remote hosting](remote.md) - serving the surface out-of-silo over gRPC.
