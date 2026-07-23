# Remote hosting

The MCP server can run **in the silo** (co-hosted with the facades it binds, resolving them in-process) or **out of the silo** as a standalone host that reaches the cluster over the network. `AddLatticeMcpRemote(...)` wires the out-of-silo topology: the same five tool modules, bound over the `Orleans.Lattice.Api.*.Grpc` clients instead of the in-process facades.

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
| `State` / `Data` / `Auth` / `Backup` / `Replication` | The per-group remote endpoint, or `null` to not serve that group. |
| `CredentialHeaderName` | Header the resolved caller credential is stamped onto for the outbound call. Defaults to `authorization`. |
| `CredentialScheme` | Scheme prefix prepended to the outbound token (`"{scheme} {token}"`). Defaults to `Bearer`; empty sends the bare token. |
| `AdministratorCredential` | The **static** admin service credential used for trusted, read-only permission introspection of each caller. See [discovery](#discovery-requires-the-auth-endpoint) below. For a long-lived server prefer a self-refreshing managed-identity token (see [Refreshing administrator token](#refreshing-the-administrator-token)). |
| `EnableDataWrites` / `EnableBackupControl` / `EnableAuthAdministration` / `EnableReplicationControl` | Forward the destructive-verb opt-in to the corresponding tool module. Ignored when that group's endpoint is unset. |

## Credential flow over the wire

The remote binding's credential-forwarding interceptor stamps the resolved caller credential onto each outbound gRPC call as `{scheme} {token}`. It resolves the credential in order: an administrator service credential for a system-origin introspection call, then the ambient `LatticeCredentialContext` stamped by the tool, then the `HttpContext` credential bridge, then anonymous. The remote cluster's gRPC binding then authenticates that credential and applies its own per-tree / per-key access gate - so enforcement still happens at the data owner, not at the MCP host.

## Discovery requires the auth endpoint

The in-silo permission-scoped discovery relies on a **system-origin bypass** to introspect a caller's effective permissions. That bypass does not cross the wire. Remotely, the discovery core must authenticate as an administrator to introspect a non-administrator caller, so serving any group's tools to non-administrator callers requires both the `Auth` endpoint and an `AdministratorCredential` to be configured. Without them, only an administrator caller can enumerate tools remotely.

## Refreshing the administrator token

`AdministratorCredential` is a **static** token. When acquired from Entra it typically carries a ~1h lifetime, so a long-lived remote MCP head silently loses its introspection capability once it expires (discovery then advertises no tools to non-administrator callers until the process is restarted or the value is rotated by hand). For an always-on server, register the managed-identity administrator source instead: it acquires the silo-audience token from an `Azure.Core` `TokenCredential`, caches it, and refreshes it a configurable skew before expiry.

```csharp
using Azure.Identity;

services.AddLatticeMcpRemote(o =>
{
    o.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = "https://cluster.internal:5001" };
    // No static o.AdministratorCredential needed.
});

services.AddLatticeMcpManagedIdentityAdministrator(o =>
{
    o.Credential = new ManagedIdentityCredential();      // or DefaultAzureCredential()
    o.Scope = "api://<silo-app-id>/.default";            // the remote silo audience
    o.RefreshSkew = TimeSpan.FromMinutes(5);             // optional; defaults to 5 minutes
});
```

The managed-identity source takes precedence over `AdministratorCredential` regardless of registration order. It is **fail-closed**: if token acquisition fails it forwards no administrator credential (the introspection call is anonymous and the remote cluster denies it), self-healing on the next successful acquisition rather than forwarding a stale token.

## Deferred tools

A few tools back facade operations that have no gRPC method yet, so they cannot be served remotely. The remote host defers them - they are simply omitted from the remote tool list rather than advertised and then failing. Currently deferred: `lattice_state_get_tree_summary`, `lattice_state_get_shard_summaries`, `lattice_state_get_physical_shard_count`, and `lattice_backup_inventory`. They remain fully available in the in-silo topology; each becomes discoverable remotely with no other change once its gRPC method is bound.

## Next

- [Setup](setup.md) - the in-silo topology.
- [Security](security.md) - the fail-closed posture the remote host preserves.
