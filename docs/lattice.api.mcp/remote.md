# Remote hosting

The MCP server can run **in the silo** (co-hosted with the facades it binds, resolving them in-process) or **out of the silo** as a standalone host that reaches the cluster over the network. `AddLatticeMcpRemote(...)` wires the out-of-silo topology: the same four tool modules, bound over the `Orleans.Lattice.Api.*.Grpc` clients instead of the in-process facades.

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
});

var app = builder.Build();
app.MapLatticeMcp();
```

Each `LatticeApiMcpRemoteEndpoint` names the served `Endpoint` (surfaced verbatim in the `lattice_capabilities` report) and optionally supplies a pre-built `CallInvoker` so a host that already owns a tuned gRPC channel (custom TLS, retries, deadlines) can reuse it instead of the address-derived default.

## Options

| Option | Purpose |
|---|---|
| `State` / `Data` / `Auth` / `Backup` | The per-group remote endpoint, or `null` to not serve that group. |
| `CredentialHeaderName` | Header the resolved caller credential is stamped onto for the outbound call. Defaults to `authorization`. |
| `CredentialScheme` | Scheme prefix prepended to the outbound token (`"{scheme} {token}"`). Defaults to `Bearer`; empty sends the bare token. |
| `AdministratorCredential` | The admin service credential used for trusted, read-only permission introspection of each caller. See [discovery](#discovery-requires-the-auth-endpoint) below. |
| `EnableDataWrites` / `EnableBackupControl` / `EnableAuthAdministration` | Forward the destructive-verb opt-in to the corresponding tool module. Ignored when that group's endpoint is unset. |

## Credential flow over the wire

The remote binding's credential-forwarding interceptor stamps the resolved caller credential onto each outbound gRPC call as `{scheme} {token}`. It resolves the credential in order: an administrator service credential for a system-origin introspection call, then the ambient `LatticeCredentialContext` stamped by the tool, then the `HttpContext` credential bridge, then anonymous. The remote cluster's gRPC binding then authenticates that credential and applies its own per-tree / per-key access gate - so enforcement still happens at the data owner, not at the MCP host.

## Discovery requires the auth endpoint

The in-silo permission-scoped discovery relies on a **system-origin bypass** to introspect a caller's effective permissions. That bypass does not cross the wire. Remotely, the discovery core must authenticate as an administrator to introspect a non-administrator caller, so serving any group's tools to non-administrator callers requires both the `Auth` endpoint and an `AdministratorCredential` to be configured. Without them, only an administrator caller can enumerate tools remotely.

## Deferred tools

A few tools back facade operations that have no gRPC method yet, so they cannot be served remotely. The remote host defers them - they are simply omitted from the remote tool list rather than advertised and then failing. Currently deferred: `lattice_state_get_tree_summary`, `lattice_state_get_shard_summaries`, `lattice_state_get_physical_shard_count`, and `lattice_backup_inventory`. They remain fully available in the in-silo topology; each becomes discoverable remotely with no other change once its gRPC method is bound.

## Next

- [Setup](setup.md) - the in-silo topology.
- [Security](security.md) - the fail-closed posture the remote host preserves.
