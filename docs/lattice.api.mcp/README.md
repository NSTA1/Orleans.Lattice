# Orleans.Lattice.Api.Mcp

A Model Context Protocol (MCP) server for [Orleans.Lattice](../../README.md) - it exposes a running lattice cluster's transport-agnostic API facades (state, data, backup, auth) as MCP tools an AI agent can discover and drive over a standard, authenticated MCP endpoint.

## What is it?

`Orleans.Lattice.Api.Mcp` is the **agent-facing control surface** of a lattice cluster. The core library is a data plane reached through grain interfaces; the `Orleans.Lattice.Api.*` packages add transport-agnostic facades over the read state, the read/write data path, the backup control plane, and the authorization admin plane. This package binds those same facades onto the official [`ModelContextProtocol`](https://www.nuget.org/packages/ModelContextProtocol) C# SDK, so a language-model agent talks to the cluster the same way it talks to any other MCP server - no bespoke client, no hand-rolled schema.

It is built from four parts:

- **A front door.** `AddLatticeMcp(...)` registers the MCP server, the streamable-HTTP transport, a fail-closed authorizer seam, and the credential bridge; `MapLatticeMcp()` maps the endpoint. The server starts with **no** tools - each tool module is opt-in.
- **Permission-scoped discovery.** A per-session configurator computes the tool list from the authenticated caller's effective permissions and adds a `lattice_capabilities` meta-tool. A caller sees and can invoke only the tools its grants allow; an ungranted tool is never listed.
- **Five tool modules.** `AddStateTools()`, `AddDataTools()`, `AddBackupTools()`, `AddAuthTools()`, and `AddReplicationTools()` each register a group of thin adapters over the matching facade, named `lattice_<group>_<verb>`. Destructive verbs (writes, backup control, auth administration, replication control) are opt-in. The replication module is served in-silo only.
- **In-silo or remote hosting.** Co-host the server on a silo that exposes the facades in-process, or run it out-of-silo with `AddLatticeMcpRemote(...)`, which binds the same tool modules over the `Orleans.Lattice.Api.*.Grpc` clients to front a cluster it is not co-located with.

## Core properties

- **Fail-closed by construction.** The default `DenyAllMcpAuthorizer`, the fail-closed credential bridge, and `RequireAuthorization` (default `true`) mean an unauthenticated session is default-denied: it can enumerate nothing and call nothing until the host opts in with a real authorizer and authenticator.
- **No re-modelled surface.** Every tool is a thin adapter over the matching `Orleans.Lattice.Api.*` facade and the same Orleans-serialized records the gRPC bindings adapt, so the MCP surface stays in lock-step with the rest of the API family with zero re-modelling.
- **Permission-scoped, not deny-after-list.** Discovery filters the tool list to the caller's effective permissions before it is returned, so an agent never sees a tool it cannot use.
- **Opt-in and least-privilege.** The server ships no tools; each module is added explicitly, and within a module the destructive verbs stay hidden until the host enables them (`enableWrites`, `enableControl`, `enableAdministration`).
- **Credential flow-through.** The credential bridge lifts the authenticated MCP session identity onto the ambient `LatticeCredentialContext`, so per-tree / per-key enforcement runs through the same access gate the gRPC bindings and the data path already use. The binding adds no authorization path of its own.

## Quick start

Co-host the MCP server on an existing `Orleans.Lattice` silo. Register the facades the tool modules need, add the MCP front door and an authorizer, add the tool modules to expose, then map the endpoint:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi()
        .AddLatticeDataApi();
});

// The MCP front door. The default authorizer denies every call, so register a
// real one (or disable enforcement behind an outer boundary) before serving.
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>();

// Opt in to the tool modules. Reads are always exposed; writes stay off here.
builder.Services.AddStateTools();
builder.Services.AddDataTools(enableWrites: false);

var app = builder.Build();
app.MapLatticeMcp();
```

An MCP client then connects to the mapped endpoint, calls `lattice_capabilities` to see which groups and tools its credential unlocks, and invokes tools such as `lattice_state_list_trees`, `lattice_data_get`, or `lattice_data_read_range`.

For a complete, runnable co-hosted silo that serves the MCP endpoint, see the [`McpServer`](../../samples/McpServer) sample under [`samples/`](../../samples).

## Reference

- [Setup](setup.md) - registering the front door, the options, and mapping the endpoint.
- [Tools](tools.md) - the five tool modules, their opt-in flags, and the full tool catalogue.
- [Security](security.md) - the fail-closed posture, the authorizer seam, the credential bridge, and permission-scoped discovery.
- [Remote hosting](remote.md) - running the server out-of-silo over the gRPC clients with `AddLatticeMcpRemote(...)`.

## See also

- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) - the read-only state facade the state tools adapt.
- [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) - the read/write data facade the data tools adapt.
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the backup control facade the backup tools adapt.
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the authorization admin facade the auth tools adapt.
- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) - the replication control facade the replication tools adapt.
