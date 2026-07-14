# Orleans.Lattice.Api.Mcp

Optional, opt-in **Model Context Protocol (MCP) server** add-on for [`Orleans.Lattice`](https://github.com/NSTA1/Orleans.Lattice). It exposes a cluster's transport-agnostic API facades - state, data, backup, and auth - as MCP tools over the official [`ModelContextProtocol`](https://www.nuget.org/packages/ModelContextProtocol) SDK, so an AI agent can discover and drive the cluster through a standard, authenticated MCP endpoint.

## What it gives you

- **Four opt-in tool modules** - `AddStateTools()` (read-only introspection), `AddDataTools()` (reads always, writes opt-in), `AddBackupTools()` (inspect always, control verbs opt-in), and `AddAuthTools()` (admin-gated introspection, admin verbs opt-in). Each tool is a thin adapter over the matching facade, named `lattice_<group>_<verb>`.
- **Permission-aware discovery** - a `lattice_capabilities` meta-tool and a per-session tool list computed from the caller's effective permissions, so a caller sees and can invoke only the tools its grants allow. An ungranted tool is never listed, not listed-then-denied.
- **Fail-closed by construction** - the default `DenyAllMcpAuthorizer`, a fail-closed credential bridge, and `RequireAuthorization` (default `true`) mean an unauthenticated session is default-denied and can enumerate or call nothing.
- **In-silo or remote hosting** - co-host the server on a silo that exposes the facades in-process, or run it out-of-silo with `AddLatticeMcpRemote(...)`, bound over the `Orleans.Lattice.Api.*.Grpc` clients, to front a cluster it is not co-located with.
- **No re-modelled surface** - the tools reuse the same facades and Orleans-serialized records the gRPC bindings adapt, so the MCP surface stays in lock-step with the rest of the API family.

Co-host it on a silo that already exposes the facades, then add the tool modules you want:

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, MyAuthorizer>();
builder.Services.AddStateTools();
builder.Services.AddDataTools(enableWrites: true);

var app = builder.Build();
app.MapLatticeMcp();
```

The credential bridge lifts the authenticated MCP session identity onto the ambient `LatticeCredentialContext`, so per-tree / per-key enforcement flows through the same access gate the gRPC bindings and the data path already use. The binding adds no authorization path of its own.

See the [MCP API documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp/README.md) for the full tool catalogue, the security and discovery model, and the remote-hosting guide.
