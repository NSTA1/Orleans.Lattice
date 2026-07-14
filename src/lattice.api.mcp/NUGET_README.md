# Orleans.Lattice.Api.Mcp

Model Context Protocol (MCP) server binding for
[Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice). Exposes the
cluster's transport-agnostic API facades (state, data, backup, auth) as MCP tools
over the official [ModelContextProtocol](https://www.nuget.org/packages/ModelContextProtocol)
SDK, so an AI agent can discover and drive the cluster through a standard MCP
endpoint.

This package is the foundation of the MCP surface: the host front door, options,
streamable-HTTP transport, and the authenticated, fail-closed credential bridge.
It ships with **no** tools registered - per-facade tool modules are added
separately.

## Wiring

In the host's service composition:

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, MyAuthorizer>();
```

And, in the ASP.NET Core endpoint composition:

```csharp
app.MapLatticeMcp();
```

The host co-hosts the MCP server on the same silo that exposes the facades.

## Security

The binding fails closed. With the default `DenyAllMcpAuthorizer`, the fail-closed
credential bridge, and `RequireAuthorization` at its `true` default, an
unauthenticated session is default-denied and can enumerate or call nothing. A
host opts in with a permissive authorizer, a real authenticator, or by turning
enforcement off behind an outer authentication boundary.

The credential bridge lifts the authenticated MCP session identity onto the
ambient `LatticeCredentialContext`, so per-tree / per-key enforcement flows
through the same access gate the gRPC bindings and data path already use. The
binding adds no authorization path of its own.
