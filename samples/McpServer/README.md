# MCP Server sample

A single-process demonstration of the optional `Orleans.Lattice.Api.Mcp` add-on.
It co-hosts a single-silo Orleans cluster with the Model Context Protocol (MCP)
server over streamable HTTP, then drives it with a real MCP client - exactly as
an AI agent or MCP-aware tool would - to show the two headline properties of the
surface:

1. **Permission-scoped discovery.** An authenticated agent that has been granted
   access discovers the state / data / auth tool set and calls a tool end-to-end
   over MCP, reading back through the data facade a value the host seeded
   directly on the demo tree.
2. **Fail-closed by default.** A caller the credential bridge cannot authenticate
   is offered *nothing* - not even the `lattice_capabilities` meta-tool.

> **Known issue: the sample registers no `ILatticeApiMcpAuthorizer`.**
> `AddLatticeMcp` therefore falls back to the default `DenyAllMcpAuthorizer`,
> which the discovery core consults for every group tool when it builds the
> tool list and again when a tool is called; `RequireAuthorization = false`
> does not lift that gate. As written, the agent is offered only the
> `lattice_capabilities` meta-tool, so the run cannot complete property 1 (its
> `lattice_data_get` call names a tool the session does not offer). Property 2
> holds either way. Registering `AllowAllMcpAuthorizer` (or your own
> `ILatticeApiMcpAuthorizer`) is the missing step for the agent journey below.

Everything runs in one process for convenience, but the client talks to the
server strictly over MCP using only the SDK's public surface
(`McpClient` + `HttpClientTransport`), so `Program.cs` doubles as a copy-paste
reference for wiring a real MCP client against a Lattice cluster.

## Run it

```
dotnet run --project samples/McpServer/McpServer.csproj
```

The sample seeds an `agent` subject with a full-access grant on a demo tree and
prints the agent's discovered tool set. It is written to then print a live
`lattice_data_get` result and show the anonymous caller being offered zero
tools before it exits, but as written the data call is refused - see the known
issue above. It listens on
`http://localhost:5290` over plain HTTP to stay dependency-free.

Authorization on the endpoint is disabled purely to keep the sample one-command
runnable with no identity provider: a demo credential bridge maps a request that
carries a marker header onto a fixed `agent` credential, and a demo authenticator
resolves that credential to the `agent` subject inside the cluster. A real
deployment leaves `RequireAuthorization` at its secure default, registers an
`ILatticeApiMcpAuthorizer`, and lifts an authenticated ASP.NET Core principal
onto the ambient credential instead.

## What to look at

- `Program.cs` - the silo + MCP host wiring (`AddLatticeMcp` / `AddStateTools` /
  `AddDataTools` / `AddAuthTools` / `MapLatticeMcp`), the rule seeding, and the
  MCP client journey.
- `DemoCredentialBridge.cs` - the fail-closed `ILatticeApiMcpCredentialBridge`
  that decides which requests are the agent and which are anonymous.
- `DemoAuthenticator.cs` - the trusted-token authenticator that resolves the
  ambient credential to a cluster subject.
- The package docs under [`docs/lattice.api.mcp/`](../../docs/lattice.api.mcp/README.md)
  cover the full tool catalogue, the security and discovery model, and the
  remote-hosting topology in depth.
