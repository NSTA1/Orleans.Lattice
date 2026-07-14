# MCP Server sample

A single-process demonstration of the optional `Orleans.Lattice.Api.Mcp` add-on.
It co-hosts a single-silo Orleans cluster with the Model Context Protocol (MCP)
server over streamable HTTP, then drives it with a real MCP client - exactly as
an AI agent or MCP-aware tool would - to show the two headline properties of the
surface:

1. **Permission-scoped discovery.** An authenticated agent that has been granted
   access discovers the state / data / auth tool set and calls a tool end-to-end
   over MCP, reading back a value the tool wrote through the data facade.
2. **Fail-closed by default.** A caller the credential bridge cannot authenticate
   is offered *nothing* - not even the `lattice_capabilities` meta-tool.

Everything runs in one process for convenience, but the client talks to the
server strictly over MCP using only the SDK's public surface
(`McpClient` + `HttpClientTransport`), so `Program.cs` doubles as a copy-paste
reference for wiring a real MCP client against a Lattice cluster.

## Run it

```
dotnet run --project samples/McpServer/McpServer.csproj
```

The sample seeds an `agent` subject with a full-access grant on a demo tree,
prints the agent's discovered tool set and a live `lattice_data_get` result, then
shows the anonymous caller being offered zero tools, and exits. It listens on
`http://localhost:5290` over plain HTTP to stay dependency-free.

Authorization on the endpoint is disabled purely to keep the sample one-command
runnable with no identity provider: a demo credential bridge maps a request that
carries a marker header onto a fixed `agent` credential, and a demo authenticator
resolves that credential to the `agent` subject inside the cluster. A real
deployment leaves `RequireAuthorization` at its secure default and lifts an
authenticated ASP.NET Core principal onto the ambient credential instead.

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
