using Orleans.Lattice.Api.Mcp.RepoContext.Host;

// The RepoContext MCP container host entry point: "codebase memory in a box".
// All wiring lives in RepoContextHostBuilder so it is unit-testable; this file is
// the thin process shell that builds and runs the host.
var app = RepoContextHostBuilder.Build(args);
await app.RunAsync();
