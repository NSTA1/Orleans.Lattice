# Configuration

This package has no package-specific options. Register the app tool source with `AddAppMcpTools()` and register `IAppMcpToolProvider` implementations in the host that already runs the Lattice MCP server. Server authorization and credential settings belong to `Orleans.Lattice.Api.Mcp`.

Provider implementations must use app-local names that pair exactly with the app manifest's tool declarations. A mismatch prevents the app's tool set from being advertised. See the [API](api.md) and [architecture](architecture.md) references.