# Public API

`Orleans.Lattice.Api.Mcp.Apps` adds app-provided tools to the existing Lattice MCP server. It exposes a registration method, provider contract, ready-made provider, and public tool-name helper.

## Types and members

| Public type | Public members |
|---|---|
| `LatticeMcpAppsServiceCollectionExtensions` | `IServiceCollection AddAppMcpTools(this IServiceCollection services)` |
| `IAppMcpToolProvider` | `AppSlug Slug { get; }`; `IReadOnlyList<McpServerTool> Tools { get; }` |
| `AppMcpToolProvider` | `AppMcpToolProvider(AppSlug slug, IEnumerable<McpServerTool> tools)`; `AppSlug Slug { get; }`; `IReadOnlyList<McpServerTool> Tools { get; }` |
| `AppMcpToolName` | `const char Separator`; `static string Compose(AppSlug slug, string toolName)`; `static bool TryParse(string? name, out AppSlug slug, [NotNullWhen(true)] out string? toolName)` |

`AddAppMcpTools` is idempotent. Provider tools use app-local names; the surface applies the app slug prefix. See [architecture](architecture.md) for pairing and authorization.

## Source map

- [Registration](../../src/lattice.api.mcp.apps/LatticeMcpAppsServiceCollectionExtensions.cs)
- [Provider contract](../../src/lattice.api.mcp.apps/IAppMcpToolProvider.cs)
- [Ready-made provider](../../src/lattice.api.mcp.apps/AppMcpToolProvider.cs)
- [Name composition](../../src/lattice.api.mcp.apps/AppMcpToolName.cs)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.Apps.AppMcpToolName`

[Source](../../src/lattice.api.mcp.apps/AppMcpToolName.cs) (line 17).

`public static class AppMcpToolName`

- `public const char Separator`
- `public static string Compose(AppSlug slug, string toolName)`
- `public static bool TryParse(string? name, out AppSlug slug, out string? toolName)`

### `Orleans.Lattice.Api.Mcp.Apps.AppMcpToolProvider`

[Source](../../src/lattice.api.mcp.apps/AppMcpToolProvider.cs) (line 16).

`public sealed class AppMcpToolProvider : IAppMcpToolProvider`

- `public AppMcpToolProvider(AppSlug slug, IEnumerable<McpServerTool> tools)`
- `public AppSlug Slug { get; }`
- `public IReadOnlyList<McpServerTool> Tools`

### `Orleans.Lattice.Api.Mcp.Apps.IAppMcpToolProvider`

[Source](../../src/lattice.api.mcp.apps/IAppMcpToolProvider.cs) (line 38).

`public interface IAppMcpToolProvider`

- `AppSlug Slug { get; }`
- `IReadOnlyList<McpServerTool> Tools { get; }`

### `Orleans.Lattice.Api.Mcp.Apps.LatticeMcpAppsServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.apps/LatticeMcpAppsServiceCollectionExtensions.cs) (line 26).

`public static class LatticeMcpAppsServiceCollectionExtensions`

- `public static IServiceCollection AddAppMcpTools(this IServiceCollection services)`
