using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Mcp.Apps;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Contributes the repository-context app's tool implementations to the installable-app
/// MCP surface: the group's own prebuilt tool instances named in
/// <see cref="RepoContextAppManifest.AppToolNames"/>, each presented under its app-local
/// name. Built once at registration; no per-session work.
/// </summary>
internal sealed class RepoContextAppMcpToolProvider : IAppMcpToolProvider
{
    private static readonly AppSlug AppSlugValue = AppSlug.Parse(RepoContextAppManifest.Slug);

    private readonly McpServerTool[] _tools;

    /// <summary>Initializes a new <see cref="RepoContextAppMcpToolProvider"/> over <paramref name="group"/>'s tools.</summary>
    /// <param name="group">The registered repository-context tool group whose tools to adapt.</param>
    /// <exception cref="ArgumentNullException"><paramref name="group"/> is <see langword="null"/>.</exception>
    public RepoContextAppMcpToolProvider(RepoContextToolGroup group)
    {
        ArgumentNullException.ThrowIfNull(group);

        var tools = new List<McpServerTool>(RepoContextAppManifest.AppToolNames.Count);
        foreach (var tool in group.Tools)
        {
            if (RepoContextAppManifest.LocalNameFor(tool.ProtocolTool.Name) is { } local)
            {
                tools.Add(new RepoContextAppToolAlias(tool, local));
            }
        }

        _tools = tools.ToArray();
    }

    /// <inheritdoc />
    public AppSlug Slug => AppSlugValue;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools => _tools;
}
