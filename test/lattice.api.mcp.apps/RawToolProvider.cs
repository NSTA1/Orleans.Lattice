using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// An unvalidated <see cref="IAppMcpToolProvider"/>: it stores whatever slug and tool array it
/// is handed. The shipped <see cref="AppMcpToolProvider"/> rejects an uninitialised slug and a
/// null tool at construction, so it cannot produce the malformed registrations the tool source
/// and the activation pairing defend against. Those defences exist because the interface is
/// public and a host may implement it directly, which is exactly what this double does.
/// </summary>
internal sealed class RawToolProvider(AppSlug slug, params McpServerTool[] tools) : IAppMcpToolProvider
{
    public AppSlug Slug { get; } = slug;

    public IReadOnlyList<McpServerTool> Tools { get; } = tools;
}
