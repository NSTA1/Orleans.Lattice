using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Presents an existing repository-context group tool under its app-local name, for the
/// installable-app tool surface, which prefixes it with <c>repo-context_</c>. Only the
/// advertised name changes: the schema, annotations, description and handler are the
/// inner tool's, and invocation delegates unchanged, so the app path runs exactly the
/// same code under the caller's credential as the group path.
/// </summary>
internal sealed class RepoContextAppToolAlias : DelegatingMcpServerTool
{
    private readonly Tool _protocolTool;

    /// <summary>Initializes a new <see cref="RepoContextAppToolAlias"/>.</summary>
    /// <param name="inner">The group tool to present.</param>
    /// <param name="localName">The app-local name to advertise it under.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public RepoContextAppToolAlias(McpServerTool inner, string localName)
        : base(inner ?? throw new ArgumentNullException(nameof(inner)))
    {
        ArgumentNullException.ThrowIfNull(localName);

        var tool = inner.ProtocolTool;
        _protocolTool = new Tool
        {
            Name = localName,
            Title = tool.Title,
            Description = tool.Description,
            InputSchema = tool.InputSchema,
            OutputSchema = tool.OutputSchema,
            Annotations = tool.Annotations,
            Icons = tool.Icons,
            Meta = tool.Meta,
        };
    }

    /// <inheritdoc />
    public override Tool ProtocolTool => _protocolTool;
}
