using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// An app-contributed tool advertised under its namespaced name <c>{slug}_{tool}</c>.
/// Built once per app-version activation; the discovery core wraps it in the shared
/// credential-stamping decorator, so by the time <see cref="InvokeAsync"/> runs the
/// caller's credential and asserted active tenant are ambient.
/// </summary>
/// <remarks>
/// Advertisement is the primary gate, and because the session's tool collection serves
/// <c>tools/call</c> too, a tool withheld from <c>tools/list</c> cannot be invoked. The
/// invocation re-runs the very same decision against the current registry snapshot, so a
/// tool advertised earlier in a long-lived session stops working as soon as its app is
/// disabled or the caller loses the grant, rather than when the session ends.
/// </remarks>
internal sealed class AppMcpNamespacedTool : DelegatingMcpServerTool
{
    private readonly Tool _protocolTool;
    private readonly AppMcpToolSource _owner;

    /// <summary>Initializes a new <see cref="AppMcpNamespacedTool"/>.</summary>
    /// <param name="inner">The app's implementation, named by its app-local name.</param>
    /// <param name="slug">The declaring app.</param>
    /// <param name="version">The declaring app version.</param>
    /// <param name="localName">The app-local tool name.</param>
    /// <param name="roleIndex">The index of the declared role in the manifest's role list.</param>
    /// <param name="owner">The tool source that re-checks invocation.</param>
    public AppMcpNamespacedTool(
        McpServerTool inner,
        AppSlug slug,
        AppVersion version,
        string localName,
        int roleIndex,
        AppMcpToolSource owner)
        : base(inner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        Slug = slug;
        Version = version;
        LocalName = localName;
        RoleIndex = roleIndex;
        _owner = owner;

        var tool = base.ProtocolTool;
        _protocolTool = new Tool
        {
            Name = AppMcpToolName.Compose(slug, localName),
            Title = tool.Title,
            Description = tool.Description,
            InputSchema = tool.InputSchema,
            OutputSchema = tool.OutputSchema,
            Annotations = tool.Annotations,
            Icons = tool.Icons,
            Meta = tool.Meta,
        };
    }

    /// <summary>The declaring app.</summary>
    public AppSlug Slug { get; }

    /// <summary>The declaring app version.</summary>
    public AppVersion Version { get; }

    /// <summary>The app-local tool name.</summary>
    public string LocalName { get; }

    /// <summary>The index of the declared role in the manifest's role list.</summary>
    public int RoleIndex { get; }

    /// <inheritdoc />
    public override Tool ProtocolTool => _protocolTool;

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!await _owner.IsInvocationPermittedAsync(this, cancellationToken).ConfigureAwait(false))
        {
            throw new McpException($"Caller is not authorized to invoke the '{_protocolTool.Name}' tool.");
        }

        return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
