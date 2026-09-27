using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// A ready-made <see cref="IAppMcpToolProvider"/> over a fixed set of prebuilt tools,
/// for app code (or an existing package adapting its tools) that has no reason to
/// implement the interface itself.
/// </summary>
/// <remarks>
/// The tools are copied once at construction, so later changes to the supplied
/// sequence are not observed. Names are not validated here: the app tool surface
/// validates the pairing against the manifest when it activates the app.
/// </remarks>
public sealed class AppMcpToolProvider : IAppMcpToolProvider
{
    private readonly McpServerTool[] _tools;

    /// <summary>Initializes a new <see cref="AppMcpToolProvider"/>.</summary>
    /// <param name="slug">The app whose declared tools <paramref name="tools"/> implement.</param>
    /// <param name="tools">The tool implementations, each named by its app-local name.</param>
    /// <exception cref="ArgumentException"><paramref name="slug"/> is the uninitialised value, or <paramref name="tools"/> contains <c>null</c>.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="tools"/> is <c>null</c>.</exception>
    public AppMcpToolProvider(AppSlug slug, IEnumerable<McpServerTool> tools)
    {
        if (slug.Value is null)
            throw new ArgumentException("The app slug is uninitialised.", nameof(slug));
        ArgumentNullException.ThrowIfNull(tools);

        var copy = tools.ToArray();
        foreach (var tool in copy)
        {
            if (tool is null)
                throw new ArgumentException("The tools cannot contain null.", nameof(tools));
        }

        Slug = slug;
        _tools = copy;
    }

    /// <inheritdoc />
    public AppSlug Slug { get; }

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools => _tools;
}
