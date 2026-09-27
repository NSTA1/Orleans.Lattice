using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The tool activation of one app version: the manifest's tool declarations paired with
/// the implementations every <see cref="IAppMcpToolProvider"/> for the app supplies,
/// built once per registry epoch and shared by every tenant that installed that version.
/// An activation either succeeds with the complete, namespaced tool set or fails as a
/// whole, contributing no tools.
/// </summary>
internal sealed class AppMcpToolActivation
{
    private readonly Dictionary<string, AppMcpNamespacedTool>? _byLocalName;

    private AppMcpToolActivation(
        AppSlug slug,
        AppVersion version,
        AppManifest? manifest,
        AppMcpNamespacedTool[] tools,
        string? failure)
    {
        Slug = slug;
        Version = version;
        Manifest = manifest;
        Tools = tools;
        Failure = failure;
        if (tools.Length > 0)
        {
            _byLocalName = new Dictionary<string, AppMcpNamespacedTool>(tools.Length, StringComparer.Ordinal);
            foreach (var tool in tools)
                _byLocalName.Add(tool.LocalName, tool);
        }
    }

    /// <summary>The app.</summary>
    public AppSlug Slug { get; }

    /// <summary>The activated app version.</summary>
    public AppVersion Version { get; }

    /// <summary>The resolved manifest, or <c>null</c> when the manifest could not be resolved.</summary>
    public AppManifest? Manifest { get; }

    /// <summary>The namespaced tools, in manifest declaration order; empty on failure.</summary>
    public AppMcpNamespacedTool[] Tools { get; }

    /// <summary>Why the activation failed, or <c>null</c> when it succeeded.</summary>
    public string? Failure { get; }

    /// <summary>Whether the activation succeeded.</summary>
    public bool Succeeded => Failure is null;

    /// <summary>Looks up an activated tool by its app-local name.</summary>
    /// <param name="localName">The app-local tool name.</param>
    /// <param name="tool">The tool when found.</param>
    /// <returns><c>true</c> when the activation carries the tool.</returns>
    public bool TryGetTool(string localName, out AppMcpNamespacedTool tool)
    {
        if (_byLocalName is not null && _byLocalName.TryGetValue(localName, out var found))
        {
            tool = found;
            return true;
        }

        tool = null!;
        return false;
    }

    /// <summary>Creates a failed activation.</summary>
    /// <param name="slug">The app.</param>
    /// <param name="version">The app version.</param>
    /// <param name="failure">The reason.</param>
    /// <returns>The failed activation.</returns>
    public static AppMcpToolActivation Failed(AppSlug slug, AppVersion version, string failure) =>
        new(slug, version, manifest: null, [], failure);

    /// <summary>
    /// Pairs <paramref name="manifest"/>'s declarations with <paramref name="providers"/>'
    /// implementations. Hard-fails when the app's slug is reserved on the tool surface (see
    /// <see cref="AppMcpToolName.IsReservedSlug"/>), a declared tool has no implementation, an
    /// implementation is not declared, a local name is declared or implemented twice, an
    /// implementation has no name, or a declaration names a role the manifest does not
    /// declare.
    /// </summary>
    /// <param name="manifest">The app version's resolved manifest.</param>
    /// <param name="providers">Every provider registered for the app's slug.</param>
    /// <param name="owner">The tool source the namespaced tools re-check invocation through.</param>
    /// <returns>The activation.</returns>
    public static AppMcpToolActivation Pair(
        AppManifest manifest,
        IReadOnlyList<IAppMcpToolProvider> providers,
        AppMcpToolSource owner)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(providers);
        ArgumentNullException.ThrowIfNull(owner);

        var slug = manifest.Identity.Slug;
        var version = manifest.Identity.Version;

        if (AppMcpToolName.IsReservedSlug(slug))
            return Failed(slug, version, $"The app slug '{slug}' is reserved on the tool surface: its tools would be named inside the built-in '{slug}{AppMcpToolName.Separator}' tool namespace.");

        var implementations = new Dictionary<string, McpServerTool>(StringComparer.Ordinal);
        foreach (var provider in providers)
        {
            foreach (var tool in provider.Tools ?? [])
            {
                var name = tool?.ProtocolTool?.Name;
                if (string.IsNullOrEmpty(name))
                    return Failed(slug, version, "A tool implementation has no name.");
                if (!implementations.TryAdd(name, tool!))
                    return Failed(slug, version, $"The tool '{name}' is implemented more than once.");
            }
        }

        var roleIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < manifest.Roles.Length; i++)
            roleIndex.TryAdd(manifest.Roles[i].Name, i);

        var declarations = manifest.McpTools ?? [];
        var declared = new HashSet<string>(StringComparer.Ordinal);
        var tools = new AppMcpNamespacedTool[declarations.Length];
        for (var i = 0; i < declarations.Length; i++)
        {
            var declaration = declarations[i];
            if (!declared.Add(declaration.Name))
                return Failed(slug, version, $"The tool '{declaration.Name}' is declared more than once.");
            if (!roleIndex.TryGetValue(declaration.Role, out var role))
                return Failed(slug, version, $"The tool '{declaration.Name}' names the undeclared role '{declaration.Role}'.");
            if (!implementations.TryGetValue(declaration.Name, out var implementation))
                return Failed(slug, version, $"The declared tool '{declaration.Name}' has no implementation.");

            tools[i] = new AppMcpNamespacedTool(implementation, slug, version, declaration.Name, role, owner);
        }

        foreach (var name in implementations.Keys)
        {
            if (!declared.Contains(name))
                return Failed(slug, version, $"The tool implementation '{name}' is not declared by the manifest.");
        }

        return new(slug, version, manifest, tools, failure: null);
    }
}
