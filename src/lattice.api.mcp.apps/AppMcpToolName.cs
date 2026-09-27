using System.Diagnostics.CodeAnalysis;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// Composes and parses the namespaced MCP tool name <c>{slug}_{tool}</c> under which the
/// app tool surface advertises an app's tool.
/// </summary>
/// <remarks>
/// A valid <see cref="AppSlug"/> never contains <see cref="Separator"/>, so the first
/// separator in a namespaced name is always the boundary between the slug and the
/// app-local tool name. Because slugs are unique in the app registry, two apps can
/// never produce the same namespaced name, and a namespaced name maps back to exactly one
/// <c>(app, tool)</c> pair.
/// </remarks>
public static class AppMcpToolName
{
    /// <summary>The separator between the app slug and the app-local tool name.</summary>
    public const char Separator = '_';

    /// <summary>Composes the namespaced name <c>{slug}_{toolName}</c>.</summary>
    /// <param name="slug">The app slug. Must not be the uninitialised value.</param>
    /// <param name="toolName">The app-local tool name. Must not be <c>null</c> or empty.</param>
    /// <returns>The namespaced tool name.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="slug"/> is the uninitialised value, or <paramref name="toolName"/> is <c>null</c> or empty.
    /// </exception>
    public static string Compose(AppSlug slug, string toolName)
    {
        if (slug.Value is null)
            throw new ArgumentException("The app slug is uninitialised.", nameof(slug));
        ArgumentException.ThrowIfNullOrEmpty(toolName);
        return string.Create(slug.Value.Length + 1 + toolName.Length, (slug.Value, toolName), static (span, state) =>
        {
            state.Value.AsSpan().CopyTo(span);
            span[state.Value.Length] = Separator;
            state.toolName.AsSpan().CopyTo(span[(state.Value.Length + 1)..]);
        });
    }

    /// <summary>
    /// Parses a namespaced tool name back into its app slug and app-local tool name.
    /// </summary>
    /// <param name="name">The namespaced tool name.</param>
    /// <param name="slug">The app slug when this returns <c>true</c>; otherwise <c>default</c>.</param>
    /// <param name="toolName">The app-local tool name when this returns <c>true</c>; otherwise <c>null</c>.</param>
    /// <returns>
    /// <c>true</c> when <paramref name="name"/> is a valid slug, the separator, and a non-empty
    /// app-local name; otherwise <c>false</c>.
    /// </returns>
    public static bool TryParse(string? name, out AppSlug slug, [NotNullWhen(true)] out string? toolName)
    {
        slug = default;
        toolName = null;
        if (string.IsNullOrEmpty(name))
            return false;

        var index = name.IndexOf(Separator);
        if (index <= 0 || index == name.Length - 1 || !AppSlug.TryParse(name[..index], out var parsed))
            return false;

        slug = parsed;
        toolName = name[(index + 1)..];
        return true;
    }
}
