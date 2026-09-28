using System.Reflection;

namespace Orleans.Lattice.Apps;

/// <summary>
/// A declarative registration of an app compiled into the image: its slug, the already-loaded assembly
/// that carries it, and the name of the embedded JSON manifest resource inside that assembly. Nothing is
/// scanned, loaded or executed; <see cref="InImageAppSource"/> reads only the named resource.
/// </summary>
public sealed record InImageAppRegistration
{
    private readonly string publisher = "first-party";
    private readonly string assetResourcePrefix;

    /// <summary>Creates a registration.</summary>
    /// <param name="slug">The slug the app is resolved under; must be a parsed, non-default slug.</param>
    /// <param name="assembly">The already-loaded assembly carrying the app and its manifest resource.</param>
    /// <param name="manifestResourceName">The manifest's embedded-resource name within <paramref name="assembly"/>.</param>
    public InImageAppRegistration(AppSlug slug, Assembly assembly, string manifestResourceName)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        ArgumentNullException.ThrowIfNull(manifestResourceName);
        if (slug.Value is null)
            throw new ArgumentException("A parsed app slug is required.", nameof(slug));
        Slug = slug;
        Assembly = assembly;
        ManifestResourceName = manifestResourceName;
        assetResourcePrefix = DefaultAssetResourcePrefix(manifestResourceName);
    }

    /// <summary>The slug the app is resolved under. The manifest must declare the same slug.</summary>
    public AppSlug Slug { get; }

    /// <summary>The already-loaded assembly carrying the app and its manifest resource.</summary>
    public Assembly Assembly { get; }

    /// <summary>The manifest's embedded-resource name within <see cref="Assembly"/>.</summary>
    public string ManifestResourceName { get; }

    /// <summary>The publisher recorded in the source provenance; defaults to first-party.</summary>
    public string Publisher
    {
        get => publisher;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            publisher = value;
        }
    }

    /// <summary>
    /// The embedded-resource name prefix of the app's UI bundle assets. An asset at relative path
    /// <c>p</c> is read from the resource named this prefix followed by <c>p</c> with every <c>/</c> mapped to
    /// <c>.</c>. Defaults to <c>{manifestResourceNamespace}.ui.</c>, where the manifest resource namespace is
    /// <see cref="ManifestResourceName"/> without its last two dot-separated segments (the manifest's base name
    /// and extension, as in <c>Contoso.Notes.AppManifest.json</c>), or plain <c>ui.</c> when the manifest
    /// name has fewer than three segments. Set it explicitly when the bundle's resource names follow any other
    /// scheme.
    /// </summary>
    /// <exception cref="ArgumentNullException">The value is <c>null</c>.</exception>
    public string AssetResourcePrefix
    {
        get => assetResourcePrefix;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            assetResourcePrefix = value;
        }
    }

    /// <summary>The default <see cref="AssetResourcePrefix"/> for a manifest resource name.</summary>
    internal static string DefaultAssetResourcePrefix(string manifestResourceName)
    {
        var extension = manifestResourceName.LastIndexOf('.');
        var baseName = extension > 0 ? manifestResourceName.LastIndexOf('.', extension - 1) : -1;
        return baseName > 0 ? string.Concat(manifestResourceName.AsSpan(0, baseName), ".ui.") : "ui.";
    }
}
