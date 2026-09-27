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
}
