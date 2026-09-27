namespace Orleans.Lattice.Apps;

/// <summary>The slug, exact version and descriptive provenance of an app artifact.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppIdentity), Immutable]
public sealed record AppIdentity
{
    /// <summary>Stable app slug, shared by tree, route and dispatch namespaces.</summary>
    [Id(0)] public required AppSlug Slug { get; init; }

    /// <summary>Version used to identify the artifact and its version-pinned consent.</summary>
    [Id(1)] public required AppVersion Version { get; init; }

    /// <summary>Declared origin, defaulting to in-image and first-party.</summary>
    [Id(2)] public AppProvenance Provenance { get; init; } = new();
}
