namespace Orleans.Lattice.Apps;

/// <summary>
/// A digest-pinned reference to an app icon: an SVG, PNG or WebP bundle asset. Consumers render
/// it only through an <c>&lt;img&gt;</c> element, never as inline SVG, so the asset cannot run script.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppIconReference), Immutable]
public sealed record AppIconReference
{
    /// <summary>Normalised, relative, lower-case bundle path ending in <c>.svg</c>, <c>.png</c> or <c>.webp</c>.</summary>
    [Id(0)] public required string Path { get; init; }

    /// <summary>Lower-case hexadecimal SHA-256 digest of the icon bytes.</summary>
    [Id(1)] public required string Digest { get; init; }
}
