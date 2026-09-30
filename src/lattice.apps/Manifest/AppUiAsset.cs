namespace Orleans.Lattice.Apps;

/// <summary>One file of an app UI bundle, pinned by its SHA-256 digest.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiAsset), Immutable]
public sealed record AppUiAsset
{
    /// <summary>Normalised, relative, <c>/</c>-separated, lower-case bundle path.</summary>
    [Id(0)] public required string Path { get; init; }

    /// <summary>Media type, drawn from <see cref="AppUiBundle.AllowedMediaTypes"/>.</summary>
    [Id(1)] public required string MediaType { get; init; }

    /// <summary>Lower-case hexadecimal SHA-256 digest of the asset bytes.</summary>
    [Id(2)] public required string Digest { get; init; }
}
