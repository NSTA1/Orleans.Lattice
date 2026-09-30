namespace Orleans.Lattice.Api.Apps;

/// <summary>A file in an app UI bundle, pinned by its manifest digest.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppUiAssetDescriptor), Immutable]
public sealed record AppUiAssetDescriptor
{
    /// <summary>The normalised, bundle-relative asset path.</summary>
    [Id(0)] public required string Path { get; init; }
    /// <summary>The declared media type.</summary>
    [Id(1)] public required string MediaType { get; init; }
    /// <summary>The manifest-pinned SHA-256 digest of the asset bytes, as lower-case hex.</summary>
    [Id(2)] public required string Sha256 { get; init; }
}
