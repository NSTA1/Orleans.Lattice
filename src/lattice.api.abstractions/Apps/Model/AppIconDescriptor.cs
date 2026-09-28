namespace Orleans.Lattice.Api.Apps;

/// <summary>A reference to a declared icon asset: its bundle-relative path and pinned digest.</summary>
/// <remarks>Consumers render the icon through an image element only, never as inline markup.</remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppIconDescriptor), Immutable]
public sealed record AppIconDescriptor
{
    /// <summary>The normalised, bundle-relative icon path.</summary>
    [Id(0)] public required string Path { get; init; }
    /// <summary>The manifest-pinned SHA-256 digest of the icon bytes, as lower-case hex.</summary>
    [Id(1)] public required string Sha256 { get; init; }
}
