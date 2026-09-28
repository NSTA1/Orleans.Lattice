namespace Orleans.Lattice.Api.Apps;

/// <summary>One file of an installed app's UI bundle, returned only after its bytes matched the manifest digest.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppUiAsset), Immutable]
public sealed record AppUiAsset
{
    /// <summary>The normalised, bundle-relative asset path.</summary>
    [Id(0)] public required string Path { get; init; }
    /// <summary>The verified asset bytes; read-only, because an immutable record is not copied across a same-silo call.</summary>
    [Id(1)] public ReadOnlyMemory<byte> Bytes { get; init; } = ReadOnlyMemory<byte>.Empty;
    /// <summary>The declared media type.</summary>
    [Id(2)] public required string MediaType { get; init; }
    /// <summary>The verified SHA-256 digest of <see cref="Bytes"/>, as lower-case hex.</summary>
    [Id(3)] public required string Sha256 { get; init; }
}
