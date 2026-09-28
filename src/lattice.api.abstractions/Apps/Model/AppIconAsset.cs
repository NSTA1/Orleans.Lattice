namespace Orleans.Lattice.Api.Apps;

/// <summary>An app's presentation icon, returned only after its bytes matched the manifest digest.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppIconAsset), Immutable]
public sealed record AppIconAsset
{
    /// <summary>The verified icon bytes; read-only, because an immutable record is not copied across a same-silo call.</summary>
    [Id(0)] public ReadOnlyMemory<byte> Bytes { get; init; } = ReadOnlyMemory<byte>.Empty;
    /// <summary>The declared media type.</summary>
    [Id(1)] public required string MediaType { get; init; }
    /// <summary>The verified SHA-256 digest of <see cref="Bytes"/>, as lower-case hex.</summary>
    [Id(2)] public required string Sha256 { get; init; }
}
