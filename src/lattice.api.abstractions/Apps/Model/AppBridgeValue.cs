namespace Orleans.Lattice.Api.Apps;

/// <summary>A key and its stored value, read through the app bridge.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppBridgeValue), Immutable]
public sealed record AppBridgeValue
{
    /// <summary>The key.</summary>
    [Id(0)] public required string Key { get; init; }
    /// <summary>The stored value bytes; read-only, because an immutable record is not copied across a same-silo call.</summary>
    [Id(1)] public ReadOnlyMemory<byte> Value { get; init; } = ReadOnlyMemory<byte>.Empty;
}
