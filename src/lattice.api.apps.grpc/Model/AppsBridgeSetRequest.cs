namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Writes one key of an app-owned tree through the app bridge.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsBridgeSetRequest), Immutable]
public sealed record AppsBridgeSetRequest
{
    /// <summary>The app, install revision and app-local tree.</summary>
    [Id(0)] public required AppBridgeTarget Target { get; init; }
    /// <summary>The key.</summary>
    [Id(1)] public required string Key { get; init; }
    /// <summary>The value bytes; read-only, because an immutable record is not copied across a same-silo call.</summary>
    [Id(2)] public ReadOnlyMemory<byte> Value { get; init; } = ReadOnlyMemory<byte>.Empty;
}
