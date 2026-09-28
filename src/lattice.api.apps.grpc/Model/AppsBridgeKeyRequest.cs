namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Addresses one key of an app-owned tree through the app bridge (a read or a delete).</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsBridgeKeyRequest), Immutable]
public sealed record AppsBridgeKeyRequest
{
    /// <summary>The app, install revision and app-local tree.</summary>
    [Id(0)] public required AppBridgeTarget Target { get; init; }
    /// <summary>The key.</summary>
    [Id(1)] public required string Key { get; init; }
}
