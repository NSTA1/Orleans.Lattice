namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves an absent value in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsBridgeGetResponse), Immutable]
public sealed record AppsBridgeGetResponse
{
    /// <summary>The stored entry, or null when the key has no live value.</summary>
    [Id(0)] public AppBridgeValue? Value { get; init; }
}
