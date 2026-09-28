namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves an absent UI asset in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsUiAssetResponse), Immutable]
public sealed record AppsUiAssetResponse
{
    /// <summary>The verified asset, or null when it is absent, undeclared, unverifiable or not granted.</summary>
    [Id(0)] public AppUiAsset? Asset { get; init; }
}