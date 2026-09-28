namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves an absent icon in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsIconResponse), Immutable]
public sealed record AppsIconResponse
{
    /// <summary>The verified icon, or null when it is absent, undeclared, unverifiable or not granted.</summary>
    [Id(0)] public AppIconAsset? Icon { get; init; }
}