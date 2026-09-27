namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves an absent description in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsDescribeResponse), Immutable]
public sealed record AppsDescribeResponse
{
    /// <summary>The description, or null when the app or version is absent.</summary>
    [Id(0)] public AppDescriptor? Descriptor { get; init; }
}
