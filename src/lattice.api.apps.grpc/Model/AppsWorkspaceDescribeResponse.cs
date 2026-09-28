namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves an absent workspace description in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsWorkspaceDescribeResponse), Immutable]
public sealed record AppsWorkspaceDescribeResponse
{
    /// <summary>The sanitised description, or null when the app is absent or the caller holds no role in it.</summary>
    [Id(0)] public WorkspaceAppDescriptor? Descriptor { get; init; }
}