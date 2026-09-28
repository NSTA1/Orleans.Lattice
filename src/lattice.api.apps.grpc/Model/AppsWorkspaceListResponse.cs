using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Carries the caller's workspace apps in a gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsWorkspaceListResponse), Immutable]
public sealed record AppsWorkspaceListResponse
{
    /// <summary>The enabled apps in which the caller holds a role, in slug order.</summary>
    [Id(0)] public ImmutableArray<WorkspaceAppSummary> Apps { get; init; } = [];
}