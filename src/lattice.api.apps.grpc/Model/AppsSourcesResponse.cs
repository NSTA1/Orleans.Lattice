using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Carries the configured app sources in a gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsSourcesResponse), Immutable]
public sealed record AppsSourcesResponse
{
    /// <summary>The configured sources, in registration order.</summary>
    [Id(0)] public ImmutableArray<AppSourceSummary> Sources { get; init; } = [];
}