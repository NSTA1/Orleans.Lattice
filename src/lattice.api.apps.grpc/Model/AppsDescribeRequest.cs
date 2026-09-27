namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Selects an app manifest without loading app code.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsDescribeRequest), Immutable]
public sealed record AppsDescribeRequest
{
    /// <summary>The app slug to inspect.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The exact version, or null for the facade's default selection.</summary>
    [Id(1)] public string? Version { get; init; }
}
