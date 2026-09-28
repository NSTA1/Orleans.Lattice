namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Selects an app version as one named source offers it, without loading app code.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsSourceAppRequest), Immutable]
public sealed record AppsSourceAppRequest
{
    /// <summary>The key of the source to read from.</summary>
    [Id(0)] public required string SourceKey { get; init; }
    /// <summary>The app slug.</summary>
    [Id(1)] public required string Slug { get; init; }
    /// <summary>The exact version, or null for the source's newest version.</summary>
    [Id(2)] public string? Version { get; init; }
}