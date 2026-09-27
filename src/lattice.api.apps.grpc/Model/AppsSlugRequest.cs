namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Identifies the app a control RPC targets, never a physical tree.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsSlugRequest), Immutable]
public sealed record AppsSlugRequest
{
    /// <summary>The app slug to dispatch to.</summary>
    [Id(0)] public required string Slug { get; init; }
}
