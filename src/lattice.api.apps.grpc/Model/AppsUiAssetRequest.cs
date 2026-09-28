namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Selects one UI bundle asset of an app's installed version.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsUiAssetRequest), Immutable]
public sealed record AppsUiAssetRequest
{
    /// <summary>The app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The normalised, bundle-relative asset path.</summary>
    [Id(1)] public required string Path { get; init; }
}