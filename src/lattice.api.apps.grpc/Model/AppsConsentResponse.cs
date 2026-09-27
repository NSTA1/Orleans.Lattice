namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Preserves absent consent in a non-null gRPC response envelope.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsConsentResponse), Immutable]
public sealed record AppsConsentResponse
{
    /// <summary>The pinned consent, or null when the app is not installed.</summary>
    [Id(0)] public AppConsentReport? Consent { get; init; }
}
