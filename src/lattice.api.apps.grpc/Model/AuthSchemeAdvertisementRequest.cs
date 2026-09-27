namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Empty request for public sign-in discovery.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AuthSchemeAdvertisementRequest), Immutable]
public sealed record AuthSchemeAdvertisementRequest;
