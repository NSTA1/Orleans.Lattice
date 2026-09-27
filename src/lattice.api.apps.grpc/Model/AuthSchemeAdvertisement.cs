using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Public sign-in schemes in server preference order.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AuthSchemeAdvertisement), Immutable]
public sealed record AuthSchemeAdvertisement
{
    /// <summary>The advertised schemes; empty means no configured advertisement.</summary>
    [Id(0)] public ImmutableArray<AuthSchemeDescriptor> Schemes { get; init; } = [];
}
