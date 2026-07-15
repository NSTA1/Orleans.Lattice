namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response for the unauthenticated auth-scheme advertisement RPC: the
/// ordered set of authentication schemes the endpoint accepts. An empty list
/// means the endpoint advertises nothing (a client falls back to a manually
/// selected or Basic scheme).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.AuthSchemeAdvertisement)]
[Immutable]
public sealed record AuthSchemeAdvertisement
{
    /// <summary>The advertised schemes, in the server's preference order.</summary>
    [Id(0)] public IReadOnlyList<AuthSchemeDescriptor> Schemes { get; init; } =
        Array.Empty<AuthSchemeDescriptor>();
}
