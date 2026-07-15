namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request for the unauthenticated auth-scheme advertisement RPC. Carries
/// no fields: the probe asks the endpoint how to authenticate before the caller
/// holds any credential.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.AuthSchemeAdvertisementRequest)]
[Immutable]
public sealed record AuthSchemeAdvertisementRequest;
