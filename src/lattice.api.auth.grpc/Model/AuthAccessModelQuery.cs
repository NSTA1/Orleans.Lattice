namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request for the no-argument access-model read RPC
/// (<c>GetAccessModelAsync</c>). A gRPC unary call always carries a request
/// message, so this empty, serializable envelope stands in for the facade
/// operation's void argument list, mirroring the other no-argument RPCs.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthAccessModelQuery)]
[Immutable]
public sealed record AuthAccessModelQuery;
