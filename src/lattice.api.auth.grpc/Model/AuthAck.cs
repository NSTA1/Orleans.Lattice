namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire acknowledgement returned by the void-returning admin RPCs (the upserts,
/// removes, and membership-edge mutations). It carries no payload: a successful
/// unary response is itself the acknowledgement, and a denied or failed call
/// arrives as an <see cref="global::Grpc.Core.RpcException"/> status instead.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthAck)]
[Immutable]
public sealed record AuthAck;
