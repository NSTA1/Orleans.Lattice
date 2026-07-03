namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response wrapping a nullable user lookup. gRPC unary responses cannot be
/// <see langword="null"/>, so <c>GetUserAsync</c> returns this envelope with
/// <see cref="User"/> set to the resolved record or <see langword="null"/> when
/// no such user exists.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthUserResult)]
[Immutable]
public sealed record AuthUserResult
{
    /// <summary>The resolved user, or <see langword="null"/> when no such user exists.</summary>
    [Id(0)] public AuthUser? User { get; init; }
}
