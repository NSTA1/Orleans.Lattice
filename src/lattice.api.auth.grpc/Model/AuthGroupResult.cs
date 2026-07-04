namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response wrapping a nullable group lookup. gRPC unary responses cannot be
/// <see langword="null"/>, so <c>GetGroupAsync</c> returns this envelope with
/// <see cref="Group"/> set to the resolved record or <see langword="null"/> when
/// no such group exists.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthGroupResult)]
[Immutable]
public sealed record AuthGroupResult
{
    /// <summary>The resolved group, or <see langword="null"/> when no such group exists.</summary>
    [Id(0)] public AuthGroup? Group { get; init; }
}
