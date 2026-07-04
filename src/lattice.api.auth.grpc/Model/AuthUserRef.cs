namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single user by id. A serializable envelope for the
/// facade operations whose only argument is a user id
/// (<c>GetUserAsync</c> / <c>RemoveUserAsync</c>).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthUserRef)]
[Immutable]
public sealed record AuthUserRef
{
    /// <summary>The user id to act on.</summary>
    [Id(0)] public required string UserId { get; init; }
}
