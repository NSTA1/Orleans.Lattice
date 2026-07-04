namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single group by id. A serializable envelope for the
/// facade operations whose only argument is a group id
/// (<c>GetGroupAsync</c> / <c>RemoveGroupAsync</c> / <c>ListGroupMembersAsync</c>).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthGroupRef)]
[Immutable]
public sealed record AuthGroupRef
{
    /// <summary>The group id to act on.</summary>
    [Id(0)] public required string GroupId { get; init; }
}
