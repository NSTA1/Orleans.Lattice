namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single member (a user or a nested group) by id. A
/// serializable envelope for <c>ListSubjectGroupsAsync</c>, whose only argument
/// is a member id.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthMemberRef)]
[Immutable]
public sealed record AuthMemberRef
{
    /// <summary>The member id (a user or nested group) to act on.</summary>
    [Id(0)] public required string MemberId { get; init; }
}
