using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request describing a single membership edge: a member of a group, plus
/// the member's kind. A serializable envelope for <c>AddMemberAsync</c> and
/// <c>RemoveMemberAsync</c>. The <see cref="MemberKind"/> is ignored on removal
/// (an edge is identified by <see cref="GroupId"/> and <see cref="MemberId"/>
/// alone).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthMemberEdge)]
[Immutable]
public sealed record AuthMemberEdge
{
    /// <summary>The parent group id.</summary>
    [Id(0)] public required string GroupId { get; init; }

    /// <summary>The member id (a user or nested group).</summary>
    [Id(1)] public required string MemberId { get; init; }

    /// <summary>Whether the member is a user or a nested group.</summary>
    [Id(2)] public MembershipMemberKind MemberKind { get; init; } = MembershipMemberKind.User;
}
