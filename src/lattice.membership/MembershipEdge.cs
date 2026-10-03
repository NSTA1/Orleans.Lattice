namespace Orleans.Lattice.Membership;

/// <summary>
/// One membership edge: <see cref="MemberId"/> is a direct member of
/// <see cref="GroupId"/>.
/// </summary>
/// <param name="GroupId">The parent group id.</param>
/// <param name="MemberId">The member (user or group) id.</param>
internal readonly record struct MembershipEdge(string GroupId, string MemberId);
