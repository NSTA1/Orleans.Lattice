namespace Orleans.Lattice.Membership;

/// <summary>
/// The admin audit anchors of the membership directory: the record whose write is
/// the commit point of each group and membership mutation (#3638).
/// </summary>
internal static class MembershipAuditAnchors
{
    /// <summary>The group record: the single write of a group upsert and of a group removal.</summary>
    /// <param name="groupId">The group id.</param>
    internal static LatticeAdminAuditAnchor Group(string groupId) =>
        new(MembershipConstants.GroupsTree, groupId);

    /// <summary>The reverse (group to member) edge: the second and last write of a membership add or removal.</summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="memberId">The member id.</param>
    internal static LatticeAdminAuditAnchor Edge(string groupId, string memberId) =>
        new(MembershipConstants.EdgesTree, LatticeMembershipDirectory.ReverseKey(groupId, memberId));
}
