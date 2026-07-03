namespace Orleans.Lattice.Membership;

/// <summary>
/// Distinguishes whether a membership edge points at an individual user or a
/// nested group. Nested groups are supported: a group may be a member of
/// another group, and subject resolution walks the graph transitively.
/// </summary>
public enum MembershipMemberKind
{
    /// <summary>The member is an individual user.</summary>
    User = 0,

    /// <summary>The member is a nested group.</summary>
    Group = 1,
}
