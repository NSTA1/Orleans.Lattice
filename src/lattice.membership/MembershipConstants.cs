namespace Orleans.Lattice.Membership;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> trees that back
/// the membership directory. These are ordinary user-addressable trees (they do
/// <b>not</b> use the core <c>_lattice_</c> system-tree prefix) so membership
/// state stays fully introspectable through the standard read / scan /
/// change-feed surface - the whole point of storing it inside Lattice rather
/// than an opaque external store. The <c>sys-membership-</c> naming convention
/// reserves them from casual collision with application trees.
/// </summary>
internal static class MembershipConstants
{
    /// <summary>The shared prefix identifying every membership-owned tree.</summary>
    internal const string TreePrefix = "sys-membership-";

    /// <summary>Tree holding user records, keyed by user id.</summary>
    internal const string UsersTree = "sys-membership-users";

    /// <summary>Tree holding group records, keyed by group id.</summary>
    internal const string GroupsTree = "sys-membership-groups";

    /// <summary>
    /// Tree holding membership edges. Each edge is stored twice for O(1)
    /// directional scans: a forward row keyed <c>f\u001f{memberId}\u001f{groupId}</c>
    /// (used by <c>GroupsOfAsync</c>) and a reverse row keyed
    /// <c>r\u001f{groupId}\u001f{memberId}</c> (used by <c>MembersOfAsync</c>).
    /// </summary>
    internal const string EdgesTree = "sys-membership-edges";

    /// <summary>Durable per-key history view name for <see cref="UsersTree"/>.</summary>
    internal const string UsersHistoryView = "sys-membership-users-history";

    /// <summary>Durable per-key history view name for <see cref="GroupsTree"/>.</summary>
    internal const string GroupsHistoryView = "sys-membership-groups-history";

    /// <summary>Durable per-key history view name for <see cref="EdgesTree"/>.</summary>
    internal const string EdgesHistoryView = "sys-membership-edges-history";

    /// <summary>Field separator used inside composite edge keys.</summary>
    internal const char EdgeSeparator = '\u001f';

    /// <summary>Forward-edge key discriminator (member -&gt; group).</summary>
    internal const char ForwardEdge = 'f';

    /// <summary>Reverse-edge key discriminator (group -&gt; member).</summary>
    internal const char ReverseEdge = 'r';

    /// <summary>Enumerates the three backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } =
        new[] { UsersTree, GroupsTree, EdgesTree };
}
