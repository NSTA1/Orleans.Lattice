namespace Orleans.Lattice.Replication;

/// <summary>
/// The well-known ids of the reserved, dogfooded <c>ILattice</c> trees that back
/// the <c>Orleans.Lattice.Membership</c> directory and the
/// <c>Orleans.Lattice.Auth</c> policy store, together with the
/// <see cref="LatticeMergeMode"/> each replicates under. These trees are the
/// deliberate exception to the "system state stays cluster-local" rule: unlike
/// the core registry tree, the membership and policy trees are ordinary
/// user-addressable trees (they do <b>not</b> use the core reserved
/// <c>_lattice_</c> tree-id prefix, so the
/// receiver apply seam never rejects them as system trees) and a multi-cluster
/// deployment usually wants a single converged identity and authorization surface
/// across sites.
/// </summary>
/// <remarks>
/// <para>
/// The <c>Orleans.Lattice.Replication</c> package cannot reference
/// <c>Orleans.Lattice.Membership</c> or <c>Orleans.Lattice.Auth</c> (they sit
/// above it in the dependency graph), so these ids are mirrored here as the
/// stable public contract they already are. They are kept in sync with the owning
/// packages' <c>MembershipConstants</c> and <c>AuthConstants</c> by the
/// <c>SystemTreeNameDriftGuardTests</c> in the membership and auth test projects
/// (each references this package and can see its own package's canonical internal
/// constants), which fail if a mirrored id ever drifts. Changing any string here
/// is a wire-format and on-disk-key break and must never be done casually.
/// </para>
/// <para>
/// Membership and policy trees replicate last-writer-wins
/// (<see cref="LatticeMergeMode.LwwRegister"/>): each group, edge, and rule
/// is an independent key whose latest HLC-stamped write wins on convergence, which
/// is the correct model for a mutable identity/authorization record. The optional
/// audit tree is append-only and replicates as an observed-remove set
/// (<see cref="LatticeMergeMode.OrSet"/>) so concurrently appended audit rows on
/// different sites all survive the merge instead of overwriting one another.
/// </para>
/// </remarks>
public static class LatticeSystemTreeNames
{
    /// <summary>The membership groups tree (mirrors <c>MembershipConstants.GroupsTree</c>).</summary>
    public const string MembershipGroups = "sys-membership-groups";

    /// <summary>The membership edges tree (mirrors <c>MembershipConstants.EdgesTree</c>).</summary>
    public const string MembershipEdges = "sys-membership-edges";

    /// <summary>The authorization policy (rules) tree (mirrors <c>AuthConstants.PolicyTree</c>).</summary>
    public const string AuthPolicy = "sys-auth-policy";

    /// <summary>
    /// The optional append-only authorization audit tree. Off by default (see
    /// <c>ReplicateLatticeSystemTrees</c>): the auth package derives its policy
    /// change history as a materialised view over the replicated
    /// <see cref="AuthPolicy"/> tree, so every site rebuilds the same history
    /// locally once the policy tree converges and shipping the audit rows as well
    /// is redundant. A deployment that dogfoods a distinct cross-site audit tree
    /// can opt it in explicitly.
    /// </summary>
    public const string AuthAudit = "sys-auth-audit";

    /// <summary>
    /// Builds the reserved-tree to <see cref="LatticeMergeMode"/> enrolment map.
    /// The membership and policy trees are always included; the append-only audit
    /// tree is included only when <paramref name="includeAudit"/> is <c>true</c>.
    /// </summary>
    /// <param name="includeAudit">Whether to enrol the append-only audit tree.</param>
    /// <returns>An ordinal-keyed map of reserved tree id to its replication merge mode.</returns>
    public static IReadOnlyDictionary<string, LatticeMergeMode> BuildEnrolmentMap(bool includeAudit)
    {
        var map = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            [MembershipGroups] = LatticeMergeMode.LwwRegister,
            [MembershipEdges] = LatticeMergeMode.LwwRegister,
            [AuthPolicy] = LatticeMergeMode.LwwRegister,
        };

        if (includeAudit)
        {
            map[AuthAudit] = LatticeMergeMode.OrSet;
        }

        return map;
    }
}
