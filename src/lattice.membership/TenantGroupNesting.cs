namespace Orleans.Lattice.Membership;

/// <summary>
/// The tenant group nesting invariant and the reserved tenant-tier namespace test,
/// shared by the membership directory's write path and the active tenant group
/// claim filter.
/// </summary>
/// <remarks>
/// <para>
/// The whole <see cref="LatticeTenantTrees.SegmentPrefix"/> (<c>t/</c>) group
/// namespace is reserved to the tenant tier, not only its well-formed ids:
/// <see cref="LatticeTenantGroupId.IsTenantGroupId"/> is an exact shape test, so
/// <c>t/default/x</c> and every malformed <c>t/...</c> id would slip past it.
/// <see cref="IsTenantTier"/> is therefore a prefix test, and a malformed
/// <c>t/</c> id is refused wherever it would join the tenant tier.
/// </para>
/// <para>
/// A tenant group may contain users, groups of the <b>same</b> tenant and cluster
/// groups. It may never become a member of a cluster group or of another
/// tenant's group. The test is keyed on the id alone, never on the edge's
/// <see cref="MembershipMemberKind"/>, because group resolution walks forward
/// edges by id regardless of the recorded kind.
/// </para>
/// </remarks>
internal static class TenantGroupNesting
{
    /// <summary>
    /// Returns <c>true</c> when <paramref name="id"/> is in the reserved tenant-tier
    /// namespace (it starts with <c>t/</c>), whether or not it is a well-formed
    /// tenant group id. Allocation-free.
    /// </summary>
    /// <param name="id">The candidate group id, or <c>null</c>.</param>
    /// <returns><c>true</c> when the id is tenant-tier; otherwise <c>false</c>.</returns>
    internal static bool IsTenantTier(string? id) =>
        id is not null && id.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Evaluates the edge "<paramref name="memberId"/> is a member of
    /// <paramref name="groupId"/>" against the nesting invariant. Allocation-free.
    /// </summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c>.</param>
    /// <param name="memberId">The member id. Must not be <c>null</c>.</param>
    /// <returns>The violation, or <see cref="TenantGroupNestingViolation.None"/> when the edge is permitted.</returns>
    internal static TenantGroupNestingViolation Evaluate(string groupId, string memberId)
    {
        var groupIsTenantTier = IsTenantTier(groupId);

        if (IsTenantTier(memberId))
        {
            if (!LatticeTenantGroupId.IsTenantGroupId(memberId))
            {
                return TenantGroupNestingViolation.MalformedTenantMember;
            }

            if (!groupIsTenantTier)
            {
                return TenantGroupNestingViolation.TenantGroupInClusterGroup;
            }

            if (!LatticeTenantGroupId.IsTenantGroupId(groupId))
            {
                return TenantGroupNestingViolation.MalformedTenantGroup;
            }

            return TenantSegment(groupId).SequenceEqual(TenantSegment(memberId))
                ? TenantGroupNestingViolation.None
                : TenantGroupNestingViolation.TenantGroupInOtherTenantGroup;
        }

        return groupIsTenantTier && !LatticeTenantGroupId.IsTenantGroupId(groupId)
            ? TenantGroupNestingViolation.MalformedTenantGroup
            : TenantGroupNestingViolation.None;
    }

    /// <summary>
    /// Throws <see cref="LatticeTenantGroupNestingException"/> when the edge
    /// violates the nesting invariant; returns otherwise.
    /// </summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c>.</param>
    /// <param name="memberId">The member id. Must not be <c>null</c>.</param>
    /// <exception cref="LatticeTenantGroupNestingException">The edge violates the invariant.</exception>
    internal static void EnsureAllowed(string groupId, string memberId)
    {
        var violation = Evaluate(groupId, memberId);
        if (violation != TenantGroupNestingViolation.None)
        {
            throw LatticeTenantGroupNestingException.Create(violation, groupId, memberId);
        }
    }

    /// <summary>The <c>{tenant}</c> slice of a well-formed tenant group id.</summary>
    private static ReadOnlySpan<char> TenantSegment(string tenantGroupId)
    {
        var rest = tenantGroupId.AsSpan(LatticeTenantTrees.SegmentPrefix.Length);
        return rest[..rest.IndexOf('/')];
    }
}
