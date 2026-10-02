namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-tenant access caps that protect the shared access-administration trees:
/// how many tenant groups, membership edges, member-set entries and tenant-tier
/// rules one tenant may own (<see cref="TenantQuotas.EffectiveMaxGroups"/>,
/// <see cref="TenantQuotas.EffectiveMaxMembershipEdges"/>,
/// <see cref="TenantQuotas.EffectiveMaxMemberSubjects"/>,
/// <see cref="TenantQuotas.EffectiveMaxTenantRules"/>). The delegated
/// access-administration facades count the tenant's current usage and call
/// <see cref="AdmitAddition"/> before each addition, so every cap is refused through
/// the same <see cref="LatticeQuotaExceededException"/> path as the tenant's other
/// quotas.
/// </summary>
/// <remarks>
/// Tenant groups, membership edges and tenant rules live in trees every tenant
/// shares (the membership trees and the policy tree), so these caps bound what one
/// tenant can cost all the others. Unlike the resource quotas they are hard
/// ceilings: <see cref="TenantQuotas.BurstPercent"/> does not apply. The member-set
/// usage is read from the tenant record itself
/// (<see cref="TenantRecord.MemberSubjectCount"/>).
/// </remarks>
public static class TenantAccessCaps
{
    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for a tenant-group cap breach.</summary>
    public const string GroupsDimension = "tenant-groups";

    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for a membership-edge cap breach.</summary>
    public const string MembershipEdgesDimension = "tenant-membership-edges";

    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for a member-set cap breach.</summary>
    public const string MemberSubjectsDimension = "tenant-member-subjects";

    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for a tenant-rule cap breach.</summary>
    public const string TenantRulesDimension = "tenant-rules";

    /// <summary>
    /// Admits adding one more item on <paramref name="dimension"/> for
    /// <paramref name="tenant"/>, given its <paramref name="currentCount"/> before
    /// the addition, or throws <see cref="LatticeQuotaExceededException"/> when the
    /// addition would take it past <paramref name="cap"/>. A tenant sitting exactly
    /// on its cap is refused the next item. An under-cap admission is branch-only and
    /// allocation-free; only a refusal allocates (the exception).
    /// </summary>
    /// <param name="tenant">The tenant the addition is for.</param>
    /// <param name="treeId">The shared tree the addition writes to, surfaced on the exception. Must not be <c>null</c>.</param>
    /// <param name="dimension">The capped dimension, one of the <c>*Dimension</c> constants on this type. Must not be <c>null</c>.</param>
    /// <param name="currentCount">The tenant's count on the dimension before the addition.</param>
    /// <param name="cap">The cap in force, for example <see cref="TenantQuotas.EffectiveMaxGroups"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> or <paramref name="dimension"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeQuotaExceededException">The addition would exceed <paramref name="cap"/>.</exception>
    public static void AdmitAddition(TenantId tenant, string treeId, string dimension, long currentCount, long cap)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(dimension);

        if (currentCount < cap)
        {
            return;
        }

        throw new LatticeQuotaExceededException(
            $"Tenant '{tenant}' has reached its {dimension} cap of {cap}; remove an existing entry or ask a platform operator to raise the cap.",
            treeId,
            dimension,
            currentCount,
            cap,
            tenant.Value ?? string.Empty);
    }
}
