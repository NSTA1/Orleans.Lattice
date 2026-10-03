namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of <see cref="ILatticeTenantPolicyAdmin.GetPostureAsync"/>: whether
/// delegated tenant access administration is enabled, the caller's standing
/// towards the tenant, and the tenant's per-tenant access caps with current usage.
/// A user interface reads it to decide which tenant access pages to show and
/// whether a create is about to hit a cap.
/// </summary>
/// <remarks>
/// <para>
/// <b>Feature off.</b> <see cref="Enabled"/> mirrors the cluster's
/// <c>LatticeTenancyOptions.DelegatedAccessAdministrationEnabled</c> flag. The
/// posture probe is the one call on the delegated surface that answers while the
/// feature is off, so a caller can tell "off" from "denied".
/// </para>
/// <para>
/// <b>Caps.</b> <see cref="Groups"/>, <see cref="MembershipEdges"/>,
/// <see cref="MemberSubjects"/> and <see cref="TenantRules"/> report the tenant's
/// <c>MaxGroups</c>, <c>MaxMembershipEdges</c>, <c>MaxMemberSubjects</c> and
/// <c>MaxTenantRules</c> quota dimensions against their current usage, in the same
/// shape as the tenant quota usage report. A <see langword="null"/>
/// <see cref="TenantQuotaDimensionUsage.Limit"/> is unbounded, never zero.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantAccessPosture)]
[Immutable]
public sealed record TenantAccessPosture
{
    /// <summary>The tenant id the posture was read for.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary><see langword="true"/> when delegated tenant access administration is enabled on the cluster.</summary>
    [Id(1)] public bool Enabled { get; init; }

    /// <summary><see langword="true"/> when the caller is an admin of the tenant, directly or through a group.</summary>
    [Id(2)] public bool CallerIsTenantAdmin { get; init; }

    /// <summary><see langword="true"/> when the caller is a platform operator.</summary>
    [Id(3)] public bool CallerIsPlatformOperator { get; init; }

    /// <summary>The tenant's group count against its <c>MaxGroups</c> cap.</summary>
    [Id(4)] public TenantQuotaDimensionUsage Groups { get; init; }

    /// <summary>The tenant's membership-edge count against its <c>MaxMembershipEdges</c> cap.</summary>
    [Id(5)] public TenantQuotaDimensionUsage MembershipEdges { get; init; }

    /// <summary>The tenant's member-set size against its <c>MaxMemberSubjects</c> cap.</summary>
    [Id(6)] public TenantQuotaDimensionUsage MemberSubjects { get; init; }

    /// <summary>The tenant's tenant-tier rule count against its <c>MaxTenantRules</c> cap.</summary>
    [Id(7)] public TenantQuotaDimensionUsage TenantRules { get; init; }
}
