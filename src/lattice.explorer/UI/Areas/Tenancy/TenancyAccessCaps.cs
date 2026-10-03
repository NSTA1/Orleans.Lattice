using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The four delegated tenant access caps as the quota editor shows them: their
/// labels, the default each falls back to when none is set, and reading and
/// writing them on <see cref="TenantQuotasDescriptor"/>. A cap that is not set
/// (<see langword="null"/>) is the default cap, never no cap.
/// </summary>
internal static class TenancyAccessCaps
{
    /// <summary>The default cap on a tenant's groups.</summary>
    public const long DefaultMaxGroups = 500;

    /// <summary>The default cap on a tenant's membership edges.</summary>
    public const long DefaultMaxMembershipEdges = 10_000;

    /// <summary>The default cap on a tenant's member set.</summary>
    public const long DefaultMaxMemberSubjects = 5_000;

    /// <summary>The default cap on a tenant's own access rules.</summary>
    public const long DefaultMaxTenantRules = 1_000;

    /// <summary>Every cap, in the order shown.</summary>
    public static IReadOnlyList<TenancyAccessCap> All { get; } =
        [TenancyAccessCap.Groups, TenancyAccessCap.MembershipEdges, TenancyAccessCap.MemberSubjects, TenancyAccessCap.TenantRules];

    /// <summary>The label of <paramref name="cap"/>.</summary>
    /// <param name="cap">The cap.</param>
    public static string Label(TenancyAccessCap cap) => cap switch
    {
        TenancyAccessCap.Groups => "Tenant groups",
        TenancyAccessCap.MembershipEdges => "Group membership edges",
        TenancyAccessCap.MemberSubjects => "Tenant members",
        _ => "Tenant access rules",
    };

    /// <summary>The cap that applies to <paramref name="cap"/> when none is set.</summary>
    /// <param name="cap">The cap.</param>
    public static long Default(TenancyAccessCap cap) => cap switch
    {
        TenancyAccessCap.Groups => DefaultMaxGroups,
        TenancyAccessCap.MembershipEdges => DefaultMaxMembershipEdges,
        TenancyAccessCap.MemberSubjects => DefaultMaxMemberSubjects,
        _ => DefaultMaxTenantRules,
    };

    /// <summary>The hint beside <paramref name="cap"/>'s field: what a blank field means.</summary>
    /// <param name="cap">The cap.</param>
    public static string Hint(TenancyAccessCap cap) =>
        $"Blank applies the default cap of {TenancyFormat.Count(Default(cap))}. It is never unbounded.";

    /// <summary>The value <paramref name="quotas"/> sets for <paramref name="cap"/>, or <see langword="null"/> when the default applies.</summary>
    /// <param name="quotas">The quotas.</param>
    /// <param name="cap">The cap.</param>
    public static long? Of(TenantQuotasDescriptor quotas, TenancyAccessCap cap) => cap switch
    {
        TenancyAccessCap.Groups => quotas.MaxGroups,
        TenancyAccessCap.MembershipEdges => quotas.MaxMembershipEdges,
        TenancyAccessCap.MemberSubjects => quotas.MaxMemberSubjects,
        _ => quotas.MaxTenantRules,
    };

    /// <summary>The cap in effect, as shown: the value set, or the default marked as such.</summary>
    /// <param name="quotas">The quotas.</param>
    /// <param name="cap">The cap.</param>
    public static string EffectiveText(TenantQuotasDescriptor quotas, TenancyAccessCap cap) =>
        Of(quotas, cap) is { } value
            ? TenancyFormat.Count(value)
            : $"{TenancyFormat.Count(Default(cap))} (default)";
}
