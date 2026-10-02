namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The resource quotas and burst allowance for a tenant. Each dimension is a
/// nullable ceiling where <c>null</c> means <em>unbounded</em> (no limit on that
/// dimension); a bounded ceiling must be non-negative. <see cref="BurstPercent"/> is
/// the transient headroom, as a percentage of the bounded ceilings, a tenant may
/// momentarily exceed before admission control engages (<c>0</c> means no burst).
/// </summary>
/// <remarks>
/// This is the tenant's declared allocation, authored by an operator and stored
/// in the registry; enforcement of it against live usage is a separate concern
/// layered on top of the registry. The reserved <see cref="TenantId.Default"/>
/// tenant carries <see cref="Unbounded"/>.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantQuotas)]
[Immutable]
public readonly record struct TenantQuotas
{
    /// <summary>
    /// The maximum total storage footprint in bytes - the write-ahead log, snapshot,
    /// and persisted leaf-state bytes of the tenant's trees - or <c>null</c> for
    /// unbounded.
    /// </summary>
    [Id(0)]
    public long? MaxBytes { get; init; }

    /// <summary>The maximum total live key count, or <c>null</c> for unbounded.</summary>
    [Id(1)]
    public long? MaxKeys { get; init; }

    /// <summary>The maximum resident memory in bytes, or <c>null</c> for unbounded.</summary>
    [Id(2)]
    public long? MaxMemoryBytes { get; init; }

    /// <summary>The maximum number of trees the tenant may own, or <c>null</c> for unbounded.</summary>
    [Id(3)]
    public long? MaxTreeCount { get; init; }

    /// <summary>The maximum sustained operations per second, or <c>null</c> for unbounded.</summary>
    [Id(4)]
    public long? MaxOpsPerSecond { get; init; }

    /// <summary>
    /// The transient burst headroom above the bounded ceilings, as a percentage
    /// (<c>0</c> for none). For example <c>20</c> permits a momentary 20% overage
    /// before admission control engages.
    /// </summary>
    [Id(5)]
    public int BurstPercent { get; init; }

    /// <summary>
    /// The default ceiling on the number of tenant groups a tenant may own, applied
    /// when <see cref="MaxGroups"/> is <c>null</c>.
    /// </summary>
    public const long DefaultMaxGroups = 500;

    /// <summary>
    /// The default ceiling on the number of membership edges a tenant's groups may
    /// hold, applied when <see cref="MaxMembershipEdges"/> is <c>null</c>.
    /// </summary>
    public const long DefaultMaxMembershipEdges = 10_000;

    /// <summary>
    /// The default ceiling on the number of entries in a tenant's member set,
    /// applied when <see cref="MaxMemberSubjects"/> is <c>null</c>.
    /// </summary>
    public const long DefaultMaxMemberSubjects = 5_000;

    /// <summary>
    /// The default ceiling on the number of tenant-tier authorization rules a
    /// tenant may own, applied when <see cref="MaxTenantRules"/> is <c>null</c>.
    /// </summary>
    public const long DefaultMaxTenantRules = 1_000;

    /// <summary>
    /// The maximum number of tenant groups (<c>t/{tenant}/{name}</c>) the tenant may
    /// own, or <c>null</c> for the default of <see cref="DefaultMaxGroups"/>. Read
    /// the cap in force through <see cref="EffectiveMaxGroups"/>.
    /// </summary>
    /// <remarks>
    /// Unlike the resource dimensions above, a <c>null</c> access cap does not mean
    /// unbounded: tenant groups live in shared membership trees, so the cap protects
    /// every other tenant and always applies. It is enforced by the delegated
    /// access-administration facades, does not take <see cref="BurstPercent"/>, and
    /// plays no part in <see cref="IsUnbounded"/>.
    /// </remarks>
    [Id(6)]
    public long? MaxGroups { get; init; }

    /// <summary>
    /// The maximum number of membership edges the tenant's groups may hold in total,
    /// or <c>null</c> for the default of <see cref="DefaultMaxMembershipEdges"/>.
    /// Read the cap in force through <see cref="EffectiveMaxMembershipEdges"/>.
    /// </summary>
    /// <remarks>A <c>null</c> access cap means the default, never unbounded; see <see cref="MaxGroups"/>.</remarks>
    [Id(7)]
    public long? MaxMembershipEdges { get; init; }

    /// <summary>
    /// The maximum number of entries in the tenant's member set, or <c>null</c> for
    /// the default of <see cref="DefaultMaxMemberSubjects"/>. Read the cap in force
    /// through <see cref="EffectiveMaxMemberSubjects"/>.
    /// </summary>
    /// <remarks>A <c>null</c> access cap means the default, never unbounded; see <see cref="MaxGroups"/>.</remarks>
    [Id(8)]
    public long? MaxMemberSubjects { get; init; }

    /// <summary>
    /// The maximum number of tenant-tier authorization rules the tenant may own, or
    /// <c>null</c> for the default of <see cref="DefaultMaxTenantRules"/>. Read the
    /// cap in force through <see cref="EffectiveMaxTenantRules"/>.
    /// </summary>
    /// <remarks>A <c>null</c> access cap means the default, never unbounded; see <see cref="MaxGroups"/>.</remarks>
    [Id(9)]
    public long? MaxTenantRules { get; init; }

    /// <summary>The tenant-group cap in force: <see cref="MaxGroups"/>, or <see cref="DefaultMaxGroups"/> when it is <c>null</c>.</summary>
    public long EffectiveMaxGroups => MaxGroups ?? DefaultMaxGroups;

    /// <summary>The membership-edge cap in force: <see cref="MaxMembershipEdges"/>, or <see cref="DefaultMaxMembershipEdges"/> when it is <c>null</c>.</summary>
    public long EffectiveMaxMembershipEdges => MaxMembershipEdges ?? DefaultMaxMembershipEdges;

    /// <summary>The member-set cap in force: <see cref="MaxMemberSubjects"/>, or <see cref="DefaultMaxMemberSubjects"/> when it is <c>null</c>.</summary>
    public long EffectiveMaxMemberSubjects => MaxMemberSubjects ?? DefaultMaxMemberSubjects;

    /// <summary>The tenant-rule cap in force: <see cref="MaxTenantRules"/>, or <see cref="DefaultMaxTenantRules"/> when it is <c>null</c>.</summary>
    public long EffectiveMaxTenantRules => MaxTenantRules ?? DefaultMaxTenantRules;

    /// <summary>
    /// The unbounded quota: every dimension <c>null</c> and no burst. This is the
    /// quota of the reserved <see cref="TenantId.Default"/> tenant. Its access caps
    /// read as their defaults, which never apply to the default tenant because it
    /// accepts no tenant groups, members or tenant rules.
    /// </summary>
    public static TenantQuotas Unbounded => default;

    /// <summary>
    /// <c>true</c> when every resource dimension is unbounded (<c>null</c>),
    /// regardless of <see cref="BurstPercent"/>. The access caps
    /// (<see cref="MaxGroups"/>, <see cref="MaxMembershipEdges"/>,
    /// <see cref="MaxMemberSubjects"/>, <see cref="MaxTenantRules"/>) are not
    /// resource dimensions and play no part.
    /// </summary>
    public bool IsUnbounded =>
        MaxBytes is null
        && MaxKeys is null
        && MaxMemoryBytes is null
        && MaxTreeCount is null
        && MaxOpsPerSecond is null;
}
