namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The read-only usage-against-quota report for one tenant returned by
/// <see cref="ILatticeTenantQuotaUsage.GetQuotaUsageAsync"/>: the current
/// consumption alongside the ceiling on every dimension the quota model defines,
/// the burst headroom, and the accrued metered overage, qualified by the
/// enforcement scope the reading was taken under. It is what turns a quota panel
/// from a list of ceilings into a set of bars.
/// </summary>
/// <remarks>
/// <para>
/// It is only ever produced for a tenant the caller is authorized to read; an
/// absent tenant and a tenant the caller may not see are unified into the same
/// fail-closed "not found" outcome at the facade, so this report never confirms
/// the existence of a tenant the caller has no right to observe.
/// </para>
/// <para>
/// Every dimension distinguishes <em>unbounded</em> (no ceiling) from a ceiling of
/// <c>0</c>, and <em>unmeasured</em> from a usage of <c>0</c> - see
/// <see cref="TenantQuotaDimensionUsage"/>. The five dimensions are carried inline
/// as value types rather than in a collection, so a report allocates once, not
/// once per dimension.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantQuotaUsageReport)]
[Immutable]
public sealed record TenantQuotaUsageReport
{
    /// <summary>The tenant id this report describes.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// <see langword="true"/> when this is the reserved legacy-adoption default
    /// tenant (<see cref="Orleans.Lattice.TenantId.DefaultId"/>), which is always
    /// unbounded on every dimension.
    /// </summary>
    [Id(1)] public bool IsDefault { get; init; }

    /// <summary>
    /// The aggregate the usage figures were read against - a converged
    /// cross-cluster sum or this cluster's local view. A consumer must present
    /// this alongside the figures rather than implying a global total.
    /// </summary>
    [Id(2)] public TenantQuotaEnforcementScope EnforcementScope { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tenancy engine had no usage view for the
    /// tenant at read time (it has never been sampled, or the warm index has not
    /// yet compiled it). The quota ceilings are still authoritative; every usage
    /// figure is reported unmeasured rather than as a fabricated <c>0</c>.
    /// </summary>
    [Id(3)] public bool HasUsage { get; init; }

    /// <summary>Stored value bytes: consumption against <c>MaxBytes</c>.</summary>
    [Id(4)] public TenantQuotaDimensionUsage Bytes { get; init; }

    /// <summary>Live key count: consumption against <c>MaxKeys</c>.</summary>
    [Id(5)] public TenantQuotaDimensionUsage Keys { get; init; }

    /// <summary>Resident memory bytes: consumption against <c>MaxMemoryBytes</c>.</summary>
    [Id(6)] public TenantQuotaDimensionUsage MemoryBytes { get; init; }

    /// <summary>Owned tree count: consumption against <c>MaxTreeCount</c>.</summary>
    [Id(7)] public TenantQuotaDimensionUsage TreeCount { get; init; }

    /// <summary>
    /// Sustained operations per second: the <c>MaxOpsPerSecond</c> ceiling. The
    /// tenancy engine's usage accounting does not sample an operation rate, so
    /// this dimension always reports an unmeasured
    /// <see cref="TenantQuotaDimensionUsage.Usage"/>.
    /// </summary>
    [Id(8)] public TenantQuotaDimensionUsage OpsPerSecond { get; init; }

    /// <summary>
    /// The tenant's transient burst headroom above the bounded ceilings, as a
    /// percentage (<c>0</c> for none). Already folded into each dimension's
    /// <see cref="TenantQuotaDimensionUsage.BurstLimit"/>; surfaced here so a
    /// consumer can label the headroom band.
    /// </summary>
    [Id(9)] public int BurstPercent { get; init; }

    /// <summary>
    /// The tenant's declared quotas for this same reading, so a consumer that only
    /// needs the ceilings does not have to reassemble them from the per-dimension
    /// figures. They are the ceilings the reported usage and overage were measured
    /// against, so the report is always internally consistent; a quota edit that
    /// has not yet reached the warm index shows on the authoritative
    /// <see cref="TenantStatusReport.Quotas"/> first.
    /// </summary>
    [Id(10)] public TenantQuotasDescriptor Quotas { get; init; }
}
