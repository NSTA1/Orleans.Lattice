namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The warm, in-memory admission inputs for a single tenant: its resolved
/// <see cref="TenantQuotas"/> plus the two usage aggregates the enforcement scope
/// chooses between - the cross-cluster <see cref="GlobalUsage"/> fold (summed over
/// every cluster's published slot) and this cluster's <see cref="LocalUsage"/>
/// slot only. Produced by <see cref="CompiledTenantUsage"/> and read on the
/// allocation-free admission path.
/// </summary>
/// <remarks>
/// A <see cref="readonly"/> struct so a warm admission read copies a few machine
/// words off the frozen snapshot rather than dereferencing and touching the heap.
/// It is in-process state only and never serialized.
/// </remarks>
internal readonly struct TenantUsageView
{
    /// <summary>Initializes a new <see cref="TenantUsageView"/>.</summary>
    /// <param name="quotas">The tenant's resolved quotas.</param>
    /// <param name="globalUsage">The cross-cluster usage fold (sum over all cluster slots).</param>
    /// <param name="localUsage">This cluster's own usage slot.</param>
    public TenantUsageView(TenantQuotas quotas, LocalUsageSample globalUsage, LocalUsageSample localUsage)
    {
        Quotas = quotas;
        GlobalUsage = globalUsage;
        LocalUsage = localUsage;
    }

    /// <summary>The tenant's resolved quotas.</summary>
    public TenantQuotas Quotas { get; }

    /// <summary>The cross-cluster usage fold: the sum over every cluster's published slot.</summary>
    public LocalUsageSample GlobalUsage { get; }

    /// <summary>This cluster's own usage slot.</summary>
    public LocalUsageSample LocalUsage { get; }

    /// <summary>
    /// The usage aggregate to admit against under <paramref name="scope"/>:
    /// <see cref="GlobalUsage"/> for <see cref="TenantEnforcementScope.GlobalConverged"/>,
    /// <see cref="LocalUsage"/> for <see cref="TenantEnforcementScope.PerCluster"/>.
    /// </summary>
    /// <param name="scope">The enforcement scope selecting the aggregate.</param>
    /// <returns>The usage sample to compare against the quota.</returns>
    public LocalUsageSample UsageFor(TenantEnforcementScope scope) =>
        scope == TenantEnforcementScope.PerCluster ? LocalUsage : GlobalUsage;
}
