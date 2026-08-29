using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Pure projections from the tenancy engine's <see cref="TenantUsageReading"/>
/// onto the transport-agnostic <see cref="TenantQuotaUsageReport"/> control-API
/// DTO. The control-API contract does not reference the tenancy engine, so this
/// facade - which references both - is the single seam that translates between
/// them, exactly as <see cref="TenantQuotasMapping"/> does for the quota
/// ceilings.
/// </summary>
/// <remarks>
/// <para>
/// <b>One coherent reading.</b> When a usage view exists, every figure in the
/// report - usage, ceilings, burst ceilings, live overage, accrued overage - is
/// taken from that single <see cref="TenantObservabilitySnapshot"/>, so the
/// numbers always agree with each other. The report's live overage is exactly
/// <see cref="TenantObservabilitySnapshot.InstantaneousOverage"/>, and its burst
/// ceiling is exactly the effective ceiling the tenancy engine's quota evaluator
/// admits against, so the control API can never disagree with enforcement.
/// </para>
/// <para>
/// <b>Unbounded and unmeasured are propagated, never flattened.</b> A
/// <see langword="null"/> ceiling stays <see langword="null"/> (rather than
/// collapsing to <c>0</c>), and a dimension the engine does not sample reports a
/// <see langword="null"/> usage.
/// </para>
/// <para>
/// Allocation: the five per-dimension figures are value types written inline into
/// the report, so a projection allocates only the report record itself.
/// </para>
/// </remarks>
internal static class TenantQuotaUsageMapping
{
    /// <summary>
    /// Projects a tenant's usage reading onto its control-API report. Every figure
    /// comes from the single supplied reading, so the numbers always agree.
    /// </summary>
    /// <param name="tenant">The tenant the report describes.</param>
    /// <param name="reading">The tenancy engine's reading for that tenant.</param>
    /// <returns>The equivalent control-API report.</returns>
    public static TenantQuotaUsageReport ToReport(TenantId tenant, TenantUsageReading reading)
    {
        var snapshot = reading.Snapshot;
        var quotas = snapshot.Quotas;
        var usage = snapshot.Usage;
        var live = snapshot.InstantaneousOverage;
        var metered = snapshot.MeteredOverage;
        var burst = quotas.BurstPercent;

        return new TenantQuotaUsageReport
        {
            TenantId = tenant.Value,
            IsDefault = tenant.IsDefault,
            EnforcementScope = ToScope(reading.Scope),
            HasUsage = true,
            Bytes = Measured(usage.Bytes, quotas.MaxBytes, burst, live.Bytes, metered.Bytes),
            Keys = Measured(usage.Keys, quotas.MaxKeys, burst, live.Keys, metered.Keys),
            MemoryBytes = Measured(usage.MemoryBytes, quotas.MaxMemoryBytes, burst, live.MemoryBytes, metered.MemoryBytes),
            TreeCount = Measured(usage.TreeCount, quotas.MaxTreeCount, burst, live.TreeCount, metered.TreeCount),

            // The engine's usage accounting samples stored bytes, live keys,
            // resident memory, and owned trees - never a sustained operation rate
            // - so the ops dimension reports its ceiling with no usage rather than
            // a fabricated zero that would render as an empty bar.
            OpsPerSecond = Unmeasured(quotas.MaxOpsPerSecond, burst),
            BurstPercent = burst,
            Quotas = TenantQuotasMapping.ToDescriptor(quotas),
        };
    }

    /// <summary>
    /// Projects a tenant that has no usage view yet onto a report carrying its
    /// authoritative declared ceilings with every dimension left <em>unmeasured</em>,
    /// rather than fabricating a usage of <c>0</c> that would render as an empty
    /// bar for a tenant whose consumption is simply unknown.
    /// </summary>
    /// <param name="tenant">The tenant the report describes.</param>
    /// <param name="declared">The tenant record's declared quotas.</param>
    /// <param name="scope">The enforcement scope governing the tenant.</param>
    /// <returns>The unmeasured control-API report.</returns>
    public static TenantQuotaUsageReport ToUnmeasuredReport(
        TenantId tenant, TenantQuotas declared, TenantEnforcementScope scope)
    {
        var burst = declared.BurstPercent;

        return new TenantQuotaUsageReport
        {
            TenantId = tenant.Value,
            IsDefault = tenant.IsDefault,
            EnforcementScope = ToScope(scope),
            HasUsage = false,
            Bytes = Unmeasured(declared.MaxBytes, burst),
            Keys = Unmeasured(declared.MaxKeys, burst),
            MemoryBytes = Unmeasured(declared.MaxMemoryBytes, burst),
            TreeCount = Unmeasured(declared.MaxTreeCount, burst),
            OpsPerSecond = Unmeasured(declared.MaxOpsPerSecond, burst),
            BurstPercent = burst,
            Quotas = TenantQuotasMapping.ToDescriptor(declared),
        };
    }

    /// <summary>Projects the engine enforcement scope onto its control-API enum.</summary>
    /// <param name="scope">The engine enforcement scope.</param>
    /// <returns>The equivalent control-API scope.</returns>
    public static TenantQuotaEnforcementScope ToScope(TenantEnforcementScope scope) => scope switch
    {
        TenantEnforcementScope.PerCluster => TenantQuotaEnforcementScope.PerCluster,
        _ => TenantQuotaEnforcementScope.GlobalConverged,
    };

    private static TenantQuotaDimensionUsage Measured(
        long usage, long? limit, int burstPercent, long overage, long meteredOverage) => new()
        {
            Usage = usage,
            Limit = limit,
            BurstLimit = BurstCeiling(limit, burstPercent),
            Overage = overage,
            MeteredOverage = meteredOverage,
        };

    private static TenantQuotaDimensionUsage Unmeasured(long? limit, int burstPercent) => new()
    {
        Usage = null,
        Limit = limit,
        BurstLimit = BurstCeiling(limit, burstPercent),
    };

    /// <summary>
    /// The burst-adjusted ceiling admission control engages at, computed exactly
    /// as the tenancy engine's quota evaluator computes it (divide before multiply
    /// so a large ceiling cannot overflow). An unbounded dimension has no burst
    /// ceiling either, so it stays <c>null</c> rather than becoming <c>0</c>.
    /// </summary>
    private static long? BurstCeiling(long? limit, int burstPercent) =>
        limit is not { } ceiling
            ? null
            : burstPercent > 0 ? ceiling + ceiling / 100L * burstPercent : ceiling;
}
