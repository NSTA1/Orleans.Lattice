namespace Orleans.Lattice.Tenancy;

/// <summary>
/// One tenant's observability projection: its identity joined with the warm usage
/// aggregate, the declared quotas, and the converged durable metered overage.
/// The shared unit produced by <c>TenantObservabilitySource</c> for both the
/// per-tenant observable gauges and the fail-closed
/// <see cref="ITenantObservabilityView"/> read surface.
/// </summary>
/// <remarks>
/// A transient, derived read result assembled from the warm per-tenant usage
/// index (quotas and the global usage fold) and the durable overage billing seam
/// (<see cref="ITenantOverageBilling"/>). It is never persisted and never crosses
/// a grain boundary as a payload, so it carries no Orleans serialization
/// attributes. A <c>readonly record struct</c> so a snapshot is copied by value
/// and the live burst/overage signal
/// (<see cref="InstantaneousOverage"/>) is derived on demand with no allocation.
/// </remarks>
public readonly record struct TenantObservabilitySnapshot
{
    /// <summary>Initializes a new <see cref="TenantObservabilitySnapshot"/>.</summary>
    /// <param name="tenant">The tenant the projection is for.</param>
    /// <param name="usage">The tenant's global usage fold (the cross-cluster sum).</param>
    /// <param name="quotas">The tenant's declared quotas.</param>
    /// <param name="meteredOverage">
    /// The tenant's converged, durable metered overage from
    /// <see cref="ITenantOverageBilling"/>, or
    /// <see cref="TenantOverageSample.Empty"/> when the tenant has never been in
    /// overage.
    /// </param>
    public TenantObservabilitySnapshot(
        TenantId tenant,
        LocalUsageSample usage,
        TenantQuotas quotas,
        TenantOverageSample meteredOverage)
    {
        Tenant = tenant;
        Usage = usage;
        Quotas = quotas;
        MeteredOverage = meteredOverage;
    }

    /// <summary>The tenant the projection is for.</summary>
    public TenantId Tenant { get; init; }

    /// <summary>The tenant's global usage fold: the cross-cluster sum of its per-cluster slots.</summary>
    public LocalUsageSample Usage { get; init; }

    /// <summary>The tenant's declared quotas (the steady-state ceilings and burst headroom).</summary>
    public TenantQuotas Quotas { get; init; }

    /// <summary>
    /// The tenant's converged, durable metered overage - the accrued Riemann-sum
    /// billing overage read from <see cref="ITenantOverageBilling"/>.
    /// </summary>
    public TenantOverageSample MeteredOverage { get; init; }

    /// <summary>
    /// The live burst/overage signal: the amount by which the current
    /// <see cref="Usage"/> exceeds the steady-state ceilings in
    /// <see cref="Quotas"/>, derived on demand (a branch-only, zero-allocation
    /// projection). Distinct from <see cref="MeteredOverage"/>, which is the
    /// durable accrued billing total.
    /// </summary>
    public TenantOverageSample InstantaneousOverage => TenantOverageSample.Above(Usage, Quotas);
}
