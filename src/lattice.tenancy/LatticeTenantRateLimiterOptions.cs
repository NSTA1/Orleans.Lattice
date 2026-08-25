namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options for the per-silo tenant request-rate limiter and its budget
/// coordinator: how often each silo re-leases its share of every tenant's
/// cluster-wide rate, which apportionment strategy the coordinator uses, and the
/// reserve floor for demand-proportional leasing. Resolved through the standard
/// options system and configured via <c>AddLatticeTenancy(...)</c> or
/// <c>ConfigureLatticeTenancy(...)</c>.
/// </summary>
/// <remarks>
/// These options never touch the per-op hot path; they govern only the
/// low-frequency coordinator (O(silos) at lease cadence). The limiter enforces
/// silo-local token buckets regardless of these settings, so a misconfiguration
/// affects only how the cluster rate is split, never whether enforcement is
/// lock-free.
/// </remarks>
public sealed class LatticeTenantRateLimiterOptions
{
    /// <summary>
    /// The default lease interval: how often the budget coordinator re-apportions
    /// each tenant's cluster rate across the live silos.
    /// </summary>
    public static readonly TimeSpan DefaultLeaseInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the budget coordinator re-leases each silo's share. Must be
    /// strictly positive; defaults to <see cref="DefaultLeaseInterval"/>. A longer
    /// interval lowers coordination cost but widens the transient overshoot bound
    /// (lease interval times cluster rate).
    /// </summary>
    public TimeSpan LeaseInterval { get; set; } = DefaultLeaseInterval;

    /// <summary>
    /// The apportionment strategy. Defaults to
    /// <see cref="TenantRateApportionmentStrategy.Demand"/> (demand-proportional
    /// leasing), which degrades to static-even when no cluster-wide demand
    /// aggregate is available.
    /// </summary>
    public TenantRateApportionmentStrategy Apportionment { get; set; } = TenantRateApportionmentStrategy.Demand;

    /// <summary>
    /// The fraction of each tenant's cluster rate that demand-proportional leasing
    /// reserves and splits evenly across silos, guaranteeing an idle silo a
    /// non-zero floor so it can never be starved out of building demand. In
    /// <c>[0, 1]</c>; defaults to <c>0.2</c>. Ignored under static-even
    /// apportionment.
    /// </summary>
    public double DemandReserveFraction { get; set; } = 0.2;
}
