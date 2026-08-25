namespace Orleans.Lattice.Tenancy;

/// <summary>
/// How the budget coordinator divides a tenant's cluster-wide request rate across
/// the currently-live silos.
/// </summary>
public enum TenantRateApportionmentStrategy
{
    /// <summary>
    /// Demand-proportional leasing: each silo periodically leases a share of the
    /// cluster rate proportional to its recent consumption, so idle budget is
    /// redistributed to busy silos while the sum stays bounded by the cluster rate.
    /// This is the default. When no cluster-wide demand aggregate is available it
    /// degrades to <see cref="StaticEven"/>, which is also the pre-lease bootstrap.
    /// </summary>
    Demand = 0,

    /// <summary>
    /// Static-even apportionment: every live silo receives an equal
    /// <c>clusterRate / liveSiloCount</c> share. Zero-coordination and always
    /// cluster-bounded; transient overshoot is bounded by silo skew only.
    /// </summary>
    StaticEven = 1,
}
