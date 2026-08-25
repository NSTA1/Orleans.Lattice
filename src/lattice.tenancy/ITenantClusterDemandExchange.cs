namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Exchanges this silo's recent per-tenant demand for the cluster-wide total, so
/// the budget coordinator can lease each silo a share proportional to its
/// consumption. Consulted at lease cadence only (O(silos)), never on the per-op
/// hot path.
/// </summary>
/// <remarks>
/// The default in-process implementation has no cross-silo view and returns
/// <c>null</c>, which makes the coordinator fall back to bounded static-even
/// apportionment (the zero-coordination default and pre-lease bootstrap). A future
/// cluster-wide aggregator can supply a real total to engage demand-proportional
/// leasing without touching the hot path.
/// </remarks>
internal interface ITenantClusterDemandExchange
{
    /// <summary>
    /// Publishes this silo's recent demand for one tenant and returns the
    /// cluster-wide total demand for that tenant across all live silos, or
    /// <c>null</c> when no cross-silo aggregate is available.
    /// </summary>
    /// <param name="tenant">The tenant whose demand is being exchanged.</param>
    /// <param name="localDemand">This silo's admitted-operation count for the tenant since the last lease cycle.</param>
    /// <param name="cancellationToken">Cancels the exchange.</param>
    /// <returns>The cluster-wide total demand for the tenant, or <c>null</c> when unavailable.</returns>
    ValueTask<long?> ExchangeAsync(TenantId tenant, long localDemand, CancellationToken cancellationToken = default);
}
