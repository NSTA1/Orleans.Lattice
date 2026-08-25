namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The public, billing-ready read seam over per-tenant quota overage. A billing
/// consumer polls it to read a tenant's durable, converged metered overage - the
/// cross-cluster sum-fold of the grow-only overage counters - as a first-class
/// aggregate, not a transient signal. Reads are the low-frequency billing side of
/// the overage layer; nothing on the warm admission path consults this seam.
/// </summary>
/// <remarks>
/// The metered overage is a monotone Riemann sum accrued by the metering cadence,
/// so a poll returns a value that only ever grows for a tenant in sustained overage.
/// Because the underlying counters are grow-only CRDTs, repeated polls converge and
/// a slow cluster's late-arriving component never regresses the aggregate.
/// </remarks>
public interface ITenantOverageBilling
{
    /// <summary>
    /// Reads a tenant's converged cross-cluster metered overage, or
    /// <see cref="TenantOverageSample.Empty"/> when the tenant has never been in
    /// overage.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's global metered overage.</returns>
    Task<TenantOverageSample> GetMeteredOverageAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates every tenant that has metered any overage, each paired with its
    /// converged cross-cluster metered overage. A tenant that has never breached its
    /// quota is absent from the stream.
    /// </summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of per-tenant metered overage.</returns>
    IAsyncEnumerable<TenantMeteredOverage> ListMeteredOverageAsync(CancellationToken cancellationToken = default);
}
