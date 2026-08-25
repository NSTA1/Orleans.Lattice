namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Supplies the cluster-wide request rate of every rate-limited tenant to the
/// budget coordinator, sized from tenant policy. Consulted at lease cadence only,
/// never on the per-op hot path.
/// </summary>
internal interface ITenantRateProvider
{
    /// <summary>
    /// Enumerates a <see cref="TenantRateSpec"/> for every tenant that currently
    /// has a configured, positive operations-per-second ceiling. Tenants with no
    /// configured rate are omitted so the limiter leaves them inert.
    /// </summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of per-tenant cluster rates.</returns>
    IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(CancellationToken cancellationToken = default);
}
