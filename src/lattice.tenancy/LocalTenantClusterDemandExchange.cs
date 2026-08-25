namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default in-process <see cref="ITenantClusterDemandExchange"/>: it has no
/// cross-silo view, so <see cref="ExchangeAsync"/> always returns <c>null</c>.
/// This drives the budget coordinator into bounded static-even apportionment - the
/// zero-coordination fallback and pre-lease bootstrap - so a deployment with no
/// cluster-wide demand aggregator still enforces a correct, cluster-bounded rate.
/// </summary>
internal sealed class LocalTenantClusterDemandExchange : ITenantClusterDemandExchange
{
    /// <inheritdoc />
    public ValueTask<long?> ExchangeAsync(TenantId tenant, long localDemand, CancellationToken cancellationToken = default) =>
        new((long?)null);
}
