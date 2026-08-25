namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantRateProvider"/>: sizes each tenant's cluster-wide
/// request rate from the durable tenant registry. It yields a
/// <see cref="TenantRateSpec"/> for every active tenant whose quota carries a
/// positive <see cref="TenantQuotas.MaxOpsPerSecond"/>; tenants that are inactive
/// or unbounded are omitted so the limiter leaves them inert.
/// </summary>
/// <remarks>
/// The registry is the same store the compiled tenant policy is compiled from, so
/// reading it directly yields the identical configured rate without depending on
/// the compiled-policy snapshot surface (which does not expose quotas). It is read
/// at lease cadence only, never on the per-op hot path.
/// </remarks>
internal sealed class RegistryTenantRateProvider : ITenantRateProvider
{
    private readonly ITenantRegistry _registry;

    /// <summary>Initializes the provider over the tenant registry.</summary>
    /// <param name="registry">The durable tenant registry. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="registry"/> is <c>null</c>.</exception>
    public RegistryTenantRateProvider(ITenantRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        _registry = registry;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!record.IsActive)
            {
                continue;
            }

            var rate = record.Quotas.MaxOpsPerSecond;
            if (rate is not > 0)
            {
                continue;
            }

            yield return new TenantRateSpec(record.Id, rate.Value, record.Quotas.BurstPercent);
        }
    }
}
