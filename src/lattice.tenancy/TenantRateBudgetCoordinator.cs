using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The low-frequency budget coordinator: once per lease interval it divides every
/// rate-limited tenant's cluster-wide rate into the share this silo may enforce
/// and (re)sizes the silo-local token buckets accordingly. It runs O(silos) work
/// at lease cadence and is never on the per-op hot path.
/// </summary>
/// <remarks>
/// <para>
/// Apportionment defaults to demand-proportional leasing: each cycle reads this
/// silo's recent demand for a tenant, exchanges it for the cluster-wide total via
/// <see cref="ITenantClusterDemandExchange"/>, and leases a proportional share.
/// When no cluster-wide aggregate is available the exchange returns <c>null</c> and
/// the coordinator falls back to bounded static-even
/// (<c>clusterRate / liveSiloCount</c>), which is also the first-cycle bootstrap
/// (zero demand) and the configured behaviour under
/// <see cref="TenantRateApportionmentStrategy.StaticEven"/>.
/// </para>
/// <para>
/// The share is floored at one operation per second so the GCRA emission interval
/// is well defined; in the pathological case of a cluster rate below the silo
/// count this admits a small, bounded overshoot rather than a zero (never-admit)
/// bucket.
/// </para>
/// </remarks>
internal sealed class TenantRateBudgetCoordinator
{
    private readonly ITenantRateProvider _rateProvider;
    private readonly ILiveSiloCountProvider _siloCountProvider;
    private readonly ITenantClusterDemandExchange _demandExchange;
    private readonly SiloLocalTenantRateLimiter _limiter;
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<LatticeTenantRateLimiterOptions> _options;

    /// <summary>Initializes the coordinator over its collaborators.</summary>
    /// <param name="rateProvider">Supplies each tenant's cluster-wide rate. Must not be <c>null</c>.</param>
    /// <param name="siloCountProvider">Supplies the live silo count. Must not be <c>null</c>.</param>
    /// <param name="demandExchange">Exchanges per-silo demand for the cluster total. Must not be <c>null</c>.</param>
    /// <param name="limiter">The silo-local limiter whose buckets are (re)sized. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The shared timestamp source (for its frequency). Must not be <c>null</c>.</param>
    /// <param name="options">The limiter options monitor. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRateBudgetCoordinator(
        ITenantRateProvider rateProvider,
        ILiveSiloCountProvider siloCountProvider,
        ITenantClusterDemandExchange demandExchange,
        SiloLocalTenantRateLimiter limiter,
        TimeProvider timeProvider,
        IOptionsMonitor<LatticeTenantRateLimiterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(rateProvider);
        ArgumentNullException.ThrowIfNull(siloCountProvider);
        ArgumentNullException.ThrowIfNull(demandExchange);
        ArgumentNullException.ThrowIfNull(limiter);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);

        _rateProvider = rateProvider;
        _siloCountProvider = siloCountProvider;
        _demandExchange = demandExchange;
        _limiter = limiter;
        _timeProvider = timeProvider;
        _options = options;
    }

    /// <summary>
    /// Runs one lease cycle: re-apportions every rate-limited tenant's cluster rate
    /// into this silo's share, resizes the token buckets, and prunes the buckets of
    /// tenants that no longer carry a configured rate.
    /// </summary>
    /// <param name="cancellationToken">Cancels the cycle.</param>
    /// <returns>A task that completes when the cycle has been applied.</returns>
    public async Task RunLeaseCycleAsync(CancellationToken cancellationToken = default)
    {
        var options = _options.CurrentValue;
        var siloCount = await _siloCountProvider.GetLiveSiloCountAsync(cancellationToken).ConfigureAwait(false);
        if (siloCount < 1)
        {
            siloCount = 1;
        }

        var frequency = _timeProvider.TimestampFrequency;
        var configured = new HashSet<string>(StringComparer.Ordinal);

        await foreach (var spec in _rateProvider.GetConfiguredRatesAsync(cancellationToken).ConfigureAwait(false))
        {
            var tenantKey = spec.Tenant.Value;
            if (tenantKey is null)
            {
                continue;
            }

            configured.Add(tenantKey);

            var localDemand = _limiter.ReadAndResetDemand(spec.Tenant);

            long share;
            if (options.Apportionment == TenantRateApportionmentStrategy.StaticEven)
            {
                share = TenantBudgetApportionment.StaticEvenShare(spec.OpsPerSecond, siloCount);
            }
            else
            {
                var total = await _demandExchange
                    .ExchangeAsync(spec.Tenant, localDemand, cancellationToken)
                    .ConfigureAwait(false);

                share = total is { } totalDemand
                    ? TenantBudgetApportionment.DemandProportionalShare(
                        spec.OpsPerSecond,
                        siloCount,
                        localDemand,
                        totalDemand,
                        options.DemandReserveFraction)
                    : TenantBudgetApportionment.StaticEvenShare(spec.OpsPerSecond, siloCount);
            }

            if (share < 1)
            {
                // A share below one op/sec would make the emission interval ill
                // defined; floor to one, accepting a small bounded overshoot only in
                // the pathological clusterRate < siloCount case.
                share = 1;
            }

            var emission = TenantTokenBucket.ComputeEmissionIntervalTicks(share, frequency);
            var tolerance = TenantTokenBucket.ComputeBurstToleranceTicks(share, spec.BurstPercent, frequency);
            _limiter.Configure(spec.Tenant, emission, tolerance);
        }

        _limiter.RetainOnly(configured);
    }
}
