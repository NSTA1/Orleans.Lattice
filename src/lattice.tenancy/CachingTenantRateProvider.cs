using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A time-to-live cache in front of an inner <see cref="ITenantRateProvider"/>, so
/// the budget coordinator re-apportions token buckets on its own (frequent) lease
/// cadence without re-reading the durable tenant registry on every tick.
/// </summary>
/// <remarks>
/// <para>
/// The inner provider streams the whole tenant registry, which is a full scan of a
/// sharded tree. Running that once per lease tick coupled a cheap, purely local
/// re-apportionment to an expensive distributed read: when the scan took longer
/// than the tick, the loop ran the scan back-to-back at a 100% duty cycle and the
/// registry never drained. Configured rates change at administrative cadence, so a
/// snapshot bounded by <see cref="LatticeTenantRateLimiterOptions.RateSnapshotTtl"/>
/// is the correct freshness for them.
/// </para>
/// <para>
/// <b>Stale-if-error.</b> A refresh that faults while a previous snapshot is
/// resident serves the stale snapshot and leaves it eligible for immediate
/// re-attempt. Propagating instead would let one slow read prune every tenant's
/// bucket through the coordinator's retain-only step, which is a worse failure than
/// briefly apportioning from slightly stale rates. With no snapshot at all (the
/// very first cycle) the fault propagates so the loop's backoff engages.
/// </para>
/// </remarks>
internal sealed class CachingTenantRateProvider : ITenantRateProvider
{
    private readonly ITenantRateProvider _inner;
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<LatticeTenantRateLimiterOptions> _options;
    private readonly ILogger<CachingTenantRateProvider> _logger;

    // Single-flight guard: the coordinator is sequential today, but the snapshot is
    // a silo singleton and a second consumer must not be able to stack whole-tree
    // scans on top of an already-running one.
    private readonly SemaphoreSlim _refreshGate = new(1, 1);

    private IReadOnlyList<TenantRateSpec>? _snapshot;
    private long _snapshotTakenAt;

    /// <summary>Initializes the cache over the inner provider and its schedule inputs.</summary>
    /// <param name="inner">The provider that actually reads the tenant registry. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The timestamp source backing the time-to-live. Must not be <c>null</c>.</param>
    /// <param name="options">The limiter options monitor (for the snapshot TTL). Must not be <c>null</c>.</param>
    /// <param name="logger">The logger for refresh failures. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public CachingTenantRateProvider(
        ITenantRateProvider inner,
        TimeProvider timeProvider,
        IOptionsMonitor<LatticeTenantRateLimiterOptions> options,
        ILogger<CachingTenantRateProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _inner = inner;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var specs = await GetSnapshotAsync(cancellationToken).ConfigureAwait(false);

        // Indexed replay over the materialised list: no enumerator allocation and no
        // registry traffic for a cache hit.
        for (var i = 0; i < specs.Count; i++)
        {
            yield return specs[i];
        }
    }

    private async ValueTask<IReadOnlyList<TenantRateSpec>> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        var ttl = ResolveTtl();
        if (TryReadFresh(ttl) is { } fresh)
        {
            return fresh;
        }

        await _refreshGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Re-check after the wait: a concurrent caller may have refreshed while
            // this one was queued, in which case its scan is redundant.
            if (TryReadFresh(ttl) is { } refreshed)
            {
                return refreshed;
            }

            var specs = new List<TenantRateSpec>();
            try
            {
                await foreach (var spec in _inner.GetConfiguredRatesAsync(cancellationToken).ConfigureAwait(false))
                {
                    specs.Add(spec);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException && _snapshot is { } stale)
            {
                _logger.LogWarning(
                    ex,
                    "Refreshing the tenant rate snapshot failed; apportioning from the previous snapshot of "
                    + "{TenantCount} tenant(s) and re-attempting on the next lease cycle.",
                    stale.Count);
                return stale;
            }

            _snapshot = specs;
            _snapshotTakenAt = _timeProvider.GetTimestamp();
            return specs;
        }
        finally
        {
            _ = _refreshGate.Release();
        }
    }

    private IReadOnlyList<TenantRateSpec>? TryReadFresh(TimeSpan ttl)
    {
        var snapshot = _snapshot;
        if (snapshot is null)
        {
            return null;
        }

        var age = _timeProvider.GetElapsedTime(_snapshotTakenAt);
        return age < ttl ? snapshot : null;
    }

    private TimeSpan ResolveTtl()
    {
        var ttl = _options.CurrentValue.RateSnapshotTtl;
        return ttl > TimeSpan.Zero ? ttl : LatticeTenantRateLimiterOptions.DefaultRateSnapshotTtl;
    }
}
