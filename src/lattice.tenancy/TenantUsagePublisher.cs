using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Rolls a tenant's per-tree usage samples up into this cluster's local
/// <see cref="LocalUsageSample"/> and publishes it into the tenant's per-cluster
/// usage slot, gated by a hysteresis band so a negligible movement does not churn
/// the registry-backed usage tree. This is the low-frequency, cadence-driven side
/// of the accounting layer (the caller supplies both the cadence and a monotonic
/// stamp), so it may allocate; it is deliberately separate from the warm,
/// allocation-free admission read.
/// </summary>
/// <remarks>
/// The publisher writes only <em>this</em> cluster's slot (keyed by
/// <see cref="ClusterOptions.ClusterId"/>); the store's CRDT merge converges it
/// with every other cluster's slot. It remembers the last sample it published per
/// tenant so <see cref="UsagePublishHysteresis"/> can compare a fresh roll-up
/// against it and suppress sub-threshold movements. A changed sample is refreshed
/// after at most five minutes of supplied cadence-clock time, so damping cannot
/// permanently hide a small quota crossing.
/// </remarks>
internal sealed class TenantUsagePublisher
{
    private readonly ITenantUsageStore _store;
    private readonly IOptionsMonitor<TenantUsageAccountingOptions> _options;
    private readonly string _clusterId;
    private const long MaxSuppressionTicks = 5 * TimeSpan.TicksPerMinute;
    private readonly ConcurrentDictionary<string, (LocalUsageSample Sample, long PublishedTicks)> _lastPublished =
        new(StringComparer.Ordinal);

    /// <summary>Initializes a new <see cref="TenantUsagePublisher"/>.</summary>
    /// <param name="store">The durable usage store this cluster's slot is published into.</param>
    /// <param name="cluster">The cluster options supplying this cluster's id (the slot key and writer id).</param>
    /// <param name="options">The usage-accounting options carrying the hysteresis band.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantUsagePublisher(
        ITenantUsageStore store,
        IOptions<ClusterOptions> cluster,
        IOptionsMonitor<TenantUsageAccountingOptions> options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        _options = options;
        _clusterId = cluster.Value.ClusterId;
    }

    /// <summary>The id of the cluster this publisher writes the usage slot for.</summary>
    public string ClusterId => _clusterId;

    /// <summary>
    /// Returns the last sample this publisher published for <paramref name="tenant"/>,
    /// or <see cref="LocalUsageSample.Empty"/> when it has published none. Exposed
    /// for diagnostics and tests.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised tenant id.</param>
    /// <returns>The last published sample, or the empty sample.</returns>
    public LocalUsageSample LastPublished(TenantId tenant)
    {
        var key = RequireTenantKey(tenant);
        return _lastPublished.TryGetValue(key, out var published) ? published.Sample : LocalUsageSample.Empty;
    }

    /// <summary>
    /// Rolls up <paramref name="perTree"/> into this cluster's local sample for
    /// <paramref name="tenant"/> and, when the roll-up clears the hysteresis band
    /// relative to the last published sample, publishes it into the tenant's
    /// per-cluster usage slot stamped with <paramref name="clock"/>. A sub-threshold
    /// movement is suppressed for at most five minutes of supplied cadence-clock
    /// time. An unchanged sample does not need refreshing.
    /// </summary>
    /// <param name="tenant">The tenant whose usage is being published. Must be an initialised tenant id.</param>
    /// <param name="perTree">The tenant's per-tree usage samples on this cluster. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic stamp for the write (supplied by the cadence driver).</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    /// <returns><c>true</c> when a slot was published; <c>false</c> when the movement was suppressed by hysteresis.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="perTree"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised 'no tenant' value.</exception>
    public async Task<bool> RollUpAndPublishAsync(
        TenantId tenant,
        IReadOnlyCollection<TreeUsageSample> perTree,
        HybridLogicalClock clock,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(perTree);
        var key = RequireTenantKey(tenant);

        var candidate = LocalUsageSample.RollUp(perTree);
        var hasPrevious = _lastPublished.TryGetValue(key, out var previous);
        var last = hasPrevious ? previous.Sample : LocalUsageSample.Empty;
        var refreshDue = hasPrevious && candidate != last
            && (Int128)clock.WallClockTicks - previous.PublishedTicks >= MaxSuppressionTicks;

        var options = _options.CurrentValue;
        if (!refreshDue
            && !UsagePublishHysteresis.ShouldPublish(last, candidate, options.PublishMinAbsoluteDelta, options.PublishMinRelativeDelta))
        {
            return false;
        }

        var record = TenantUsageRecord.Create(tenant);
        record.SetLocalSample(_clusterId, candidate, clock, _clusterId);
        await _store.PublishAsync(record, cancellationToken).ConfigureAwait(false);

        _lastPublished[key] = (candidate, clock.WallClockTicks);
        return true;
    }

    private static string RequireTenantKey(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot publish a usage slot.",
                nameof(tenant));
        }

        return tenant.Value;
    }
}
