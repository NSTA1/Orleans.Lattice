using System.Collections.Concurrent;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantRateLimiter"/>: a per-silo singleton holding one
/// lock-free <see cref="TenantTokenBucket"/> per rate-limited tenant in a
/// concurrent map. The per-op <see cref="TryAcquire"/> is a single ordinal
/// dictionary probe plus, when a bucket exists, a lock-free token decrement - no
/// grain hop, no lock, and no allocation. Buckets are created, resized, and pruned
/// only by the budget coordinator at lease cadence
/// (<see cref="Configure"/>, <see cref="RetainOnly"/>), never on the hot path.
/// </summary>
/// <remarks>
/// A tenant with no bucket is inert (always admitted), which covers both "no
/// tenant" (the uninitialised id) and "no configured rate", so an unconfigured
/// deployment pays only one dictionary probe and is otherwise unthrottled.
/// </remarks>
internal sealed class SiloLocalTenantRateLimiter : ITenantRateLimiter
{
    private readonly ConcurrentDictionary<string, TenantTokenBucket> _buckets =
        new(StringComparer.Ordinal);

    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes the limiter over the shared silo time provider.</summary>
    /// <param name="timeProvider">The monotonic timestamp source the buckets are driven by. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <c>null</c>.</exception>
    public SiloLocalTenantRateLimiter(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
    }

    /// <summary>The number of rate-limited tenants currently tracked. Exposed for tests.</summary>
    internal int BucketCount => _buckets.Count;

    /// <inheritdoc />
    public bool TryAcquire(TenantId tenant)
    {
        var key = tenant.Value;
        if (key is null)
        {
            // The uninitialised "no tenant" value is never throttled.
            return true;
        }

        if (!_buckets.TryGetValue(key, out var bucket))
        {
            // No configured rate for this tenant: inert (unthrottled).
            return true;
        }

        return bucket.TryAcquire(_timeProvider.GetTimestamp());
    }

    /// <summary>
    /// Installs or updates the token bucket for <paramref name="tenant"/> with the
    /// given GCRA parameters. When a bucket with the same parameters already
    /// exists it is left in place (preserving its arrival-time state); otherwise it
    /// is replaced. Called by the budget coordinator at lease cadence.
    /// </summary>
    /// <param name="tenant">The tenant to configure. The uninitialised id is ignored.</param>
    /// <param name="emissionIntervalTicks">The sustained emission interval in timestamp ticks.</param>
    /// <param name="burstToleranceTicks">The burst tolerance in timestamp ticks.</param>
    internal void Configure(TenantId tenant, long emissionIntervalTicks, long burstToleranceTicks)
    {
        var key = tenant.Value;
        if (key is null)
        {
            return;
        }

        _buckets.AddOrUpdate(
            key,
            static (_, arg) => new TenantTokenBucket(arg.Emission, arg.Tolerance),
            static (_, existing, arg) => existing.Matches(arg.Emission, arg.Tolerance)
                ? existing
                : new TenantTokenBucket(arg.Emission, arg.Tolerance),
            (Emission: emissionIntervalTicks, Tolerance: burstToleranceTicks));
    }

    /// <summary>
    /// Atomically reads and resets the admitted-operation count for
    /// <paramref name="tenant"/> since the previous read, or <c>0</c> when the
    /// tenant has no bucket. Called by the budget coordinator at lease cadence.
    /// </summary>
    /// <param name="tenant">The tenant whose demand to read.</param>
    /// <returns>The admitted-operation count since the previous read.</returns>
    internal long ReadAndResetDemand(TenantId tenant)
    {
        var key = tenant.Value;
        if (key is null)
        {
            return 0;
        }

        return _buckets.TryGetValue(key, out var bucket) ? bucket.ReadAndResetDemand() : 0;
    }

    /// <summary>
    /// Removes the buckets of every tenant not present in
    /// <paramref name="configuredTenantIds"/>, so a tenant whose rate was cleared
    /// (or whose definition was removed) becomes inert again. Called by the budget
    /// coordinator at lease cadence.
    /// </summary>
    /// <param name="configuredTenantIds">The tenant-id values that still have a configured rate. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configuredTenantIds"/> is <c>null</c>.</exception>
    internal void RetainOnly(IReadOnlySet<string> configuredTenantIds)
    {
        ArgumentNullException.ThrowIfNull(configuredTenantIds);

        List<string>? stale = null;
        foreach (var key in _buckets.Keys)
        {
            if (!configuredTenantIds.Contains(key))
            {
                (stale ??= []).Add(key);
            }
        }

        if (stale is null)
        {
            return;
        }

        foreach (var key in stale)
        {
            _buckets.TryRemove(key, out _);
        }
    }
}
