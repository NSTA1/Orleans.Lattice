using System.Collections.Concurrent;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The app bridge's per-<c>(caller, tenant, app)</c> fixed-window rate limiter. A partition's window starts at
/// its first request and admits <see cref="LatticeAppBridgeOptions.RateLimitPermitLimit"/> requests until
/// <see cref="LatticeAppBridgeOptions.RateLimitWindow"/> has elapsed.
/// </summary>
/// <remarks>
/// The number of tracked partitions is bounded. When the bound is reached, expired partitions are pruned, and
/// a request for a new partition that still finds the table full is refused: the limiter fails closed rather
/// than growing without bound. A warm acquisition allocates nothing.
/// </remarks>
internal sealed class AppBridgeRateLimiter
{
    /// <summary>The most partitions tracked at once.</summary>
    public const int MaxPartitions = 10_000;

    private readonly ConcurrentDictionary<Partition, Window> _windows = new();
    private readonly int _permitLimit;
    private readonly TimeSpan _window;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new <see cref="AppBridgeRateLimiter"/>.</summary>
    /// <param name="options">The bridge options.</param>
    /// <param name="time">The time source, or null for the system clock.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The permit limit is below 1 or the window is not positive.</exception>
    public AppBridgeRateLimiter(LatticeAppBridgeOptions options, TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.RateLimitPermitLimit, 1, nameof(options));
        if (options.RateLimitWindow <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.RateLimitWindow, "The rate-limit window must be positive.");
        }

        _permitLimit = options.RateLimitPermitLimit;
        _window = options.RateLimitWindow;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>The number of partitions currently tracked.</summary>
    public int PartitionCount => _windows.Count;

    /// <summary>Takes one permit from the caller's partition for an app.</summary>
    /// <param name="subjectId">The caller's subject id.</param>
    /// <param name="tenant">The caller's active tenant.</param>
    /// <param name="slug">The app.</param>
    /// <returns><c>true</c> when a permit was taken; <c>false</c> when the request must be refused.</returns>
    public bool TryAcquire(string subjectId, TenantId tenant, AppSlug slug)
    {
        var now = _time.GetTimestamp();
        var key = new Partition(subjectId, tenant, slug);
        if (!_windows.TryGetValue(key, out var window))
        {
            if (_windows.Count >= MaxPartitions)
            {
                Prune(now);
                if (_windows.Count >= MaxPartitions)
                {
                    return false;
                }
            }

            window = _windows.GetOrAdd(key, static _ => new Window());
        }

        return window.TryAcquire(_time, now, _window, _permitLimit);
    }

    // Cold path: only reached once the table is full.
    private void Prune(long now)
    {
        foreach (var entry in _windows)
        {
            if (entry.Value.IsExpired(_time, now, _window))
            {
                _windows.TryRemove(entry);
            }
        }
    }

    private readonly record struct Partition(string SubjectId, TenantId Tenant, AppSlug Slug);

    private sealed class Window
    {
        private long _start;
        private int _count;
        private bool _started;

        public bool TryAcquire(TimeProvider time, long now, TimeSpan length, int limit)
        {
            lock (this)
            {
                if (!_started || time.GetElapsedTime(_start, now) >= length)
                {
                    _start = now;
                    _count = 0;
                    _started = true;
                }

                if (_count >= limit)
                {
                    return false;
                }

                _count++;
                return true;
            }
        }

        public bool IsExpired(TimeProvider time, long now, TimeSpan length)
        {
            lock (this)
            {
                return !_started || time.GetElapsedTime(_start, now) >= length;
            }
        }
    }
}
