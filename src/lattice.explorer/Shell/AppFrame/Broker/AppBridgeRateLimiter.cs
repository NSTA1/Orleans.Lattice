namespace Orleans.Lattice.Explorer.Shell.Framing.Broker;

/// <summary>
/// One frame's token-bucket rate limit and concurrency limit (epic #3807, E5). Every
/// request that reaches the limit step takes a token, and every relayed request holds
/// one of at most <see cref="MaxInFlight"/> slots until it completes.
/// </summary>
/// <remarks>
/// Time comes from an injected <see cref="TimeProvider"/>, so tests drive it without
/// waiting. The limiter is per frame: one frame exhausting its budget never starves another.
/// </remarks>
internal sealed class AppBridgeRateLimiter
{
    /// <summary>The bucket's capacity: the largest burst a frame may send.</summary>
    public const int Capacity = 20;

    /// <summary>Tokens restored per second.</summary>
    public const double RefillPerSecond = 10;

    /// <summary>The most requests a frame may have in flight at once.</summary>
    public const int MaxInFlight = 4;

    private readonly TimeProvider _time;
    private readonly Lock _gate = new();
    private double _tokens = Capacity;
    private long _lastRefill;
    private int _inFlight;

    /// <summary>Creates a full bucket.</summary>
    /// <param name="time">The clock.</param>
    /// <exception cref="ArgumentNullException"><paramref name="time"/> is <see langword="null"/>.</exception>
    public AppBridgeRateLimiter(TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
        _lastRefill = time.GetTimestamp();
    }

    /// <summary>The requests now in flight.</summary>
    public int InFlight
    {
        get
        {
            lock (_gate)
            {
                return _inFlight;
            }
        }
    }

    /// <summary>
    /// Admits one request: it needs a free concurrency slot and a token. On success the
    /// caller owns a slot and must call <see cref="Exit"/> exactly once.
    /// </summary>
    /// <param name="concurrencyLimited">Set when the refusal was for concurrency rather than rate.</param>
    /// <returns>Whether the request was admitted.</returns>
    public bool TryEnter(out bool concurrencyLimited)
    {
        lock (_gate)
        {
            var now = _time.GetTimestamp();
            var elapsed = _time.GetElapsedTime(_lastRefill, now).TotalSeconds;
            if (elapsed > 0)
            {
                _tokens = Math.Min(Capacity, _tokens + (elapsed * RefillPerSecond));
                _lastRefill = now;
            }

            if (_inFlight >= MaxInFlight)
            {
                concurrencyLimited = true;
                return false;
            }

            concurrencyLimited = false;
            if (_tokens < 1)
            {
                return false;
            }

            _tokens -= 1;
            _inFlight++;
            return true;
        }
    }

    /// <summary>Releases the slot an admitted request held.</summary>
    public void Exit()
    {
        lock (_gate)
        {
            if (_inFlight > 0)
            {
                _inFlight--;
            }
        }
    }
}
