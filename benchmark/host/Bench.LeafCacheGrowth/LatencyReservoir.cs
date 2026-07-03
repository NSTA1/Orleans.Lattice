namespace Orleans.Lattice.Benchmark.LeafCacheGrowth;

/// <summary>
/// Fixed-capacity reservoir sampler for per-read latency ticks. Bounds memory
/// during a multi-million-read workload while still yielding accurate
/// percentile estimates: the first <see cref="_samples"/>.Length reads are
/// retained verbatim, after which each subsequent read replaces a random slot
/// with decreasing probability (Algorithm R). Percentiles are computed from a
/// sorted copy of the retained sample.
/// </summary>
internal sealed class LatencyReservoir
{
    private readonly long[] _samples;
    private readonly Random _rng;
    private long _seen;
    private int _filled;

    public LatencyReservoir(int capacity, int seed)
    {
        _samples = new long[capacity];
        _rng = new Random(seed);
    }

    public void Add(long ticks)
    {
        if (_filled < _samples.Length)
        {
            _samples[_filled++] = ticks;
        }
        else
        {
            // Algorithm R: replace slot j with probability capacity/seen.
            var j = (long)(_rng.NextDouble() * (_seen + 1));
            if (j < _samples.Length)
                _samples[(int)j] = ticks;
        }
        _seen++;
    }

    /// <summary>
    /// Returns the requested percentile (0..1) of the retained samples in
    /// stopwatch ticks, or 0 when no samples have been recorded.
    /// </summary>
    public double Percentile(double p)
    {
        if (_filled == 0) return 0;
        var sorted = new long[_filled];
        Array.Copy(_samples, sorted, _filled);
        Array.Sort(sorted);
        var rank = (int)Math.Clamp(Math.Ceiling(p * _filled) - 1, 0, _filled - 1);
        return sorted[rank];
    }
}
