namespace Orleans.Lattice;

/// <summary>
/// Thread-safe bounded reservoir of observed payload samples that backs
/// <see cref="AutoTrainingCompressionDictionaryProvider"/>. Samples are admitted
/// subject to a per-sample byte cap and a sampling probability, and the
/// reservoir is bounded by both a sample count and a total byte budget; when a
/// bound is reached the oldest sample is evicted (FIFO drop-oldest) so memory
/// use stays strictly bounded regardless of traffic volume.
/// <para>
/// The sampling decision is injectable (the <c>sampler</c> constructor
/// parameter) so tests can drive admission deterministically; production uses
/// <see cref="Random.Shared"/>. The sampler is consulted only when
/// <c>samplingRate</c> is below <c>1.0</c>, so a fully-sampling reservoir is
/// deterministic without any RNG.
/// </para>
/// </summary>
internal sealed class CompressionDictionaryTrainingReservoir
{
    private readonly int _maxSampleCount;
    private readonly long _maxReservoirBytes;
    private readonly int _maxSampleBytes;
    private readonly double _samplingRate;
    private readonly Func<double> _sampler;
    private readonly object _gate = new();
    private readonly Queue<byte[]> _samples = new();
    private long _totalBytes;

    /// <summary>
    /// Initialises a reservoir with the supplied bounds and sampling behaviour.
    /// </summary>
    /// <param name="maxSampleCount">Maximum retained sample count (>= 1).</param>
    /// <param name="maxReservoirBytes">Maximum retained total bytes (>= 1).</param>
    /// <param name="maxSampleBytes">Per-sample byte ceiling (>= 1).</param>
    /// <param name="samplingRate">Admission probability in <c>[0, 1]</c>.</param>
    /// <param name="sampler">
    /// Optional sampling-decision source returning a value in <c>[0, 1)</c>;
    /// defaults to <see cref="Random.Shared"/>. Consulted only when
    /// <paramref name="samplingRate"/> is below <c>1.0</c>.
    /// </param>
    public CompressionDictionaryTrainingReservoir(
        int maxSampleCount,
        long maxReservoirBytes,
        int maxSampleBytes,
        double samplingRate,
        Func<double>? sampler = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSampleCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxReservoirBytes, 1L);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSampleBytes, 1);
        if (double.IsNaN(samplingRate) || samplingRate < 0.0 || samplingRate > 1.0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(samplingRate), samplingRate, "samplingRate must be a number in [0, 1].");
        }

        _maxSampleCount = maxSampleCount;
        _maxReservoirBytes = maxReservoirBytes;
        _maxSampleBytes = maxSampleBytes;
        _samplingRate = samplingRate;
        _sampler = sampler ?? Random.Shared.NextDouble;
    }

    /// <summary>Current retained sample count.</summary>
    public int SampleCount
    {
        get { lock (_gate) { return _samples.Count; } }
    }

    /// <summary>Current retained total bytes across all samples.</summary>
    public long TotalBytes
    {
        get { lock (_gate) { return _totalBytes; } }
    }

    /// <summary>
    /// Attempts to admit a copy of <paramref name="payload"/> into the reservoir.
    /// Returns <see langword="false"/> without copying when the payload is empty,
    /// exceeds the per-sample byte cap, or loses the sampling draw; otherwise
    /// copies the bytes in (evicting the oldest samples as needed to honour the
    /// count and byte bounds) and returns <see langword="true"/>.
    /// </summary>
    /// <param name="payload">The observed payload bytes.</param>
    /// <returns>
    /// <see langword="true"/> when the payload was admitted; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public bool TryObserve(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty || payload.Length > _maxSampleBytes)
        {
            return false;
        }

        if (_samplingRate < 1.0)
        {
            if (_samplingRate <= 0.0 || _sampler() >= _samplingRate)
            {
                return false;
            }
        }

        var copy = payload.ToArray();
        lock (_gate)
        {
            _samples.Enqueue(copy);
            _totalBytes += copy.Length;

            while (_samples.Count > _maxSampleCount
                || (_totalBytes > _maxReservoirBytes && _samples.Count > 1))
            {
                var evicted = _samples.Dequeue();
                _totalBytes -= evicted.Length;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns a point-in-time snapshot of the retained samples as a fresh
    /// array; the caller owns the returned array and its elements.
    /// </summary>
    /// <returns>The retained samples, oldest first.</returns>
    public byte[][] Snapshot()
    {
        lock (_gate)
        {
            return _samples.ToArray();
        }
    }
}
