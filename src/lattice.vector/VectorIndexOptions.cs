namespace Orleans.Lattice.Vector;

/// <summary>
/// The tunable accuracy / latency trade-off of a <see cref="VectorIndex"/>, plus
/// the explicit seed that makes its construction deterministic.
/// <para>
/// An options instance is validated and copied when an index is constructed, so
/// mutating it afterwards has no effect on the index. Every knob other than
/// <see cref="Dimensions"/> has a workable default, and the two sizing knobs
/// (<see cref="PartitionCount"/> and <see cref="Probes"/>) accept <c>0</c> to
/// mean "derive from the corpus size at training time".
/// </para>
/// </summary>
public sealed class VectorIndexOptions
{
    private int _dimensions;
    private int _partitionCount;
    private int _probes;
    private int _trainingSampleSize = 32_768;
    private int _maxTrainingIterations = 10;
    private int _minimumTrainingCount = 1_024;

    /// <summary>The largest number of partitions <see cref="AutoPartitionCount"/> will derive.</summary>
    public const int MaximumPartitionCount = 16_384;

    /// <summary>
    /// The number of components every vector in the index has. Required: an index
    /// rejects any vector or query whose length differs.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int Dimensions
    {
        get => _dimensions;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _dimensions = value;
        }
    }

    /// <summary>
    /// The similarity kernel the index ranks by. Defaults to
    /// <see cref="VectorDistanceMetric.Cosine"/>.
    /// </summary>
    public VectorDistanceMetric Metric { get; set; } = VectorDistanceMetric.Cosine;

    /// <summary>
    /// The number of partitions to train, or <c>0</c> (the default) to derive it
    /// from the corpus size with <see cref="AutoPartitionCount"/>. A larger value
    /// makes each partition smaller, so a probe costs less but more probes are
    /// needed for the same recall.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public int PartitionCount
    {
        get => _partitionCount;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _partitionCount = value;
        }
    }

    /// <summary>
    /// The number of partitions a search scores, or <c>0</c> (the default) to
    /// derive it from the trained partition count with
    /// <see cref="AutoProbes"/>. This is the primary accuracy / latency dial:
    /// probing every partition is exact but costs a full scan, while probing one
    /// is fastest and least accurate.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public int Probes
    {
        get => _probes;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _probes = value;
        }
    }

    /// <summary>
    /// The seed for the deterministic pseudo-random number generator that draws
    /// the training sample and the initial centroids. Two indexes built from the
    /// same vectors with the same seed and options train identically.
    /// </summary>
    public ulong Seed { get; set; } = 0x9E3779B97F4A7C15UL;

    /// <summary>
    /// The largest number of vectors the training pass clusters over. Training
    /// cost is proportional to this times the partition count, so capping it
    /// keeps a build over a very large corpus bounded. Defaults to 32768.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int TrainingSampleSize
    {
        get => _trainingSampleSize;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _trainingSampleSize = value;
        }
    }

    /// <summary>
    /// The number of Lloyd refinement iterations the training pass runs. Defaults
    /// to 10; the marginal recall gain past roughly a dozen iterations is small
    /// relative to the build cost.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int MaxTrainingIterations
    {
        get => _maxTrainingIterations;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _maxTrainingIterations = value;
        }
    }

    /// <summary>
    /// The smallest corpus worth partitioning. Below it, training is a no-op and
    /// the index stays in <see cref="VectorIndexState.Building"/>, where an
    /// exhaustive scan of the contiguous block is both exact and faster than
    /// probing. Defaults to 1024.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int MinimumTrainingCount
    {
        get => _minimumTrainingCount;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _minimumTrainingCount = value;
        }
    }

    /// <summary>
    /// The partition count derived from a corpus size when
    /// <see cref="PartitionCount"/> is <c>0</c>: the square root of the count,
    /// clamped to <c>[1, </c><see cref="MaximumPartitionCount"/><c>]</c>. This
    /// balances the two halves of a probe - ranking the centroids and scanning
    /// the probed posting lists - so total query cost grows with the square root
    /// of the corpus rather than with the corpus.
    /// </summary>
    /// <param name="count">The number of live vectors to be partitioned.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public static int AutoPartitionCount(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count == 0)
        {
            return 0;
        }

        var derived = (int)Math.Round(Math.Sqrt(count));
        return Math.Clamp(derived, 1, MaximumPartitionCount);
    }

    /// <summary>
    /// The probe count derived from a partition count when <see cref="Probes"/>
    /// is <c>0</c>: twice the square root of the partition count, at least 8 and
    /// never more than the partition count.
    /// <para>
    /// The shape of this rule is what keeps query cost sub-linear. A search costs
    /// <c>C</c> centroid comparisons plus <c>probes * (n / C)</c> vector
    /// comparisons; with <c>C = sqrt(n)</c> a probe count that is a fixed
    /// <i>fraction</i> of <c>C</c> would make the second term proportional to
    /// <c>n</c> again, so the fraction of the corpus scanned must fall as the
    /// corpus grows. Under this rule it does: about 17 percent at ten thousand
    /// vectors, 12 percent at the seventy thousand of a large live corpus, and 6
    /// percent at a million. The measured recall of the default is published in
    /// the package readme.
    /// </para>
    /// </summary>
    /// <param name="partitionCount">The trained partition count.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is negative.</exception>
    public static int AutoProbes(int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionCount);
        if (partitionCount == 0)
        {
            return 0;
        }

        var derived = Math.Max(8, 2 * (int)Math.Ceiling(Math.Sqrt(partitionCount)));
        return Math.Min(derived, partitionCount);
    }

    /// <summary>
    /// Throws when the options cannot build a usable index. Called by the
    /// <see cref="VectorIndex"/> constructor; call it directly to fail fast at
    /// configuration time.
    /// </summary>
    /// <exception cref="ArgumentException"><see cref="Dimensions"/> was never set, or <see cref="Metric"/> is not a defined member.</exception>
    public void Validate()
    {
        if (_dimensions <= 0)
        {
            throw new ArgumentException(
                "VectorIndexOptions.Dimensions must be set to the vector dimensionality before an index is built.",
                nameof(Dimensions));
        }

        if (Metric is not (VectorDistanceMetric.Cosine or VectorDistanceMetric.DotProduct))
        {
            throw new ArgumentException(
                $"VectorIndexOptions.Metric '{Metric}' is not a defined VectorDistanceMetric member.",
                nameof(Metric));
        }
    }

    /// <summary>
    /// Returns an independent copy of these options, so an index is unaffected by
    /// later mutation of the instance it was constructed from.
    /// </summary>
    public VectorIndexOptions Clone() => new()
    {
        _dimensions = _dimensions,
        Metric = Metric,
        _partitionCount = _partitionCount,
        _probes = _probes,
        Seed = Seed,
        _trainingSampleSize = _trainingSampleSize,
        _maxTrainingIterations = _maxTrainingIterations,
        _minimumTrainingCount = _minimumTrainingCount,
    };
}
