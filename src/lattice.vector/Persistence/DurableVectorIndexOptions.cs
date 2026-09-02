namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// How a <see cref="DurableVectorIndex"/> lays itself out on a store and how much
/// work it does per step.
/// <para>
/// The two sizing knobs both exist to keep a unit of work bounded rather than
/// proportional to the corpus: <see cref="MaxItemsPerChunk"/> bounds a record,
/// and <see cref="IngestBatchSize"/> bounds a build step. Neither affects what
/// the index answers.
/// </para>
/// </summary>
public sealed class DurableVectorIndexOptions
{
    private string _keyPrefix = "vidx/";
    private int _maxItemsPerChunk = 1_024;
    private int _ingestBatchSize = 4_096;
    private int _keyReservationBlock = 1_024;

    /// <summary>
    /// The configuration of the underlying index. Required: at minimum its
    /// dimensionality must match the source's.
    /// </summary>
    public VectorIndexOptions Index { get; set; } = new();

    /// <summary>
    /// The key prefix every durable record of this index sits under. Defaults to
    /// <c>vidx/</c>. Give each index its own prefix, and prefer a tree that holds
    /// nothing else: recovery deletes whole key ranges under this prefix.
    /// </summary>
    /// <exception cref="ArgumentNullException">The value is null.</exception>
    public string KeyPrefix
    {
        get => _keyPrefix;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _keyPrefix = value;
        }
    }

    /// <summary>
    /// The largest number of centroids or vectors one durable record carries, so
    /// no record grows with the corpus. Defaults to 1024, which at a typical
    /// embedding width is a record of a few megabytes.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int MaxItemsPerChunk
    {
        get => _maxItemsPerChunk;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _maxItemsPerChunk = value;
        }
    }

    /// <summary>
    /// How many source vectors one background build step consumes before it
    /// checkpoints and returns. Defaults to 4096. Smaller steps hand the host
    /// back control sooner; larger ones checkpoint less often.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int IngestBatchSize
    {
        get => _ingestBatchSize;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _ingestBatchSize = value;
        }
    }

    /// <summary>
    /// How many index keys one durable reservation of the key dictionary covers.
    /// Defaults to 1024. A larger block writes the watermark less often; the
    /// identifiers left unused by a crash are burned, which is free in a 64-bit
    /// space.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public int KeyReservationBlock
    {
        get => _keyReservationBlock;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _keyReservationBlock = value;
        }
    }

    /// <summary>
    /// Throws when the options cannot open an index. Called when one is opened;
    /// call it directly to fail fast at configuration time.
    /// </summary>
    /// <exception cref="ArgumentException">The index options are missing or unusable.</exception>
    public void Validate()
    {
        if (Index is null)
        {
            throw new ArgumentException(
                "DurableVectorIndexOptions.Index must be set to the index configuration.", nameof(Index));
        }

        Index.Validate();
    }

    /// <summary>Returns an independent copy of these options.</summary>
    public DurableVectorIndexOptions Clone() => new()
    {
        Index = Index?.Clone() ?? new VectorIndexOptions(),
        _keyPrefix = _keyPrefix,
        _maxItemsPerChunk = _maxItemsPerChunk,
        _ingestBatchSize = _ingestBatchSize,
        _keyReservationBlock = _keyReservationBlock,
    };
}
