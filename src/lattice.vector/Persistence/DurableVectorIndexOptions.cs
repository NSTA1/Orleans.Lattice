namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// How a <see cref="DurableVectorIndex"/> lays itself out on a store and how much
/// work it does per step.
/// <para>
/// The sizing knobs all exist to keep a unit of work bounded rather than
/// proportional to the corpus: <see cref="MaxItemsPerChunk"/> bounds a record,
/// and <see cref="IngestBatchSize"/> together with
/// <see cref="IngestSliceBudget"/> bound a build step. None of them affects what
/// the index answers.
/// </para>
/// <para>
/// <b>Why a step needs both a work bound and a time bound.</b> A count alone
/// bounds the step only in units of <i>source items</i>, and the host that drives
/// the pump cares about units of <i>time</i>: it has a turn to release, a
/// reminder to let through, and a query to answer. Those coincide only while the
/// per-item cost is small and predictable, which is exactly what a remote store
/// of record does not guarantee. Measured on an 8,158-file repository whose
/// source streams over grain calls, a 4,096-item step ran for over twenty
/// minutes, so the count that was chosen to keep a step short did nothing of the
/// kind. <see cref="IngestSliceBudget"/> is the bound denominated in the unit the
/// caller actually needs.
/// </para>
/// </summary>
public sealed class DurableVectorIndexOptions
{
    /// <summary>
    /// The default wall-clock ceiling on one ingest step. Chosen well below the
    /// 30-second call timeout an Orleans reminder tick is delivered under, so a
    /// coordinator pumping this index still lets its keep-alive through while a
    /// build is running.
    /// </summary>
    public static readonly TimeSpan DefaultIngestSliceBudget = TimeSpan.FromSeconds(5);

    private string _keyPrefix = "vidx/";
    private int _maxItemsPerChunk = 1_024;
    private int _ingestBatchSize = 4_096;
    private int _keyReservationBlock = 1_024;
    private TimeSpan _ingestSliceBudget = DefaultIngestSliceBudget;
    private TimeProvider _timeProvider = TimeProvider.System;

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
    /// The wall-clock ceiling on one background build step. A step stops at the
    /// first source item that finds the budget spent, checkpoints, and returns,
    /// so the host gets its turn back on a schedule it can reason about however
    /// slow the source is. Defaults to
    /// <see cref="DefaultIngestSliceBudget"/>; a non-positive value removes the
    /// bound and leaves <see cref="IngestBatchSize"/> as the only one.
    /// <para>
    /// The budget is checked only <i>after</i> an item has been consumed, so a
    /// step always makes progress. A budget too small for even one item degrades
    /// to one item per step, never to a step that consumes nothing and spins.
    /// </para>
    /// </summary>
    public TimeSpan IngestSliceBudget
    {
        get => _ingestSliceBudget;
        set => _ingestSliceBudget = value;
    }

    /// <summary>
    /// The clock <see cref="IngestSliceBudget"/> is measured against. Defaults to
    /// <see cref="TimeProvider.System"/>; a test substitutes a fake so a step's
    /// bound is asserted deterministically rather than by waiting.
    /// </summary>
    /// <exception cref="ArgumentNullException">The value is null.</exception>
    public TimeProvider TimeProvider
    {
        get => _timeProvider;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _timeProvider = value;
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
        _ingestSliceBudget = _ingestSliceBudget,
        _timeProvider = _timeProvider,
    };
}
