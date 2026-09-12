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
    /// no record grows with the corpus. Defaults to 1024.
    /// <para>
    /// This is a ceiling, not the size a record is actually written at. An item
    /// count bounds a record only in units of <i>vectors</i>, and the store of
    /// record cares about units of <i>bytes</i>: those coincide only at a fixed
    /// dimensionality, which is exactly what an index does not have. At 768
    /// dimensions a 1024-item record is 3.15 MiB, and every full record is that
    /// size to the byte because every full record holds the same item count. See
    /// <see cref="MaxChunkBytes"/> for the bound that is denominated in the unit
    /// the store actually needs.
    /// </para>
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
    /// A flush accumulates chunk records up to this many bytes before issuing a
    /// write, so one round trip stays bounded no matter how large a partition or
    /// an ingest batch is.
    /// </summary>
    internal const int WriteBatchBytes = 4 * 1024 * 1024;

    /// <summary>
    /// How many chunk records one store write is expected to coalesce. Batching
    /// only coalesces anything if a batch can hold several records: a record
    /// sized at most <see cref="MaxItemsPerChunk"/> items can occupy nearly a
    /// whole batch on a wide embedding, which leaves the batch carrying barely
    /// more than one record and makes the bound above inert.
    /// </summary>
    internal const int MinChunksPerWriteBatch = 16;

    /// <summary>
    /// The byte ceiling one chunk record is sized against, so a record is a small
    /// fraction of a write batch rather than the whole of one.
    /// </summary>
    internal const int MaxChunkBytes = WriteBatchBytes / MinChunksPerWriteBatch;

    /// <summary>
    /// The number of items a chunk record is actually written at: the largest
    /// count that keeps a record within <see cref="MaxChunkBytes"/>, capped by the
    /// configured <see cref="MaxItemsPerChunk"/>.
    /// <para>
    /// The count is derived from the index's own dimensionality rather than from
    /// anything about the host, so a narrow index keeps its configured item count
    /// and a wide one is bounded by bytes instead. Nothing has to be tuned and no
    /// setting has to change for a record to stay a reasonable size.
    /// </para>
    /// </summary>
    internal int EffectiveItemsPerChunk => ResolveItemsPerChunk(Index?.Dimensions ?? 0, MaxItemsPerChunk);

    /// <summary>
    /// The item count a chunk of the given dimensionality is written at.
    /// </summary>
    /// <param name="dimensions">The index's dimensionality; a non-positive value leaves the ceiling in force.</param>
    /// <param name="maxItemsPerChunk">The configured ceiling on a record's item count.</param>
    internal static int ResolveItemsPerChunk(int dimensions, int maxItemsPerChunk)
    {
        // A vector chunk carries a header, then each item as its coordinates
        // followed by its identifier. Centroid chunks carry no identifier, so
        // sizing both against the vector stride keeps a centroid record under the
        // same ceiling rather than over it.
        //
        // A dimensionality of zero is the "not yet known" case, and it needs no
        // branch of its own: the stride degenerates to the identifier alone, the
        // quotient runs far past any sane ceiling, and the clamp hands back the
        // configured count. A negative one cannot arrive, because
        // VectorIndexOptions.Dimensions rejects a non-positive value on the way
        // in.
        var stride = ((long)dimensions * sizeof(float)) + sizeof(long);
        var budget = MaxChunkBytes
            - VectorIndexFormat.ChunkHeaderSize
            - VectorIndexPersistenceFormat.RecordHeaderSize;

        // A single item wider than the whole budget still has to be written, so
        // the floor is one item rather than none.
        return (int)Math.Clamp(budget / stride, 1, maxItemsPerChunk);
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
    /// <para>
    /// That guarantee is about the BUDGET, and it was once misread as being about
    /// the step. It is not: a step that faults - because the source reached its
    /// store of record through a call that timed out, say - takes an exit the
    /// budget never governs. Such a step used to discard every item it had
    /// already consumed, so a source that faulted reliably at the same place
    /// re-read the same range forever and banked nothing (#2536). It now
    /// checkpoints what it consumed before the fault propagates, which makes the
    /// step's progress monotone under a repeating fault as well as under a spent
    /// budget. The fault itself is still raised to the caller.
    /// </para>
    /// <para>
    /// <b>And the budget is a deadline, not only a sample.</b> Banking on a fault
    /// closed one half of #2536 and left the other half open, which the same
    /// deployment then measured: banking is conditional on having consumed
    /// something, and the fault that was actually firing was a source page that
    /// stalled BEFORE yielding its first item. Zero items consumed means the
    /// in-loop sample above is never reached, so the step is not bounded at all,
    /// and it means there is nothing to bank, so the cursor never moves and the
    /// next step re-reads the identical range. The build does not converge slowly
    /// under that fault - it does not converge. The budget is therefore also
    /// armed as a cancellation deadline that is handed to the source and raced
    /// against each read, so a step returns within its budget whether the source
    /// is slow, stuck, or silent. A step stopped by the deadline banks and
    /// returns rather than throwing: it is a bounded slice, not a failure.
    /// </para>
    /// <para>
    /// <b>The deadline starts at a WAIT, not at the slice.</b> The two bounds above
    /// are compatible only because of where the second one starts. A deadline
    /// running from the top of the slice measures the clock rather than the
    /// source, so on a budget too small for one item it fires before the first
    /// read is even issued: the source is handed a token that is already
    /// cancelled, yields nothing, and the slice banks nothing and moves no cursor
    /// - which is the very stall the after-consumption sample exists to prevent,
    /// reintroduced above it. #2651 measured that as a coin flip rather than a
    /// constant failure, because a timer racing a source read resolves
    /// differently on every run. Four independent measurements of the same
    /// byte-identical tree, by three people: 21 failures in 30 and 3 in 10 (both
    /// with no rebuild in the run window), 12 in 25 run sequentially, and 19 in
    /// 20 when every run was preceded by a rebuild. Do not quote any of these as
    /// THE rate. The first two share a condition and still straddle the third
    /// from both sides, so the tempting reading - that the rate tracks how loaded
    /// the machine is - does not survive its own data. What every point does
    /// agree on is that a byte-identical tree does not have a stable rate here,
    /// which is the signature of a race and not of a fixed per-run probability.
    /// The only sound use of these numbers is the PAIRED one: a before and after
    /// arm measured under the same conditions, interleaved so machine state is
    /// shared between them. Interleaving validates the COMPARISON; it tells you
    /// nothing about the absolute rate. Arming the deadline when the step first
    /// has to wait is what keeps both bounds intact, and it is the arming MOMENT
    /// that carries this, not any one branch: reinstating the old moment turns
    /// the fixture red 20 times in 20.
    /// </para>
    /// <para>
    /// A slice that has banked nothing gets the full budget for that wait rather
    /// than what remains of it. Truncating it protects no progress - there is none
    /// - while guaranteeing the stall, and it is not a hypothetical difference: a
    /// clock charged per item can read as already past the budget on a slice that
    /// has consumed nothing at all, which turns the remaining budget negative and
    /// cancels the first read on the spot.
    /// </para>
    /// <para>
    /// Bounding the step is also what bounds the TURN a host pumps it on. An
    /// unbounded step held its coordinator's non-reentrant activation for the
    /// whole of a measured four and a half minutes, behind which that
    /// coordinator's own keep-alive reminder timed out at thirty seconds - so the
    /// pump that was supposed to retry the build was starved by the build
    /// (#2483).
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
