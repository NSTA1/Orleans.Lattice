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
    private int _maxIngestSliceExtensions;
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
    /// The byte ceiling a chunk record would need to respect for a write batch to
    /// coalesce several of them, rather than carrying barely more than one.
    /// </summary>
    internal const int ChunkBytesForBatchCoalescing = WriteBatchBytes / MinChunksPerWriteBatch;

    /// <summary>
    /// The byte ceiling a chunk record must respect to be allocated off the large
    /// object heap. The runtime's LOH threshold is 85,000 bytes, and this leaves
    /// headroom for the chunk and record headers a payload is wrapped in.
    /// <para>
    /// <b>Why the heap a chunk lands on is a correctness concern, not a tuning
    /// one.</b> A chunk is one contiguous allocation on the read path, and the
    /// LOH is not compacted between collections. A chunk above the threshold is
    /// therefore allocated from a heap that fragments and is only ever collected
    /// in gen2, so a steady read load churns the LOH while gen2 stays small - and
    /// the failure that produces is not "out of memory" but "no contiguous run
    /// this wide", which is why an affordability check denominated in total bytes
    /// can pass and the allocation still fail. Measured on the repository-context
    /// vector index: 8.3 to 12.9 GB of LOH against a gen2 of 108 MB, with the
    /// affordability gate recording the claim as FITTED immediately before an
    /// <c>OutOfMemoryException</c> materialising a single 84 MB leaf snapshot.
    /// </para>
    /// <para>
    /// This matches the value and the reasoning that
    /// <c>PooledPayloadSequence.ChunkBytes</c> already uses in the file storage
    /// provider for the same reason.
    /// </para>
    /// <para>
    /// <b>This bound is no longer what keeps a whole leaf materialisable.</b> An
    /// earlier revision of this comment argued the per-record ceiling had to be
    /// tight enough that a <i>full</i> core leaf - <c>DefaultMaxLeafKeys</c>
    /// records of <c>MaxChunkBytes</c> each - stayed affordable as one contiguous
    /// buffer, because a leaf that cannot be captured has no durable snapshot
    /// coverage, which leaves its durable-materialiser pin without a usable
    /// offset, which pins the tree's WAL trim floor at zero and grows the WAL
    /// without bound.
    /// </para>
    /// <para>
    /// That reasoning was sound about the consequence and wrong about the owner.
    /// It made an application-layer package responsible for a core invariant by
    /// copying a core constant it cannot see change, so a core that raised
    /// <c>DefaultMaxLeafKeys</c> would silently invalidate a bound chosen here.
    /// The core now segments a leaf snapshot at capture, so no whole-leaf
    /// contiguous buffer is materialised at any leaf width and the invariant is
    /// enforced where the constant lives. This package sizes chunks only for the
    /// two reasons it genuinely owns: batch coalescing, and staying off the large
    /// object heap.
    /// </para>
    /// </summary>
    internal const int ChunkBytesOffLargeObjectHeap = 64 * 1024;

    /// <summary>
    /// The byte ceiling one chunk record is sized against: the tighter of the two
    /// independent bounds above, so a record is both a small fraction of a write
    /// batch and small enough to be allocated off the large object heap.
    /// <para>
    /// Taking the minimum of two named bounds rather than writing one constant
    /// keeps each reason auditable and keeps them from silently trading against
    /// each other: raising <see cref="WriteBatchBytes"/> for throughput cannot
    /// push a chunk onto the LOH, and lowering it cannot be mistaken for having
    /// addressed the heap concern.
    /// </para>
    /// </summary>
    internal const int MaxChunkBytes = ChunkBytesOffLargeObjectHeap < ChunkBytesForBatchCoalescing
        ? ChunkBytesOffLargeObjectHeap
        : ChunkBytesForBatchCoalescing;

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
    /// The smallest leaf key bound <see cref="ResolveMaxLeafKeys(long)"/> returns:
    /// a leaf holding one key has nothing to split, so a bound below two could
    /// never be honoured.
    /// </summary>
    internal const int MinMaxLeafKeys = 2;

    /// <summary>
    /// The leaf key bound (<c>MaxLeafKeys</c>) to register a Lattice tree that
    /// holds durable vector-index records with, so that the tree's key bound and
    /// its byte bound (<see cref="LatticeOptions.MaxLeafBytes"/>) cross at about
    /// the same leaf size and neither of them is dead.
    /// <para>
    /// A leaf splits on whichever bound it crosses first: more keys than
    /// <c>MaxLeafKeys</c>, or more bytes than <see cref="LatticeOptions.MaxLeafBytes"/>.
    /// The core default key bound of 128 is sized for ordinary trees, whose
    /// records are small. A vector-index record is a byte-bounded chunk of up to
    /// 64 KiB, so 128 of them fill only about 8 MiB of a 64 MiB byte bound: the
    /// key bound fires at an eighth of the leaf size the byte bound admits and the
    /// byte bound never fires at all. The tree then holds several times the leaves
    /// it needs, and every leaf carries its own grain activation, snapshot and
    /// durable-materialiser pin.
    /// </para>
    /// <para>
    /// The bound returned here is the byte budget divided by the largest record a
    /// durable index writes, so a leaf of full-size records reaches both bounds
    /// together, and a leaf of smaller records (a partial chunk, a commit record)
    /// is still capped by the key bound rather than growing without limit. At the
    /// defaults (a 64 MiB byte bound) it is 1024.
    /// </para>
    /// <para>
    /// It is a structural pin, so it takes effect only when the tree is first
    /// registered. A tree that already exists keeps the bound it was created
    /// with; resize it to adopt this one.
    /// </para>
    /// </summary>
    /// <param name="maxLeafBytes">
    /// The tree's <see cref="LatticeOptions.MaxLeafBytes"/>. A non-positive value
    /// (the byte bound disabled) is sized against
    /// <see cref="LatticeOptions.DefaultMaxLeafBytes"/> instead, because the key
    /// bound is then the only bound a leaf has.
    /// </param>
    /// <returns>The key bound, never less than two.</returns>
    public static int ResolveMaxLeafKeys(long maxLeafBytes) => ResolveMaxLeafKeys(maxLeafBytes, MaxChunkBytes);

    /// <summary>
    /// The leaf key bound for a byte budget and a largest-record size: the seam
    /// behind <see cref="ResolveMaxLeafKeys(long)"/>, parameterised on the record
    /// size so the derivation can be exercised away from the shipped chunk ceiling.
    /// </summary>
    /// <param name="maxLeafBytes">The tree's byte bound; non-positive means disabled.</param>
    /// <param name="maxRecordBytes">The largest record the tree holds. Must be positive.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxRecordBytes"/> is not positive.</exception>
    internal static int ResolveMaxLeafKeys(long maxLeafBytes, int maxRecordBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRecordBytes);
        var budget = maxLeafBytes > 0 ? maxLeafBytes : LatticeOptions.DefaultMaxLeafBytes;
        return (int)Math.Clamp(budget / maxRecordBytes, MinMaxLeafKeys, int.MaxValue);
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
    /// The deadline described below is a timer, and a timer cannot wait longer
    /// than <c>0xFFFFFFFE</c> milliseconds (about 49.7 days), so a longer budget
    /// arms it at that ceiling rather than faulting the slice.
    /// </para>
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
    /// How many further <see cref="IngestSliceBudget"/> periods a slice that has
    /// banked <b>nothing</b> may be granted before the deadline fires anyway.
    /// Defaults to <c>0</c>, which reproduces the elapsed-only bound above
    /// exactly.
    /// <para>
    /// <b>This exists because the budget measures wall-clock that can include
    /// time in which progress is impossible (issue #4071).</b> The budget is
    /// armed at the first wait, which correctly bounds a source that is slow,
    /// stuck, or silent. It does not distinguish those from a source that is
    /// merely QUEUED: when the source streams over grain calls whose leaves must
    /// first take a per-silo WAL replay permit, the slice can be unable to
    /// complete even one item before the budget is spent. It then banks nothing,
    /// moves no cursor, and the next slice re-reads the identical range - so the
    /// build makes no progress at all while every component of it is behaving as
    /// designed.
    /// </para>
    /// <para>
    /// <b>The same defect was already measured and fixed one layer up.</b> Issue
    /// #3284 found the index OPEN in exactly this position - a mean permit wait
    /// above the slice budget, so every slice expired having banked zero - and
    /// fixed it by arming that deadline on PROGRESS rather than on elapsed time
    /// alone. This is that mechanism applied to the ingest slice, which was left
    /// on the elapsed-only bound. Issue #4071 measured a 24.6 s mean permit queue
    /// wait against this 5 s budget, and 114 of 152 non-faulted ingest slices
    /// banking nothing.
    /// </para>
    /// <para>
    /// <b>The cap is load-bearing in both directions.</b> Without one this would
    /// be an unbounded slice again, and bounding the slice is what bounds the
    /// host's turn (see above). With one, a source that genuinely answers nothing
    /// still exhausts the extensions, still banks nothing, and is still reported
    /// through <see cref="VectorIndexBuildProgress.SlicesDeadlinedWithoutProgress"/>,
    /// so the starvation signal stays reachable rather than being suppressed by
    /// the fix. The cap converts "expired having banked nothing" from the normal
    /// outcome under permit contention back into the exceptional one it was meant
    /// to be.
    /// </para>
    /// <para>
    /// <b>Default <c>0</c> on purpose.</b> This type is consumed by hosts whose
    /// sources are local and prompt, for which the elapsed-only bound is already
    /// correct and an extension would only lengthen a turn. A host whose source
    /// streams over a contended store opts in; see
    /// <c>RepoContextAnnOptions.MaxIngestSliceExtensions</c>, which sets it to
    /// match the open path's own cap. A negative value is rejected rather than
    /// clamped, so a mis-set option is loud.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public int MaxIngestSliceExtensions
    {
        get => _maxIngestSliceExtensions;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _maxIngestSliceExtensions = value;
        }
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
    /// An optional observer that receives per-slice stage timings from the build,
    /// so a host can publish them on its own meter. Null by design, and null by
    /// default: this package declares no instruments of its own. See
    /// <see cref="IVectorIndexBuildObserver"/>.
    /// </summary>
    public IVectorIndexBuildObserver? BuildObserver { get; set; }

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
    /// <remarks>
    /// Every field is listed explicitly, so a property added above and not added
    /// here is silently dropped the moment the options reach
    /// <see cref="DurableVectorIndex.OpenAsync(IVectorIndexStore, IVectorSource, DurableVectorIndexOptions, VectorIndexLoadMode, CancellationToken)"/>,
    /// which clones before use. That failure is invisible at the call site - the
    /// option is set, and simply has no effect - so add new properties here in the
    /// same change that introduces them.
    /// </remarks>
    public DurableVectorIndexOptions Clone() => new()
    {
        Index = Index?.Clone() ?? new VectorIndexOptions(),
        _keyPrefix = _keyPrefix,
        _maxItemsPerChunk = _maxItemsPerChunk,
        _ingestBatchSize = _ingestBatchSize,
        _keyReservationBlock = _keyReservationBlock,
        _ingestSliceBudget = _ingestSliceBudget,
        _maxIngestSliceExtensions = _maxIngestSliceExtensions,
        _timeProvider = _timeProvider,
        BuildObserver = BuildObserver,
    };
}
