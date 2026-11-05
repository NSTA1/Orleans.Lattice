using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The WAL shard's per-leaf author attribution (#5833, revision 2, B1): for each leaf,
/// the two distinct activations with the highest offsets this shard activation
/// allocated to that leaf's records, stamped or pre-arm alike. An arm reads from it the
/// highest offset any other activation of the leaf allocated
/// (<see cref="ForeignHighLocked"/>), which the arming activation must have replayed
/// past before it may advance live.
/// </summary>
/// <remarks>
/// <para>
/// It is held in memory only and lost with the activation: everything below the head
/// recovered at activation is unattributed, so the floor starts there, exactly as the
/// unstamped watermark's does (<c>ReadPositionHonestRestartForgetsUnstamped</c>).
/// </para>
/// <para>
/// The table is bounded by <see cref="MaxLeafAuthors"/>. When an insertion would exceed
/// it, the half of the entries whose highest offsets are lowest is evicted (review S2),
/// and each evicted entry's highest offset raises the floor of its own hash bucket
/// (#5851, O2b): a leaf's foreign high is read against the floor of the bucket its id
/// hashes to, never another bucket's, so it covers every record of the leaf an eviction
/// dropped (<c>EvictionDropsForeignHigh</c>) and is conservative - it can only hold an arm
/// back, never let one pass an unreplayed record. One floor for the whole shard held every
/// leaf back to the highest evicted record, about the median leaf's newest, so after a fold
/// almost no donor's frontier reached it. Bucketing confines that to the
/// <c>1 / LeafAuthorFloorBucketCount</c> of leaves that share an evicted leaf's bucket.
/// The bucket floors are a fixed array per shard activation, allocated when attribution
/// resets, max-merged only, reset to the restart floor with the activation and never
/// persisted. <see cref="_leafAuthorsFloor"/> stays the highest floor of all, for a
/// reader that names no leaf.
/// </para>
/// <para>
/// Updating it on an append is a lookup and a value-type store under
/// <see cref="_stateGate"/>, in the step that assigns the offset; only a leaf's first
/// record in this activation inserts an entry.
/// </para>
/// </remarks>
internal sealed partial class WalShardGrain
{
    /// <summary>The most leaves whose authors one shard activation tracks before it folds them into the floor.</summary>
    internal const int MaxLeafAuthors = 16_384;

    /// <summary>
    /// The number of hashed eviction-floor buckets a shard activation allocates when its
    /// attribution resets (#5851, O2b); a power of two, rounded up if not. A cost knob:
    /// 8 bytes a bucket per WAL shard activation, against how many leaves an eviction
    /// holds back. Read once per activation, so a change applies to activations after it.
    /// </summary>
    internal static int LeafAuthorFloorBucketCount { get; set; } = 1024;

    private Dictionary<Guid, LeafAuthors>? _leafAuthors;

    // Everything at or below it is unattributed: the head recovered at activation, minus
    // one. A fold never raises it; it raises the evicted leaf's bucket floor instead.
    private long _leafAuthorsRestartFloor = -1;

    // Per bucket, the highest offset an evicted leaf of that bucket held, never below the
    // restart floor (#5851, O2b). Allocated once per activation at reset, never per append.
    private long[]? _leafAuthorBucketFloors;

    // The highest floor of all: the restart floor raised by every fold. Only a reader that
    // names no leaf reads it.
    private long _leafAuthorsFloor = -1;

    /// <summary>Starts attribution afresh at activation, below the recovered head. Called before any append is admitted.</summary>
    private void ResetLeafAuthors(long recoveredNextOffset)
    {
        _leafAuthors = null;
        _leafAuthorsRestartFloor = recoveredNextOffset - 1;
        _leafAuthorsFloor = _leafAuthorsRestartFloor;
        var count = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, LeafAuthorFloorBucketCount));
        if (_leafAuthorBucketFloors is null || _leafAuthorBucketFloors.Length != count)
        {
            _leafAuthorBucketFloors = new long[count];
        }

        Array.Fill(_leafAuthorBucketFloors, _leafAuthorsRestartFloor);
    }

    /// <summary>
    /// The bucket <paramref name="leafId"/>'s eviction floor is kept in, of
    /// <paramref name="bucketCount"/> (a power of two): a stable mix of the id's bytes.
    /// </summary>
    internal static int LeafAuthorBucket(Guid leafId, int bucketCount)
    {
        Span<byte> bytes = stackalloc byte[16];
        leafId.TryWriteBytes(bytes);
        var h = BinaryPrimitives.ReadUInt64LittleEndian(bytes) ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdUL;
        h ^= h >> 33;
        return (int)(h & (ulong)(bucketCount - 1));
    }

    /// <summary>
    /// The floor <paramref name="leafId"/>'s foreign high is read against: its own bucket's
    /// eviction floor, never below the restart floor (#5851, O2b). Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long LeafAuthorFloorLocked(Guid leafId)
    {
        var floors = _leafAuthorBucketFloors;
        // RED PROOF (#5851, O2b): every leaf reads the floor of all buckets.
        _ = floors;
        return _leafAuthorsFloor;
    }

    /// <summary>
    /// Attributes the record allocated at <paramref name="offset"/> under
    /// <paramref name="stamp"/> to its leaf and activation, and returns the leaf's
    /// foreign high for that activation, which the append's response reports to it with
    /// the shared watermark: a duplicate or stale activation's record is then read past
    /// before the leaf advances over its own (#5833, revision 2). A stamp that names no
    /// leaf (a coordinator terminal, a bulk seed), or no activation (an earlier bucket
    /// build), is not reported; the first is not attributed either. Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long NoteLeafAuthorLocked(in WalAppendStamp stamp, long offset)
    {
        if (stamp.LeafId == Guid.Empty)
        {
            return -1;
        }

        _leafAuthors ??= new Dictionary<Guid, LeafAuthors>();
        if (!_leafAuthors.TryGetValue(stamp.LeafId, out var authors))
        {
            if (_leafAuthors.Count >= MaxLeafAuthors)
            {
                FoldLeafAuthorsLocked();
            }

            authors = LeafAuthors.Empty;
        }

        authors = authors.With(stamp.Activation, offset);
        _leafAuthors[stamp.LeafId] = authors;
        return stamp.Activation == Guid.Empty ? -1 : Math.Max(LeafAuthorFloorLocked(stamp.LeafId), authors.ForeignHigh(stamp.Activation));
    }

    /// <summary>
    /// The highest offset of a record of <paramref name="leafId"/> that an activation
    /// other than <paramref name="activation"/> may have allocated here: the attributed
    /// high of every other activation, never below the unattributed floor. Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long ForeignHighLocked(Guid leafId, Guid activation)
    {
        var high = LeafAuthorFloorLocked(leafId);
        if (_leafAuthors is not null && _leafAuthors.TryGetValue(leafId, out var authors))
        {
            high = Math.Max(high, authors.ForeignHigh(activation));
        }

        return high;
    }

    /// <summary>How many folds this activation has made; test seam (review S2).</summary>
    internal int LeafAuthorFoldsForTesting { get; private set; }

    /// <summary>
    /// Evicts the entries whose highest offsets are lowest - at least half the table, and
    /// every entry of a table of one (review S2) - raising each evicted entry's own bucket
    /// floor to its highest offset (#5851, O2b), and the floor of all with them.
    /// </summary>
    private void FoldLeafAuthorsLocked()
    {
        var authors = _leafAuthors!;
        var count = authors.Count;
        if (count == 0)
        {
            return;
        }

        LeafAuthorFoldsForTesting++;
        var highs = ArrayPool<long>.Shared.Rent(count);
        try
        {
            var i = 0;
            foreach (var entry in authors.Values)
            {
                highs[i++] = entry.High1;
            }

            Array.Sort(highs, 0, count);

            // Ties at the cut are evicted with it, so a fold always frees at least half.
            var cut = highs[count - (count / 2) - 1];
            foreach (var (leaf, entry) in authors)
            {
                if (entry.High1 <= cut)
                {
                    if (_leafAuthorBucketFloors is { } floors)
                    {
                        var bucket = LeafAuthorBucket(leaf, floors.Length);
                        floors[bucket] = Math.Max(floors[bucket], entry.High1);
                    }

                    _leafAuthorsFloor = Math.Max(_leafAuthorsFloor, entry.High1);
                    authors.Remove(leaf);
                }
            }
        }
        finally
        {
            ArrayPool<long>.Shared.Return(highs);
        }
    }

    /// <summary>The leaf-author table's size; test seam.</summary>
    internal int LeafAuthorCountForTesting
    {
        get
        {
            lock (_stateGate)
            {
                return _leafAuthors?.Count ?? 0;
            }
        }
    }

    /// <summary>Reads <see cref="ForeignHighLocked"/> under the lock; test seam.</summary>
    internal long ForeignHighForTesting(Guid leafId, Guid activation)
    {
        lock (_stateGate)
        {
            return ForeignHighLocked(leafId, activation);
        }
    }

    /// <summary>Folds the table as an overflowing insertion would; test seam (<c>EvictionDropsForeignHigh</c>).</summary>
    internal void FoldLeafAuthorsForTesting()
    {
        lock (_stateGate)
        {
            if (_leafAuthors is not null)
            {
                FoldLeafAuthorsLocked();
            }
        }
    }

    /// <summary>
    /// One leaf's top two distinct authoring activations by highest allocated offset:
    /// <c>High1 &gt;= High2</c>, and <c>Token1 != Token2</c> whenever both are set.
    /// </summary>
    internal readonly record struct LeafAuthors(Guid Token1, long High1, Guid Token2, long High2)
    {
        public static LeafAuthors Empty => new(Guid.Empty, -1, Guid.Empty, -1);

        /// <summary>The highest offset by any activation other than <paramref name="activation"/>.</summary>
        public long ForeignHigh(Guid activation) =>
            High1 >= 0 && Token1 == activation && activation != Guid.Empty ? High2 : High1;

        /// <summary>This table with <paramref name="activation"/>'s record at <paramref name="offset"/> noted.</summary>
        public LeafAuthors With(Guid activation, long offset)
        {
            if (High1 < 0)
            {
                return new(activation, offset, Guid.Empty, -1);
            }

            if (activation == Token1)
            {
                return offset > High1 ? this with { High1 = offset } : this;
            }

            if (High2 >= 0 && activation == Token2)
            {
                return offset > High1
                    ? new(activation, offset, Token1, High1)
                    : offset > High2 ? this with { High2 = offset } : this;
            }

            // A third activation, or the second's first record: it displaces the lower.
            return offset > High1
                ? new(activation, offset, Token1, High1)
                : offset > High2 ? this with { Token2 = activation, High2 = offset } : this;
        }
    }
}
