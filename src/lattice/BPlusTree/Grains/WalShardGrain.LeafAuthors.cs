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
    /// <inheritdoc />
    public Task<WalSplitCut> GetSplitCutAsync(WalAppendStamp reader, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInternalOrigin(LatticeOperation.Read);
        EnsureInitialized();
        lock (_stateGate)
        {
            var bound = reader.LeafId == Guid.Empty
                ? _leafAuthorsFloor
                : ForeignHighLocked(reader.LeafId, reader.Activation, reader.ReaderTag);
            if (reader.Close1.IsSet)
            {
                bound = Math.Max(bound, StaleHighLocked(reader.Close1, reader.LeafId));
            }

            if (reader.Close2.IsSet)
            {
                bound = Math.Max(bound, StaleHighLocked(reader.Close2, reader.LeafId));
            }

            return Task.FromResult(new WalSplitCut(DurableContiguousTailOffset(), bound, _unstampedWatermark));
        }
    }

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
        return floors is null
            ? _leafAuthorsFloor
            : Math.Max(_leafAuthorsRestartFloor, floors[LeafAuthorBucket(leafId, floors.Length)]);
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
        if (stamp.Forwarder != Guid.Empty)
        {
            // F2': every forwarder on the chain. Only the first hop is tagged; a chain hop
            // is never excluded from any reader's foreign high (S-b).
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder, 1, stamp.ForwarderTag, offset);
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder2, 2, stamp.ForwarderTag2, offset);
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder3, 3, stamp.ForwarderTag3, offset);
            authors = _leafAuthors.TryGetValue(stamp.LeafId, out var current) ? current : authors;
        }

        var reported = stamp.Activation == Guid.Empty
            ? -1
            : Math.Max(LeafAuthorFloorLocked(stamp.LeafId), authors.ForeignHigh(stamp.Activation, stamp.ReaderTag));
        if (stamp.Close1.IsSet)
        {
            reported = Math.Max(reported, StaleHighLocked(stamp.Close1, stamp.LeafId));
        }

        if (stamp.Close2.IsSet)
        {
            reported = Math.Max(reported, StaleHighLocked(stamp.Close2, stamp.LeafId));
        }

        return reported;
    }

    /// <summary>
    /// The donor's stale high for a receiver whose marker holds <paramref name="close"/>
    /// pending (#5837): the highest offset of a record of the donor allocated here by an
    /// activation other than the closing one under a generation no later than the
    /// close's, epoch-only records counting as generation 0, and of every record the
    /// donor forwarded but those tagged with the split that created
    /// <paramref name="receiver"/>; never below the donor's own eviction floor (#5851, O2b),
    /// which covers every record of the donor a fold dropped. A displaced
    /// generation or activation is always counted. Called under <see cref="_stateGate"/>.
    /// </summary>
    private long StaleHighLocked(in WalPendingClose close, Guid receiver)
    {
        var high = LeafAuthorFloorLocked(close.Donor);
        if (_leafAuthors is not null && _leafAuthors.TryGetValue(close.Donor, out var authors))
        {
            high = Math.Max(high, authors.StaleHigh(close.Token, receiver));
        }

        return high;
    }

    /// <summary>
    /// Attributes a forwarded record to its forwarder as well (#5837, condition 4): under
    /// no activation token, by the narrow that moved its key (X2). Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private void NoteForwardLocked(Guid author, Guid forwarder, int hop, Guid tag, long offset)
    {
        if (forwarder == Guid.Empty || forwarder == author)
        {
            return;
        }

        if (!_leafAuthors!.TryGetValue(forwarder, out var authors))
        {
            if (_leafAuthors.Count >= MaxLeafAuthors)
            {
                FoldLeafAuthorsLocked();
            }

            authors = LeafAuthors.Empty;
        }

        _leafAuthors[forwarder] = authors.WithForwardHop(hop, tag, offset);
    }

    /// <summary>
    /// The highest offset of a record of <paramref name="leafId"/> that an activation
    /// other than <paramref name="activation"/> may have allocated here: the attributed
    /// high of every other activation, never below the unattributed floor. Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long ForeignHighLocked(Guid leafId, Guid activation, Guid readerTag = default)
    {
        var high = LeafAuthorFloorLocked(leafId);
        if (_leafAuthors is not null && _leafAuthors.TryGetValue(leafId, out var authors))
        {
            high = Math.Max(high, authors.ForeignHigh(activation, readerTag));
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
                highs[i++] = entry.AllHigh;
            }

            Array.Sort(highs, 0, count);

            // Ties at the cut are evicted with it, so a fold always frees at least half.
            var cut = highs[count - (count / 2) - 1];
            foreach (var (leaf, entry) in authors)
            {
                // The entry's overall high, forwards included (#5837, EvictionDropsForwardHigh).
                if (entry.AllHigh <= cut)
                {
                    if (_leafAuthorBucketFloors is { } floors)
                    {
                        var bucket = LeafAuthorBucket(leaf, floors.Length);
                        floors[bucket] = Math.Max(floors[bucket], entry.AllHigh);
                    }

                    _leafAuthorsFloor = Math.Max(_leafAuthorsFloor, entry.AllHigh);
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
    internal long ForeignHighForTesting(Guid leafId, Guid activation, Guid readerTag = default)
    {
        lock (_stateGate)
        {
            return ForeignHighLocked(leafId, activation, readerTag);
        }
    }

    /// <summary>Reads <see cref="StaleHighLocked"/> under the lock; test seam.</summary>
    internal long StaleHighForTesting(WalPendingClose close, Guid receiver)
    {
        lock (_stateGate)
        {
            return StaleHighLocked(in close, receiver);
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
    /// <c>High1 &gt;= High2</c>, and <c>Token1 != Token2</c> whenever both are set; and the
    /// records it forwarded to an unlinked sibling (#5837, condition 4), as the highs of
    /// its two highest narrow tags (<c>ForwardTag1</c>, <c>ForwardTag2</c>) and a
    /// <c>ForwardFolded</c> high for every tag displaced from them (X6), which is never
    /// excluded. For the per-donor stale high, <c>StaleFolded</c> holds every displaced
    /// activation's high, which is always counted.
    /// </summary>
    internal readonly record struct LeafAuthors(
        Guid Token1,
        long High1,
        Guid Token2,
        long High2,
        Guid ForwardTag1 = default,
        long ForwardHigh1 = -1,
        Guid ForwardTag2 = default,
        long ForwardHigh2 = -1,
        long ForwardFolded = -1,
        long StaleFolded = -1)
    {
        public static LeafAuthors Empty => new(Guid.Empty, -1, Guid.Empty, -1);

        /// <summary>The highest offset of any record attributed to the leaf, forwards included.</summary>
        public long AllHigh => Math.Max(
            Math.Max(Math.Max(High1, StaleFolded), ForwardFolded),
            Math.Max(ForwardHigh1, ForwardHigh2));

        /// <summary>
        /// The stale high for a close written by <paramref name="closingToken"/>, seen by the
        /// receiver <paramref name="receiver"/>; see <c>StaleHighLocked</c>. While the close
        /// is pending, every record of every other activation counts, whatever routing
        /// generation admitted it (a5e2e2ef's B1): a link elsewhere in the shard raises the
        /// generation, and a stale duplicate's moved-key write admitted under the raised one
        /// still sits above the close head.
        /// </summary>
        public long StaleHigh(Guid closingToken, Guid receiver)
        {
            var high = Math.Max(StaleFolded, ForwardFolded);
            if (false && High1 >= 0 && (Token1 != closingToken || closingToken == Guid.Empty)) // RED-PROOF M18 (StaleHighFiltersByAdmittedGeneration): the latest token's high is filtered out
            {
                high = Math.Max(high, High1);
            }

            if (High2 >= 0 && (Token2 != closingToken || closingToken == Guid.Empty))
            {
                high = Math.Max(high, High2);
            }

            // Only the receiver's own creating split moved those keys to it (#5837, F2').
            if (ForwardHigh1 >= 0 && (receiver == Guid.Empty || ForwardTag1 != receiver))
            {
                high = Math.Max(high, ForwardHigh1);
            }

            if (ForwardHigh2 >= 0 && (receiver == Guid.Empty || ForwardTag2 != receiver))
            {
                high = Math.Max(high, ForwardHigh2);
            }

            return high;
        }

        /// <summary>The highest offset by any activation other than <paramref name="activation"/>.</summary>
        public long ForeignHigh(Guid activation) =>
            High1 >= 0 && Token1 == activation && activation != Guid.Empty ? High2 : High1;

        /// <summary>
        /// The highest offset by any activation other than <paramref name="activation"/>,
        /// or forwarded by the leaf under any narrow tag but <paramref name="readerTag"/>
        /// (#5837, X1-X4). A forward has no activation token, so it is foreign to every
        /// activation; the reader's own narrow moved its keys away, so those forwards are
        /// excluded. A displaced tag's forwards are never excluded (X6).
        /// </summary>
        public long ForeignHigh(Guid activation, Guid readerTag)
        {
            var high = Math.Max(ForeignHigh(activation), ForwardFolded);
            if (ForwardHigh1 >= 0 && (readerTag == Guid.Empty || ForwardTag1 != readerTag))
            {
                high = Math.Max(high, ForwardHigh1);
            }

            if (ForwardHigh2 >= 0 && (readerTag == Guid.Empty || ForwardTag2 != readerTag))
            {
                high = Math.Max(high, ForwardHigh2);
            }

            return high;
        }

        /// <summary>
        /// This table with a record the leaf forwarded at <paramref name="offset"/> noted, as
        /// hop <paramref name="hop"/> (1-based) of the write's forward chain (#5837, F2' and
        /// a5e2e2ef's S-b): the first hop under its narrow <paramref name="tag"/>, which only
        /// that narrow's reader may exclude; a chain hop (2 or later) folds into the high no
        /// reader excludes.
        /// </summary>
        public LeafAuthors WithForwardHop(int hop, Guid tag, long offset) =>
            hop <= 1 ? WithForward(tag, offset) : this with { ForwardFolded = Math.Max(ForwardFolded, offset) };

        /// <summary>This table with a record the leaf forwarded under <paramref name="tag"/> at <paramref name="offset"/> noted.</summary>
        public LeafAuthors WithForward(Guid tag, long offset)
        {
            if (ForwardHigh1 >= 0 && tag == ForwardTag1)
            {
                return offset > ForwardHigh1 ? this with { ForwardHigh1 = offset } : this;
            }

            if (ForwardHigh2 >= 0 && tag == ForwardTag2)
            {
                return offset > ForwardHigh2 ? this with { ForwardHigh2 = offset } : this;
            }

            if (ForwardHigh1 < 0)
            {
                return this with { ForwardTag1 = tag, ForwardHigh1 = offset };
            }

            if (ForwardHigh2 < 0)
            {
                return this with { ForwardTag2 = tag, ForwardHigh2 = offset };
            }

            // A third tag displaces the lower, whose high folds into the never-excluded high (X6).
            return ForwardHigh1 <= ForwardHigh2
                ? this with { ForwardFolded = Math.Max(ForwardFolded, ForwardHigh1), ForwardTag1 = tag, ForwardHigh1 = offset }
                : this with { ForwardFolded = Math.Max(ForwardFolded, ForwardHigh2), ForwardTag2 = tag, ForwardHigh2 = offset };
        }

        /// <summary>
        /// This table with <paramref name="activation"/>'s record at <paramref name="offset"/>
        /// noted. A third activation displaces the lower of the two tracked, whose high folds
        /// into the always-counted <c>StaleFolded</c>.
        /// </summary>
        public LeafAuthors With(Guid activation, long offset)
        {
            // 'with' keeps the forward fields (#5837).
            if (High1 < 0)
            {
                return this with { Token1 = activation, High1 = offset, Token2 = Guid.Empty, High2 = -1 };
            }

            if (activation == Token1)
            {
                return offset > High1 ? this with { High1 = offset } : this;
            }

            if (High2 >= 0 && activation == Token2)
            {
                return offset > High1
                    ? this with { Token1 = activation, High1 = offset, Token2 = Token1, High2 = High1 }
                    : offset > High2 ? this with { High2 = offset } : this;
            }

            if (offset > High1)
            {
                return this with
                {
                    Token1 = activation,
                    High1 = offset,
                    Token2 = Token1,
                    High2 = High1,
                    StaleFolded = Math.Max(StaleFolded, High2),
                };
            }

            return offset > High2
                ? this with { Token2 = activation, High2 = offset, StaleFolded = Math.Max(StaleFolded, High2) }
                : this with { StaleFolded = Math.Max(StaleFolded, offset) };
        }
    }
}
