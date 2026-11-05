using System.Buffers;

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
/// it, the half of the entries whose highest offsets are lowest is evicted, and the
/// highest offset among them becomes the floor every leaf's foreign high is read against
/// (<c>EvictionDropsForeignHigh</c>), which is conservative: it can only hold an arm back,
/// never let one pass an unreplayed record. Evicting the older half, never the whole table
/// (review S2), keeps that floor near the median rather than the newest record, so a WAL
/// shard serving far more leaves than the bound - one shard serves a whole (tree,
/// partition) - does not hold every arm back to its newest record, and folds once per
/// half-table of new leaves at an amortised cost of a sort.
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

    private Dictionary<Guid, LeafAuthors>? _leafAuthors;

    // Everything at or below it is unattributed: the head recovered at activation, minus
    // one, raised by any fold of the table.
    private long _leafAuthorsFloor = -1;

    /// <summary>Starts attribution afresh at activation, below the recovered head. Called before any append is admitted.</summary>
    private void ResetLeafAuthors(long recoveredNextOffset)
    {
        _leafAuthors = null;
        _leafAuthorsFloor = recoveredNextOffset - 1;
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
        if (stamp.Forwarder != Guid.Empty && stamp.Forwarder != stamp.LeafId)
        {
            NoteForwardLocked(stamp.Forwarder, stamp.ForwarderTag, offset);
            authors = _leafAuthors.TryGetValue(stamp.LeafId, out var current) ? current : authors;
        }

        return stamp.Activation == Guid.Empty
            ? -1
            : Math.Max(_leafAuthorsFloor, authors.ForeignHigh(stamp.Activation, stamp.ReaderTag));
    }

    /// <summary>
    /// Attributes a forwarded record to its forwarder as well (#5837, condition 4): under
    /// no activation token, by the narrow that moved its key (X2). Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private void NoteForwardLocked(Guid forwarder, Guid tag, long offset)
    {
        if (!_leafAuthors!.TryGetValue(forwarder, out var authors))
        {
            if (_leafAuthors.Count >= MaxLeafAuthors)
            {
                FoldLeafAuthorsLocked();
            }

            authors = LeafAuthors.Empty;
        }

        _leafAuthors[forwarder] = authors.WithForward(tag, offset);
    }

    /// <summary>
    /// The highest offset of a record of <paramref name="leafId"/> that an activation
    /// other than <paramref name="activation"/> may have allocated here: the attributed
    /// high of every other activation, never below the unattributed floor. Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long ForeignHighLocked(Guid leafId, Guid activation, Guid readerTag = default)
    {
        var high = _leafAuthorsFloor;
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
    /// every entry of a table of one - raising the floor to the highest offset an evicted
    /// entry held (review S2).
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
    /// excluded.
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
        long ForwardFolded = -1)
    {
        public static LeafAuthors Empty => new(Guid.Empty, -1, Guid.Empty, -1);

        /// <summary>The highest offset of any record attributed to the leaf, forwards included.</summary>
        public long AllHigh => Math.Max(Math.Max(High1, ForwardFolded), Math.Max(ForwardHigh1, ForwardHigh2));

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
