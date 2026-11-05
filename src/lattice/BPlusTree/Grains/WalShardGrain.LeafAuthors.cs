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

        var shard = stamp.Initiator.Kind == WalRoutingInitiatorKind.ShardRoot ? stamp.Initiator.Index : -1;
        authors = authors.With(stamp.Activation, offset, shard, shard >= 0 ? stamp.Generation : 0);
        _leafAuthors[stamp.LeafId] = authors;
        if (stamp.Forwarder != Guid.Empty)
        {
            // F2': every forwarder on the chain, each by its own narrow tag.
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder, stamp.ForwarderTag, offset);
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder2, stamp.ForwarderTag2, offset);
            NoteForwardLocked(stamp.LeafId, stamp.Forwarder3, stamp.ForwarderTag3, offset);
            authors = _leafAuthors.TryGetValue(stamp.LeafId, out var current) ? current : authors;
        }

        var reported = stamp.Activation == Guid.Empty
            ? -1
            : Math.Max(_leafAuthorsFloor, authors.ForeignHigh(stamp.Activation, stamp.ReaderTag));
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
    /// <paramref name="receiver"/>; never below the unattributed floor. A displaced
    /// generation or activation is always counted. Called under <see cref="_stateGate"/>.
    /// </summary>
    private long StaleHighLocked(in WalPendingClose close, Guid receiver)
    {
        var high = _leafAuthorsFloor;
        if (_leafAuthors is not null && _leafAuthors.TryGetValue(close.Donor, out var authors))
        {
            high = Math.Max(high, authors.StaleHigh(close.Token, close.ShardIndex, close.Generation, receiver));
        }

        return high;
    }

    /// <summary>
    /// Attributes a forwarded record to its forwarder as well (#5837, condition 4): under
    /// no activation token, by the narrow that moved its key (X2). Called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private void NoteForwardLocked(Guid author, Guid forwarder, Guid tag, long offset)
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
    /// excluded. For the per-donor stale high, each tracked activation also keeps the
    /// highs of its two latest generations (<c>Gens1</c>, <c>Gens2</c>), and
    /// <c>StaleFolded</c> holds every displaced generation's and activation's high, which
    /// is always counted.
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
        GenerationHighs Gens1 = default,
        GenerationHighs Gens2 = default,
        long StaleFolded = -1)
    {
        public static LeafAuthors Empty => new(Guid.Empty, -1, Guid.Empty, -1);

        /// <summary>The highest offset of any record attributed to the leaf, forwards included.</summary>
        public long AllHigh => Math.Max(
            Math.Max(Math.Max(High1, StaleFolded), ForwardFolded),
            Math.Max(ForwardHigh1, ForwardHigh2));

        /// <summary>
        /// The stale high for a close written by <paramref name="closingToken"/> at shard
        /// <paramref name="shardIndex"/>'s generation <paramref name="generation"/>, seen by
        /// the receiver <paramref name="receiver"/>; see <c>StaleHighLocked</c>.
        /// </summary>
        public long StaleHigh(Guid closingToken, int shardIndex, long generation, Guid receiver)
        {
            var high = Math.Max(StaleFolded, ForwardFolded);
            if (High1 >= 0 && (Token1 != closingToken || closingToken == Guid.Empty))
            {
                high = Math.Max(high, Gens1.HighAtOrBelow(shardIndex, generation));
            }

            if (High2 >= 0 && (Token2 != closingToken || closingToken == Guid.Empty))
            {
                high = Math.Max(high, Gens2.HighAtOrBelow(shardIndex, generation));
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

        /// <summary>This table with <paramref name="activation"/>'s record at <paramref name="offset"/> noted, as an epoch-only record.</summary>
        public LeafAuthors With(Guid activation, long offset) => With(activation, offset, -1, 0);

        /// <summary>
        /// This table with <paramref name="activation"/>'s record at <paramref name="offset"/>
        /// noted, admitted under shard <paramref name="shardIndex"/>'s generation
        /// <paramref name="generation"/> (<paramref name="shardIndex"/> -1 and generation 0
        /// for an epoch-only record).
        /// </summary>
        public LeafAuthors With(Guid activation, long offset, int shardIndex, long generation)
        {
            // 'with' keeps the forward fields (#5837).
            var folded = StaleFolded;
            if (High1 < 0)
            {
                return this with
                {
                    Token1 = activation,
                    High1 = offset,
                    Gens1 = default(GenerationHighs).With(shardIndex, generation, offset, ref folded),
                    Token2 = Guid.Empty,
                    High2 = -1,
                    Gens2 = default,
                    StaleFolded = folded,
                };
            }

            if (activation == Token1)
            {
                var gens = Gens1.With(shardIndex, generation, offset, ref folded);
                return this with { High1 = Math.Max(High1, offset), Gens1 = gens, StaleFolded = folded };
            }

            if (High2 >= 0 && activation == Token2)
            {
                var gens = Gens2.With(shardIndex, generation, offset, ref folded);
                return offset > High1
                    ? this with { Token1 = activation, High1 = offset, Gens1 = gens, Token2 = Token1, High2 = High1, Gens2 = Gens1, StaleFolded = folded }
                    : this with { High2 = Math.Max(High2, offset), Gens2 = gens, StaleFolded = folded };
            }

            // A third activation, or the second's first record: it displaces the lower,
            // whose generations fold into the always-counted high.
            var fresh = default(GenerationHighs).With(shardIndex, generation, offset, ref folded);
            if (offset > High1)
            {
                return this with
                {
                    Token1 = activation,
                    High1 = offset,
                    Gens1 = fresh,
                    Token2 = Token1,
                    High2 = High1,
                    Gens2 = Gens1,
                    StaleFolded = Math.Max(folded, High2),
                };
            }

            return offset > High2
                ? this with { Token2 = activation, High2 = offset, Gens2 = fresh, StaleFolded = Math.Max(folded, High2) }
                : this with { StaleFolded = Math.Max(folded, offset) };
        }
    }

    /// <summary>
    /// One activation's highs for its two latest routing generations (#5837, per-donor
    /// stale high), each keyed by the shard root whose generation it is (-1 and
    /// generation 0 for epoch-only records). An empty slot has <c>End</c> 0; a slot's
    /// high is <c>End - 1</c>.
    /// </summary>
    internal readonly record struct GenerationHighs(
        int ShardA,
        long GenerationA,
        long EndA,
        int ShardB,
        long GenerationB,
        long EndB)
    {
        /// <summary>
        /// The highest offset whose generation a close at shard <paramref name="shardIndex"/>'s
        /// generation <paramref name="generation"/> must count: every slot but one of that
        /// shard root's later generations.
        /// </summary>
        public long HighAtOrBelow(int shardIndex, long generation)
        {
            var high = -1L;
            if (EndA > 0 && (ShardA != shardIndex || GenerationA <= generation))
            {
                high = EndA - 1;
            }

            if (EndB > 0 && (ShardB != shardIndex || GenerationB <= generation))
            {
                high = Math.Max(high, EndB - 1);
            }

            return high;
        }

        /// <summary>
        /// These slots with a record at <paramref name="offset"/> noted; a generation they
        /// displace, or one older than both they hold, is folded into
        /// <paramref name="folded"/>.
        /// </summary>
        public GenerationHighs With(int shardIndex, long generation, long offset, ref long folded)
        {
            var end = offset + 1;
            if (EndA > 0 && ShardA == shardIndex && GenerationA == generation)
            {
                return end > EndA ? this with { EndA = end } : this;
            }

            if (EndB > 0 && ShardB == shardIndex && GenerationB == generation)
            {
                return end > EndB ? this with { EndB = end } : this;
            }

            if (EndA == 0)
            {
                return this with { ShardA = shardIndex, GenerationA = generation, EndA = end };
            }

            if (EndB == 0)
            {
                return this with { ShardB = shardIndex, GenerationB = generation, EndB = end };
            }

            if (generation < GenerationA && generation < GenerationB)
            {
                folded = Math.Max(folded, offset);
                return this;
            }

            if (GenerationA <= GenerationB)
            {
                folded = Math.Max(folded, EndA - 1);
                return this with { ShardA = shardIndex, GenerationA = generation, EndA = end };
            }

            folded = Math.Max(folded, EndB - 1);
            return this with { ShardB = shardIndex, GenerationB = generation, EndB = end };
        }
    }
}
