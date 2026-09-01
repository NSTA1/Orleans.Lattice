using System.Buffers;
using System.Text;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounded, seekable backing store for a lazily hydrated
/// <see cref="LeafEntryCache"/>. Holds the validated
/// <see cref="LeafSnapshotCodec"/> frame a leaf activated over, tracks which
/// fixed-size runs of rows ("blocks") have been materialised into the cache,
/// and answers the seeks a key-scoped hydration needs.
/// <para>
/// The whole point is that activation cost stops being a function of blob
/// size. The frame's trailing index table gives every row an absolute start
/// offset, so a key lookup is a binary search over key probes (each of which
/// reads a key slice and never the payload) followed by decoding only the rows
/// actually wanted. A leaf that answers one key therefore pays for one block,
/// not for the whole leaf.
/// </para>
/// <para>
/// Two invariants make eviction safe, and both are enforced here rather than
/// by convention:
/// </para>
/// <list type="number">
/// <item><description>
/// A block is only installed for a frame whose rows are in <em>strictly</em>
/// ascending ordinal key order (<see cref="LeafSnapshotCodec.IsAscendingByKey"/>).
/// A seek over an unsorted or duplicate-bearing frame would silently miss rows
/// that are present, which is a data-invisibility bug rather than a slow path,
/// so such a frame is refused outright and the caller falls back to a full
/// decode.
/// </description></item>
/// <item><description>
/// A block that has taken any mutation is <see cref="Pin(int)">pinned</see> and
/// never evicted. Evicting it would drop the mutated row and let the next
/// hydration resurrect the stale snapshot value underneath it. Only clean
/// blocks - whose rows are still byte-identical to the frame - can be dropped
/// and re-read.
/// </description></item>
/// </list>
/// </summary>
internal sealed class LeafSnapshotHydrationSource
{
    /// <summary>
    /// Rows per hydration block. Blocks are the unit of both materialisation
    /// and eviction: a point read hydrates the one block its key falls in, and
    /// eviction drops whole clean blocks. Fixed rather than configurable so the
    /// per-block bookkeeping arrays are sized once at install time; 32 keeps a
    /// point read's over-read small while amortising the pooled key buffer and
    /// the block bitmaps over enough rows to stay negligible.
    /// </summary>
    internal const int BlockRows = 32;

    private readonly byte[] _frame;
    private readonly int _rowCount;
    private readonly int _blockCount;
    private readonly bool[] _hydrated;
    private readonly bool[] _pinned;
    private readonly int[] _touch;
    private readonly string[]?[] _blockKeys;
    private int _hydratedBlocks;
    private int _clock;
    private long _bytesRead;
    private long _rowsMaterialised;
    private long _seeks;

    private LeafSnapshotHydrationSource(byte[] frame, int rowCount, long stateBytes, long liveRows)
    {
        _frame = frame;
        _rowCount = rowCount;
        TotalStateBytes = stateBytes;
        TotalLiveRows = liveRows;
        _blockCount = rowCount == 0 ? 0 : ((rowCount - 1) / BlockRows) + 1;
        _hydrated = _blockCount == 0 ? [] : new bool[_blockCount];
        _pinned = _blockCount == 0 ? [] : new bool[_blockCount];
        _touch = _blockCount == 0 ? [] : new int[_blockCount];
        _blockKeys = _blockCount == 0 ? [] : new string[_blockCount][];
    }

    /// <summary>
    /// Wraps <paramref name="frame"/> as a hydration source, or returns
    /// <see langword="false"/> when the frame cannot safely back a bounded
    /// read: an unreadable header or row region, or rows that are not strictly
    /// ascending by ordinal key. A refusal is never an error - the caller falls
    /// back to decoding the frame in full, which is exactly today's behaviour.
    /// </summary>
    /// <param name="frame">Validated frame bytes.</param>
    /// <param name="source">Receives the source on success.</param>
    internal static bool TryCreate(byte[] frame, out LeafSnapshotHydrationSource source)
    {
        source = null!;
        if (frame is null
            || !LeafSnapshotCodec.TryGetRowCount(frame, out var rowCount)
            || !LeafSnapshotCodec.TryComputeCacheAggregates(frame, out var stateBytes, out var liveRows)
            || !LeafSnapshotCodec.IsAscendingByKey(frame))
        {
            return false;
        }

        source = new LeafSnapshotHydrationSource(frame, rowCount, stateBytes, liveRows);
        return true;
    }

    /// <summary>Total number of rows the frame carries.</summary>
    internal int RowCount => _rowCount;

    /// <summary>
    /// The frame this source reads through. Exposed so a caller that has to
    /// abandon bounded reads can fall back to streaming the whole frame.
    /// </summary>
    internal byte[] Frame => _frame;

    /// <summary>Number of hydration blocks the frame is divided into.</summary>
    internal int BlockCount => _blockCount;

    /// <summary>
    /// Summed logical payload footprint of every row in the frame, using the
    /// same formula as <see cref="LeafEntryCache.EntryBytes"/>.
    /// </summary>
    internal long TotalStateBytes { get; }

    /// <summary>Number of non-tombstone rows in the frame.</summary>
    internal long TotalLiveRows { get; }

    /// <summary>Number of blocks currently materialised into the cache.</summary>
    internal int HydratedBlockCount => _hydratedBlocks;

    /// <summary><see langword="true"/> once every block has been materialised.</summary>
    internal bool IsFullyHydrated => _hydratedBlocks == _blockCount;

    /// <summary>
    /// Running total of frame bytes actually consumed by decoded rows. The
    /// evidence that a bounded hydration is bounded: it grows with the rows a
    /// caller asked for and not with the size of the blob.
    /// </summary>
    internal long BytesRead => _bytesRead;

    /// <summary>Running count of rows decoded out of the frame.</summary>
    internal long RowsMaterialised => _rowsMaterialised;

    /// <summary>
    /// Running count of key seeks performed. A seek is a binary search over the
    /// index table costing <c>O(log n)</c> allocation-free key probes and
    /// reading no payload at all, which is why it does not move
    /// <see cref="BytesRead"/>.
    /// </summary>
    internal long Seeks => _seeks;

    /// <summary>Index of the block holding row <paramref name="rowIndex"/>.</summary>
    /// <param name="rowIndex">Zero-based row index.</param>
    internal static int BlockOf(int rowIndex) => rowIndex / BlockRows;

    /// <summary>First row index of <paramref name="block"/>.</summary>
    /// <param name="block">Zero-based block index.</param>
    internal static int BlockStart(int block) => block * BlockRows;

    /// <summary>Exclusive last row index of <paramref name="block"/>.</summary>
    /// <param name="block">Zero-based block index.</param>
    internal int BlockEndExclusive(int block) => Math.Min(_rowCount, BlockStart(block) + BlockRows);

    /// <summary>Whether <paramref name="block"/> has been materialised.</summary>
    /// <param name="block">Zero-based block index.</param>
    internal bool IsHydrated(int block) => (uint)block < (uint)_blockCount && _hydrated[block];

    /// <summary>Whether <paramref name="block"/> is pinned against eviction.</summary>
    /// <param name="block">Zero-based block index.</param>
    internal bool IsPinned(int block) => (uint)block < (uint)_blockCount && _pinned[block];

    /// <summary>
    /// Pins <paramref name="block"/> so it can never be evicted. Called for any
    /// block whose rows have been mutated since they were read out of the
    /// frame, because re-reading such a block would resurrect the snapshot
    /// value the mutation replaced.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal void Pin(int block)
    {
        if ((uint)block < (uint)_blockCount)
        {
            _pinned[block] = true;
        }
    }

    /// <summary>
    /// Records that <paramref name="block"/> was touched by the current
    /// operation, so eviction can drop the least recently used clean block
    /// first.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal void Touch(int block)
    {
        if ((uint)block < (uint)_blockCount)
        {
            _touch[block] = ++_clock;
        }
    }

    /// <summary>
    /// Finds the zero-based index of the first row whose key is greater than or
    /// equal to <paramref name="keyUtf8"/>, allocating nothing. A key past the
    /// last row yields <see cref="RowCount"/>.
    /// </summary>
    /// <param name="keyUtf8">Inclusive lower-bound key, UTF-8 encoded.</param>
    /// <param name="index">Receives the lower-bound row index on success.</param>
    internal bool TryFindLowerBound(ReadOnlySpan<byte> keyUtf8, out int index)
    {
        _seeks++;
        return LeafSnapshotCodec.TryFindFirstRowAtOrAfter(_frame, keyUtf8, out index);
    }

    /// <summary>
    /// Reports whether the row at <paramref name="index"/> carries exactly
    /// <paramref name="keyUtf8"/>, without decoding the row or allocating.
    /// </summary>
    /// <param name="index">Zero-based row index.</param>
    /// <param name="keyUtf8">Key to compare, UTF-8 encoded.</param>
    internal bool RowKeyEquals(int index, ReadOnlySpan<byte> keyUtf8)
    {
        if (!LeafSnapshotCodec.TryReadRowKeyUtf8At(_frame, index, out var probe))
        {
            return false;
        }

        return probe.SequenceEqual(keyUtf8);
    }

    /// <summary>
    /// Decodes only the key of the row at <paramref name="index"/>, leaving its
    /// payload untouched. Used by key-only walks, which would otherwise pay for
    /// every value in the leaf to answer a question about keys.
    /// </summary>
    /// <param name="index">Zero-based row index.</param>
    /// <param name="key">Receives the decoded key on success.</param>
    internal bool TryReadRowKeyAt(int index, out string key)
    {
        if (!LeafSnapshotCodec.TryReadRowKeyUtf8At(_frame, index, out var keyUtf8))
        {
            key = string.Empty;
            return false;
        }

        key = Encoding.UTF8.GetString(keyUtf8);
        return true;
    }

    /// <summary>
    /// Decodes the row at <paramref name="index"/>, accounting the frame bytes
    /// the decode consumed.
    /// </summary>
    /// <param name="index">Zero-based row index.</param>
    /// <param name="row">Receives the decoded row on success.</param>
    internal bool TryReadRowAt(int index, out LeafSnapshotRow row)
    {
        if (!LeafSnapshotCodec.TryReadRowAt(_frame, index, out row, out var bytesConsumed))
        {
            return false;
        }

        _bytesRead += bytesConsumed;
        _rowsMaterialised++;
        return true;
    }

    /// <summary>
    /// Rents the per-block key buffer the caller fills while materialising
    /// <paramref name="block"/>. Ownership passes to this source on
    /// <see cref="CommitHydrated(int)"/>; the buffer records which keys the
    /// block put into the cache so an eviction can remove exactly those and
    /// nothing else.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal string[] BeginHydrate(int block)
    {
        var buffer = ArrayPool<string>.Shared.Rent(BlockRows);
        _blockKeys[block] = buffer;
        return buffer;
    }

    /// <summary>
    /// Marks <paramref name="block"/> materialised after
    /// <see cref="BeginHydrate(int)"/>'s buffer has been filled.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal void CommitHydrated(int block)
    {
        if (_hydrated[block])
        {
            return;
        }

        _hydrated[block] = true;
        _hydratedBlocks++;
        Touch(block);
    }

    /// <summary>
    /// Abandons a hydration that <see cref="BeginHydrate(int)"/> started but
    /// did not commit, returning the pooled buffer. Used when a row fails to
    /// decode, which can only mean the frame was installed without validation.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal void AbandonHydrate(int block)
    {
        ReturnBuffer(block);
    }

    /// <summary>
    /// The keys <paramref name="block"/> materialised into the cache, in frame
    /// order. Valid only while the block is hydrated.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal ReadOnlySpan<string> HydratedKeys(int block)
    {
        var buffer = _blockKeys[block];
        return buffer is null
            ? []
            : buffer.AsSpan(0, BlockEndExclusive(block) - BlockStart(block));
    }

    /// <summary>
    /// Selects the least recently used evictable block - hydrated, unpinned,
    /// and outside the range the current operation is working on - or returns
    /// <see langword="false"/> when nothing may be dropped.
    /// </summary>
    /// <param name="protectedFirst">First block index the caller is using and must keep.</param>
    /// <param name="protectedLastInclusive">Last block index the caller is using and must keep.</param>
    /// <param name="block">Receives the block to evict on success.</param>
    internal bool TrySelectEvictionCandidate(int protectedFirst, int protectedLastInclusive, out int block)
    {
        block = -1;
        var best = int.MaxValue;
        for (var i = 0; i < _blockCount; i++)
        {
            if (!_hydrated[i] || _pinned[i] || (i >= protectedFirst && i <= protectedLastInclusive))
            {
                continue;
            }

            if (_touch[i] < best)
            {
                best = _touch[i];
                block = i;
            }
        }

        return block >= 0;
    }

    /// <summary>
    /// Marks <paramref name="block"/> no longer materialised and releases its
    /// pooled key buffer. The caller must already have removed the block's keys
    /// from the cache.
    /// </summary>
    /// <param name="block">Zero-based block index.</param>
    internal void MarkEvicted(int block)
    {
        if (!_hydrated[block])
        {
            return;
        }

        _hydrated[block] = false;
        _hydratedBlocks--;
        ReturnBuffer(block);
    }

    /// <summary>
    /// Releases every pooled key buffer this source holds. Called when the
    /// cache drops the source outright (a <c>Clear</c> or a fresh rehydrate).
    /// </summary>
    internal void Release()
    {
        for (var i = 0; i < _blockCount; i++)
        {
            ReturnBuffer(i);
        }
    }

    private void ReturnBuffer(int block)
    {
        var buffer = _blockKeys[block];
        if (buffer is null)
        {
            return;
        }

        _blockKeys[block] = null;
        // Clear on return: the buffer holds key string references, and a
        // pooled array that kept them alive would pin a released snapshot.
        ArrayPool<string>.Shared.Return(buffer, clearArray: true);
    }
}
