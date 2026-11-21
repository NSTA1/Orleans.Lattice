using System.Text;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Structural accessors that answer a question about a lazily hydrated
/// snapshot's <em>shape</em> without materialising its rows (issue #2771).
/// <para>
/// The whole-cache accessors - <see cref="Keys"/>, <see cref="EnumerateRows"/>
/// and <see cref="UnderlyingRows"/> - each call <see cref="HydrateAll"/>
/// unconditionally, so every one of them costs the whole leaf however little
/// of it the caller needs. That is invisible at a call site, because all three
/// read as projections over an in-memory dictionary, and it is what made
/// dividing an oversized leaf require first holding all of it. The accessors
/// here are the bounded alternative for the split path, and they are named so
/// the cost is legible where they are called rather than only where they are
/// defined.
/// </para>
/// </summary>
internal sealed partial class LeafEntryCache
{
    /// <summary>
    /// Returns a key that strictly bisects the cache - at least one key sorts
    /// below it and at least one key (itself) sorts at or above it - reading
    /// only the frame's ordinal index and decoding no payload, so no hydration
    /// block is materialised.
    /// <para>
    /// Returns <see langword="false"/> when no lazily hydrated snapshot is
    /// attached (the cache is already resident, so the ordinary ordered view
    /// costs nothing extra) or when a strictly interior pivot cannot be
    /// established from the frame alone. A refusal is never an error: the
    /// caller falls back to the ordered view, which is exactly today's
    /// behaviour.
    /// </para>
    /// <para>
    /// The pivot is the median of the whole leaf: the union of the frame rows
    /// still owned by the source and the rows resident in the dictionary,
    /// including writes taken since the frame attached (issue #5607). Taking
    /// the frame's own median instead divided a leaf that had grown past its
    /// snapshot far off-centre, leaving an undersized donor. The union is
    /// walked by key only - resident keys in order, frame rows counted per
    /// unhydrated block - so placing the cut still decodes no payload.
    /// </para>
    /// </summary>
    /// <param name="key">Receives the bisecting key on success.</param>
    /// <remarks>
    /// Convenience form for callers that do not need the refusal reason. The
    /// split path is not one of them - it has taken the reason-reporting
    /// overload since issue #2837, because a refusal's cost depends on which
    /// reason fired - so every caller of this form is a fixture. It is kept
    /// because eight assertions across four fixtures read better without a
    /// discard, and it must stay a pure delegation or those fixtures would
    /// assert about a different decision from the one production takes;
    /// <c>The_reason_reporting_overload_agrees_with_the_bare_one</c> pins that.
    /// </remarks>
    internal bool TryGetBisectingKeyWithoutHydrating(out string key)
        => TryGetBisectingKeyWithoutHydrating(out key, out _);

    /// <summary>
    /// As <see cref="TryGetBisectingKeyWithoutHydrating(out string)"/>, and also
    /// reports why a refusal occurred.
    /// <para>
    /// The reason is not diagnostic decoration. A refusal's cost depends
    /// entirely on which one fired, because it is decided by whether a frame is
    /// still attached (issue #2856).
    /// <see cref="LeafBisectRefusalReason.NoSnapshotAttached"/> is reported
    /// exactly when <c>_hydration</c> is null, so every row is already resident
    /// and the ordered-view fallback materialises nothing: on a leaf replayed
    /// from the write-ahead log nothing was ever forfeited, and on a leaf that
    /// had a frame <see cref="LastDetachSeam"/> names the earlier surface that
    /// already paid for the whole leaf. By contrast
    /// <see cref="LeafBisectRefusalReason.FrameKeyUnreadable"/> and
    /// <see cref="LeafBisectRefusalReason.NoKeySortsBelowPivot"/> refuse with a
    /// frame still attached, so the fallback itself materialises the whole
    /// remainder of the leaf and detaches the frame. Without the reason a
    /// caller sees only <see langword="false"/> and cannot tell an expensive
    /// refusal from a free one.
    /// </para>
    /// </summary>
    /// <param name="key">Receives the bisecting key on success.</param>
    /// <param name="reason">Receives the refusal reason, or
    /// <see cref="LeafBisectRefusalReason.None"/> on success.</param>
    internal bool TryGetBisectingKeyWithoutHydrating(out string key, out LeafBisectRefusalReason reason)
    {
        key = string.Empty;
        var source = _hydration;
        if (source is null)
        {
            reason = LeafBisectRefusalReason.NoSnapshotAttached;
            return false;
        }

        var count = Count;
        if (count < 2)
        {
            reason = LeafBisectRefusalReason.TooFewRows;
            return false;
        }

        // Issue #5607: the pivot is the median of the whole leaf - the union of
        // the frame rows still owned by the source and the rows resident in
        // the dictionary - not the frame's own median. The two differ as soon
        // as the leaf takes writes after attaching: a leaf reactivated from a
        // 16-row snapshot that then took 32 ascending writes divided at the
        // frame's 8th key and left an 8-row donor, so a leaf that should have
        // halved instead shed almost all of itself and split again at once.
        //
        // The union is disjoint by construction. A write hydrates the block it
        // lands in before storing (HydrateForKey), and eviction drops only
        // clean blocks, so no resident key is also a row of an unhydrated
        // block. Count is exactly the size of that union, which makes the
        // ordinal count/2 >= 1 the median with at least one key sorting below
        // it - the strictly interior pivot SplitAsync needs.
        //
        // The walk visits resident keys in order and, between consecutive
        // ones, counts the unhydrated frame rows that sort before each by a
        // lower-bound probe of the frame's ordinal index. It reads frame keys
        // only, decodes no payload and materialises no block.
        var target = count / 2;
        var below = 0;
        var frameCursor = 0;
        string? found = null;
        foreach (var resident in _rows.Keys)
        {
            var frameBound = LowerBound(source, resident);
            var unhydrated = UnhydratedRowsIn(source, frameCursor, frameBound);
            if (below + unhydrated > target)
            {
                if (!TryReadUnhydratedFrameKey(source, frameCursor, frameBound, target - below, out found))
                {
                    reason = LeafBisectRefusalReason.FrameKeyUnreadable;
                    return false;
                }

                break;
            }

            below += unhydrated;
            frameCursor = frameBound;
            if (below == target)
            {
                found = resident;
                break;
            }

            below++;
        }

        if (found is null)
        {
            var rowCount = source.RowCount;
            var unhydrated = UnhydratedRowsIn(source, frameCursor, rowCount);
            if (below + unhydrated <= target)
            {
                // The walk ran out of rows before reaching the median ordinal,
                // so the cache's row accounting disagrees with the rows it
                // holds. Returning any key here could leave an empty donor -
                // the non-terminating shape IsLeafOverCapacity's Count > 1
                // conjunct exists to exclude - so refuse and let the ordered
                // view decide from the rows themselves.
                reason = LeafBisectRefusalReason.NoKeySortsBelowPivot;
                return false;
            }

            if (!TryReadUnhydratedFrameKey(source, frameCursor, rowCount, target - below, out found))
            {
                reason = LeafBisectRefusalReason.FrameKeyUnreadable;
                return false;
            }
        }

        key = found;
        reason = LeafBisectRefusalReason.None;
        return true;
    }

    // The number of frame rows in [fromInclusive, toExclusive) that belong to a
    // block not currently hydrated, and so are owned by the source rather than
    // the resident dictionary. Whole blocks at a time; reads no key.
    private static int UnhydratedRowsIn(LeafSnapshotHydrationSource source, int fromInclusive, int toExclusive)
    {
        if (fromInclusive >= toExclusive)
        {
            return 0;
        }

        var total = 0;
        var lastBlock = LeafSnapshotHydrationSource.BlockOf(toExclusive - 1);
        for (var block = LeafSnapshotHydrationSource.BlockOf(fromInclusive); block <= lastBlock; block++)
        {
            if (source.IsHydrated(block))
            {
                continue;
            }

            var start = Math.Max(fromInclusive, LeafSnapshotHydrationSource.BlockStart(block));
            var end = Math.Min(toExclusive, source.BlockEndExclusive(block));
            total += end - start;
        }

        return total;
    }

    // Reads the key of the nth (0-based) unhydrated frame row in
    // [fromInclusive, toExclusive), skipping hydrated blocks whole. Decodes the
    // one key it returns and no payload.
    private static bool TryReadUnhydratedFrameKey(
        LeafSnapshotHydrationSource source,
        int fromInclusive,
        int toExclusive,
        int nth,
        out string key)
    {
        if (fromInclusive < toExclusive)
        {
            var remaining = nth;
            var lastBlock = LeafSnapshotHydrationSource.BlockOf(toExclusive - 1);
            for (var block = LeafSnapshotHydrationSource.BlockOf(fromInclusive); block <= lastBlock; block++)
            {
                if (source.IsHydrated(block))
                {
                    continue;
                }

                var start = Math.Max(fromInclusive, LeafSnapshotHydrationSource.BlockStart(block));
                var end = Math.Min(toExclusive, source.BlockEndExclusive(block));
                if (remaining < end - start)
                {
                    return source.TryReadRowKeyAt(start + remaining, out key);
                }

                remaining -= end - start;
            }
        }

        key = string.Empty;
        return false;
    }

    /// <summary>
    /// Returns the interior keys at which a transfer starting from
    /// <paramref name="startInclusive"/> should be cut into batches, so that
    /// each batch's decoded footprint stays near
    /// <paramref name="targetBatchBytes"/>. Reads frame keys when attached,
    /// otherwise resident rows and their accounted lengths; no hydration
    /// block is materialised and no deferred payload is decoded.
    /// <para>
    /// The batch width is derived at runtime from this frame's own measured
    /// mean row footprint, so it adapts to the data rather than to a tuned
    /// constant, and it is rounded up to a whole number of hydration blocks
    /// because a block is the unit of both materialisation and eviction.
    /// </para>
    /// <para>
    /// Without a frame, each batch is bounded by the actual row footprints,
    /// except that an indivisible oversized row travels alone. An empty result
    /// means batching is disabled or the remaining range fits a single batch,
    /// never that the absence of a frame prevented planning.
    /// </para>
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound of the transfer.</param>
    /// <param name="targetBatchBytes">Target decoded footprint per batch; non-positive returns empty.</param>
    internal IReadOnlyList<string> GetTransferBatchBoundariesWithoutHydrating(
        string startInclusive,
        long targetBatchBytes)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);

        if (targetBatchBytes <= 0)
        {
            return [];
        }

        var source = _hydration;
        if (source is null)
        {
            return GetResidentTransferBatchBoundaries(startInclusive, targetBatchBytes);
        }

        var rowCount = source.RowCount;
        if (rowCount == 0)
        {
            return [];
        }

        // Mean row footprint measured from this frame, not assumed. Both terms
        // are properties of the data in hand, which is what keeps the bound
        // adaptive: the same code divides a leaf of any size at the same peak.
        var averageRowBytes = Math.Max(1L, source.TotalStateBytes / rowCount);
        var rowsPerBatch = Math.Max(
            LeafSnapshotHydrationSource.BlockRows,
            targetBatchBytes / averageRowBytes);

        // Round up to whole hydration blocks so a batch never half-materialises
        // a block it then leaves resident.
        rowsPerBatch = ((rowsPerBatch + LeafSnapshotHydrationSource.BlockRows - 1)
            / LeafSnapshotHydrationSource.BlockRows) * LeafSnapshotHydrationSource.BlockRows;

        if (rowsPerBatch >= rowCount)
        {
            return [];
        }

        var keyUtf8 = Encoding.UTF8.GetBytes(startInclusive);
        if (!source.TryFindLowerBound(keyUtf8, out var lowerBound))
        {
            return [];
        }

        var stride = (int)rowsPerBatch;
        var firstBoundary = (long)lowerBound + stride;
        if (firstBoundary >= rowCount)
        {
            return [];
        }

        // The walk's length is a closed form of the stride and the row count, so
        // the list is sized exactly once rather than doubling its way there. A
        // leaf that bisects into tens of batches otherwise allocates - and then
        // discards - every intermediate backing array it grew through.
        var boundaries = new List<string>((int)((rowCount - firstBoundary + stride - 1) / stride));
        for (var index = (int)firstBoundary; index < rowCount; index += stride)
        {
            if (source.TryReadRowKeyAt(index, out var boundary))
            {
                boundaries.Add(boundary);
            }
        }

        return boundaries;
    }

    private IReadOnlyList<string> GetResidentTransferBatchBoundaries(string startInclusive, long targetBatchBytes)
    {
        if (_stateBytes <= targetBatchBytes)
        {
            return [];
        }

        List<string>? boundaries = null;
        long batchBytes = 0;
        var hasRows = false;
        // Do not use EnumerateRange: it drains deferred payloads. Their recorded
        // lengths suffice, and the existing key strings can be reused as cuts.
        foreach (var (key, row) in new RangeRows(_rows, startInclusive, null))
        {
            var rowBytes = AccountedRowBytes(key, row);
            if (hasRows && rowBytes > targetBatchBytes - batchBytes)
            {
                (boundaries ??= []).Add(key);
                batchBytes = 0;
            }
            batchBytes += rowBytes;
            hasRows = true;
        }
        return boundaries is null ? [] : boundaries;
    }

    /// <summary>
    /// Returns contiguous, half-open key windows that together cover every row
    /// the cache holds, sized so each window's decoded footprint stays near the
    /// cache's own resident budget. Reads only frame keys, so no hydration
    /// block is materialised and no payload is decoded.
    /// <para>
    /// This is the bounded substitute for <see cref="EnumerateRows"/> when a
    /// caller genuinely needs to visit every row but does not need to
    /// <em>retain</em> them. Walking these windows through
    /// <see cref="EnumerateRange"/> materialises each in turn and lets
    /// <c>TrimToBudget</c> evict the ones already visited, where
    /// <see cref="HydrateAll"/> ends in <c>DetachSnapshot</c> and is therefore
    /// irreversible: once it runs, every row is resident for the life of the
    /// activation and no later eviction can recover the footprint.
    /// </para>
    /// <para>
    /// The first window is unbounded below and the last unbounded above, so
    /// rows written since the snapshot attached - which lie outside the frame's
    /// key range entirely - still fall in exactly one window. Every row is
    /// visited exactly once, which is what lets an order-independent fold over
    /// these windows equal the same fold over the whole cache.
    /// </para>
    /// <para>
    /// Returns a single unbounded window when nothing is lazily hydrated, or
    /// when the cache has no resident budget to trim against. In both cases
    /// streaming buys nothing, and the single window makes the caller's loop
    /// degenerate to exactly today's one-pass behaviour.
    /// </para>
    /// </summary>
    internal IReadOnlyList<(string? StartInclusive, string? EndExclusive)> GetFullScanWindowsWithoutHydrating()
    {
        var source = _hydration;
        if (source is null || _residentBudgetBytes <= 0)
        {
            return [(null, null)];
        }

        var rowCount = source.RowCount;
        var rowsPerWindow = ComputeRowsPerBatch(source, _residentBudgetBytes);
        if (rowsPerWindow <= 0 || rowsPerWindow >= rowCount)
        {
            return [(null, null)];
        }

        var stride = (int)rowsPerWindow;

        // One window per stride, plus the final open-ended one: a closed form,
        // so the list is sized once instead of growing through every
        // intermediate array on a leaf wide enough to need many windows.
        var windows = new List<(string?, string?)>((rowCount + stride - 1) / stride);
        string? start = null;
        for (var index = stride; index < rowCount; index += stride)
        {
            if (!source.TryReadRowKeyAt(index, out var boundary))
            {
                break;
            }

            windows.Add((start, boundary));
            start = boundary;
        }

        windows.Add((start, null));
        return windows;
    }

    // Rows per bounded batch, derived from this frame's own measured mean row
    // footprint rather than from a tuned constant, then rounded up to whole
    // hydration blocks because a block is the unit of both materialisation and
    // eviction. Adaptive by construction: a 226 MB leaf and a 2 GB leaf yield
    // different row counts but the same peak footprint.
    private static long ComputeRowsPerBatch(LeafSnapshotHydrationSource source, long targetBatchBytes)
    {
        var rowCount = source.RowCount;
        if (rowCount == 0 || targetBatchBytes <= 0)
        {
            return 0;
        }

        var averageRowBytes = Math.Max(1L, source.TotalStateBytes / rowCount);
        var rowsPerBatch = Math.Max(
            LeafSnapshotHydrationSource.BlockRows,
            targetBatchBytes / averageRowBytes);

        return ((rowsPerBatch + LeafSnapshotHydrationSource.BlockRows - 1)
            / LeafSnapshotHydrationSource.BlockRows) * LeafSnapshotHydrationSource.BlockRows;
    }

    /// <summary>
    /// Returns the recorded per-key <see cref="LatticeMergeMode"/> for a row
    /// that is <em>already resident</em>, reading the merge-mode side-map
    /// directly and never hydrating, touching, or trimming.
    /// <para>
    /// This exists because <c>GetMergeMode</c> cannot be called from inside an
    /// <c>EnumerateRange</c> walk. It routes through <c>HydrateForKey</c>,
    /// which ends in <c>TrimToBudget</c>, and a trim removes rows from the
    /// backing dictionary - structurally modifying the very collection the
    /// walk's enumerator is positioned on. The hazard is invisible today only
    /// because every whole-cache walk calls <c>HydrateAll</c> first, which
    /// detaches the frame and makes <c>HydrateForKey</c> return at its null
    /// check; it appears the moment a caller stops detaching, which is exactly
    /// what the bounded window walk does.
    /// </para>
    /// <para>
    /// Equivalent to <c>GetMergeMode</c> for any key a walk is currently
    /// yielding, because materialising a block restores that block's recorded
    /// merge modes from the frame (<c>InsertHydratedRow</c>), so a resident row
    /// always carries its mode alongside it. For a key that is <em>not</em>
    /// resident this returns <see langword="null"/> rather than hydrating to
    /// find out, so it is only correct where residency is already established.
    /// </para>
    /// </summary>
    /// <param name="key">The entry key; must be resident.</param>
    internal LatticeMergeMode? GetMergeModeWithoutHydrating(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        return _mergeModes is not null && _mergeModes.TryGetValue(key, out var mode)
            ? mode
            : null;
    }
}
