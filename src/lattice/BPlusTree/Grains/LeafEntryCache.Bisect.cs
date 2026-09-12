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
    /// The pivot is the frame's median row rather than the median of the union
    /// of frame and materialised rows, so a leaf carrying writes taken since
    /// it attached may divide slightly off-centre. That is a balance property,
    /// not a correctness one - <c>SplitAsync</c> needs a pivot that leaves both
    /// sides non-empty, not an exact median - and the alternative is to pay for
    /// the whole leaf to place the cut.
    /// </para>
    /// </summary>
    /// <param name="key">Receives the bisecting key on success.</param>
    internal bool TryGetBisectingKeyWithoutHydrating(out string key)
    {
        key = string.Empty;
        var source = _hydration;
        if (source is null)
        {
            return false;
        }

        var rowCount = source.RowCount;
        if (rowCount < 2)
        {
            return false;
        }

        if (!source.TryReadRowKeyAt(rowCount / 2, out var candidate))
        {
            return false;
        }

        // The frame is strictly ascending by key (enforced at TryCreate via
        // LeafSnapshotCodec.IsAscendingByKey), and rowCount >= 2 puts the
        // midpoint at index >= 1, so the candidate sorts strictly above the
        // frame's first row. Establishing that some key really does sort below
        // it then reduces to establishing that the frame's first row, or some
        // materialised row below the candidate, is still present.
        //
        // Checked rather than assumed, and deliberately kept even though
        // reverting it reddens nothing reached from the grain.
        //
        // Its unreachability from SplitAsync is INCIDENTAL, NOT STRUCTURAL. It
        // holds only because the frame is strictly ascending, so a materialised
        // block 0 necessarily sorts below the frame's median - remove that one
        // property and the refusal path is live. Anyone re-running a
        // perturbation analysis from the grain side will therefore find this
        // clause "dead" and be tempted to delete it. It is not dead: this is a
        // cache-surface operation, so the guard protects every caller it will
        // ever have, not the single caller it has today, and the state it
        // refuses is reachable directly at that surface - which is what
        // A_pivot_with_nothing_below_it_is_refused_rather_than_returned
        // constructs and pins by name.
        //
        // What it prevents is not a new invariant. An unguarded bisect could
        // return a pivot with nothing below it, migrating every entry and
        // leaving an empty donor - the non-terminating shape
        // IsLeafOverCapacity's Count > 1 conjunct already exists to exclude.
        // This enforces that same existing invariant at a second seam.
        bool someKeySortsBelow;
        if (!source.IsHydrated(0))
        {
            // The frame's first row is still owned by the source, so it is
            // present in the projection and sorts below the candidate.
            someKeySortsBelow = true;
        }
        else
        {
            someKeySortsBelow = TryGetFirstMaterialisedKey(out var firstMaterialised)
                && string.CompareOrdinal(firstMaterialised, candidate) < 0;
        }

        if (!someKeySortsBelow)
        {
            return false;
        }

        key = candidate;
        return true;
    }

    /// <summary>
    /// Returns the interior keys at which a transfer starting from
    /// <paramref name="startInclusive"/> should be cut into batches, so that
    /// each batch's decoded footprint stays near
    /// <paramref name="targetBatchBytes"/>. Reads only frame keys, so no
    /// hydration block is materialised and no payload is decoded.
    /// <para>
    /// The batch width is derived at runtime from this frame's own measured
    /// mean row footprint, so it adapts to the data rather than to a tuned
    /// constant, and it is rounded up to a whole number of hydration blocks
    /// because a block is the unit of both materialisation and eviction.
    /// </para>
    /// <para>
    /// An empty result means "transfer in one pass": either nothing is lazily
    /// hydrated, or the whole remaining range already fits a single batch.
    /// </para>
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound of the transfer.</param>
    /// <param name="targetBatchBytes">Target decoded footprint per batch; non-positive returns empty.</param>
    internal IReadOnlyList<string> GetTransferBatchBoundariesWithoutHydrating(
        string startInclusive,
        long targetBatchBytes)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);

        var source = _hydration;
        if (source is null || targetBatchBytes <= 0)
        {
            return [];
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
        var boundaries = new List<string>();
        for (var index = lowerBound + stride; index < rowCount; index += stride)
        {
            if (source.TryReadRowKeyAt(index, out var boundary))
            {
                boundaries.Add(boundary);
            }
        }

        return boundaries;
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

        var windows = new List<(string?, string?)>();
        string? start = null;
        for (var index = (int)rowsPerWindow; index < rowCount; index += (int)rowsPerWindow)
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
    /// The lowest key currently materialised into the backing dictionary, or
    /// <see langword="false"/> when nothing is materialised. Reads the sorted
    /// dictionary directly and never hydrates.
    /// <para>
    /// Deliberately does not drain deferred rows: a deferred row's key is
    /// already present in the dictionary and only its bytes are absent, so a
    /// key-only question is answerable without paying to re-serialise them.
    /// </para>
    /// </summary>
    /// <param name="key">Receives the lowest materialised key on success.</param>
    private bool TryGetFirstMaterialisedKey(out string key)
    {
        foreach (var candidate in _rows.Keys)
        {
            key = candidate;
            return true;
        }

        key = string.Empty;
        return false;
    }
}
