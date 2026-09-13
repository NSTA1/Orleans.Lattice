namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Why <c>LeafEntryCache.TryGetBisectingKeyWithoutHydrating</c> declined to
/// supply a split pivot from the snapshot frame alone.
/// <para>
/// A refusal is never an error: the caller falls back to the ordered view. The
/// reason matters because the fallback's cost is not uniform across them.
/// <see cref="NoSnapshotAttached"/> is the only reason that can indicate a
/// forfeited fast path, and even then only on a leaf that had a frame to begin
/// with - a leaf replayed from the write-ahead log never attaches one, so its
/// rows are already resident and the fallback costs nothing extra. Every other
/// reason describes a frame that is present but unusable, which is a property
/// of the frame's shape rather than of the leaf's size.
/// </para>
/// </summary>
internal enum LeafBisectRefusalReason
{
    /// <summary>No refusal: a pivot was established from the frame alone.</summary>
    None = 0,

    /// <summary>
    /// No lazily hydrated snapshot is attached, so there is no frame to bisect.
    /// Either the leaf never attached one, or an earlier whole-cache operation
    /// detached it for the life of the activation. The two are distinguished by
    /// <c>LeafEntryCache.LastDetachSeam</c>, which stays
    /// <see cref="LeafSnapshotDetachSeam.None"/> in the first case.
    /// </summary>
    NoSnapshotAttached = 1,

    /// <summary>The frame declares fewer than two rows, so it has no interior pivot.</summary>
    TooFewRows = 2,

    /// <summary>The frame's median row key could not be read.</summary>
    FrameKeyUnreadable = 3,

    /// <summary>
    /// No key sorts below the candidate pivot, so returning it would migrate
    /// every entry and leave an empty donor.
    /// </summary>
    NoKeySortsBelowPivot = 4,
}
