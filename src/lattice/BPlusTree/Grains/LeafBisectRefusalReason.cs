namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Why <c>LeafEntryCache.TryGetBisectingKeyWithoutHydrating</c> declined to
/// supply a split pivot from the snapshot frame alone.
/// <para>
/// A refusal is never an error: the caller falls back to the ordered view
/// (<c>LeafEntryCache.Keys</c>), whose whole-cache hydration returns at once
/// when no frame is attached and otherwise materialises every row the frame
/// still owns and detaches it for the life of the activation. The fallback's
/// cost is therefore decided by whether a frame is still attached when the
/// refusal fires, and the reasons split cleanly on that line (issue #2856):
/// <see cref="NoSnapshotAttached"/> is reported exactly when no frame is
/// attached, so it is free at the refusal; every other reason describes a
/// frame that is still attached but unusable, and of those
/// <see cref="FrameKeyUnreadable"/> and <see cref="NoKeySortsBelowPivot"/> are
/// the refusals at which the fallback itself materialises the whole leaf.
/// </para>
/// </summary>
internal enum LeafBisectRefusalReason
{
    /// <summary>No refusal: a pivot was established from the frame alone.</summary>
    None = 0,

    /// <summary>
    /// No lazily hydrated snapshot is attached, so there is no frame to bisect,
    /// and every row is already resident: the fallback materialises nothing.
    /// <para>
    /// Either the leaf never attached a frame (it was replayed from the
    /// write-ahead log), or an earlier whole-cache operation already
    /// materialised or discarded it. <c>LeafEntryCache.LastDetachSeam</c>
    /// separates the two - it stays <see cref="LeafSnapshotDetachSeam.None"/> in
    /// the first case and names the forfeiting surface in the second. A named
    /// seam is still worth acting on, but the cost it records was paid at that
    /// seam, earlier and typically on the read path, not by the division that
    /// observes it.
    /// </para>
    /// </summary>
    NoSnapshotAttached = 1,

    /// <summary>
    /// The frame declares fewer than two rows, so it has no interior pivot. A
    /// frame is still attached, but the fallback materialises at most one row.
    /// </summary>
    TooFewRows = 2,

    /// <summary>
    /// The frame's median row key could not be read. The frame is still
    /// attached with its rows, so the fallback materialises the whole remainder
    /// of the leaf and detaches the frame - an expensive refusal.
    /// </summary>
    FrameKeyUnreadable = 3,

    /// <summary>
    /// No key sorts below the candidate pivot, so returning it would migrate
    /// every entry and leave an empty donor. The frame is still attached with
    /// its rows, so the fallback materialises the whole remainder of the leaf
    /// and detaches the frame - an expensive refusal.
    /// </summary>
    NoKeySortsBelowPivot = 4,
}
