namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Which cache surface detached the lazily hydrated snapshot frame.
/// <para>
/// Detaching is irreversible for the life of the activation: nothing
/// re-attaches a frame, so every row the cache holds stays resident and no
/// later eviction can recover the footprint. A leaf that is subsequently asked
/// to divide can then only do so through the ordered view, which materialises
/// the whole leaf - the cost the frame exists to avoid. Recording the surface
/// that detached makes that forfeiture attributable, because the split seam
/// itself cannot tell a leaf that never had a frame from one whose frame an
/// unrelated whole-leaf operation consumed.
/// </para>
/// </summary>
internal enum LeafSnapshotDetachSeam
{
    /// <summary>No frame has been detached on this cache.</summary>
    None = 0,

    /// <summary>Every row was cleared, which discards any frame with them.</summary>
    Clear = 1,

    /// <summary>The ordered key view hydrated the whole cache.</summary>
    KeysAccessor = 2,

    /// <summary>The whole-cache row enumeration hydrated the whole cache.</summary>
    EnumerateRowsAccessor = 3,

    /// <summary>The backing dictionary accessor hydrated the whole cache.</summary>
    UnderlyingRowsAccessor = 4,

    /// <summary>The state-bytes backfill hydrated the whole cache.</summary>
    StateBytesBackfill = 5,

    /// <summary>
    /// Ranged hydration materialised the final outstanding block, so the frame
    /// held nothing further and was released. This is the benign case: the rows
    /// were paid for one bounded window at a time rather than all at once.
    /// </summary>
    RangeHydrationCompleted = 6,

    /// <summary>
    /// A declared frame row could not be decoded, so the whole frame was
    /// decoded instead of leaving the cache half populated.
    /// </summary>
    FrameDecodeFallback = 7,
}
