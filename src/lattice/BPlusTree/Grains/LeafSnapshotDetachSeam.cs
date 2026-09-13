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

    /// <summary>
    /// The backing dictionary accessor hydrated the whole cache.
    /// <para>
    /// Recorded only by <c>LeafEntryCache.UnderlyingRows</c>, whose sole caller
    /// is <c>BPlusLeafGrain.EntriesForTest</c> - a test-only live window that is
    /// deliberately retained because fixtures seed rows through it, so it cannot
    /// be converted to a bounded copy. A deployed process therefore never
    /// records this seam and never emits <c>detach_seam=underlying_rows_accessor</c>
    /// on <c>orleans.lattice.leaf.bisect_refusals</c>. That absence is expected
    /// and carries no information about leaf behaviour; do not read it as
    /// evidence that no whole-cache accessor consumed a frame.
    /// </para>
    /// <para>
    /// It is kept rather than deleted because the surface really does detach and
    /// this is its correct attribution. Reporting some other seam would be
    /// misattribution, and reporting <see cref="None"/> would make a detached
    /// frame indistinguishable from one that was never attached - the exact
    /// benign-versus-harmful collapse the seam exists to resolve, and the
    /// property <c>LeafSnapshotDetachAttributionTests</c> pins by name.
    /// </para>
    /// </summary>
    UnderlyingRowsAccessor = 4,

    // Ordinal 5 was StateBytesBackfill, assigned by a one-shot LeafStateBytes
    // migration helper that never had a production caller in its entire
    // history and backfilled a persisted slot that does not exist (the cache's
    // running counter is rebuilt from the rows on every activation). Removed
    // in issue #2865 rather than retained the way RangeHydrationCompleted
    // below is: that member was once recorded and so has historical series to
    // keep stable, whereas nothing ever emitted this one. The ordinal is left
    // unused so no new member silently inherits it.

    /// <summary>
    /// Retained for wire and diagnostic stability. Ranged and keyed hydration
    /// once released the frame when their final outstanding block completed the
    /// source; issue #2843 established that doing so forfeits the bisect a
    /// subsequent leaf division needs, because completing a bounded read is not
    /// a reason to discard the frame. Ranged and keyed hydration now retain the
    /// frame, so no surface assigns this value; it is never recorded as a live
    /// detach seam and remains only so the enum ordinals do not shift.
    /// </summary>
    RangeHydrationCompleted = 6,

    /// <summary>
    /// A declared frame row could not be decoded, so the whole frame was
    /// decoded instead of leaving the cache half populated.
    /// </summary>
    FrameDecodeFallback = 7,
}
