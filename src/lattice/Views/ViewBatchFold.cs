using System.Runtime.InteropServices;

namespace Orleans.Lattice;

/// <summary>
/// Folds a view drain batch once, producing in a single pass both the results
/// that <see cref="ViewKeyCollisionDetector"/> and <see cref="ViewWriteCoalescer"/>
/// produce in two.
/// <para>
/// The maintainer runs both over the <b>same</b> batch on adjacent lines of the
/// drain, and both group by <see cref="ViewWrite.Key"/> under
/// <see cref="StringComparer.Ordinal"/> - the detector to find a view key
/// attributable to two distinct source keys, the coalescer to pick the
/// last-writer-wins survivor for that key. Because the grouping is identical, one
/// keyed pass can carry both decisions in one slot per key: the batch is walked
/// once instead of twice, one of the two per-key dictionaries disappears
/// entirely, and the detector's separate colliding-key <see cref="HashSet{T}"/> is
/// replaced by a flag on the slot that already exists.
/// </para>
/// <para>
/// This type is a fused implementation detail, not a replacement for the two
/// public helpers: they remain the standalone surface (and the range-delete drain
/// path, which cannot coalesce by view key, still detects collisions through
/// <see cref="ViewKeyCollisionDetector"/> alone).
/// </para>
/// </summary>
internal static class ViewBatchFold
{
    /// <summary>
    /// Per-view-key state carried through the single fold pass: where the key's
    /// current survivor sits in the survivor list, the first source key that was
    /// attributed to it, and whether it has already been reported as colliding.
    /// </summary>
    private struct KeySlot
    {
        /// <summary>Index into the survivor list of this key's current winner.</summary>
        public int SurvivorIndex;

        /// <summary>
        /// The first non-null <see cref="ViewWrite.SourceKey"/> seen for this view
        /// key, or <see langword="null"/> while only unattributed writes have been
        /// seen. Mirrors the detector's first-source map, which likewise only
        /// records attributable writes.
        /// </summary>
        public string? FirstSource;

        /// <summary>Whether this key has already been added to the colliding list.</summary>
        public bool Collided;
    }

    /// <summary>
    /// Walks <paramref name="writes"/> once, returning the coalesced survivors and
    /// the colliding view keys. The result is element-for-element identical to
    /// calling <see cref="ViewWriteCoalescer.Coalesce(IEnumerable{ViewWrite})"/> and
    /// <see cref="ViewKeyCollisionDetector.Detect(IEnumerable{ViewWrite})"/>
    /// separately over the same batch.
    /// </summary>
    /// <param name="writes">The batch to fold. Must not be <see langword="null"/>.</param>
    public static ViewBatchFoldResult Fold(IReadOnlyList<ViewWrite> writes)
    {
        ArgumentNullException.ThrowIfNull(writes);

        // One slot per distinct view key serves both decisions, so the batch is
        // presized exactly as the coalescer presizes its own index: at most one
        // slot and one survivor per input write, so a sparse batch presizes small.
        var slots = new Dictionary<string, KeySlot>(writes.Count, StringComparer.Ordinal);
        var survivors = new List<ViewWrite>(writes.Count);

        // The colliding list stays unpresized: a well-configured injective re-key
        // never collides, so on the common path it never allocates a backing store
        // (an empty List defers it).
        var colliding = new List<string>();

        // Index-based iteration rather than foreach: `writes` is interface-typed
        // and the caller always passes a List<ViewWrite>, whose struct enumerator
        // would be boxed by an IReadOnlyList foreach.
        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];

            // Single hash probe per write, inserting the slot if the key is new.
            // The two prior passes probed two separate dictionaries.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(slots, write.Key, out var existed);
            if (!existed)
            {
                slot.SurvivorIndex = survivors.Count;
                slot.FirstSource = write.SourceKey;
                slot.Collided = false;
                survivors.Add(write);
                continue;
            }

            // Coalesce: last-writer-wins on the source HLC. A write whose timestamp
            // ties the incumbent does not displace it, so first-seen wins an exact
            // tie and the fold stays deterministic for a fixed input order.
            if (write.Timestamp.CompareTo(survivors[slot.SurvivorIndex].Timestamp) > 0)
            {
                survivors[slot.SurvivorIndex] = write;
            }

            // Detect: a write with no source key is not attributable to a single
            // source and is ignored, exactly as the standalone detector ignores it.
            if (write.SourceKey is not { } source)
            {
                continue;
            }

            if (slot.FirstSource is null)
            {
                // Only unattributed writes had been seen for this key, so this is
                // the first source attributed to it - the detector would have
                // created its first-source entry at precisely this write.
                slot.FirstSource = source;
                continue;
            }

            if (!slot.Collided && !string.Equals(slot.FirstSource, source, StringComparison.Ordinal))
            {
                slot.Collided = true;
                colliding.Add(write.Key);
            }
        }

        return new ViewBatchFoldResult(survivors, colliding);
    }
}
