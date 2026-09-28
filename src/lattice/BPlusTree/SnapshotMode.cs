namespace Orleans.Lattice;

/// <summary>
/// Controls whether a snapshot operation locks the source tree during the copy.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotMode)]
public enum SnapshotMode
{
    /// <summary>
    /// Every source shard the snapshot copies is marked as deleted before any is
    /// copied, blocking its reads and writes, and is unmarked as soon as it has
    /// been copied rather than when the whole snapshot completes. No write can
    /// therefore change a shard while it is copied. The copy is subject to the
    /// shard-mapping limit described on <see cref="ILattice.SnapshotAsync"/>.
    /// </summary>
    Offline,

    /// <summary>
    /// The source tree remains available for reads and writes throughout the
    /// snapshot, and the writes it accepts while the copy runs are mirrored to
    /// the destination, except typed CRDT delta applies and bulk appends; see
    /// <see cref="ILattice.SnapshotAsync"/> for that limit and for the
    /// shard-mapping limit that applies in both modes.
    /// <para>
    /// The source tree first enters a shadow-forwarding phase in which each
    /// mirrored mutation is forwarded to the corresponding shard on the
    /// destination. A per-shard background drain then copies existing entries
    /// into the destination. Last-writer-wins resolution (highest HLC wins per
    /// key) merges the forwarded and the drained versions of each key, so no
    /// distributed lock, durable shadow queue, or two-phase commit is required.
    /// </para>
    /// <para>
    /// Cost: every mirrored mutation on the source during the snapshot pays one
    /// additional grain-call hop to reach the destination. The cost is
    /// bounded to the drain window and disappears as soon as the snapshot
    /// completes.
    /// </para>
    /// </summary>
    Online
}
