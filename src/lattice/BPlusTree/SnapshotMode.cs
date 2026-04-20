namespace Orleans.Lattice;

/// <summary>
/// Controls whether a snapshot operation locks the source tree during the copy.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotMode)]
public enum SnapshotMode
{
    /// <summary>
    /// The source tree is marked as deleted during the snapshot, blocking all
    /// reads and writes until the snapshot completes and the shards are unmarked.
    /// Guarantees a fully consistent point-in-time copy.
    /// </summary>
    Offline,

    /// <summary>
    /// The source tree remains available for reads and writes throughout the
    /// snapshot. Strictly consistent: every write the source accepts before
    /// the snapshot completes is reflected on the destination, with no
    /// data loss.
    /// <para>
    /// The source tree first enters a shadow-forwarding phase in which every
    /// accepted mutation is mirrored to the corresponding shard on the
    /// destination. A per-shard background drain then copies existing entries
    /// into the destination. LWW commutativity (highest HLC wins per key)
    /// guarantees that the parallel forward and the drain converge to the
    /// same final state regardless of interleaving, so no distributed lock,
    /// durable shadow queue, or two-phase commit is required.
    /// </para>
    /// <para>
    /// Cost: every mutation on the source during the snapshot pays one
    /// additional grain-call hop to mirror to the destination. The cost is
    /// bounded to the drain window and disappears as soon as the snapshot
    /// completes.
    /// </para>
    /// </summary>
    Online
}
