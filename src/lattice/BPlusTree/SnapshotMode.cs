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
    /// The source tree remains available for reads and writes during the snapshot.
    /// Each shard is drained independently without locking. Concurrent mutations
    /// may cause minor inconsistencies: a key written or deleted between draining
    /// shard 0 and shard 1 may appear in one but not the other. The result is a
    /// best-effort point-in-time copy, similar to a non-repeatable-read isolation
    /// level.
    /// </summary>
    Online
}
