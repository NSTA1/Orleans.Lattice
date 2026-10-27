namespace Orleans.Lattice;

/// <summary>
/// The checkpoint-relevant projection of one leaf's durable row, read once by
/// <see cref="WalFloorHolderReader.ReadLeafCheckpointSnapshotAsync"/> so every
/// partition pin of that leaf can be classified without re-reading storage
/// (issue #5097). Process-local only; never serialized.
/// </summary>
/// <param name="RecordExists">Whether the storage provider holds a row for the leaf.</param>
/// <param name="HasTreeId">Whether the row still carries a tree id (issue #3105).</param>
/// <param name="ProjectionCheckpointOffset">Partition 0's scalar checkpoint slot.</param>
/// <param name="ProjectionCheckpointOffsetAssigned">Whether partition 0's scalar slot was ever assigned (issue #2703).</param>
/// <param name="ProjectionCheckpointOffsetsByPartition">The per-partition checkpoint array, or <see langword="null"/>.</param>
internal readonly record struct LeafCheckpointSnapshot(
    bool RecordExists,
    bool HasTreeId,
    long ProjectionCheckpointOffset,
    bool? ProjectionCheckpointOffsetAssigned,
    long[]? ProjectionCheckpointOffsetsByPartition);
