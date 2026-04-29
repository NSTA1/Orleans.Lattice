namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Dormant seam over the streaming as-of snapshot export. Lets the core
/// library drain a leaf-key-range subset of a tree''s primary state
/// without taking a hard reference on the
/// <c>Orleans.Lattice.Replication</c> package.
/// <para>
/// Resolved via <see cref="System.IServiceProvider"/> as a nullable
/// service: when the replication package''s
/// <c>AddLatticeReplication</c> extension has registered a concrete
/// adapter the seam yields it, otherwise the resolution returns
/// <see langword="null"/>.
/// </para>
/// <para>
/// <b>Dormancy.</b> the dormant seam ships this interface and its concrete
/// adapter without any foreground call site. the future replay coordinator''s
/// <c>SnapshotThenWal</c> recovery path drives <see cref="StreamAsync"/>
/// when a leaf grain has fallen off its WAL tail and needs to rebuild
/// its projection from a snapshot before tail-replaying the WAL.
/// </para>
/// </summary>
internal interface ILeafSnapshotProvider
{
    /// <summary>
    /// Streams every live entry of the tree whose key falls in the
    /// half-open range <c>[leafKeyRangeStart, leafKeyRangeEnd)</c>, in
    /// implementation-defined order. Entries are translated to
    /// <see cref="LatticeMutation"/> with <see cref="MutationKind.Set"/>
    /// so a consumer can drive an <c>ILeafProjection.Apply</c> loop
    /// uniformly against either the WAL feed
    /// (<see cref="ICommitLogReader"/>) or this snapshot feed.
    /// </summary>
    /// <param name="treeId">The logical tree id. Must not be null or empty.</param>
    /// <param name="shardIndex">The WAL shard (partition) index whose backing snapshot is being drained. Must be non-negative.</param>
    /// <param name="leafKeyRangeStart">Inclusive start of the key range to stream. Pass an empty string to start at the lexicographic minimum.</param>
    /// <param name="leafKeyRangeEnd">Exclusive end of the key range to stream. Pass <see langword="null"/> to stream through the lexicographic maximum.</param>
    /// <param name="cancellationToken">Cancellation token observed on every yielded entry.</param>
    IAsyncEnumerable<LatticeMutation> StreamAsync(
        string treeId,
        int shardIndex,
        string leafKeyRangeStart,
        string? leafKeyRangeEnd,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the WAL offset associated with the snapshot view — the
    /// offset a tail-replay should resume from after the snapshot drain
    /// completes. Today the adapter returns the WAL head offset at the
    /// moment of the call; the future replay coordinator may refine the contract once the
    /// snapshot/tail boundary is exercised by the per-shard replay
    /// coordinator.
    /// </summary>
    /// <param name="treeId">The logical tree id. Must not be null or empty.</param>
    /// <param name="shardIndex">The WAL shard (partition) index. Must be non-negative.</param>
    /// <param name="cancellationToken">Cancellation token propagated to the underlying WAL grain call.</param>
    Task<long> GetSnapshotOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default);
}
