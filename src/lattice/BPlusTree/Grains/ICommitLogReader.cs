namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Dormant seam over the per-shard write-ahead log read path. Pairs
/// with <see cref="ICommitLogWriter"/> to give the core library
/// offset-cursored access to the WAL without taking a hard reference on
/// the <c>Orleans.Lattice.Replication</c> package.
/// <para>
/// Resolved via <see cref="System.IServiceProvider"/> as a nullable
/// service: when the replication package''s
/// <c>AddLatticeReplication</c> extension has registered a concrete
/// adapter the seam yields it, otherwise the resolution returns
/// <see langword="null"/>.
/// </para>
/// <para>
/// <b>Dormancy.</b> the dormant seam ships this interface and its concrete
/// adapter without any foreground call site. the future replay coordinator''s per-shard replay
/// coordinator drives <see cref="ReadAsync"/> when a leaf grain
/// activates with a stale projection-checkpoint offset.
/// </para>
/// </summary>
internal interface ICommitLogReader
{
    /// <summary>
    /// Yields every entry on the per-shard WAL whose offset is strictly
    /// greater than <paramref name="fromOffsetExclusive"/>, in ascending
    /// offset order. The enumeration takes a snapshot of the WAL at
    /// call time and completes once that snapshot is exhausted; to pick
    /// up entries committed after the call, callers re-subscribe with
    /// the last yielded offset.
    /// </summary>
    /// <param name="treeId">The logical tree id whose WAL is being read. Must not be null or empty.</param>
    /// <param name="shardIndex">The WAL shard (partition) index. Must be non-negative.</param>
    /// <param name="fromOffsetExclusive">Strict lower-bound offset; the feed yields entries with <c>offset &gt; fromOffsetExclusive</c>. Pass <c>-1</c> to read from the start of the WAL (offset <c>0</c> inclusive).</param>
    /// <param name="cancellationToken">Cancellation token observed between every page read and every yielded entry.</param>
    IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the next offset that <see cref="ICommitLogWriter.AppendAsync"/>
    /// will assign for <c>(treeId, shardIndex)</c>. Equal to the number
    /// of entries currently persisted on the WAL shard. <c>0</c> when
    /// the WAL shard is empty.
    /// </summary>
    /// <param name="treeId">The logical tree id. Must not be null or empty.</param>
    /// <param name="shardIndex">The WAL shard (partition) index. Must be non-negative.</param>
    /// <param name="cancellationToken">Cancellation token propagated to the underlying WAL grain call.</param>
    Task<long> GetHeadOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default);
}
