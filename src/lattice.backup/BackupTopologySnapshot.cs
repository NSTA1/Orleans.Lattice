namespace Orleans.Lattice.Backup;

/// <summary>
/// A structural snapshot of the captured tree's shard topology: the physical
/// shard count, the virtual shard space, and the per-shard structural digests of
/// the shard roots at the capture's consistency cut. Recorded for both integrity
/// verification (a restore can re-derive and compare digests) and disaster
/// recovery (the topology is reproduced on restore).
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupTopologySnapshot)]
[Immutable]
public sealed record BackupTopologySnapshot
{
    /// <summary>Initializes a new <see cref="BackupTopologySnapshot"/>.</summary>
    /// <param name="shardCount">The physical shard count of the captured tree. Must be positive.</param>
    /// <param name="virtualShardCount">The virtual shard space size of the captured tree. Must be positive.</param>
    /// <param name="shardRootDigests">
    /// The per-shard structural digest of each shard root at the capture cut, in
    /// physical shard-index order. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="shardCount"/> or <paramref name="virtualShardCount"/> is not positive.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="shardRootDigests"/> is <c>null</c>.</exception>
    public BackupTopologySnapshot(
        int shardCount,
        int virtualShardCount,
        IReadOnlyList<string> shardRootDigests)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualShardCount);
        ArgumentNullException.ThrowIfNull(shardRootDigests);
        ShardCount = shardCount;
        VirtualShardCount = virtualShardCount;
        ShardRootDigests = shardRootDigests;
    }

    /// <summary>The physical shard count of the captured tree.</summary>
    [Id(0)]
    public int ShardCount { get; init; }

    /// <summary>The virtual shard space size of the captured tree.</summary>
    [Id(1)]
    public int VirtualShardCount { get; init; }

    /// <summary>The per-shard structural digest of each shard root, in physical shard-index order.</summary>
    [Id(2)]
    public IReadOnlyList<string> ShardRootDigests { get; init; }
}
