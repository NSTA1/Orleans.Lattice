namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal testability seam over the tree's live routing. Replication-package
/// consumers need either the physical shard indices its routing map reaches
/// (<see cref="GetShardIndicesAsync"/>) or the shard-root keys of those shards
/// (<see cref="GetShardRootKeysAsync"/>), but resolving routing takes an
/// <c>IGrainFactory</c> and the tree's <c>ILattice</c> grain - tedious to
/// substitute in unit tests. This seam exposes only those lookups so the
/// consumers' tests can stub a single method instead of the full grain graph.
/// <para>
/// The default implementation (<see cref="DefaultShardCountProvider"/>) reads a
/// freshly resolved routing snapshot; hosts that need a different source (e.g.
/// tests, benchmarks) can register their own implementation before
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </para>
/// <para>
/// The pinned <c>ShardCount</c> of a tree's resolved options is deliberately not
/// offered: an adaptive split or a reshard moves slots to a physical index above
/// the pin without changing it, and a fold retires indices below it, so
/// <c>0..ShardCount-1</c> both misses live shards and names retired ones (#3753,
/// #4206).
/// </para>
/// </summary>
internal interface IShardCountProvider
{
    /// <summary>
    /// Returns every physical shard index the tree's live routing map can send a
    /// key to, in ascending order, for callers that address a shard by index
    /// through the tree's <c>ILattice</c> grain (the anti-entropy digest probe).
    /// </summary>
    /// <param name="treeId">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<int>> GetShardIndicesAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the <c>{physicalTreeId}/{shardIndex}</c> grain key of every shard
    /// root the tree's live routing can send a key to, for callers that must reach
    /// every shard holding the tree's data (the saga write fence, the local
    /// vector-clock seeder).
    /// <para>
    /// A resize or restore swaps the tree onto a new physical id behind an alias,
    /// so <c>{treeId}/{i}</c> can name a retired copy; the keys therefore carry the
    /// resolved physical id.
    /// </para>
    /// </summary>
    /// <param name="treeId">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<string>> GetShardRootKeysAsync(string treeId, CancellationToken cancellationToken = default);
}
