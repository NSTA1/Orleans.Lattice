namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal testability seam over the tree's live routing. Replication-package
/// consumers need either the freshly resolved routing itself - physical tree id
/// and live shard map (<see cref="GetShardRoutingAsync"/>) - or the shard-root keys
/// of the shards it reaches
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
    /// Returns the tree's freshly resolved routing - the physical tree id behind
    /// any alias together with the live shard map - for callers that address
    /// every shard root directly and need both halves from one resolve (the
    /// anti-entropy digest probe, which would otherwise pay a forced routing
    /// read per shard through the tree's <c>ILattice</c> grain).
    /// </summary>
    /// <param name="treeId">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<RoutingInfo> GetShardRoutingAsync(string treeId, CancellationToken cancellationToken = default);

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
