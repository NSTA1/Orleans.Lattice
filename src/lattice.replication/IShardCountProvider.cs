namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal testability seam over the core library''s
/// <see cref="BPlusTree.LatticeOptionsResolver"/> shard-count component and
/// the tree's live routing. Replication-package consumers need either the
/// pinned <c>ShardCount</c> of the resolved options for a tree
/// (<see cref="GetShardCountAsync"/>) or the shard-root keys its routing map
/// reaches (<see cref="GetShardRootKeysAsync"/>), but the resolver itself takes
/// <c>IGrainFactory</c> + <c>IOptionsMonitor&lt;LatticeOptions&gt;</c>
/// and chains through <c>ILatticeRegistry.GetEntryAsync</c> for non-system
/// trees - tedious to substitute in unit tests. This seam exposes only
/// those lookups so the consumers' tests can stub a single
/// method instead of the full grain-factory + registry graph.
/// <para>
/// The default implementation
/// (<see cref="DefaultShardCountProvider"/>) wraps
/// <see cref="BPlusTree.LatticeOptionsResolver"/>; hosts that need a
/// different shard-count source (e.g. tests, benchmarks) can register
/// their own implementation before
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </para>
/// </summary>
internal interface IShardCountProvider
{
    /// <summary>
    /// Returns the resolved shard count for <paramref name="treeId"/>.
    /// Lazy first-use seeding via the registry is performed by the
    /// underlying resolver; the call is a single grain hop in steady
    /// state.
    /// </summary>
    /// <param name="treeId">The tree id whose shard count to resolve. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<int> GetShardCountAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the <c>{physicalTreeId}/{shardIndex}</c> grain key of every shard
    /// root the tree's live routing can send a key to, for callers that must reach
    /// every shard holding the tree's data (the saga write fence, the local
    /// vector-clock seeder).
    /// <para>
    /// The pinned shard count from <see cref="GetShardCountAsync"/> is not a
    /// substitute: an adaptive split moves slots to a physical index above the pin
    /// without changing it, and a resize or restore swaps the tree onto a new
    /// physical id behind an alias, so <c>{treeId}/0..ShardCount-1</c> can both
    /// miss live shards and name a retired copy.
    /// </para>
    /// </summary>
    /// <param name="treeId">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<string>> GetShardRootKeysAsync(string treeId, CancellationToken cancellationToken = default);
}