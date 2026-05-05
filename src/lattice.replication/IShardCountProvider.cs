namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal testability seam over the core library''s
/// <see cref="BPlusTree.LatticeOptionsResolver"/> shard-count component.
/// Replication-package consumers (notably
/// <see cref="LatticeReplicationLocalVcSeeder"/>) need only the
/// <c>ShardCount</c> field of the resolved options for a tree, but the
/// resolver itself takes <c>IGrainFactory</c> + <c>IOptionsMonitor&lt;LatticeOptions&gt;</c>
/// and chains through <c>ILatticeRegistry.GetEntryAsync</c> for non-system
/// trees - tedious to substitute in unit tests. This seam exposes only
/// the shard-count lookup so the consumer's tests can stub a single
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
}