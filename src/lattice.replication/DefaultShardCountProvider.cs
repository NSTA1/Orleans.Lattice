using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IShardCountProvider"/> implementation backed by
/// the core library''s
/// <see cref="LatticeOptionsResolver"/> and the tree's
/// <see cref="ILattice"/> routing. <see cref="GetShardCountAsync"/> forwards
/// to <see cref="LatticeOptionsResolver.ResolveAsync(string)"/> and
/// returns the <c>ShardCount</c> field of the result. The resolver
/// chains through <see cref="ILatticeRegistry"/> for non-system trees
/// and applies lazy first-use seeding so the call is idempotent across
/// callers and silos. <see cref="GetShardRootKeysAsync"/> reads a freshly
/// resolved <see cref="RoutingInfo"/> so the keys follow both the alias
/// (the physical tree id) and the shard map (every physical shard a slot
/// routes to, including an adaptive split's target above the pin).
/// </summary>
internal sealed class DefaultShardCountProvider(LatticeOptionsResolver resolver, IGrainFactory grainFactory)
    : IShardCountProvider
{
    private readonly LatticeOptionsResolver _resolver =
        resolver ?? throw new ArgumentNullException(nameof(resolver));

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<int> GetShardCountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var resolved = await _resolver.ResolveAsync(treeId).ConfigureAwait(false);
        return resolved.ShardCount;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetShardRootKeysAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Force a refresh: LatticeGrain is a stateless worker that caches routing
        // per activation, and a caller enumerating shards to fence or walk must
        // see a split or alias swap that landed after that cache was filled.
        var routing = await _grainFactory.GetGrain<ILattice>(treeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken)
            .ConfigureAwait(false);

        var indices = routing.Map.GetPhysicalShardIndices();
        var keys = new string[indices.Count];
        for (var i = 0; i < indices.Count; i++)
        {
            keys[i] = $"{routing.PhysicalTreeId}/{indices[i]}";
        }

        return keys;
    }
}