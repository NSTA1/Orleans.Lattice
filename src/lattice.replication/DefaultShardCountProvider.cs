using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IShardCountProvider"/> implementation backed by the tree's
/// <see cref="ILattice"/> routing. Both lookups read a freshly resolved
/// <see cref="RoutingInfo"/>, so they follow the shard map (every physical shard a
/// slot routes to, including an adaptive split's or a reshard's target above the
/// pinned count, and none a fold retired) and, for the shard-root keys, the alias
/// (the physical tree id).
/// </summary>
internal sealed class DefaultShardCountProvider(IGrainFactory grainFactory)
    : IShardCountProvider
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<IReadOnlyList<int>> GetShardIndicesAsync(string treeId, CancellationToken cancellationToken = default)
    {
        var routing = await ResolveRoutingAsync(treeId, cancellationToken).ConfigureAwait(false);
        return routing.Map.GetPhysicalShardIndices();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetShardRootKeysAsync(string treeId, CancellationToken cancellationToken = default)
    {
        var routing = await ResolveRoutingAsync(treeId, cancellationToken).ConfigureAwait(false);
        var indices = routing.Map.GetPhysicalShardIndices();
        var keys = new string[indices.Count];
        for (var i = 0; i < indices.Count; i++)
        {
            keys[i] = $"{routing.PhysicalTreeId}/{indices[i]}";
        }

        return keys;
    }

    private async Task<RoutingInfo> ResolveRoutingAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Force a refresh: LatticeGrain is a stateless worker that caches routing
        // per activation, and a caller enumerating shards to fence, walk or probe
        // must see a split, reshard or alias swap that landed after that cache was
        // filled.
        return await _grainFactory.GetGrain<ILattice>(treeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken)
            .ConfigureAwait(false);
    }
}
