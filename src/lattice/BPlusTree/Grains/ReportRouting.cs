using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>Fresh report routing without a stateless-worker hop on ordinary trees.</summary>
internal static class ReportRouting
{
    internal static bool CanReadEntry(string treeId) =>
        !treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        && !ReplicationApplyScope.IsActive
        && !LatticeBootstrapShadowRouteContext.TryGetPhysicalTreeId(treeId, out _);

    internal static Task<RoutingInfo> ResolveAsync(
        IGrainFactory factory, string treeId, TreeRegistryEntry? entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!CanReadEntry(treeId))
            return factory.GetGrain<ILattice>(treeId)
                .GetRoutingAsync(forceRefresh: true, cancellationToken).AsTask();

        // Match LatticeGrain.GetRoutingSlowAsync: alias and map are one row,
        // and a missing/invalid shard pin uses canonical defaults, not live config.
        var map = entry?.ShardMap ?? ShardMap.GetOrCreateDefaultShared(
            LatticeConstants.DefaultVirtualShardCount,
            entry?.ShardCount is > 0 and var pinned ? pinned : LatticeConstants.DefaultShardCount);
        return Task.FromResult(new RoutingInfo(entry?.PhysicalTreeId ?? treeId, map));
    }
}
