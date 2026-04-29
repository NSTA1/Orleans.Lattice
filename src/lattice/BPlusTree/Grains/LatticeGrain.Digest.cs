namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Public-surface forwarder for
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/>. Resolves the per-tree
/// <see cref="ShardMap"/> via the existing <see cref="LatticeGrain.GetRoutingAsync"/>
/// helper, validates that <c>shardIndex</c> corresponds to a physical shard
/// owned by this tree, and dispatches to <see cref="IShardRootGrain.GetShardProjectionDigestAsync"/>.
/// Guarded by <see cref="LatticeGrain.ThrowIfSystemTree"/> so reserved
/// system trees (registry, replication WAL prefix) cannot be inspected
/// through the public surface.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetLeafProjectionDigestAsync(
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Validate the supplied shard index against the per-tree map; this
        // catches off-by-one mistakes early instead of dispatching against
        // a non-existent shard grain that would activate empty.
        var physicalShards = shardMap.GetPhysicalShardIndices();
        if (!physicalShards.Contains(shardIndex))
        {
            throw new ArgumentOutOfRangeException(
                nameof(shardIndex),
                shardIndex,
                $"Shard index {shardIndex} is not a physical shard of tree '{TreeId}'. " +
                $"Valid indices: [{string.Join(", ", physicalShards)}].");
        }

        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        return await shard.GetShardProjectionDigestAsync(cancellationToken);
    }
}
