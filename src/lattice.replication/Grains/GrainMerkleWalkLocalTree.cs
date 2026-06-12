using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// An <see cref="IMerkleWalkLocalTree"/> backed by the local cluster's grains.
/// Reaches the shard root through <see cref="IShardRootGrain"/> and resolves
/// nodes through the core library's internal leaf and internal-node grains.
/// Strictly read-only.
/// </summary>
internal sealed class GrainMerkleWalkLocalTree(
    IGrainFactory grainFactory,
    string physicalTreeId,
    int shardIndex)
    : IMerkleWalkLocalTree
{
    /// <inheritdoc />
    public async ValueTask<MerkleWalkLocalNode?> GetRootAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var shardRoot = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        var rootRef = await shardRoot.GetRootNodeRefAsync().ConfigureAwait(false);
        if (rootRef is null)
        {
            return null;
        }

        return await ResolveAsync(rootRef.Value.NodeId, rootRef.Value.IsLeaf, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MerkleWalkLocalNode> ResolveAsync(
        GrainId nodeId,
        bool isLeaf,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (isLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(nodeId);
            var leafDigest = await leaf.GetProjectionDigestAsync().ConfigureAwait(false);
            return new MerkleWalkLocalNode
            {
                IsLeaf = true,
                Digest = leafDigest,
                Children = Array.Empty<MerkleWalkLocalChild>(),
            };
        }

        var node = grainFactory.GetGrain<IBPlusInternalGrain>(nodeId);
        var digest = await node.GetSubtreeProjectionDigestAsync().ConfigureAwait(false);
        var routing = await node.GetRoutingTableAsync().ConfigureAwait(false);

        var separators = routing.SeparatorKeys;
        var childIds = routing.ChildIds;
        var count = Math.Min(separators.Length, childIds.Length);
        var children = new MerkleWalkLocalChild[count];
        for (var i = 0; i < count; i++)
        {
            children[i] = new MerkleWalkLocalChild
            {
                SeparatorKey = separators[i],
                NodeId = childIds[i],
                ChildIsLeaf = routing.ChildrenAreLeaves,
            };
        }

        return new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = digest,
            Children = children,
        };
    }
}
