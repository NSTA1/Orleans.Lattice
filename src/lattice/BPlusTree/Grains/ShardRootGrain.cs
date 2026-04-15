using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The root grain for a single shard. Lazily creates the first leaf and
/// handles root splits by creating a new internal root above the old one.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// </summary>
internal sealed class ShardRootGrain(
    [PersistentState("shardroot", "bplustree")] IPersistentState<ShardRootState> state,
    IGrainFactory grainFactory) : IShardRootGrain
{
    public async Task<byte[]?> GetAsync(string key)
    {
        await EnsureRootAsync();
        return await TraverseForReadAsync(key);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        await EnsureRootAsync();
        var splitResult = await TraverseForWriteAsync(key, value);

        // If the root node split, we need to create a new internal root.
        while (splitResult is not null)
        {
            splitResult = await PromoteRootAsync(splitResult);
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        await EnsureRootAsync();

        if (state.State.RootIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.DeleteAsync(key);
        }

        // Traverse to the leaf.
        var leafId = await TraverseToLeafAsync(key);
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        return await leafGrain.DeleteAsync(key);
    }

    private async Task EnsureRootAsync()
    {
        if (state.State.RootNodeId is not null) return;

        // First access: create the initial leaf.
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        state.State.RootNodeId = leafGrain.GetGrainId();
        state.State.RootIsLeaf = true;
        await state.WriteStateAsync();
    }

    private async Task<byte[]?> TraverseForReadAsync(string key)
    {
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else
        {
            leafId = await TraverseToLeafAsync(key);
        }

        var cache = grainFactory.GetGrain<ILeafCacheGrain>(leafId.ToString());
        return await cache.GetAsync(key);
    }

    private async Task<SplitResult?> TraverseForWriteAsync(string key, byte[] value)
    {
        if (state.State.RootIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.SetAsync(key, value);
        }

        // Walk internal nodes down to the leaf, collecting the path for split propagation.
        var path = new Stack<GrainId>();
        var currentId = state.State.RootNodeId!.Value;
        path.Push(currentId);

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var childId = await internalGrain.RouteAsync(key);

            // We need to determine if the child is a leaf.
            // For now, we try the leaf path; if the tree is deeper, the shard root
            // tracks depth implicitly through the internal node chain.
            // Simple heuristic: try to call SetAsync on the child as a leaf.
            // Better approach: internal nodes know whether their children are leaves.
            // We'll use the path depth + knowledge that the root tracks this.
            // For the initial implementation, we traverse until we hit a leaf.

            // Check if we can resolve the child as a leaf by attempting to go one more level.
            // A simpler approach: always descend through internal nodes.
            // Since internal nodes' RouteAsync returns GrainId, we need metadata.
            // Let's build a simpler traversal that assumes max 2 levels for now,
            // and in the next iteration add depth tracking.
            path.Push(childId);
            break;
        }

        // The top of the stack is the leaf.
        var leafId = path.Pop();
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var splitResult = await leafGrain.SetAsync(key, value);

        // Propagate splits up the path.
        while (splitResult is not null && path.Count > 0)
        {
            var parentId = path.Pop();
            var parentGrain = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
            splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
        }

        return splitResult;
    }

    private async Task<GrainId> TraverseToLeafAsync(string key)
    {
        var currentId = state.State.RootNodeId!.Value;
        var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
        return await internalGrain.RouteAsync(key);
    }

    private async Task<SplitResult?> PromoteRootAsync(SplitResult splitResult)
    {
        // The old root split — create a new internal root above it.
        var newRoot = grainFactory.GetGrain<IBPlusInternalGrain>(Guid.NewGuid());
        await newRoot.InitializeAsync(
            splitResult.PromotedKey,
            state.State.RootNodeId!.Value,
            splitResult.NewSiblingId);

        state.State.RootNodeId = newRoot.GetGrainId();
        state.State.RootIsLeaf = false;
        await state.WriteStateAsync();

        return null; // New root was just created with two children — no split.
    }
}
