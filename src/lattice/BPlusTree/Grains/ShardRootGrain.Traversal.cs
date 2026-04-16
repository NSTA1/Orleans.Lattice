namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tree traversal logic for read, write, and leaf-location operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
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

    private async Task<bool> TraverseForExistsAsync(string key)
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
        return await cache.ExistsAsync(key);
    }

    private async Task<Dictionary<string, byte[]>> TraverseForBatchReadAsync(List<string> keys)
    {
        // Group keys by their target leaf.
        var leafBuckets = new Dictionary<GrainId, List<string>>();
        foreach (var key in keys)
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

            if (!leafBuckets.TryGetValue(leafId, out var bucket))
            {
                bucket = [];
                leafBuckets[leafId] = bucket;
            }
            bucket.Add(key);
        }

        // Batch read from each leaf cache.
        var result = new Dictionary<string, byte[]>();
        foreach (var (leafId, bucket) in leafBuckets)
        {
            var cache = grainFactory.GetGrain<ILeafCacheGrain>(leafId.ToString());
            var values = await cache.GetManyAsync(bucket);
            foreach (var (k, v) in values)
            {
                result[k] = v;
            }
        }
        return result;
    }

    private async Task<SplitResult?> TraverseForWriteAsync(string key, byte[] value)
    {
        if (state.State.RootIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.SetAsync(key, value);
        }

        var path = StackPool.Get();
        try
        {
        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var (childId, childrenAreLeaves) = await internalGrain.RouteWithMetadataAsync(key);

            if (childrenAreLeaves)
            {
                path.Push(currentId);
                path.Push(childId);
                break;
            }

            path.Push(currentId);
            currentId = childId;
        }

        var leafId = path.Pop();
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var splitResult = await leafGrain.SetAsync(key, value);

        while (splitResult is not null && path.Count > 0)
        {
            var parentId = path.Pop();
            var parentGrain = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
            splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
        }

        return splitResult;
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private async Task<GrainId> TraverseToLeafAsync(string key)
    {
        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var (childId, childrenAreLeaves) = await internalGrain.RouteWithMetadataAsync(key);

            if (childrenAreLeaves)
            {
                return childId;
            }

            currentId = childId;
        }
    }

    private async Task<GrainId> TraverseToLeftmostLeafAsync()
    {
        if (state.State.RootIsLeaf)
        {
            return state.State.RootNodeId!.Value;
        }

        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var (childId, childrenAreLeaves) = await internalGrain.GetLeftmostChildWithMetadataAsync();

            if (childrenAreLeaves)
            {
                return childId;
            }

            currentId = childId;
        }
    }

    private async Task<GrainId> TraverseToRightmostLeafAsync()
    {
        if (state.State.RootIsLeaf)
        {
            return state.State.RootNodeId!.Value;
        }

        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var (childId, childrenAreLeaves) = await internalGrain.GetRightmostChildWithMetadataAsync();

            if (childrenAreLeaves)
            {
                return childId;
            }

            currentId = childId;
        }
    }

    private async Task<SplitResult?> PromoteRootAsync(SplitResult splitResult)
    {
        state.State.PendingPromotion = splitResult;
        state.State.PendingPromotionRootWasLeaf = state.State.RootIsLeaf;
        await state.WriteStateAsync();

        await CompletePromotionAsync();
        return null;
    }

    /// <summary>
    /// Completes (or resumes) a root promotion whose intent has already been persisted.
    /// </summary>
    private async Task CompletePromotionAsync()
    {
        var pending = state.State.PendingPromotion!;
        var childrenAreLeaves = state.State.PendingPromotionRootWasLeaf;

        var shardKey = context.GrainId.Key.ToString()!;
        var deterministicId = DeterministicGuid(
            shardKey + "/root-above/" + state.State.RootNodeId!.Value);

        var newRoot = grainFactory.GetGrain<IBPlusInternalGrain>(deterministicId);
        await newRoot.SetTreeIdAsync(TreeId);
        await newRoot.InitializeAsync(
            pending.PromotedKey,
            state.State.RootNodeId!.Value,
            pending.NewSiblingId,
            childrenAreLeaves);

        state.State.RootNodeId = newRoot.GetGrainId();
        state.State.RootIsLeaf = false;
        state.State.PendingPromotion = null;
        await state.WriteStateAsync();
    }
}
