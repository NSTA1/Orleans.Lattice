namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Lifecycle operations: soft-delete, recovery, and purge.
/// </summary>
internal sealed partial class ShardRootGrain
{
    public async Task MarkDeletedAsync()
    {
        if (state.State.IsDeleted) return;
        state.State.IsDeleted = true;
        await state.WriteStateAsync();
    }

    public Task<bool> IsDeletedAsync() => Task.FromResult(state.State.IsDeleted);

    public async Task UnmarkDeletedAsync()
    {
        if (!state.State.IsDeleted) return;
        state.State.IsDeleted = false;
        await state.WriteStateAsync();
    }

    public async Task PurgeAsync()
    {
        if (state.State.RootNodeId is null)
        {
            await state.ClearStateAsync();
            return;
        }

        GrainId? leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId;
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var internalNodeIds = new List<GrainId>();
        if (!state.State.RootIsLeaf)
        {
            await CollectInternalNodeIds(state.State.RootNodeId!.Value, internalNodeIds);
        }

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var nextId = await leaf.GetNextSiblingAsync();
            await leaf.ClearGrainStateAsync();
            leafId = nextId;
        }

        foreach (var internalId in internalNodeIds)
        {
            var internalNode = grainFactory.GetGrain<IBPlusInternalGrain>(internalId);
            await internalNode.ClearGrainStateAsync();
        }

        await state.ClearStateAsync();
    }

    private async Task CollectInternalNodeIds(GrainId rootNodeId, List<GrainId> collected)
    {
        var stack = new Stack<GrainId>();
        stack.Push(rootNodeId);

        while (stack.Count > 0)
        {
            var nodeId = stack.Pop();
            collected.Add(nodeId);

            var node = grainFactory.GetGrain<IBPlusInternalGrain>(nodeId);
            if (await node.AreChildrenLeavesAsync())
                continue;

            var children = await node.GetChildIdsAsync();
            // Push in reverse order to preserve traversal order (optional).
            for (int i = children.Count - 1; i >= 0; i--)
            {
                stack.Push(children[i]);
            }
        }
    }
}
