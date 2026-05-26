namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Lifecycle operations: soft-delete, recovery, and purge.
/// </summary>
internal sealed partial class ShardRootGrain
{
    public async Task MarkDeletedAsync()
    {
        if (state.State.IsDeleted) return;

        // Snapshot the pre-mutation IsDeleted. Without this revert, the
        // idempotency guard above short-circuits every retry from this
        // activation - turning a transient storage failure into a permanent
        // split-brain (Class B "persisted / in-memory divergence on write
        // failure, idempotency-guarded" anti-pattern).
        var isDeletedSnapshot = state.State.IsDeleted;
        state.State.IsDeleted = true;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            throw;
        }
    }

    public Task<bool> IsDeletedAsync() => Task.FromResult(state.State.IsDeleted);

    /// <inheritdoc />
    public async Task WarmUpAsync()
    {
        // Drive the shard root through PrepareForOperationAsync so the
        // first hot-path write does not pay shard-root activation, state
        // hydration, OR root materialization. PrepareForOperationAsync is
        // idempotent: on a populated shard it sync-completes (the
        // RootNodeId-not-null fast path); on a brand-new empty shard it
        // runs EnsureRootAsync, which is exactly the path the first
        // traffic write would take. The resulting root leaf id is
        // deterministic-from-shard-key, so warm-up creates no extra
        // grains beyond what the first write would have created itself -
        // it just moves that work to startup time.
        await PrepareForOperationAsync();

        // Pre-activate this shard's current root node. For an empty
        // bench tree this is the deterministic root leaf that
        // EnsureRootAsync just produced. A read-only ping on that
        // grain forces its placement-directory entry, grain-storage
        // ReadStateAsync, and OnActivateAsync to run while the silo is
        // idle. For a populated tree with RootIsLeaf=false, we ping the
        // root internal node instead; that absorbs the first internal-
        // node first-touch on the routing path. We intentionally do NOT
        // walk deeper - traversal warmup of the full subtree would be
        // O(nodes) RPCs and is out of scope for the lightweight startup
        // probe.
        if (state.State.RootNodeId is null)
        {
            // EnsureRootAsync above sets RootNodeId on success; a null
            // here would only be reached on a thrown WriteStateAsync
            // whose Class B revert has rolled back, in which case the
            // caller's bounded retry loop will re-issue WarmUpAsync.
            return;
        }

        var rootId = state.State.RootNodeId!.Value;
        if (state.State.RootIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootId);
            // CountAsync is a cheap read-only probe on IBPlusLeafGrain
            // (returns the live key count from in-memory state).
            await leaf.CountAsync();
        }
        else
        {
            var internalNode = grainFactory.GetGrain<IBPlusInternalGrain>(rootId);
            // AreChildrenLeavesAsync is read-only and trivial - it
            // returns a single bool from the routing snapshot.
            await internalNode.AreChildrenLeavesAsync();
        }
    }

    public async Task UnmarkDeletedAsync()
    {
        if (!state.State.IsDeleted) return;

        // See MarkDeletedAsync for the snapshot/restore rationale.
        var isDeletedSnapshot = state.State.IsDeleted;
        state.State.IsDeleted = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            throw;
        }
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
