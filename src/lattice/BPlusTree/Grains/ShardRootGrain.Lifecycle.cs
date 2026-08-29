using Microsoft.Extensions.Logging;

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
        if (RootIsLeafTyped)
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
        // Decide leaf-vs-internal by node TYPE so a corrupt RootIsLeaf flag
        // over an internal root (issue 899) still purges the internal subtree
        // and the leaf chain rather than treating the internal root as a leaf.
        var rootIsLeafTyped = RootIsLeafTyped;
        if (rootIsLeafTyped)
        {
            leafId = state.State.RootNodeId;
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var internalNodeIds = new List<GrainId>();
        if (!rootIsLeafTyped)
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

    /// <summary>
    /// Upper bound on the number of nodes a single
    /// <see cref="ReseedNodeBindingsAsync"/> pass will re-assert. The repair
    /// runs inside one grain call, and an unbounded node walk in one grain call
    /// is precisely what stranded the topology in the first place - a
    /// <c>PurgeTreeAsync</c> that blew the grain-call timeout part-way through
    /// its own walk. A recovery that timed out would be no better than the
    /// unbound leaf it is trying to repair, so the walk is capped and the
    /// overrun is reported rather than allowed to run long.
    /// </summary>
    private const int MaxReseedNodes = 4096;

    /// <summary>
    /// Maximum re-assert calls in flight at once inside
    /// <see cref="ReseedNodeBindingsAsync"/>. Bounded for the reason
    /// <see cref="BoundedFanOut"/> documents - a burst of one call per node all
    /// racing a single Orleans response deadline degrades into deadline
    /// failures rather than latency - but wide enough that the repair does not
    /// walk the budget one strictly sequential round trip at a time.
    /// </summary>
    private const int ReseedFanOutWidth = 16;

    /// <inheritdoc />
    public async Task ReseedNodeBindingsAsync()
    {
        // No topology to re-assert: EnsureRootAsync seeds a fresh root leaf,
        // binding included, on the first operation after recovery.
        if (state.State.RootNodeId is null) return;

        var rootIsLeafTyped = RootIsLeafTyped;

        // Probe before walking. PurgeAsync clears the leftmost leaf before any
        // other node in this shard, so a leftmost leaf that still carries its
        // tree-id binding proves no node here was cleared: the healthy
        // delete/recover cycle pays one edge descent plus one RPC and skips the
        // walk entirely.
        //
        // Best-effort: a probe that cannot resolve or reach the leftmost leaf
        // (a topology whose internal root was itself cleared, a leaf whose silo
        // is momentarily unreachable) must not fail the recovery that called
        // it. Recovery succeeds today without this repair, and an unbound node
        // still surfaces loudly and typed on the write path, so degrading to
        // "no repair" is strictly no worse than the status quo whereas throwing
        // would make recovery newly fragile.
        string? boundTreeId;
        try
        {
            var leftmostLeafId = rootIsLeafTyped
                ? state.State.RootNodeId!.Value
                : await TraverseToLeftmostLeafAsync();
            boundTreeId = await grainFactory.GetGrain<IBPlusLeafGrain>(leftmostLeafId).GetTreeIdAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Could not probe node bindings for shard {ShardIndex} of tree {TreeId} after recovery; "
                + "a node left unbound by an interrupted purge would keep rejecting typed CRDT writes to its key range.",
                MyShardIndex,
                TreeId);
            return;
        }

        // Healthy shard - nothing was torn down, so nothing to re-assert.
        if (boundTreeId is not null) return;

        var leafIds = new List<GrainId>();
        var internalIds = new List<GrainId>();
        var truncated = await CollectBindingTargetsAsync(rootIsLeafTyped, leafIds, internalIds);

        // Leaves first: an unbound leaf is what actually fails the write path,
        // whereas an unbound internal node only degrades its per-tree options
        // lookup. Both setters are idempotent, so a node that survived the
        // interrupted purge with its binding intact costs one short-circuited
        // RPC and no storage write.
        await BoundedFanOut.RunAsync(leafIds.Count, ReseedFanOutWidth, async slot =>
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[slot]);
            await leaf.SetTreeIdAsync(TreeId);
            await leaf.SetShardIndexAsync(MyShardIndex);
        });

        await BoundedFanOut.RunAsync(internalIds.Count, ReseedFanOutWidth, slot =>
            grainFactory.GetGrain<IBPlusInternalGrain>(internalIds[slot]).SetTreeIdAsync(TreeId));

        if (truncated)
        {
            logger.LogWarning(
                "Re-asserted node bindings on the first {NodeCount} nodes of shard {ShardIndex} of tree {TreeId} "
                + "after an interrupted purge, but the shard has more than the {MaxReseedNodes}-node repair budget; "
                + "keys routed to the nodes beyond it may still reject typed CRDT writes.",
                leafIds.Count + internalIds.Count,
                MyShardIndex,
                TreeId,
                MaxReseedNodes);
        }
        else
        {
            logger.LogWarning(
                "Re-asserted node bindings on {LeafCount} leaf and {InternalCount} internal node(s) of shard "
                + "{ShardIndex} of tree {TreeId}: an earlier purge was interrupted after clearing node state but "
                + "before clearing this shard root, leaving the recovered tree routing writes to unbound nodes.",
                leafIds.Count,
                internalIds.Count,
                MyShardIndex,
                TreeId);
        }
    }

    /// <summary>
    /// Collects every node this shard still routes to, splitting them into
    /// leaves and internal nodes, and returns whether the
    /// <see cref="MaxReseedNodes"/> budget truncated the walk.
    /// <para>
    /// Descends through the internal nodes rather than following the leaf
    /// sibling chain: the chain is exactly what an interrupted purge severs
    /// (clearing a leaf wipes its sibling pointers), whereas an internal node
    /// keeps its child ids until the purge reaches the internal sweep - which
    /// only starts once every leaf has already been cleared. Descending
    /// therefore reaches precisely the leaves routing can still deliver a write
    /// to, which is the set that has to be bound for the tree to be writable.
    /// </para>
    /// </summary>
    private async Task<bool> CollectBindingTargetsAsync(
        bool rootIsLeafTyped,
        List<GrainId> leafIds,
        List<GrainId> internalIds)
    {
        var rootNodeId = state.State.RootNodeId!.Value;
        if (rootIsLeafTyped)
        {
            leafIds.Add(rootNodeId);
            return false;
        }

        var stack = new Stack<GrainId>();
        stack.Push(rootNodeId);

        while (stack.Count > 0)
        {
            if (leafIds.Count + internalIds.Count >= MaxReseedNodes)
                return true;

            var nodeId = stack.Pop();
            internalIds.Add(nodeId);

            var node = grainFactory.GetGrain<IBPlusInternalGrain>(nodeId);
            // A node the purge already cleared reports no children, so the walk
            // simply stops there: everything below it is unreachable by routing
            // too, and re-binding an id nothing can route to buys nothing.
            var childrenAreLeaves = await node.AreChildrenLeavesAsync();
            var children = await node.GetChildIdsAsync();
            if (childrenAreLeaves)
            {
                leafIds.AddRange(children);
                continue;
            }

            for (int i = children.Count - 1; i >= 0; i--)
            {
                stack.Push(children[i]);
            }
        }

        return false;
    }
}
