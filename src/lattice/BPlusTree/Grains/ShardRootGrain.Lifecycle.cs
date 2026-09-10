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

        // Opt-in leaf-cache pre-warm (issue #332). Off unless
        // LatticeOptions.LeafCachePreWarmCount is positive. Ranks this shard's
        // persisted leaf-access Markov chain by long-run read probability and
        // primes that many LeafCacheGrain activations on this silo - the same
        // silo that will serve the reads, because this grain is the only caller
        // of the stateless-worker cache. Strictly best-effort: every failure is
        // swallowed inside, so pre-warm can never fail warm-up.
        await PreWarmLeafCachesAsync();
    }

    /// <inheritdoc />
    public Task ForceDeactivateAsync()
    {
        // Test-only deactivation seam, mirroring
        // BPlusLeafGrain.ForceDeactivateAsync. Wraps the protected
        // Grain.DeactivateOnIdle() extension so integration tests can drive a
        // real deactivate/reactivate cycle - and therefore a real
        // OnDeactivateAsync leaf-access-model flush through the real Orleans
        // serializer and storage provider - without waiting on the silo's
        // idle-collection scheduler. The runtime schedules the deactivation
        // after the current grain turn completes, so the caller must poll or
        // briefly wait before observing the fresh activation; blocking here
        // would deadlock, because OnDeactivateAsync can only run once this
        // turn ends.
        this.DeactivateOnIdle();
        return Task.CompletedTask;
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

        // DELIBERATELY NOT WORK-BOUNDED (issue 1956). Do not apply
        // LeafWalkBudget here. Two reasons: the tree is already offline by
        // contract when this runs (DeleteTreeAsync makes every subsequent read
        // and write throw), so no live traffic is waiting on this shard; and
        // the walk is destructive, so it cannot resume by key the way a read
        // walk can - once a leaf's state is cleared its sibling pointer is gone,
        // which is why nextId is read before the clear. Bounded by observability.
        var walk = new AtomicLeafWalk("PurgeShardAsync");
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var nextId = await leaf.GetNextSiblingAsync();
            await leaf.ClearGrainStateAsync();
            walk.RecordLeafVisited();
            leafId = nextId;
        }

        walk.ReportIfSlow(logger, context.GrainId);

        await ClearInternalNodesAsync(internalNodeIds);

        await state.ClearStateAsync();
    }

    /// <summary>
    /// Clears every collected internal node, in bounded overlapped waves.
    /// </summary>
    /// <remarks>
    /// Unlike the leaf chain - which must stay serial because a leaf's sibling
    /// pointer has to be read before its state is cleared - the internal-node set
    /// is fully materialised by <see cref="CollectInternalNodeIds"/> before the
    /// first clear is issued, so the clears are independent of one another and of
    /// the traversal that produced them, and their completion order is
    /// immaterial. Issued one at a time they turn a purge of an I-node tree into
    /// I sequential round trips; issued in bounded waves they cost
    /// ceil(I / <see cref="BoundedFanOut.DefaultWidth"/>) instead. The purge runs
    /// against a tree that is already offline by contract, so nothing observes an
    /// intermediate state of this sweep either way.
    /// </remarks>
    private Task ClearInternalNodesAsync(List<GrainId> internalNodeIds) =>
        BoundedFanOut.ForEachAsync(
            internalNodeIds,
            BoundedFanOut.DefaultWidth,
            id => grainFactory.GetGrain<IBPlusInternalGrain>(id).ClearGrainStateAsync());

    /// <summary>
    /// Collects every internal node id beneath <paramref name="rootNodeId"/>
    /// (inclusive) so <see cref="ClearInternalNodesAsync"/> can sweep them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>One call per node, not two.</b> The walk used to ask each node
    /// <c>AreChildrenLeavesAsync</c> and then <c>GetChildIdsAsync</c> - two
    /// round trips to learn two fields of the same state.
    /// <c>GetRoutingTableAsync</c> returns both in one snapshot, so a walk over
    /// I nodes issues I calls rather than 2I.
    /// </para>
    /// <para>
    /// <b>Level-parallel, not serial.</b> The old walk was a depth-first stack
    /// that awaited each node before it knew the next one to visit, so the whole
    /// pre-walk was strictly sequential - and it is what feeds the sweep that
    /// already runs in bounded overlapped waves, so the sweep was fast and the
    /// walk that finds its input was not. A level's nodes are known in full once
    /// the level above has been read, and reading a node is a pure query, so a
    /// level's reads are independent and overlap safely.
    /// <see cref="BoundedFanOut.ReadAheadAsync"/> keeps them in input order and
    /// holds only <see cref="BoundedFanOut.DefaultWidth"/> reads in flight, so a
    /// wide level does not burst one call per node. The levels themselves stay
    /// ordered, since a level's ids are only known from the level above.
    /// </para>
    /// <para>
    /// The collected order changes from depth-first to breadth-first. The set is
    /// identical, and its only consumer clears the nodes in a fan-out whose
    /// completion order is already undefined, so nothing depends on the
    /// traversal order.
    /// </para>
    /// </remarks>
    private async Task CollectInternalNodeIds(GrainId rootNodeId, List<GrainId> collected)
    {
        var level = new List<GrainId> { rootNodeId };

        while (level.Count > 0)
        {
            collected.AddRange(level);

            var next = new List<GrainId>();
            await foreach (var routing in BoundedFanOut.ReadAheadAsync(
                level,
                BoundedFanOut.DefaultWidth,
                id => grainFactory.GetGrain<IBPlusInternalGrain>(id).GetRoutingTableAsync()))
            {
                if (routing.ChildrenAreLeaves)
                {
                    continue;
                }

                next.AddRange(routing.ChildIds);
            }

            level = next;
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

        // Walk unconditionally rather than probing one node and inferring the
        // rest. An earlier revision of this repair probed the leftmost leaf on
        // the theory that PurgeAsync clears it first, so a bound leftmost leaf
        // proved the whole shard was intact. That inference does not hold: a
        // split inherits the donor's binding verbatim, so an unbound leaf mints
        // an unbound sibling anywhere in the key range while the leftmost leaf
        // stays perfectly bound. Recovery is a rare operator action, so paying
        // the full walk to be correct is the right trade.
        var leafIds = new List<GrainId>();
        var internalIds = new List<GrainId>();
        bool truncated;
        try
        {
            truncated = await CollectBindingTargetsAsync(RootIsLeafTyped, leafIds, internalIds);
        }
        catch (Exception ex)
        {
            // Best-effort: a topology whose internal root was itself cleared has
            // nothing to descend, and a node's silo may be momentarily
            // unreachable. Recovery succeeds today without this repair and an
            // unbound node still surfaces loudly and typed on the write path
            // (where it is now also self-repairing), so degrading to "no repair"
            // is strictly no worse than the status quo, whereas throwing would
            // make recovery newly fragile.
            logger.LogWarning(
                ex,
                "Could not walk shard {ShardIndex} of tree {TreeId} to re-assert node bindings after recovery; "
                + "a node left unbound by an interrupted purge would keep rejecting typed CRDT writes to its key "
                + "range until the write path re-binds it.",
                MyShardIndex,
                TreeId);
            return;
        }

        // Leaves first: an unbound leaf is what actually fails the write path,
        // whereas an unbound internal node only degrades its per-tree options
        // lookup. Both setters are idempotent and short-circuit inside the
        // callee, so a node that is already bound costs one RPC and no storage
        // write - which is what makes an unconditional walk affordable.
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
                + "after recovery, but the shard has more than the {MaxReseedNodes}-node repair budget; "
                + "keys routed to the nodes beyond it are re-bound by the write path on their next typed CRDT write.",
                leafIds.Count + internalIds.Count,
                MyShardIndex,
                TreeId,
                MaxReseedNodes);
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
