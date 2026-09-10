using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bottom-up bulk-load and streaming bulk-append operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
    public async Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        if (state.State.RootNodeId is not null)
            throw new InvalidOperationException("BulkLoadAsync requires an empty shard. This shard already has data.");

        if (sortedEntries.Count == 0) return;

        RecordWrite(sortedEntries.Count);

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        var (leafIds, separators) = PlanBulkLoadLeaves(
            shardKey, operationId, "bulk", maxLeafKeys, sortedEntries.Count, i => sortedEntries[i].Key);

        // Stamp every entry's version in one sequential pass up front. The
        // clock is a strictly increasing sequence, so it cannot be ticked from
        // inside the overlapped wave below; ticking it here yields exactly the
        // versions the serial loop assigned, entry for entry.
        var clocks = new HybridLogicalClock[sortedEntries.Count];
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < clocks.Length; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            clocks[i] = clock;
        }

        await SeedBulkLoadLeavesAsync(leafIds, maxLeafKeys, sortedEntries.Count, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var kv = sortedEntries[start + j];
                batch[kv.Key] = LwwValue<byte[]>.Create(kv.Value, clocks[start + j]);
            }

            return batch;
        });

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
    }

    /// <inheritdoc />
    public async Task BulkLoadRawAsync(
        string operationId,
        List<LwwEntry> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        if (state.State.RootNodeId is not null)
            throw new InvalidOperationException("BulkLoadRawAsync requires an empty shard. This shard already has data.");

        if (sortedEntries.Count == 0) return;

        RecordWrite(sortedEntries.Count);

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        // Identical B+ tree assembly to BulkLoadAsync - the only difference is
        // that every entry's LwwValue (HLC version AND ExpiresAtTicks /
        // TTL) flows through verbatim instead of being re-stamped with a fresh
        // zero-based clock. Used by snapshot / restore (TreeSnapshotGrain) so
        // TTL metadata survives the transfer end-to-end.
        var (leafIds, separators) = PlanBulkLoadLeaves(
            shardKey, operationId, "bulkraw", maxLeafKeys, sortedEntries.Count, i => sortedEntries[i].Key);

        await SeedBulkLoadLeavesAsync(leafIds, maxLeafKeys, sortedEntries.Count, (start, count) =>
        {
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var e = sortedEntries[start + j];
                batch[e.Key] = e.ToLwwValue();
            }

            return batch;
        });

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
    }

    /// <summary>
    /// Computes every bulk-load leaf's deterministic identity and promoted
    /// separator up front, without issuing a single grain call.
    /// </summary>
    /// <remarks>
    /// Materialising the whole chain first is what makes
    /// <see cref="SeedBulkLoadLeavesAsync"/> safe to overlap: a leaf's sibling
    /// pointers are then known before any leaf is touched, so each leaf can
    /// stamp both of its own links at birth instead of the chain being wired up
    /// one leaf behind the loop. The ids are a pure function of the shard key,
    /// the operation id and the leaf ordinal, exactly as before, so a resumed
    /// bulk load addresses the same grains.
    /// </remarks>
    private (List<GrainId> LeafIds, List<string?> Separators) PlanBulkLoadLeaves(
        string shardKey,
        string operationId,
        string segment,
        int maxLeafKeys,
        int entryCount,
        Func<int, string> keyAt)
    {
        var leafCount = (entryCount + maxLeafKeys - 1) / maxLeafKeys;
        var leafIds = new List<GrainId>(leafCount);
        var separators = new List<string?>(leafCount);

        for (int i = 0, leafIndex = 0; i < entryCount; i += maxLeafKeys, leafIndex++)
        {
            var deterministicId = DeterministicGuid($"{shardKey}/{segment}/{operationId}/leaf/{leafIndex}");
            leafIds.Add(grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId).GetGrainId());
            separators.Add(leafIndex == 0 ? null : keyAt(i));
        }

        return (leafIds, separators);
    }

    /// <summary>
    /// Seeds and fills every planned bulk-load leaf in bounded overlapped waves.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Two calls per leaf, not five.</b> The serial version issued
    /// <c>SetTreeIdAsync</c>, <c>SetShardIndexAsync</c>, <c>MergeEntriesAsync</c>
    /// and then wired the sibling chain with a <c>SetNextSiblingAsync</c> on the
    /// previous leaf and a <c>SetPrevSiblingAsync</c> on this one - five gated,
    /// separately-persisted round trips per leaf, awaited one at a time. The
    /// first four collapse into the single <see cref="SiblingInitialization"/>
    /// batch that the leaf-split donor already uses, which acquires the split
    /// gate once and persists once for the whole batch. Because
    /// <see cref="PlanBulkLoadLeaves"/> has already computed the chain, each leaf
    /// stamps <i>both</i> of its links itself, so no leaf is touched by another
    /// leaf's unit of work.
    /// </para>
    /// <para>
    /// <b>Why the waves are safe.</b> Every leaf is a distinct, freshly created
    /// grain, and after the collapse no unit writes to a leaf other than its own,
    /// so the units are mutually independent and their completion order is
    /// immaterial. Nothing can observe an intermediate state either: the shard's
    /// root is published only by <see cref="FinalizeBulkLoadTreeAsync"/>, after
    /// this method has returned, and <c>BulkLoadAsync</c> refuses to run against
    /// a shard that already has a root. Within a unit the two calls stay ordered,
    /// because the birth seam must seed the durable materialiser block pin before
    /// <c>MergeEntriesAsync</c> makes the leaf's data reachable in the WAL - the
    /// same ordering the serial version had.
    /// </para>
    /// <para>
    /// The batch for a leaf is built inside its unit rather than up front, so the
    /// live batch set stays O(<see cref="BoundedFanOut.DefaultWidth"/>) instead of
    /// holding one dictionary per leaf for the whole load.
    /// </para>
    /// </remarks>
    private Task SeedBulkLoadLeavesAsync(
        List<GrainId> leafIds,
        int maxLeafKeys,
        int entryCount,
        Func<int, int, Dictionary<string, LwwValue<byte[]>>> buildBatch)
    {
        var slots = new int[leafIds.Count];
        for (int i = 0; i < slots.Length; i++)
        {
            slots[i] = i;
        }

        var treeId = TreeId;
        var shardIndex = MyShardIndex;

        return BoundedFanOut.ForEachAsync(
            slots,
            BoundedFanOut.DefaultWidth,
            async slot =>
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[slot]);
                await leaf.InitializeSiblingAsync(new SiblingInitialization
                {
                    TreeId = treeId,
                    ShardIndex = shardIndex,
                    // Bulk-load leaves carry no ownership range, exactly as the
                    // serial version left them - it never called SetKeyRangeAsync.
                    LowKeyInclusive = null,
                    HighKeyExclusive = null,
                    PrevSibling = slot > 0 ? leafIds[slot - 1] : null,
                    NextSibling = slot + 1 < leafIds.Count ? leafIds[slot + 1] : null,
                });

                var start = slot * maxLeafKeys;
                var count = Math.Min(maxLeafKeys, entryCount - start);
                await leaf.MergeEntriesAsync(buildBatch(start, count));
            });
    }

    /// <summary>
    /// Shared bottom-up internal-node assembly used by both
    /// <see cref="BulkLoadAsync"/> and <see cref="BulkLoadRawAsync"/>. Builds
    /// internal nodes from <paramref name="leafIds"/> / <paramref name="separators"/>,
    /// persists the root, and records <paramref name="operationId"/> as complete.
    /// </summary>
    private async Task FinalizeBulkLoadTreeAsync(
        string operationId,
        List<GrainId> leafIds,
        List<string?> separators,
        int maxChildren)
    {
        if (leafIds.Count == 1)
        {
            state.State.RootNodeId = leafIds[0];
            state.State.RootIsLeaf = true;
            state.State.LastCompletedBulkOperationId = operationId;
            await WriteShardStateAsync();
            return;
        }

        var shardKey = context.GrainId.Key.ToString()!;
        var currentLevel = new List<(string? separator, GrainId id)>(leafIds.Count);
        for (int i = 0; i < leafIds.Count; i++)
            currentLevel.Add((separators[i], leafIds[i]));

        bool childrenAreLeaves = true;
        int level = 0;

        while (currentLevel.Count > 1)
        {
            var nodeCount = (currentLevel.Count + maxChildren - 1) / maxChildren;
            var nextLevel = new List<(string? separator, GrainId id)>(nodeCount);
            var plans = new List<(GrainId Id, List<string?> Seps, List<GrainId> Ids)>(nodeCount);
            int nodeIndex = 0;

            for (int i = 0; i < currentLevel.Count; i += maxChildren)
            {
                int end = Math.Min(i + maxChildren, currentLevel.Count);
                var chunkSize = end - i;

                var promotedSeparator = currentLevel[i].separator;

                var seps = new List<string?>(chunkSize) { null };
                var ids = new List<GrainId>(chunkSize) { currentLevel[i].id };
                for (int j = i + 1; j < end; j++)
                {
                    seps.Add(currentLevel[j].separator);
                    ids.Add(currentLevel[j].id);
                }

                var deterministicId = DeterministicGuid($"{shardKey}/bulk/{operationId}/internal/{level}/{nodeIndex++}");
                var nodeId = grainFactory.GetGrain<IBPlusInternalGrain>(deterministicId).GetGrainId();

                plans.Add((nodeId, seps, ids));
                nextLevel.Add((promotedSeparator, nodeId));
            }

            // One level's nodes are siblings: each is a distinct, freshly created
            // grain initialised from a disjoint slice of the level below, and none
            // reads another, so the two calls that build a node are independent of
            // every other node's. Issued one node at a time a level cost N
            // sequential round trips; issued in bounded waves it costs
            // ceil(N / BoundedFanOut.DefaultWidth). The levels themselves stay
            // strictly ordered - a level's ids must exist before the level above
            // can name them as children - and the tree is unreachable until the
            // root is published below, so nothing observes a half-built level.
            var levelChildrenAreLeaves = childrenAreLeaves;
            var levelTreeId = TreeId;
            await BoundedFanOut.ForEachAsync(
                plans,
                BoundedFanOut.DefaultWidth,
                async plan =>
                {
                    var node = grainFactory.GetGrain<IBPlusInternalGrain>(plan.Id);
                    await node.SetTreeIdAsync(levelTreeId);
                    await node.InitializeWithChildrenAsync(plan.Seps, plan.Ids, levelChildrenAreLeaves);
                });

            currentLevel = nextLevel;
            childrenAreLeaves = false;
            level++;
        }

        state.State.RootNodeId = currentLevel[0].id;
        state.State.RootIsLeaf = false;
        state.State.LastCompletedBulkOperationId = operationId;
        await WriteShardStateAsync();
    }

    public async Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        EnsureInternalOrigin(LatticeOperation.BulkLoad);
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;
        RecordWrite(sortedEntries.Count);

        if (state.State.PendingBulkGraft is not null)
        {
            if (state.State.PendingBulkGraft.OperationId == operationId)
            {
                await CompleteBulkGraftAsync();
                return;
            }
            await CompleteBulkGraftAsync();
        }

        if (sortedEntries.Count == 0) return;

        await EnsureRootAsync();
        await ResumePendingPromotionAsync();

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var clock = HybridLogicalClock.Zero;

        // Decided by node TYPE so a corrupt RootIsLeaf flag over an internal
        // root (issue 899) descends to the rightmost leaf rather than
        // blind-casting the internal root to IBPlusLeafGrain.
        GrainId rightmostLeafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : await TraverseToRightmostLeafAsync();

        var rightmostLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(rightmostLeafId);
        var existingKeys = await rightmostLeaf.GetKeysAsync();
        int space = maxLeafKeys - existingKeys.Count;

        int idx = 0;
        if (space > 0)
        {
            int take = Math.Min(space, sortedEntries.Count);
            var batch = new Dictionary<string, LwwValue<byte[]>>(take);
            for (int i = 0; i < take; i++, idx++)
            {
                clock = HybridLogicalClock.Tick(clock);
                batch[sortedEntries[idx].Key] = LwwValue<byte[]>.Create(sortedEntries[idx].Value, clock);
            }
            await rightmostLeaf.MergeEntriesAsync(batch);
        }

        if (idx >= sortedEntries.Count)
        {
            state.State.LastCompletedBulkOperationId = operationId;
            await WriteShardStateAsync();
            return;
        }

        var graftEntries = new List<GraftEntry>();
        GrainId? prevNewLeafId = null;
        int leafIndex = 0;

        while (idx < sortedEntries.Count)
        {
            int take = Math.Min(maxLeafKeys, sortedEntries.Count - idx);
            var separator = sortedEntries[idx].Key;

            var batch = new Dictionary<string, LwwValue<byte[]>>(take);
            for (int i = 0; i < take; i++, idx++)
            {
                clock = HybridLogicalClock.Tick(clock);
                batch[sortedEntries[idx].Key] = LwwValue<byte[]>.Create(sortedEntries[idx].Value, clock);
            }

            var deterministicId = DeterministicGuid($"{shardKey}/append/{operationId}/leaf/{leafIndex++}");
            var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
            var newId = newLeaf.GetGrainId();
            await newLeaf.SetTreeIdAsync(TreeId);
            await newLeaf.SetShardIndexAsync(MyShardIndex);
            await newLeaf.MergeEntriesAsync(batch);

            if (prevNewLeafId is not null)
            {
                var prevLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(prevNewLeafId.Value);
                await prevLeaf.SetNextSiblingAsync(newId);
                await newLeaf.SetPrevSiblingAsync(prevNewLeafId.Value);
            }

            graftEntries.Add(new GraftEntry { SeparatorKey = separator, LeafId = newId });
            prevNewLeafId = newId;
        }

        state.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = operationId,
            ExistingRightmostLeafId = rightmostLeafId,
            NewLeaves = graftEntries,
            RootWasLeaf = state.State.RootIsLeaf,
        };
        await WriteShardStateAsync();

        await CompleteBulkGraftAsync();
    }

    /// <summary>
    /// Completes (or resumes) a bulk-append graft whose intent has been persisted.
    /// </summary>
    private async Task CompleteBulkGraftAsync()
    {
        var graft = state.State.PendingBulkGraft!;

        var existingLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.ExistingRightmostLeafId);
        var firstNewLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.NewLeaves[0].LeafId);
        await existingLeaf.SetNextSiblingAsync(graft.NewLeaves[0].LeafId);
        await firstNewLeaf.SetPrevSiblingAsync(graft.ExistingRightmostLeafId);

        // Hoisted out of the per-entry loop: a single Stack<GrainId> is reused
        // across every entry in graft.NewLeaves via Clear() (which preserves
        // the backing array). This eliminates ~152 B of per-entry transient
        // allocation (24 B Stack header + ~128 B GrainId[4] backing on first
        // Push) on the steady-state non-RootIsLeaf branch. The Stack is
        // unused on the RootIsLeaf early-continue branch.
        var path = new Stack<GrainId>();

        foreach (var entry in graft.NewLeaves)
        {
            if (state.State.RootIsLeaf)
            {
                // Rare branch: only the very first entry of the very first
                // graft on a flat-tree shard hits this; PromoteRootAsync
                // persists the SplitResult via state.PendingPromotion, so
                // the allocation is required here.
                var splitResult = new SplitResult
                {
                    PromotedKey = entry.SeparatorKey,
                    NewSiblingId = entry.LeafId,
                    ChildIsLeaf = true,
                };
                await PromoteRootAsync(splitResult);
                continue;
            }

            path.Clear();
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var node = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
                path.Push(currentId);

                if (await node.AreChildrenLeavesAsync())
                    break;

                currentId = await node.GetRightmostChildAsync();
            }

            // Track the pending promoted (key, child) as locals instead of
            // boxing them in a SplitResult instance. The vast majority of
            // entries trigger a leaf split that the parent internal absorbs
            // without re-splitting, so pendingHasValue flips false on the
            // first null return from AcceptSplitAsync. The class allocation
            // is only paid in the very rare case where the root itself
            // needs to split (caught by the `if (pendingHasValue)` tail).
            //
            // pendingChildIsLeaf tracks the node-type of pendingChild as it
            // bubbles up: it starts as `true` (the graft-supplied leaf id)
            // and flips to `false` as soon as an internal-parent's
            // AcceptSplitAsync returns a non-null bubble (whose NewSiblingId
            // is the freshly-split internal sibling). This value is stamped
            // into the final PromoteRootAsync's SplitResult so the
            // CompletePromotionAsync seam can dispatch SeedChildParentAsync
            // through IBPlusLeafGrain or IBPlusInternalGrain without
            // re-reading the (potentially raced) RootIsLeaf flag.
            var pendingKey = entry.SeparatorKey;
            var pendingChild = entry.LeafId;
            var pendingChildIsLeaf = true;
            var pendingHasValue = true;
            while (pendingHasValue && path.Count > 0)
            {
                var parentId = path.Pop();
                var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
                var bubble = await parent.AcceptSplitAsync(pendingKey, pendingChild);
                InvalidateRoutingTable(parentId);
                if (bubble is null)
                {
                    pendingHasValue = false;
                }
                else
                {
                    pendingKey = bubble.PromotedKey;
                    pendingChild = bubble.NewSiblingId;
                    pendingChildIsLeaf = bubble.ChildIsLeaf;
                }
            }

            if (pendingHasValue)
            {
                await PromoteRootAsync(new SplitResult
                {
                    PromotedKey = pendingKey,
                    NewSiblingId = pendingChild,
                    ChildIsLeaf = pendingChildIsLeaf,
                });
            }
        }

        state.State.PendingBulkGraft = null;
        state.State.LastCompletedBulkOperationId = graft.OperationId;
        await WriteShardStateAsync();
    }

    /// <summary>
    /// If a previous bulk-append graft was interrupted, resume it now.
    /// </summary>
    private async Task ResumePendingBulkGraftAsync()
    {
        if (state.State.PendingBulkGraft is null) return;
        await CompleteBulkGraftAsync();
    }
}
