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
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        if (state.State.RootNodeId is not null)
            throw new InvalidOperationException("BulkLoadAsync requires an empty shard. This shard already has data.");

        if (sortedEntries.Count == 0) return;

        RecordWrite();

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;
        var clock = HybridLogicalClock.Zero;

        var leafIds = new List<GrainId>();
        var separators = new List<string?>();
        GrainId? prevLeafId = null;
        int leafIndex = 0;

        for (int i = 0; i < sortedEntries.Count; i += maxLeafKeys)
        {
            int count = Math.Min(maxLeafKeys, sortedEntries.Count - i);
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                clock = HybridLogicalClock.Tick(clock);
                var kv = sortedEntries[i + j];
                batch[kv.Key] = LwwValue<byte[]>.Create(kv.Value, clock);
            }

            var deterministicId = DeterministicGuid($"{shardKey}/bulk/{operationId}/leaf/{leafIndex++}");
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
            var leafId = leaf.GetGrainId();
            await leaf.SetTreeIdAsync(TreeId);
            await leaf.MergeEntriesAsync(batch);

            if (prevLeafId is not null)
            {
                var prevLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(prevLeafId.Value);
                await prevLeaf.SetNextSiblingAsync(leafId);
                await leaf.SetPrevSiblingAsync(prevLeafId.Value);
            }

            separators.Add(leafIds.Count == 0 ? null : sortedEntries[i].Key);
            leafIds.Add(leafId);
            prevLeafId = leafId;
        }

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
    }

    /// <inheritdoc />
    public async Task BulkLoadRawAsync(
        string operationId,
        List<LwwEntry> sortedEntries)
    {
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        if (state.State.RootNodeId is not null)
            throw new InvalidOperationException("BulkLoadRawAsync requires an empty shard. This shard already has data.");

        if (sortedEntries.Count == 0) return;

        RecordWrite();

        var shardKey = context.GrainId.Key.ToString()!;
        var options = await GetOptionsAsync();
        var maxLeafKeys = options.MaxLeafKeys;
        var maxChildren = options.MaxInternalChildren;

        var leafIds = new List<GrainId>();
        var separators = new List<string?>();
        GrainId? prevLeafId = null;
        int leafIndex = 0;

        // Identical B+ tree assembly to BulkLoadAsync — the only difference is
        // that every entry's LwwValue (HLC version AND ExpiresAtTicks /
        // TTL) flows through verbatim instead of being re-stamped with a fresh
        // zero-based clock. Used by snapshot / restore (TreeSnapshotGrain) so
        // TTL metadata survives the transfer end-to-end.
        for (int i = 0; i < sortedEntries.Count; i += maxLeafKeys)
        {
            int count = Math.Min(maxLeafKeys, sortedEntries.Count - i);
            var batch = new Dictionary<string, LwwValue<byte[]>>(count);
            for (int j = 0; j < count; j++)
            {
                var e = sortedEntries[i + j];
                batch[e.Key] = e.ToLwwValue();
            }

            var deterministicId = DeterministicGuid($"{shardKey}/bulkraw/{operationId}/leaf/{leafIndex++}");
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
            var leafId = leaf.GetGrainId();
            await leaf.SetTreeIdAsync(TreeId);
            await leaf.MergeEntriesAsync(batch);

            if (prevLeafId is not null)
            {
                var prevLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(prevLeafId.Value);
                await prevLeaf.SetNextSiblingAsync(leafId);
                await leaf.SetPrevSiblingAsync(prevLeafId.Value);
            }

            separators.Add(leafIds.Count == 0 ? null : sortedEntries[i].Key);
            leafIds.Add(leafId);
            prevLeafId = leafId;
        }

        await FinalizeBulkLoadTreeAsync(operationId, leafIds, separators, maxChildren);
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
            await state.WriteStateAsync();
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
            var nextLevel = new List<(string? separator, GrainId id)>();
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
                var node = grainFactory.GetGrain<IBPlusInternalGrain>(deterministicId);
                await node.SetTreeIdAsync(TreeId);
                await node.InitializeWithChildrenAsync(seps, ids, childrenAreLeaves);

                nextLevel.Add((promotedSeparator, node.GetGrainId()));
            }

            currentLevel = nextLevel;
            childrenAreLeaves = false;
            level++;
        }

        state.State.RootNodeId = currentLevel[0].id;
        state.State.RootIsLeaf = false;
        state.State.LastCompletedBulkOperationId = operationId;
        await state.WriteStateAsync();
    }

    public async Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        ThrowIfDeleted();
        if (state.State.LastCompletedBulkOperationId == operationId) return;
        RecordWrite();

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

        GrainId rightmostLeafId = state.State.RootIsLeaf
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
            await state.WriteStateAsync();
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
        await state.WriteStateAsync();

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

        foreach (var entry in graft.NewLeaves)
        {
            var splitResult = new SplitResult { PromotedKey = entry.SeparatorKey, NewSiblingId = entry.LeafId };

            if (state.State.RootIsLeaf)
            {
                await PromoteRootAsync(splitResult);
                continue;
            }

            var path = new Stack<GrainId>();
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var node = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
                path.Push(currentId);

                if (await node.AreChildrenLeavesAsync())
                    break;

                currentId = await node.GetRightmostChildAsync();
            }

            SplitResult? pending = splitResult;
            while (pending is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
                pending = await parent.AcceptSplitAsync(pending.PromotedKey, pending.NewSiblingId);
            }

            if (pending is not null)
            {
                await PromoteRootAsync(pending);
            }
        }

        state.State.PendingBulkGraft = null;
        state.State.LastCompletedBulkOperationId = graft.OperationId;
        await state.WriteStateAsync();
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
