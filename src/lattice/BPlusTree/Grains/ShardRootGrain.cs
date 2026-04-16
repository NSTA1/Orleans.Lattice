using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The root grain for a single shard. Lazily creates the first leaf and
/// handles root splits by creating a new internal root above the old one.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// </summary>
internal sealed class ShardRootGrain(
    IGrainContext context,
    [PersistentState("shardroot", LatticeOptions.StorageProviderName)] IPersistentState<ShardRootState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IShardRootGrain
{
    private string? _treeId;
    private string TreeId => _treeId ??= ComputeTreeId();
    private string ComputeTreeId()
    {
        var key = context.GrainId.Key.ToString()!;
        return key[..key.LastIndexOf('/')];
    }

    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    private static readonly ObjectPool<Stack<GrainId>> StackPool =
        new DefaultObjectPoolProvider().Create(new StackPoolPolicy());

    private sealed class StackPoolPolicy : PooledObjectPolicy<Stack<GrainId>>
    {
        public override Stack<GrainId> Create() => new();
        public override bool Return(Stack<GrainId> obj) { obj.Clear(); return true; }
    }

    private const int MaxRetries = 2;

    public async Task<byte[]?> GetAsync(string key)
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();
        return await TraverseForReadAsync(key);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var splitResult = await TraverseForWriteAsync(key, value);

                // If the root node split, we need to create a new internal root.
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                return;
            }
            catch when (attempt < MaxRetries)
            {
                // The failed grain will be deactivated by Orleans. On retry, a fresh
                // activation loads clean state and the recovery guards resume any
                // interrupted split.
            }
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
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
            catch when (attempt < MaxRetries)
            {
                // Retry — same rationale as SetAsync.
            }
        }
    }

    public async Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();

        // Determine the starting leaf.
        var seekKey = continuationToken ?? startInclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        // Walk the sibling chain, collecting keys until the page is full.
        var keys = new List<string>(pageSize);
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive);

            foreach (var key in leafKeys)
            {
                // Skip keys already returned in previous pages.
                if (continuationToken is not null &&
                    string.Compare(key, continuationToken, StringComparison.Ordinal) <= 0)
                    continue;

                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new KeysPage { Keys = keys, HasMore = false };

            leafId = nextSibling.Value;
        }

        return new KeysPage { Keys = keys, HasMore = true };
    }

    public async Task<KeysPage> GetSortedKeysBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();

        // Determine the starting leaf (rightmost, or the leaf for the seek key).
        var seekKey = continuationToken ?? endExclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToRightmostLeafAsync();
        }

        // Walk the sibling chain backward, collecting keys in reverse until the page is full.
        var keys = new List<string>(pageSize);
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive);

            // Walk the leaf's keys in reverse order.
            for (int i = leafKeys.Count - 1; i >= 0; i--)
            {
                var key = leafKeys[i];

                // Skip keys already returned in previous pages.
                if (continuationToken is not null &&
                    string.Compare(key, continuationToken, StringComparison.Ordinal) >= 0)
                    continue;

                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            var prevSibling = await leafGrain.GetPrevSiblingAsync();
            if (prevSibling is null)
                return new KeysPage { Keys = keys, HasMore = false };

            leafId = prevSibling.Value;
        }

        return new KeysPage { Keys = keys, HasMore = true };
    }

    public async Task<GrainId?> GetLeftmostLeafIdAsync()
    {
        if (state.State.RootNodeId is null) return null;
        return await TraverseToLeftmostLeafAsync();
    }

    private async Task EnsureRootAsync()
    {
        if (state.State.RootNodeId is not null) return;

        // Use a deterministic GrainId derived from this shard's own identity
        // so that a crash-retry reuses the same leaf instead of creating an orphan.
        var shardKey = context.GrainId.Key.ToString()!;
        var deterministicId = DeterministicGuid(shardKey);
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
        await leafGrain.SetTreeIdAsync(TreeId);
        state.State.RootNodeId = leafGrain.GetGrainId();
        state.State.RootIsLeaf = true;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// If a previous root promotion was interrupted (Phase 1 persisted but
    /// Phase 2 did not complete), resume it now.
    /// </summary>
    private async Task ResumePendingPromotionAsync()
    {
        if (state.State.PendingPromotion is null) return;
        await CompletePromotionAsync();
    }

    /// <summary>
    /// Produces a deterministic <see cref="Guid"/> from <paramref name="input"/>
    /// using a SHA-256 hash truncated to 16 bytes. This ensures crash-retries
    /// in <see cref="EnsureRootAsync"/> reuse the same grain identity.
    /// </summary>
    private static Guid DeterministicGuid(string input)
    {
        var hash = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(input));
        return new Guid(hash.AsSpan(0, 16));
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
        var path = StackPool.Get();
        try
        {
        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId);
            var childId = await internalGrain.RouteAsync(key);
            var childrenAreLeaves = await internalGrain.AreChildrenLeavesAsync();

            if (childrenAreLeaves)
            {
                // currentId is the parent of the leaf — push it for split propagation.
                path.Push(currentId);
                path.Push(childId); // the leaf
                break;
            }

            // The child is another internal node — descend further.
            path.Push(currentId);
            currentId = childId;
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
            var childId = await internalGrain.RouteAsync(key);
            var childrenAreLeaves = await internalGrain.AreChildrenLeavesAsync();

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
            var childId = await internalGrain.GetLeftmostChildAsync();
            var childrenAreLeaves = await internalGrain.AreChildrenLeavesAsync();

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
            var childId = await internalGrain.GetRightmostChildAsync();
            var childrenAreLeaves = await internalGrain.AreChildrenLeavesAsync();

            if (childrenAreLeaves)
            {
                return childId;
            }

            currentId = childId;
        }
    }

    private async Task<SplitResult?> PromoteRootAsync(SplitResult splitResult)
    {
        // Phase 1: Persist the split result that triggered the promotion so that
        // a crash-retry can resume without creating orphan roots.
        state.State.PendingPromotion = splitResult;
        state.State.PendingPromotionRootWasLeaf = state.State.RootIsLeaf;
        await state.WriteStateAsync();

        // Phase 2: Create the new internal root.
        await CompletePromotionAsync();
        return null; // New root was just created with two children — no split.
    }

    /// <summary>
    /// Completes (or resumes) a root promotion whose intent has already been persisted.
    /// Uses a deterministic <see cref="Guid"/> derived from the shard identity and
    /// the old root's <see cref="GrainId"/> so that crash-retries reuse the same grain
    /// instead of creating orphans.
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

    public async Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        ThrowIfDeleted();
        // Idempotency: skip if this exact operation already completed.
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        if (state.State.RootNodeId is not null)
            throw new InvalidOperationException("BulkLoadAsync requires an empty shard. This shard already has data.");

        if (sortedEntries.Count == 0) return;

        var shardKey = context.GrainId.Key.ToString()!;
        var maxLeafKeys = Options.MaxLeafKeys;
        var maxChildren = Options.MaxInternalChildren;
        var clock = HybridLogicalClock.Zero;

        // Phase 1: Create leaves with deterministic IDs, filling each to capacity.
        var leafIds = new List<GrainId>();
        var separators = new List<string?>(); // separator[0] = null (leftmost)
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

            // Wire doubly-linked list.
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

        // Phase 2: If only one leaf, it is the root.
        if (leafIds.Count == 1)
        {
            state.State.RootNodeId = leafIds[0];
            state.State.RootIsLeaf = true;
            state.State.LastCompletedBulkOperationId = operationId;
            await state.WriteStateAsync();
            return;
        }

        // Phase 3: Build internal nodes bottom-up with deterministic IDs.
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

        // Atomic commit: set root pointer and mark operation complete.
        state.State.RootNodeId = currentLevel[0].id;
        state.State.RootIsLeaf = false;
        state.State.LastCompletedBulkOperationId = operationId;
        await state.WriteStateAsync();
    }

    public async Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries)
    {
        ThrowIfDeleted();
        // Idempotency: skip if this exact operation already completed.
        if (state.State.LastCompletedBulkOperationId == operationId) return;

        // If a previous graft for this operation was interrupted, resume it.
        if (state.State.PendingBulkGraft is not null)
        {
            if (state.State.PendingBulkGraft.OperationId == operationId)
            {
                await CompleteBulkGraftAsync();
                return;
            }
            // A different graft was interrupted — complete it first.
            await CompleteBulkGraftAsync();
        }

        if (sortedEntries.Count == 0) return;

        await EnsureRootAsync();
        await ResumePendingPromotionAsync();

        var shardKey = context.GrainId.Key.ToString()!;
        var maxLeafKeys = Options.MaxLeafKeys;
        var clock = HybridLogicalClock.Zero;

        // Step 1: Find the rightmost leaf and fill remaining space.
        // MergeEntriesAsync is idempotent (LWW), so safe on retry.
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
            // Everything fit in the existing rightmost leaf — no graft needed.
            state.State.LastCompletedBulkOperationId = operationId;
            await state.WriteStateAsync();
            return;
        }

        // Step 2: Create new leaves in isolation with deterministic IDs.
        // Sibling links are wired only among new leaves (NOT to existing tree yet).
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

            // Wire sibling links among new leaves only.
            if (prevNewLeafId is not null)
            {
                var prevLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(prevNewLeafId.Value);
                await prevLeaf.SetNextSiblingAsync(newId);
                await newLeaf.SetPrevSiblingAsync(prevNewLeafId.Value);
            }

            graftEntries.Add(new GraftEntry { SeparatorKey = separator, LeafId = newId });
            prevNewLeafId = newId;
        }

        // Step 3: Persist the graft intent — this is the atomic commit point.
        // After this write, a crash-retry will find PendingBulkGraft and resume.
        state.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = operationId,
            ExistingRightmostLeafId = rightmostLeafId,
            NewLeaves = graftEntries,
            RootWasLeaf = state.State.RootIsLeaf,
        };
        await state.WriteStateAsync();

        // Step 4: Execute the graft (wire to existing tree + propagate separators).
        await CompleteBulkGraftAsync();
    }

    /// <summary>
    /// Completes (or resumes) a bulk-append graft whose intent has been persisted.
    /// Wires the first new leaf to the existing rightmost leaf, then propagates
    /// separators into the internal node tree.
    /// </summary>
    private async Task CompleteBulkGraftAsync()
    {
        var graft = state.State.PendingBulkGraft!;

        // Wire the existing rightmost leaf → first new leaf (idempotent SetNext/SetPrev).
        var existingLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.ExistingRightmostLeafId);
        var firstNewLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(graft.NewLeaves[0].LeafId);
        await existingLeaf.SetNextSiblingAsync(graft.NewLeaves[0].LeafId);
        await firstNewLeaf.SetPrevSiblingAsync(graft.ExistingRightmostLeafId);

        // Propagate each new leaf as a split into the internal node tree.
        foreach (var entry in graft.NewLeaves)
        {
            var splitResult = new SplitResult { PromotedKey = entry.SeparatorKey, NewSiblingId = entry.LeafId };

            if (state.State.RootIsLeaf)
            {
                await PromoteRootAsync(splitResult);
                continue;
            }

            // Walk the rightmost internal-node path to find the leaf-parent.
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

            // Accept the split at the leaf-parent and propagate upward.
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

        // Graft complete — clear intent and record completion.
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

    private void ThrowIfDeleted()
    {
        if (state.State.IsDeleted)
            throw new InvalidOperationException("This tree has been deleted and is no longer accessible.");
    }

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
            // Nothing to purge — clear our own state and return.
            await state.ClearStateAsync();
            return;
        }

        // Walk the leaf chain from leftmost, clearing each leaf.
        GrainId? leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId;
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        // Collect internal node IDs to clear afterward.
        var internalNodeIds = new List<GrainId>();
        if (!state.State.RootIsLeaf)
        {
            await CollectInternalNodeIds(state.State.RootNodeId!.Value, internalNodeIds);
        }

        // Clear all leaves.
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var nextId = await leaf.GetNextSiblingAsync();
            await leaf.ClearGrainStateAsync();
            leafId = nextId;
        }

        // Clear all internal nodes.
        foreach (var internalId in internalNodeIds)
        {
            var internalNode = grainFactory.GetGrain<IBPlusInternalGrain>(internalId);
            await internalNode.ClearGrainStateAsync();
        }

        // Clear the shard root's own state.
        await state.ClearStateAsync();
    }

    private async Task CollectInternalNodeIds(GrainId nodeId, List<GrainId> collected)
    {
        collected.Add(nodeId);
        var node = grainFactory.GetGrain<IBPlusInternalGrain>(nodeId);
        if (await node.AreChildrenLeavesAsync())
            return;

        var children = await node.GetChildIdsAsync();
        foreach (var childId in children)
        {
            await CollectInternalNodeIds(childId, collected);
        }
    }
}
