using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The root grain for a single shard. Lazily creates the first leaf and
/// handles root splits by creating a new internal root above the old one.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// </summary>
internal sealed class ShardRootGrain(
    IGrainContext context,
    [PersistentState("shardroot", "bplustree")] IPersistentState<ShardRootState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IShardRootGrain
{
    private string TreeId => context.GrainId.Key.ToString()!
        [..context.GrainId.Key.ToString()!.LastIndexOf('/')];

    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    private const int MaxRetries = 2;

    public async Task<byte[]?> GetAsync(string key)
    {
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        return await TraverseForReadAsync(key);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();

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
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();

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
        var path = new Stack<GrainId>();
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
}
