using System.IO;
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
internal sealed partial class ShardRootGrain(
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

    private LatticeOptions? _cachedOptions;

    /// <summary>
    /// Returns the effective options for this tree. On first access, checks the
    /// registry for per-tree overrides; falls back to <see cref="IOptionsMonitor{LatticeOptions}"/>.
    /// Cached for the grain's lifetime.
    /// </summary>
    private async Task<LatticeOptions> GetOptionsAsync()
    {
        if (_cachedOptions is not null) return _cachedOptions;

        if (!TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var entry = await registry.GetEntryAsync(TreeId);
            if (entry is not null)
            {
                var baseOptions = optionsMonitor.Get(TreeId);
                _cachedOptions = new LatticeOptions
                {
                    MaxLeafKeys = entry.MaxLeafKeys ?? baseOptions.MaxLeafKeys,
                    MaxInternalChildren = entry.MaxInternalChildren ?? baseOptions.MaxInternalChildren,
                    ShardCount = entry.ShardCount ?? baseOptions.ShardCount,
                    KeysPageSize = baseOptions.KeysPageSize,
                    TombstoneGracePeriod = baseOptions.TombstoneGracePeriod,
                    SoftDeleteDuration = baseOptions.SoftDeleteDuration,
                    CacheTtl = baseOptions.CacheTtl,
                };
                return _cachedOptions;
            }
        }

        _cachedOptions = optionsMonitor.Get(TreeId);
        return _cachedOptions;
    }

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
        await PrepareForOperationAsync();
        return await TraverseForReadAsync(key);
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await PrepareForOperationAsync();
        return await TraverseForExistsAsync(key);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await PrepareForOperationAsync();
        return await TraverseForBatchReadAsync(keys);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        await PrepareForOperationAsync();

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
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // The failed grain will be deactivated by Orleans. On retry, a fresh
                // activation loads clean state and the recovery guards resume any
                // interrupted split.
            }
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value)
    {
        await PrepareForOperationAsync();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var result = await TraverseForGetOrSetAsync(key, value);

                // If the key was already live, no write occurred — return existing value.
                if (result.ExistingValue is not null)
                {
                    return result.ExistingValue;
                }

                // A write occurred — propagate any split.
                var splitResult = result.Split;
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                return null;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // The failed grain will be deactivated by Orleans. On retry, a fresh
                // activation loads clean state and the recovery guards resume any
                // interrupted split.
            }
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        foreach (var entry in entries)
        {
            await SetAsync(entry.Key, entry.Value);
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        await PrepareForOperationAsync();

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
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // Retry — same rationale as SetAsync.
            }
        }
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        await PrepareForOperationAsync();

        // Find the starting leaf for the range.
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else
        {
            leafId = await TraverseToLeafAsync(startInclusive);
        }

        // Walk the leaf chain, tombstoning matching entries in each leaf.
        // We stop when a leaf deletes nothing and already has live keys at or
        // beyond endExclusive — meaning the range is fully covered.
        var totalDeleted = 0;
        while (true)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var deleted = await leafGrain.DeleteRangeAsync(startInclusive, endExclusive);
            totalDeleted += deleted;

            // If nothing was deleted, all remaining keys in this leaf (and
            // subsequent leaves) are outside the range — we can stop.
            if (deleted == 0)
                break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                break;

            leafId = nextSibling.Value;
        }

        return totalDeleted;
    }

    public async Task<int> CountAsync()
    {
        await PrepareForOperationAsync();

        if (state.State.RootNodeId is null)
            return 0;

        // Find the leftmost leaf and walk the chain.
        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return 0;

        var total = 0;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            total += await leaf.CountAsync();

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return total;
    }

    public async Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        await PrepareForOperationAsync();

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
        await PrepareForOperationAsync();

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

        // Register the tree in the registry before creating the root node.
        // This ensures the tree is discoverable before any data is written.
        // System trees (e.g. the registry itself) skip self-registration.
        if (!state.State.IsRegistered &&
            !TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.RegisterAsync(TreeId);
            state.State.IsRegistered = true;
        }

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

    private void ThrowIfDeleted()
    {
        if (state.State.IsDeleted)
            throw new InvalidOperationException("This tree has been deleted and is no longer accessible.");
    }

    private async Task PrepareForOperationAsync()
    {
        ThrowIfDeleted();
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();
    }
}
