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
        ThrowIfRejectedForKey(key);
        RecordRead();
        return await TraverseForReadAsync(key);
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordRead();
        return await TraverseForReadWithVersionAsync(key);
    }

    /// <inheritdoc />
    public async Task<LwwEntry?> GetRawEntryAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordRead();

        if (state.State.RootNodeId is null) return null;

        var leafId = state.State.RootIsLeaf
            ? state.State.RootNodeId!.Value
            : await TraverseToLeafAsync(key);
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var raw = await leaf.GetRawEntryAsync(key);
        if (raw is null) return null;
        if (raw.Value.IsTombstone) return null;
        return raw;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordRead();
        return await TraverseForExistsAsync(key);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForAnyKey(keys);
        RecordRead();
        return await TraverseForBatchReadAsync(keys);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

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

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key);
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

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, long expiresAtTicks)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var splitResult = await TraverseForWriteWithExpiryAsync(key, value, expiresAtTicks);

                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                // The target fetches the authoritative entry via the normal merge
                // path so expiry is preserved end-to-end.
                await ForwardLocalWriteToShadowIfNeededAsync(key);
                return;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

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

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key);
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

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var result = await TraverseForSetIfVersionAsync(key, value, expectedVersion);

                if (!result.Success)
                {
                    return false;
                }

                // A write occurred — propagate any split.
                var splitResult = result.Split;
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key);
                return true;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        // Reject-check up-front so the batch fails fast rather than partially applying.
        ThrowIfRejectedForAnyKey(entries.Select(e => e.Key));
        // SetAsync internally re-checks reject + shadow-forwards each entry.
        foreach (var entry in entries)
        {
            await SetAsync(entry.Key, entry.Value);
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                bool result;
                if (state.State.RootIsLeaf)
                {
                    var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
                    result = await leaf.DeleteAsync(key);
                }
                else
                {
                    // Traverse to the leaf.
                    var leafId = await TraverseToLeafAsync(key);
                    var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
                    result = await leafGrain.DeleteAsync(key);
                }

                // tombstone forwarding is handled by the comprehensive
                // cleanup phase of the split coordinator. See
                // ForwardLocalWriteToShadowIfNeededAsync XML doc for rationale.
                return result;
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
        // range deletes do not currently shadow-forward tombstones — see
        // ForwardLocalWriteToShadowIfNeededAsync XML doc. The cleanup phase of
        // the split coordinator restores convergence by re-tombstoning moved-slot entries on T after the swap. No explicit reject check is performed
        // here because the LatticeGrain has already routed the range delete
        // to the correct shard via the current ShardMap.
        RecordWrite();

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
        // Terminate on the first leaf that reports PastRange=true (FX-011):
        // deleting zero is NOT a valid termination signal on multi-shard trees,
        // where early leaves can be sparse yet later leaves contain range-matching
        // entries.
        var totalDeleted = 0;
        while (true)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var result = await leafGrain.DeleteRangeAsync(startInclusive, endExclusive);
            totalDeleted += result.Deleted;

            if (result.PastRange)
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
        RecordRead();

        if (state.State.RootNodeId is null)
            return 0;

        // Find the leftmost leaf and walk the chain.
        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return 0;

        // if any virtual slots have been split away, we cannot trust
        // the leaf-level count (it includes orphan moved-slot entries). Walk
        // the keys and filter. The fast path (no splits) is preserved when
        // MovedAwaySlots is empty.
        var hasMovedAway = state.State.MovedAwaySlots.Count > 0
            && state.State.MovedAwayVirtualShardCount is not null;

        var total = 0;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            if (hasMovedAway)
            {
                var keys = await leaf.GetKeysAsync(null, null);
                for (int i = 0; i < keys.Count; i++)
                {
                    if (!IsSlotMovedAway(keys[i])) total++;
                }
            }
            else
            {
                total += await leaf.CountAsync();
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return total;
    }

    /// <inheritdoc />
    public async Task<ShardCountResult> CountWithMovedAwayAsync()
    {
        await PrepareForOperationAsync();
        RecordRead();

        if (state.State.RootNodeId is null)
            return new ShardCountResult { Count = 0 };

        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return new ShardCountResult { Count = 0 };

        var hasActiveSplit = state.State.SplitInProgress is { } sip
            && (sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject
                || sip.Phase == ShardSplitPhase.Complete);
        var hasMovedAway = state.State.MovedAwaySlots.Count > 0
            && state.State.MovedAwayVirtualShardCount is not null;

        var total = 0;
        HashSet<int>? movedSet = null;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            if (hasActiveSplit || hasMovedAway)
            {
                var keys = await leaf.GetKeysAsync(null, null);
                for (int i = 0; i < keys.Count; i++)
                {
                    if (TryGetMovedAwaySlot(keys[i], out var movedSlot))
                    {
                        (movedSet ??= []).Add(movedSlot);
                        continue;
                    }
                    total++;
                }
            }
            else
            {
                total += await leaf.CountAsync();
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return new ShardCountResult
        {
            Count = total,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    /// <inheritdoc />
    public async Task<int> CountForSlotsAsync(int[] sortedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return 0;

        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return 0;

        var total = 0;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            var keys = await leaf.GetKeysAsync(null, null);
            for (int i = 0; i < keys.Count; i++)
            {
                var slot = ShardMap.GetVirtualSlot(keys[i], virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) >= 0)
                    total++;
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return total;
    }

    private static int[] SortedSlotsArray(HashSet<int> set)
    {
        var arr = new int[set.Count];
        var i = 0;
        foreach (var v in set) arr[i++] = v;
        Array.Sort(arr);
        return arr;
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

    public async Task MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries)
    {
        await PrepareForOperationAsync();
        RecordWrite();

        if (entries.Count == 0)
        {
            return;
        }

        // Root-is-leaf fast path: route the entire batch to the single leaf
        // in one grain call and one WriteStateAsync.
        if (state.State.RootIsLeaf)
        {
            await MergeGroupAsync(entries);
            return;
        }

        // Group entries by target leaf so each leaf is called exactly once.
        // Per-leaf WriteStateAsync collapses from O(entries) to O(leaves) —
        // the dominant storage-I/O win. Internal-node routing RPCs are still
        // paid during grouping and re-paid during apply (the apply phase
        // re-traverses so that a split produced by an earlier group is
        // observed by later groups) — that cost is O((N+L)·D) lightweight
        // in-memory reads against the persisted internal nodes, dominated
        // in practice by the O(L) storage writes.
        //
        // Distinct groups target distinct leaves, so a split produced by one
        // group cannot re-route another group's keys (internal splits create
        // sibling internals but preserve child GrainIds; leaf splits only
        // affect the one leaf being written to).
        var groups = new Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>();
        foreach (var (key, lww) in entries)
        {
            var leafId = await TraverseToLeafWithRetryAsync(key);
            if (!groups.TryGetValue(leafId, out var group))
            {
                // Pre-size conservatively: if entries spread evenly across
                // leaves the per-group size is entries.Count / expected-leaves;
                // we cap at the incoming count to avoid over-allocating on
                // tiny batches.
                group = new Dictionary<string, LwwValue<byte[]>>(
                    capacity: Math.Min(entries.Count, 16));
                groups[leafId] = group;
            }
            group[key] = lww;
        }

        foreach (var group in groups.Values)
        {
            await MergeGroupAsync(group);
        }
    }

    /// <summary>
    /// Traverses to the leaf owning <paramref name="key"/> with transient-exception retry.
    /// Mirrors the resilience the per-entry path had before leaf-grouped routing.
    /// </summary>
    private async Task<GrainId> TraverseToLeafWithRetryAsync(string key)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await TraverseToLeafAsync(key);
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    /// <summary>
    /// Applies a pre-grouped batch of entries to a single leaf, re-traversing
    /// against the current topology and propagating any resulting split up to
    /// the root. Retries on transient Orleans / storage exceptions; the leaf's
    /// <c>MergeManyAsync</c> is LWW-idempotent, so replay is safe.
    /// </summary>
    private async Task MergeGroupAsync(Dictionary<string, LwwValue<byte[]>> group)
    {
        // Any key in the group routes to the same leaf under the current
        // topology; pick one via foreach-break to avoid the LINQ enumerator
        // boxing allocation of entries.Keys.First().
        string? pivotKey = null;
        foreach (var k in group.Keys) { pivotKey = k; break; }
        // group is never empty here (callers only invoke with non-empty groups).

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var splitResult = await TraverseForMergeAsync(pivotKey!, group);

                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                return;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    private async Task<SplitResult?> TraverseForMergeAsync(string key, Dictionary<string, LwwValue<byte[]>> entries)
    {
        if (state.State.RootIsLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.MergeManyAsync(entries);
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
            var splitResult = await leafGrain.MergeManyAsync(entries);

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

    public async Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

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
        HashSet<int>? movedSet = null;
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as afterExclusive so the leaf filters
            // at the source — avoids transferring keys that would be
            // discarded here.
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, afterExclusive: continuationToken);

            foreach (var key in leafKeys)
            {
                if (TryGetMovedAwaySlot(key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            leafId = nextSibling.Value;
        }

        return new KeysPage
        {
            Keys = keys,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<KeysPage> GetSortedKeysBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

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
        HashSet<int>? movedSet = null;
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as beforeExclusive so the leaf filters
            // at the source — avoids transferring keys that would be
            // discarded here.
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, beforeExclusive: continuationToken);

            // Walk the leaf's keys in reverse order.
            for (int i = leafKeys.Count - 1; i >= 0; i--)
            {
                var key = leafKeys[i];
                if (TryGetMovedAwaySlot(key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            var prevSibling = await leafGrain.GetPrevSiblingAsync();
            if (prevSibling is null)
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            leafId = prevSibling.Value;
        }

        return new KeysPage
        {
            Keys = keys,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<EntriesPage> GetSortedEntriesBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

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

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        HashSet<int>? movedSet = null;
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as afterExclusive so the leaf filters
            // at the source — avoids serializing byte[] values that would be
            // discarded here.
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, continuationToken);

            foreach (var entry in leafEntries)
            {
                if (TryGetMovedAwaySlot(entry.Key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                entries.Add(entry);
                if (entries.Count >= pageSize)
                    break;
            }

            if (entries.Count >= pageSize)
                break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            leafId = nextSibling.Value;
        }

        return new EntriesPage
        {
            Entries = entries,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<EntriesPage> GetSortedEntriesBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

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

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        HashSet<int>? movedSet = null;
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as beforeExclusive so the leaf filters
            // at the source — avoids serializing byte[] values that would be
            // discarded here.
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, beforeExclusive: continuationToken);

            for (int i = leafEntries.Count - 1; i >= 0; i--)
            {
                var entry = leafEntries[i];
                if (TryGetMovedAwaySlot(entry.Key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                entries.Add(entry);
                if (entries.Count >= pageSize)
                    break;
            }

            if (entries.Count >= pageSize)
                break;

            var prevSibling = await leafGrain.GetPrevSiblingAsync();
            if (prevSibling is null)
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            leafId = prevSibling.Value;
        }

        return new EntriesPage
        {
            Entries = entries,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    /// <inheritdoc />
    public async Task<KeysPage> GetSortedKeysBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return new KeysPage { Keys = [], HasMore = false };

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

        var keys = new List<string>(pageSize);
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, afterExclusive: continuationToken);

            foreach (var key in leafKeys)
            {
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) < 0) continue;
                keys.Add(key);
                if (keys.Count >= pageSize) break;
            }

            if (keys.Count >= pageSize) break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new KeysPage { Keys = keys, HasMore = false };

            leafId = nextSibling.Value;
        }

        return new KeysPage { Keys = keys, HasMore = true };
    }

    /// <inheritdoc />
    public async Task<EntriesPage> GetSortedEntriesBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return new EntriesPage { Entries = [], HasMore = false };

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

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, continuationToken);

            foreach (var entry in leafEntries)
            {
                var slot = ShardMap.GetVirtualSlot(entry.Key, virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) < 0) continue;
                entries.Add(entry);
                if (entries.Count >= pageSize) break;
            }

            if (entries.Count >= pageSize) break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new EntriesPage { Entries = entries, HasMore = false };

            leafId = nextSibling.Value;
        }

        return new EntriesPage { Entries = entries, HasMore = true };
    }

    public async Task<GrainId?> GetLeftmostLeafIdAsync()
    {
        // The leftmost leaf is the first in the chain, or the root if no chain exists.
        return state.State.RootNodeId is null
            ? null
            : state.State.RootIsLeaf
                ? state.State.RootNodeId
                : await TraverseToLeftmostLeafAsync();
    }
}
