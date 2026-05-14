using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tree traversal logic for read, write, and leaf-location operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
    // Per-activation cache of the most recently resolved ILeafCacheGrain
    // reference, keyed by leaf GrainId. Eliminates a fresh
    // "leaf/<32-hex>" string allocation per read on the hot path. In the
    // common RootIsLeaf=true case (microbench, single-leaf workloads),
    // every call hits this cache; multi-leaf workloads degrade gracefully
    // to "cache the most-recently-used leaf" which is still a win for
    // sequential access patterns and a no-op for diverse access.
    //
    // Invalidation: implicit. The cache key check on every access means
    // any leaf-id rotation (root-split promotes a leaf to internal,
    // causing RootNodeId to change) is detected on the next call and the
    // cache is refreshed against the new leafId.
    private GrainId _cachedLeafCacheKey;
    private ILeafCacheGrain? _cachedLeafCache;

    // Per-activation cache of the most recently resolved IBPlusLeafGrain
    // reference, keyed by leaf GrainId. Eliminates the
    // grainFactory.GetGrain<IBPlusLeafGrain>(leafId) materialisation on
    // every Set/Get/CAS/GetOrSet write-or-traversal call into the leaf.
    // Same invalidation semantics as _cachedLeafCache above: implicit on
    // leaf-id mismatch, so root-splits and multi-leaf rotations refresh
    // the slot on the next call.
    private GrainId _cachedLeafKey;
    private IBPlusLeafGrain? _cachedLeaf;

    // Per-activation cache of the most recently resolved IBPlusInternalGrain
    // reference, keyed by the internal node's GrainId. Eliminates the
    // grainFactory.GetGrain<IBPlusInternalGrain>(currentId) materialisation
    // on every traversal step through an internal node. Mirrors the
    // _cachedLeaf shape directly - a single-slot LRU keyed by GrainId
    // equality. Hit rate per traversal:
    //   * depth-2 tree (root-internal + leaves): 100% after first miss,
    //     since every traversal calls GetGrain<IBPlusInternalGrain>(rootId)
    //     and the root is invariant for the activation's lifetime.
    //   * depth-3+ tree: hits on the root slot for every traversal; level-1+
    //     nodes flip through the slot on each call. A future cycle could
    //     widen the slot to a 2-slot LRU (root + most-recent) if a
    //     deeper-tree microbench shows level-1 misses dominating.
    // Invalidation is implicit on the GrainId equality check, the same
    // pattern that keeps _cachedLeaf safe across root promotions: any
    // RootNodeId rotation produces a mismatched currentId on the next
    // traversal, which routes through ResolveInternalGrainSlow to refresh
    // the slot under the new id.
    private GrainId _cachedInternalKey;
    private IBPlusInternalGrain? _cachedInternal;

    // Per-activation cache of internal-node *routing tables*, keyed by the
    // internal node's GrainId. Each entry is a point-in-time
    // RoutingTableSnapshot (separator keys + child ids + ChildrenAreLeaves
    // flag) fetched once via IBPlusInternalGrain.GetRoutingTableAsync and
    // reused thereafter to perform key-to-child routing locally inside this
    // grain - eliminating the per-traversal-step
    // RouteWithMetadataAsync cross-grain RPC for every internal node ever
    // visited by this activation.
    //
    // Invalidation: explicit, via InvalidateRoutingTable(internalId), called
    // on every site that issues IBPlusInternalGrain.AcceptSplitAsync against
    // an internal node. AcceptSplitAsync is the only call shape that mutates
    // an existing internal node's children list (insert+sort, with possible
    // self-split that further trims children). Brand-new internals created
    // via InitializeAsync / InitializeWithChildrenAsync have no prior cache
    // entry and therefore need no invalidation. The crash-recovery branch
    // inside BPlusInternalGrain.AcceptSplitAsync that nests a sibling
    // AcceptSplitAsync call is reachable only after a partial-split failure
    // and is documented as a tolerated invalidation hole - the only effect
    // of a stale entry there is one extra cross-grain hop on the next
    // routing query, which is negligible compared to the recovery cost
    // itself.
    //
    // Lifetime: the dictionary is lazily allocated on first miss. Per-entry
    // footprint is dominated by the separator-key strings + GrainId array;
    // for an internal node of fanout F the snapshot holds F separator
    // strings + F GrainIds + a bool. Per-activation memory is therefore
    // O(touched-internal-nodes × fanout). For pathological access patterns
    // a future cycle could add an LRU cap; for the workloads this cycle
    // targets (deep-tree microbench, production trees with bounded
    // internal-fanout via MaxInternalChildren) the unbounded dictionary
    // is correct and small.
    private Dictionary<GrainId, RoutingTableSnapshot>? _routingTableCache;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ILeafCacheGrain ResolveLeafCacheGrainSlow(GrainId leafId)
    {
        var cache = grainFactory.GetGrain<ILeafCacheGrain>(leafId.ToString());
        _cachedLeafCacheKey = leafId;
        _cachedLeafCache = cache;
        return cache;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private IBPlusLeafGrain ResolveLeafGrainSlow(GrainId leafId)
    {
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        _cachedLeafKey = leafId;
        _cachedLeaf = leaf;
        return leaf;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private IBPlusInternalGrain ResolveInternalGrainSlow(GrainId internalId)
    {
        var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(internalId);
        _cachedInternalKey = internalId;
        _cachedInternal = internalGrain;
        return internalGrain;
    }

    /// <summary>
    /// Returns the routing-table snapshot for the internal node identified
    /// by <paramref name="internalId"/>. On cache hit (the common case
    /// after the first traversal through any given internal) this is a
    /// fully synchronous local lookup completing via
    /// <see cref="ValueTask{T}"/> - no grain dispatch, no Task allocation.
    /// On cache miss the snapshot is fetched once via
    /// <see cref="IBPlusInternalGrain.GetRoutingTableAsync"/> (one extra
    /// cross-grain call relative to today's
    /// <see cref="IBPlusInternalGrain.RouteWithMetadataAsync"/> shape,
    /// paid only on the first descent through that internal) and cached
    /// for all subsequent descents. Callers invoke
    /// <see cref="RoutingTableSnapshot.Route"/> on the returned snapshot
    /// to perform the per-key routing decision locally.
    /// </summary>
    private ValueTask<RoutingTableSnapshot> GetRoutingTableSnapshotAsync(GrainId internalId)
    {
        if (_routingTableCache is { } cache && cache.TryGetValue(internalId, out var snapshot))
        {
            return new ValueTask<RoutingTableSnapshot>(snapshot);
        }
        return GetRoutingTableSnapshotSlowAsync(internalId);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async ValueTask<RoutingTableSnapshot> GetRoutingTableSnapshotSlowAsync(GrainId internalId)
    {
        var grain = (_cachedInternal is { } existing && _cachedInternalKey.Equals(internalId))
            ? existing
            : ResolveInternalGrainSlow(internalId);
        var snapshot = await grain.GetRoutingTableAsync();
        (_routingTableCache ??= new Dictionary<GrainId, RoutingTableSnapshot>())[internalId] = snapshot;
        return snapshot;
    }

    /// <summary>
    /// Invalidates the cached routing-table snapshot for
    /// <paramref name="internalId"/>. Must be called by every site that
    /// issues <see cref="IBPlusInternalGrain.AcceptSplitAsync"/> against an
    /// internal node - that is the only call shape capable of mutating an
    /// existing internal node's children list. The method is a no-op when
    /// the cache is unallocated or the entry is absent (e.g. the very
    /// first split before any read traversed through the parent).
    /// </summary>
    private void InvalidateRoutingTable(GrainId internalId)
    {
        _routingTableCache?.Remove(internalId);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<byte[]?> TraverseForReadAsync(string key)
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

        var cache = (_cachedLeafCache is { } existing && _cachedLeafCacheKey.Equals(leafId))
            ? existing
            : ResolveLeafCacheGrainSlow(leafId);
        return await cache.GetAsync(key);
    }

    private async Task<VersionedValue> TraverseForReadWithVersionAsync(string key)
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

        var leaf = (_cachedLeaf is { } existing && _cachedLeafKey.Equals(leafId))
            ? existing
            : ResolveLeafGrainSlow(leafId);
        return await leaf.GetWithVersionAsync(key);
    }

    private async Task<bool> TraverseForExistsAsync(string key)
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

        var cache = (_cachedLeafCache is { } existing && _cachedLeafCacheKey.Equals(leafId))
            ? existing
            : ResolveLeafCacheGrainSlow(leafId);
        return await cache.ExistsAsync(key);
    }

    private async Task<Dictionary<string, byte[]>> TraverseForBatchReadAsync(List<string> keys)
    {
        // Group keys by their target leaf.
        var leafBuckets = new Dictionary<GrainId, List<string>>();
        foreach (var key in keys)
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

            if (!leafBuckets.TryGetValue(leafId, out var bucket))
            {
                bucket = [];
                leafBuckets[leafId] = bucket;
            }
            bucket.Add(key);
        }

        // Batch read from each leaf cache.
        var result = new Dictionary<string, byte[]>();
        foreach (var (leafId, bucket) in leafBuckets)
        {
            var cache = (_cachedLeafCache is { } existing && _cachedLeafCacheKey.Equals(leafId))
                ? existing
                : ResolveLeafCacheGrainSlow(leafId);
            var values = await cache.GetManyAsync(bucket);
            foreach (var (k, v) in values)
            {
                result[k] = v;
            }
        }
        return result;
    }

    private async Task<SplitResult?> TraverseForWriteAsync(string key, byte[] value)
    {
        if (state.State.RootIsLeaf)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = (_cachedLeaf is { } existing && _cachedLeafKey.Equals(rootLeafId))
                ? existing
                : ResolveLeafGrainSlow(rootLeafId);
            await RecordAffectedLeafIfPreparedAsync(rootLeafId);
            return await leaf.SetAsync(key, value);
        }

        var path = StackPool.Get();
        try
        {
        var currentId = state.State.RootNodeId!.Value;

        while (true)
        {
            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            var (childId, childrenAreLeaves) = snapshot.Route(key);

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
        var leafGrain = (_cachedLeaf is { } existingLeaf && _cachedLeafKey.Equals(leafId))
            ? existingLeaf
            : ResolveLeafGrainSlow(leafId);
        await RecordAffectedLeafIfPreparedAsync(leafId);
        var splitResult = await leafGrain.SetAsync(key, value);

        while (splitResult is not null && path.Count > 0)
        {
            var parentId = path.Pop();
            var parentGrain = (_cachedInternal is { } existingParent && _cachedInternalKey.Equals(parentId))
                ? existingParent
                : ResolveInternalGrainSlow(parentId);
            splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
            InvalidateRoutingTable(parentId);
        }

        return splitResult;
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    /// <summary>
    /// Write path used by <see cref="ShardRootGrain.SetAsync(string, byte[], long)"/>
    ///. Identical to <see cref="TraverseForWriteAsync"/> except the
    /// final leaf write carries an absolute expiry.
    /// </summary>
    private async Task<SplitResult?> TraverseForWriteWithExpiryAsync(string key, byte[] value, long expiresAtTicks)
    {
        if (state.State.RootIsLeaf)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = (_cachedLeaf is { } existing && _cachedLeafKey.Equals(rootLeafId))
                ? existing
                : ResolveLeafGrainSlow(rootLeafId);
            await RecordAffectedLeafIfPreparedAsync(rootLeafId);
            return await leaf.SetAsync(key, value, expiresAtTicks);
        }

        var path = StackPool.Get();
        try
        {
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);

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
            var leafGrain = (_cachedLeaf is { } existingLeaf && _cachedLeafKey.Equals(leafId))
                ? existingLeaf
                : ResolveLeafGrainSlow(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            var splitResult = await leafGrain.SetAsync(key, value, expiresAtTicks);

            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = (_cachedInternal is { } existingParent && _cachedInternalKey.Equals(parentId))
                    ? existingParent
                    : ResolveInternalGrainSlow(parentId);
                splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            return splitResult;
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private async Task<GetOrSetResult> TraverseForGetOrSetAsync(string key, byte[] value)
    {
        if (state.State.RootIsLeaf)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = (_cachedLeaf is { } existing && _cachedLeafKey.Equals(rootLeafId))
                ? existing
                : ResolveLeafGrainSlow(rootLeafId);
            return await leaf.GetOrSetAsync(key, value);
        }

        var path = StackPool.Get();
        try
        {
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);

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
            var leafGrain = (_cachedLeaf is { } existingLeaf && _cachedLeafKey.Equals(leafId))
                ? existingLeaf
                : ResolveLeafGrainSlow(leafId);
            var result = await leafGrain.GetOrSetAsync(key, value);

            // If the key was already live, no write occurred - no splits to propagate.
            if (result.ExistingValue is not null)
            {
                return result;
            }

            // Propagate splits up the tree.
            var splitResult = result.Split;
            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = (_cachedInternal is { } existingParent && _cachedInternalKey.Equals(parentId))
                    ? existingParent
                    : ResolveInternalGrainSlow(parentId);
                splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            return new GetOrSetResult { Split = splitResult };
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private async Task<CasResult> TraverseForSetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        if (state.State.RootIsLeaf)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = (_cachedLeaf is { } existing && _cachedLeafKey.Equals(rootLeafId))
                ? existing
                : ResolveLeafGrainSlow(rootLeafId);
            return await leaf.SetIfVersionAsync(key, value, expectedVersion);
        }

        var path = StackPool.Get();
        try
        {
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);

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
            var leafGrain = (_cachedLeaf is { } existingLeaf && _cachedLeafKey.Equals(leafId))
                ? existingLeaf
                : ResolveLeafGrainSlow(leafId);
            var result = await leafGrain.SetIfVersionAsync(key, value, expectedVersion);

            // If CAS failed, no write occurred - no splits to propagate.
            if (!result.Success)
            {
                return result;
            }

            // Propagate splits up the tree.
            var splitResult = result.Split;
            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = (_cachedInternal is { } existingParent && _cachedInternalKey.Equals(parentId))
                    ? existingParent
                    : ResolveInternalGrainSlow(parentId);
                splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            return new CasResult
            {
                Success = true,
                CurrentVersion = result.CurrentVersion,
                Split = splitResult
            };
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private ValueTask<GrainId> TraverseToLeafAsync(string key)
    {
        // Sync fast path: every internal hop's routing snapshot is served
        // out of _routingTableCache (a Dictionary<GrainId, ...> populated
        // on first miss and only invalidated on AcceptSplitAsync). In the
        // steady state - the workload that PointRead / GetWithVersion /
        // Exists / BatchRead actually exercise after warmup - every
        // GetRoutingTableSnapshotAsync call sync-completes via the
        // ValueTask<RoutingTableSnapshot> ctor at line 137, and the entire
        // traversal walks root → ... → leaf without yielding. Returning
        // a sync-completed ValueTask<GrainId> from this loop avoids the
        // async state-machine box and Task<GrainId> heap allocation that
        // an `async Task<GrainId>` method would force on every caller -
        // measurable on PointRead_DeeperTree, where the loop runs N
        // times per call (N = internal-tree depth).
        //
        // Slow path: only taken when a routing snapshot's ValueTask has
        // not sync-completed (cache miss → cross-grain
        // GetRoutingTableAsync RPC). The pending ValueTask is forwarded
        // to TraverseToLeafSlowAsync which awaits it, then resumes the
        // remaining traversal steps in async form. Each subsequent step
        // is a fresh GetRoutingTableSnapshotAsync, which may itself
        // sync-complete (cache hit on the next level) or suspend again;
        // the slow tail handles either uniformly.
        var currentId = state.State.RootNodeId!.Value;
        while (true)
        {
            var snapshotTask = GetRoutingTableSnapshotAsync(currentId);
            if (!snapshotTask.IsCompletedSuccessfully)
            {
                return TraverseToLeafSlowAsync(currentId, key, snapshotTask);
            }
            var snapshot = snapshotTask.Result;
            var (childId, childrenAreLeaves) = snapshot.Route(key);
            if (childrenAreLeaves)
            {
                return new ValueTask<GrainId>(childId);
            }
            currentId = childId;
        }
    }

    private async ValueTask<GrainId> TraverseToLeafSlowAsync(
        GrainId currentId,
        string key,
        ValueTask<RoutingTableSnapshot> pendingSnapshot)
    {
        // Resume from the snapshot fetch that did not sync-complete.
        var snapshot = await pendingSnapshot;
        var (childId, childrenAreLeaves) = snapshot.Route(key);
        if (childrenAreLeaves)
        {
            return childId;
        }
        currentId = childId;

        // Continue with the remainder of the traversal. Subsequent hops
        // may sync-complete out of _routingTableCache (cache hits on
        // already-warmed internal nodes); the await machinery elides
        // suspension for any ValueTask whose IsCompletedSuccessfully is
        // already true, so the slow tail pays at most one suspension
        // per cache miss for the rest of the walk.
        while (true)
        {
            snapshot = await GetRoutingTableSnapshotAsync(currentId);
            (childId, childrenAreLeaves) = snapshot.Route(key);
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
            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            var childId = snapshot.ChildIds[0];
            var childrenAreLeaves = snapshot.ChildrenAreLeaves;

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
            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            var childId = snapshot.ChildIds[snapshot.ChildIds.Length - 1];
            var childrenAreLeaves = snapshot.ChildrenAreLeaves;

            if (childrenAreLeaves)
            {
                return childId;
            }

            currentId = childId;
        }
    }

    private async Task<SplitResult?> PromoteRootAsync(SplitResult splitResult)
    {
        state.State.PendingPromotion = splitResult;
        state.State.PendingPromotionRootWasLeaf = state.State.RootIsLeaf;
        await state.WriteStateAsync();

        await CompletePromotionAsync();
        return null;
    }

    /// <summary>
    /// Completes (or resumes) a root promotion whose intent has already been persisted.
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
