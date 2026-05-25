using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tree traversal logic for read, write, and leaf-location operations.
/// </summary>
internal sealed partial class ShardRootGrain
{
    // Per-activation cache of resolved ILeafCacheGrain references, keyed
    // by leaf GrainId. Eliminates the fresh "leaf/<32-hex>" string
    // allocation per read on the hot path. Each entry survives for the
    // activation's lifetime; because the only thing that "invalidates" a
    // cache entry is the source leaf being deleted (drop / migrate /
    // re-key), and the resolved grain reference is itself just a routing
    // handle, stale entries are harmless - the resolved grain would
    // simply fail to address a leaf that no longer exists, which is
    // exactly the behaviour any uncached lookup would produce too.
    //
    // The previous shape was a single-slot LRU which produced a write to
    // the slot on every miss. That write made the cache unsafe to share
    // across interleaved grain turns. Switching to ConcurrentDictionary
    // makes the cache (a) thread-safe across the concurrent turns that
    // SetManyAsync's [AlwaysInterleave] annotation enables, and
    // (b) strictly higher hit-rate for multi-leaf workloads (every
    // previously-seen leaf remains a hit, not just the most-recent one).
    private readonly ConcurrentDictionary<GrainId, ILeafCacheGrain> _leafCacheGrains = new();

    // Per-activation cache of resolved IBPlusLeafGrain references, keyed
    // by leaf GrainId. Same rationale as _leafCacheGrains above: the
    // grain reference is just a routing handle, so caching it for the
    // activation's lifetime is safe and concurrent-turn-friendly.
    private readonly ConcurrentDictionary<GrainId, IBPlusLeafGrain> _leafGrains = new();

    // Per-activation cache of resolved IBPlusInternalGrain references,
    // keyed by the internal node's GrainId. Same rationale as the leaf
    // caches above. Hit rate after the first traversal:
    //   * depth-2 tree (root-internal + leaves): 100% on the root after
    //     the first descent, since every traversal queries the root.
    //   * depth-3+ tree: 100% on every previously-visited internal.
    // The unbounded dictionary footprint is O(touched-internal-nodes);
    // for the workloads this cycle targets (bounded MaxInternalChildren,
    // production trees with reasonable fanout) the memory cost is
    // negligible.
    private readonly ConcurrentDictionary<GrainId, IBPlusInternalGrain> _internalGrains = new();

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
    // Lifetime: per-entry footprint is dominated by the separator-key
    // strings + GrainId array; for an internal node of fanout F the
    // snapshot holds F separator strings + F GrainIds + a bool.
    // Per-activation memory is therefore O(touched-internal-nodes × fanout).
    // For pathological access patterns a future cycle could add an LRU
    // cap; for the workloads this cycle targets (deep-tree microbench,
    // production trees with bounded internal-fanout via
    // MaxInternalChildren) the unbounded dictionary is correct and small.
    private readonly ConcurrentDictionary<GrainId, RoutingTableSnapshot> _routingTableCache = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ILeafCacheGrain ResolveLeafCacheGrain(GrainId leafId)
        => _leafCacheGrains.TryGetValue(leafId, out var existing)
            ? existing
            : ResolveLeafCacheGrainSlow(leafId);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ILeafCacheGrain ResolveLeafCacheGrainSlow(GrainId leafId)
        => _leafCacheGrains.GetOrAdd(leafId, static (id, gf) => gf.GetGrain<ILeafCacheGrain>(id.ToString()), grainFactory);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IBPlusLeafGrain ResolveLeafGrain(GrainId leafId)
        => _leafGrains.TryGetValue(leafId, out var existing)
            ? existing
            : ResolveLeafGrainSlow(leafId);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private IBPlusLeafGrain ResolveLeafGrainSlow(GrainId leafId)
        => _leafGrains.GetOrAdd(leafId, static (id, gf) => gf.GetGrain<IBPlusLeafGrain>(id), grainFactory);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IBPlusInternalGrain ResolveInternalGrain(GrainId internalId)
        => _internalGrains.TryGetValue(internalId, out var existing)
            ? existing
            : ResolveInternalGrainSlow(internalId);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private IBPlusInternalGrain ResolveInternalGrainSlow(GrainId internalId)
        => _internalGrains.GetOrAdd(internalId, static (id, gf) => gf.GetGrain<IBPlusInternalGrain>(id), grainFactory);

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
        if (_routingTableCache.TryGetValue(internalId, out var snapshot))
        {
            return new ValueTask<RoutingTableSnapshot>(snapshot);
        }
        return GetRoutingTableSnapshotSlowAsync(internalId);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async ValueTask<RoutingTableSnapshot> GetRoutingTableSnapshotSlowAsync(GrainId internalId)
    {
        var grain = ResolveInternalGrain(internalId);
        var snapshot = await grain.GetRoutingTableAsync();
        _routingTableCache[internalId] = snapshot;
        return snapshot;
    }

    /// <summary>
    /// Invalidates the cached routing-table snapshot for
    /// <paramref name="internalId"/>. Must be called by every site that
    /// issues <see cref="IBPlusInternalGrain.AcceptSplitAsync"/> against an
    /// internal node - that is the only call shape capable of mutating an
    /// existing internal node's children list. The method is a no-op when
    /// the entry is absent (e.g. the very first split before any read
    /// traversed through the parent).
    /// </summary>
    private void InvalidateRoutingTable(GrainId internalId)
    {
        _routingTableCache.TryRemove(internalId, out _);
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

#if LATTICE_DIAG
        // DIAG read-routing: capture the resolved leaf id alongside
        // the moved-away mask state at the moment of routing so that a
        // stale-read trace can be correlated against the post-migration
        // shard-side mask. A read that routes to a leaf NOT marked in
        // MovedAwaySlots while that key has in fact migrated is the
        // exact signature of the V_{N-2} regression hunted by Section 14.
        DiagSink.Write($"[DIAG read-routing] gid={context.GrainId} key={key} leafId={leafId} rootIsLeaf={state.State.RootIsLeaf} movedSlots=[{string.Join(',', state.State.MovedAwaySlots.Keys)}] phase={state.State.SplitInProgress?.Phase.ToString() ?? "(none)"}");
#endif
        var cache = ResolveLeafCacheGrain(leafId);
        return await cache.GetAsync(key);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<VersionedValue> TraverseForReadWithVersionAsync(string key)
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

        var leaf = ResolveLeafGrain(leafId);
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

        var cache = ResolveLeafCacheGrain(leafId);
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

#if LATTICE_DIAG
            // DIAG read-routing (batch path). Same intent as the
            // single-key TraverseForReadAsync emission - records the
            // resolved leaf id + moved-away state per key so the chaos
            // trace can distinguish "shard routed to the new owner" from
            // "shard routed to the stale source" on every observed read.
            DiagSink.Write($"[DIAG read-routing] gid={context.GrainId} key={key} leafId={leafId} rootIsLeaf={state.State.RootIsLeaf} movedSlots=[{string.Join(',', state.State.MovedAwaySlots.Keys)}] phase={state.State.SplitInProgress?.Phase.ToString() ?? "(none)"} batch=true");
#endif
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
            var cache = ResolveLeafCacheGrain(leafId);
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
            var leaf = ResolveLeafGrain(rootLeafId);
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
        var leafGrain = ResolveLeafGrain(leafId);
        await RecordAffectedLeafIfPreparedAsync(leafId);
        var splitResult = await leafGrain.SetAsync(key, value);

        while (splitResult is not null && path.Count > 0)
        {
            var parentId = path.Pop();
            var parentGrain = ResolveInternalGrain(parentId);
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
            var leaf = ResolveLeafGrain(rootLeafId);
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
            var leafGrain = ResolveLeafGrain(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            var splitResult = await leafGrain.SetAsync(key, value, expiresAtTicks);

            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = ResolveInternalGrain(parentId);
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
            var leaf = ResolveLeafGrain(rootLeafId);
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
            var leafGrain = ResolveLeafGrain(leafId);
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
                var parentGrain = ResolveInternalGrain(parentId);
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
            var leaf = ResolveLeafGrain(rootLeafId);
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
            var leafGrain = ResolveLeafGrain(leafId);
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
                var parentGrain = ResolveInternalGrain(parentId);
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

    private async Task<CrdtApplyResult> TraverseForCrdtApplyAsync(string key, LatticeMergeMode mode, byte[] deltaBytes)
    {
        if (state.State.RootIsLeaf)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = ResolveLeafGrain(rootLeafId);
            return await leaf.ApplyCrdtDeltaAsync(key, mode, deltaBytes);
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
            var leafGrain = ResolveLeafGrain(leafId);
            var result = await leafGrain.ApplyCrdtDeltaAsync(key, mode, deltaBytes);

            // Propagate splits up the tree.
            var splitResult = result.Split;
            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = ResolveInternalGrain(parentId);
                splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            return new CrdtApplyResult
            {
                Version = result.Version,
                Split = splitResult,
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
        // out of _routingTableCache (a ConcurrentDictionary<GrainId, ...>
        // populated on first miss and only invalidated on
        // AcceptSplitAsync). In the
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
        await WriteShardStateAsync();

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
        await WriteShardStateAsync();
    }
}
