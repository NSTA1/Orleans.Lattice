using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
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

    // Upper bound on the number of levels DescendToLeafAsync will walk
    // before concluding the topology is cyclic / corrupt. A real B+ tree
    // shard is far shallower than this (production fanout keeps even
    // billion-key trees under ~6 levels); the cap exists only so a
    // pathological self-referential routing pointer surfaces as a typed
    // exception instead of an unbounded loop.
    private const int MaxTreeDescentLevels = 64;

    // Cached GrainType of the leaf grain, resolved once per activation from
    // the grain factory. Used by the sorted-scan defensive guard
    // (DescendToLeafAsync) to decide, by node TYPE rather than by a
    // potentially-inconsistent ChildrenAreLeaves routing flag, whether a
    // node id addresses a leaf or an internal node. See issue 899: a baked
    // inconsistent topology (an internal node whose persisted
    // childrenAreLeaves bit is true over internal children, or a leaf
    // sibling pointer that crosses a node level) previously steered the
    // scan's leaf walk onto an internal grain and threw InvalidCastException
    // when that internal reference was invoked through IBPlusLeafGrain.
    private GrainType? _leafGrainType;
    private bool _leafGrainTypeResolved;

    /// <summary>
    /// Resolves (once per activation) the <see cref="GrainType"/> that the
    /// grain factory assigns to leaf grains, used by the sorted-scan guard to
    /// tell a leaf node id from an internal node id. Resolution asks the
    /// factory for a leaf reference and reads its grain id type. When the
    /// factory cannot yield a runtime-typed reference (for example a
    /// unit-test fake that does not model real grain references) the leaf
    /// type is left unresolved and <paramref name="leafType"/> is undefined;
    /// callers then treat ids as leaves, degrading the guard to the historical
    /// blind-walk behaviour for those fakes (which never model the cross-level
    /// corruption the guard defends against anyway).
    /// </summary>
    private bool TryGetLeafGrainType(out GrainType leafType)
    {
        if (!_leafGrainTypeResolved)
        {
            _leafGrainTypeResolved = true;
            try
            {
                _leafGrainType = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.Empty).GetGrainId().Type;
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or InvalidCastException or NotSupportedException or NullReferenceException)
            {
                // The grain factory is not a runtime factory (e.g. a unit-test
                // fake): leave the leaf type unresolved so the guard becomes a
                // no-op rather than throwing on every scan.
                _leafGrainType = null;
            }
        }

        leafType = _leafGrainType ?? default;
        return _leafGrainType.HasValue;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="nodeId"/> addresses
    /// a leaf grain (as opposed to an internal node grain), decided purely by
    /// the grain TYPE encoded in the id rather than by any routing-table flag.
    /// When the leaf grain type cannot be resolved (a non-runtime factory) this
    /// returns <see langword="true"/> so the scan guard degrades to a no-op.
    /// </summary>
    private bool IsLeafGrainId(GrainId nodeId)
        => !TryGetLeafGrainType(out var leafType) || nodeId.Type == leafType;

    /// <summary>
    /// Returns <see langword="true"/> only when this shard's root is BOTH
    /// flagged as a leaf (<see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.RootIsLeaf"/>) AND the
    /// persisted root node id actually addresses a leaf grain. A
    /// baked-inconsistent topology that left the <c>RootIsLeaf</c> bit true over
    /// an internal root (issue 899) returns <see langword="false"/> here, so a
    /// caller's flat-tree fast path is skipped and the internal-rooted branch
    /// runs instead of blind-casting the internal root to
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>. When the root id is unset, or the leaf
    /// grain type cannot be resolved (a non-runtime test factory, where
    /// <see cref="IsLeafGrainId"/> is always true), this is exactly
    /// <c>RootIsLeaf</c>, so healthy trees and fakes are unaffected.
    /// </summary>
    private bool RootIsLeafTyped
        => state.State.RootIsLeaf
           && state.State.RootNodeId is { } rootId
           && IsLeafGrainId(rootId);

    /// <summary>
    /// Defensive guard for the sorted-scan leaf walk. Given a node id that the
    /// scan believes addresses a leaf, returns a guaranteed leaf-typed id by
    /// descending through any internal node(s) the id actually resolves to,
    /// taking the leftmost child at each level (or the rightmost when
    /// <paramref name="rightmost"/> is set, for reverse scans). When the id is
    /// already leaf-typed this is a synchronous no-op that returns the id
    /// unchanged, so the common correct-topology path pays nothing beyond a
    /// <see cref="GrainType"/> comparison.
    /// <para>
    /// This guard ensures the scan never blind-casts an internal node id to
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/> (the InvalidCastException of issue 899):
    /// whether the offending id arrives from a leftmost / rightmost traversal
    /// that trusted a corrupt <c>ChildrenAreLeaves</c> flag, or from a leaf
    /// next / prev sibling pointer that crosses a node level, the scan
    /// re-descends to a real leaf and continues rather than crashing.
    /// </para>
    /// </summary>
    private async ValueTask<GrainId> DescendToLeafAsync(GrainId nodeId, bool rightmost)
    {
        if (IsLeafGrainId(nodeId))
            return nodeId;

        var currentId = nodeId;
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            if (IsLeafGrainId(currentId))
                return currentId;

            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            if (snapshot.ChildIds.Length == 0)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} sorted-scan descent reached internal node {currentId} with no children.");
            }

            currentId = rightmost ? snapshot.ChildIds[^1] : snapshot.ChildIds[0];
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} sorted-scan descent from {nodeId} exceeded {MaxTreeDescentLevels} levels without reaching a leaf; tree topology may be corrupt.");
    }

    /// <summary>
    /// Key-routed sibling of <see cref="DescendToLeafAsync"/> for the point
    /// read / write paths. Given a node id that a caller believed addressed the
    /// leaf owning <paramref name="key"/>, returns a guaranteed leaf-typed id by
    /// continuing to route on <paramref name="key"/> through any internal node(s)
    /// the id actually resolves to. When the id is already leaf-typed this is a
    /// synchronous no-op returning the id unchanged, so the common
    /// correct-topology path pays nothing beyond a <see cref="GrainType"/>
    /// comparison.
    /// <para>
    /// This guard ensures a read or write never blind-casts an internal node id
    /// to <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/> (the InvalidCastException of issue 899)
    /// when a baked-inconsistent topology mislabels an internal node as a leaf -
    /// a persisted <c>RootIsLeaf</c> bit left true over an internal root, or a
    /// routing snapshot whose <c>ChildrenAreLeaves</c> flag is true over internal
    /// children. Unlike the scan guard it descends by key-routing rather than
    /// leftmost / rightmost, so it lands on the leaf that actually owns the key.
    /// When the leaf grain type cannot be resolved (a non-runtime test factory)
    /// <see cref="IsLeafGrainId"/> is always true and this degrades to a no-op.
    /// </para>
    /// </summary>
    private async ValueTask<GrainId> DescendToLeafForKeyAsync(GrainId nodeId, string key)
    {
        if (IsLeafGrainId(nodeId))
            return nodeId;

        var currentId = nodeId;
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            if (IsLeafGrainId(currentId))
                return currentId;

            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            if (snapshot.ChildIds.Length == 0)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} key-routed descent reached internal node {currentId} with no children.");
            }

            var (childId, _) = snapshot.Route(key);
            currentId = childId;
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} key-routed descent from {nodeId} for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf; tree topology may be corrupt.");
    }

    /// <summary>
    /// Resolves the leaf grain id that owns <paramref name="key"/> within this
    /// shard, recording every internal ancestor walked into
    /// <paramref name="path"/> (deepest last) so a write caller can propagate
    /// leaf splits back up the tree with the same shape the inline
    /// path-pop loops used. Termination is decided by node TYPE
    /// (<see cref="IsLeafGrainId"/>), not by the persisted <c>RootIsLeaf</c> /
    /// <c>ChildrenAreLeaves</c> routing flags: a baked-inconsistent topology
    /// (issue 899) that flags an internal node as a leaf no longer steers the
    /// write onto an internal grain and throws InvalidCastException - the descent
    /// continues by key-routing, recording each extra internal ancestor, until a
    /// real leaf grain id is reached. When the leaf grain type cannot be resolved
    /// (a non-runtime test factory) <see cref="IsLeafGrainId"/> is always true,
    /// so the type-guard loop never runs and behaviour is identical to the
    /// pre-guard flag-trusting walk.
    /// </summary>
    private async ValueTask<GrainId> ResolveWriteLeafAsync(string key, Stack<GrainId> path)
    {
        var currentId = state.State.RootNodeId!.Value;

        // Flag-trusting walk that also records the ancestor path. Skipped when
        // the persisted RootIsLeaf flag claims a single-leaf tree; the type
        // guard below corrects either an internal root mislabelled as a leaf or
        // a ChildrenAreLeaves flag that fired one level too early.
        if (!state.State.RootIsLeaf)
        {
            var descended = false;
            for (var level = 0; level < MaxTreeDescentLevels; level++)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);
                path.Push(currentId);
                currentId = childId;
                if (childrenAreLeaves)
                {
                    descended = true;
                    break;
                }
            }

            if (!descended)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} descent for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
            }
        }

        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            if (IsLeafGrainId(currentId))
                return currentId;

            path.Push(currentId);
            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            var (childId, _) = snapshot.Route(key);
            currentId = childId;
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} write descent for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf; tree topology may be corrupt.");
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
        if (_routingTableCache.TryGetValue(internalId, out var snapshot))
        {
            return new ValueTask<RoutingTableSnapshot>(snapshot);
        }
        return GetRoutingTableSnapshotSlowAsync(internalId);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async ValueTask<RoutingTableSnapshot> GetRoutingTableSnapshotSlowAsync(GrainId internalId)
    {
        // A fetch that overlaps an invalidation may carry the pre-mutation
        // table, and caching it would pin that stale table until the node next
        // splits - routing every key of a newly linked child to its donor.
        // Cache only when no invalidation ran while the fetch was in flight
        // (issue #3523); the caller still gets the snapshot it fetched.
        var generation = Volatile.Read(ref _routingGeneration);
        var grain = ResolveInternalGrain(internalId);
        var snapshot = await grain.GetRoutingTableAsync();
        if (generation == Volatile.Read(ref _routingGeneration))
        {
            _routingTableCache[internalId] = snapshot;
        }

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
    /// <para>
    /// Every call also advances <see cref="_routingGeneration"/>, the cheap
    /// local signal <see cref="MergeGroupAsync"/> reads to decide whether a
    /// batch grouped earlier may now straddle a leaf boundary (issue #2125),
    /// and that a cache-miss fetch reads to decide whether the table it
    /// fetched may still be cached (issue #3523).
    /// Internal rather than private so the merge re-grouping tests can move
    /// routing under a batch without a real split.
    /// </para>
    /// </summary>
    internal void InvalidateRoutingTable(GrainId internalId)
    {
        _routingTableCache.TryRemove(internalId, out _);
        Interlocked.Increment(ref _routingGeneration);
    }

    /// <summary>
    /// Advances on every <see cref="InvalidateRoutingTable"/> call. Read by
    /// <see cref="MergeManyAsync"/> when it groups a batch and re-read by
    /// <see cref="MergeGroupAsync"/> before it dispatches a group, so a group
    /// built against routing that has since moved is re-grouped instead of
    /// being handed wholesale to the leaf its first key routed to. It is a
    /// plain in-memory counter: reading it adds no grain call, and it resets
    /// with the activation, which is harmless because a group never outlives
    /// the call that built it.
    /// </summary>
    private long _routingGeneration;

    /// <summary>
    /// How many times <see cref="MergeGroupAsync"/> took the re-grouping path.
    /// A test seam only: it lets a unit test prove the normal path (first
    /// attempt, unchanged routing) never re-groups.
    /// </summary>
    internal long MergeRegroupCount => Volatile.Read(ref _mergeRegroupCount);

    private long _mergeRegroupCount;

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

        // Guard: route to a real leaf even if a corrupt RootIsLeaf /
        // ChildrenAreLeaves flag resolved an internal node (issue 899).
        if (!IsLeafGrainId(leafId))
        {
            leafId = await DescendToLeafForKeyAsync(leafId, key);
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
        RecordLeafAccess(leafId);
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

        // Guard: route to a real leaf even if a corrupt RootIsLeaf /
        // ChildrenAreLeaves flag resolved an internal node (issue 899).
        if (!IsLeafGrainId(leafId))
        {
            leafId = await DescendToLeafForKeyAsync(leafId, key);
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

        // Guard: route to a real leaf even if a corrupt RootIsLeaf /
        // ChildrenAreLeaves flag resolved an internal node (issue 899).
        if (!IsLeafGrainId(leafId))
        {
            leafId = await DescendToLeafForKeyAsync(leafId, key);
        }

        var cache = ResolveLeafCacheGrain(leafId);
        RecordLeafAccess(leafId);
        return await cache.ExistsAsync(key);
    }

    private async Task<Dictionary<string, byte[]>> TraverseForBatchReadAsync(List<string> keys)
    {
        // Single-leaf fast path: when the root is itself a leaf every key
        // targets the same leaf cache, so skip the per-key leaf-bucketing
        // dictionary, the per-key bucket lists, and the result-merge dictionary
        // entirely and issue one GetManyAsync over the whole key list. Only
        // taken when the root node id really is a leaf grain; a corrupt
        // RootIsLeaf flag resolving an internal node (issue 899) still needs the
        // per-key descent handled by the general path below.
        if (state.State.RootIsLeaf && IsLeafGrainId(state.State.RootNodeId!.Value))
        {
            var rootLeafId = state.State.RootNodeId!.Value;
#if LATTICE_DIAG
            foreach (var key in keys)
            {
                DiagSink.Write($"[DIAG read-routing] gid={context.GrainId} key={key} leafId={rootLeafId} rootIsLeaf=true movedSlots=[{string.Join(',', state.State.MovedAwaySlots.Keys)}] phase={state.State.SplitInProgress?.Phase.ToString() ?? "(none)"} batch=true");
            }
#endif
            var rootCache = ResolveLeafCacheGrain(rootLeafId);
            RecordLeafAccess(rootLeafId);
            return await rootCache.GetManyAsync(keys);
        }

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

            // Guard: route to a real leaf even if a corrupt RootIsLeaf /
            // ChildrenAreLeaves flag resolved an internal node (issue 899).
            if (!IsLeafGrainId(leafId))
            {
                leafId = await DescendToLeafForKeyAsync(leafId, key);
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

        // Single-bucket shortcut: a multi-level tree whose batch happened to
        // land entirely on one leaf. Hand the bucket straight to that leaf and
        // return its own dictionary, skipping the dispatch array, the
        // Task.WhenAll, and the merge target entirely - the same discipline the
        // RootIsLeaf fast path above applies one level up.
        if (leafBuckets.Count == 1)
        {
            foreach (var (leafId, bucket) in leafBuckets)
            {
                var soleCache = ResolveLeafCacheGrain(leafId);
                RecordLeafAccess(leafId);
                return await soleCache.GetManyAsync(bucket);
            }
        }

        // Fan out the per-leaf reads in parallel, mirroring the per-leaf
        // dispatch SetManyLocalOnlyAsync already does on the write path (which
        // in turn mirrors LatticeGrain's per-shard fan-out). The shard-root
        // grain is single-activation, so every parallel await resumes on the
        // same grain turn; the two caches this touches are mutated only in the
        // sequential resolve pass below, never inside the parallel region, so
        // the fan-out adds no interleaving hazard the write path does not
        // already carry.
        //
        // This is a correctness fix, not a micro-optimisation. Sequentially
        // awaiting one leaf at a time makes the call's latency the SUM of every
        // bucket's round trip, so a batch spanning many leaves - a
        // point-probe over scattered keys, which is exactly what the
        // vector-membership probe issues - walks off the end of the Orleans
        // response deadline and surfaces as a TimeoutException on
        // ILattice.GetManyAsync rather than as slowness. Fanning out makes the
        // latency the MAX instead, which is what keeps a wide batch inside the
        // deadline no matter how scattered the keys are.
        //
        // Resolve the leaf-cache reference and record the access for every
        // bucket FIRST, on the sequential pass: ResolveLeafCacheGrain populates
        // the reference cache and RecordLeafAccess mutates the access model, so
        // both stay outside the parallel region by construction.
        var dispatch = new Task<Dictionary<string, byte[]>>[leafBuckets.Count];
        var dispatched = 0;
        foreach (var (leafId, bucket) in leafBuckets)
        {
            var cache = ResolveLeafCacheGrain(leafId);
            RecordLeafAccess(leafId);
            dispatch[dispatched++] = cache.GetManyAsync(bucket);
        }

        var fetched = await Task.WhenAll(dispatch);

        // Presize the merge target to keys.Count: the loop writes one entry per
        // returned key and the returned count is bounded above by the number of
        // requested keys (fewer when some keys are absent), so keys.Count is a
        // tight upper bound that eliminates the dictionary's geometric
        // grow/rehash chain on a multi-leaf batch read.
        var result = new Dictionary<string, byte[]>(keys.Count);
        foreach (var values in fetched)
        {
            foreach (var (k, v) in values)
            {
                result[k] = v;
            }
        }
        return result;
    }

    /// <summary>
    /// Delivers a split's promoted separator to every ancestor that must route
    /// to the new sibling, recording the linkage as a <b>durable intent</b>
    /// first so it cannot be forfeited by a failure part-way up (issue #3265).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is the seam the defect lived in, so read what it is for before
    /// simplifying it back.</b> By the time a <see cref="SplitResult"/> reaches
    /// this method the new sibling is already fully durable: it has been
    /// created and persisted, spliced into the doubly-linked sibling chain, and
    /// it has published a WAL materialiser pin. Only one thing is still
    /// outstanding, and it is the one thing that makes the sibling
    /// <em>reachable</em> - the separator that teaches its parent to route to
    /// it.
    /// </para>
    /// <para>
    /// That request used to live in a local variable, in five near-identical
    /// loops here and four more on the batch paths. Every durable step of a
    /// split therefore survived a failure and the single non-durable step did
    /// not, which is an ordering that can only ever fail one way: the sibling
    /// outlives the instruction to link it. What is left is a leaf that no
    /// descent reaches, still spliced into the chain, still holding a pin - and
    /// a leaf that is not in the tree can never advance a pin, so the pin stops
    /// the WAL trim for good. Trim is upstream of compaction, so the log then
    /// grows with no bound at all.
    /// </para>
    /// <para>
    /// Recording the intent before asking the parent inverts that ordering: the
    /// instruction to link now outlives the failure, and
    /// <see cref="ResumePendingChildLinksAsync"/> replays it on the next
    /// operation. Replay is safe because <c>AcceptSplitAsync</c> is idempotent
    /// on the separator/child pair - a duplicate delivery is recognised and
    /// skipped.
    /// </para>
    /// <para>
    /// The ancestor path the caller captured on its way down is deliberately
    /// not what the split is linked against. Under concurrent
    /// <c>SetManyAsync</c> turns that path goes stale between capture and link,
    /// and a separator delivered to a parent whose range no longer covers it is
    /// accepted and routed to by nothing (issue #3523). Every link instead runs
    /// under <see cref="_splitLinkGate"/> and re-descends from the current root;
    /// see <see cref="LinkSplitAsync"/>. A parent that divides while accepting
    /// has its own division recorded before the landed link is retired, one
    /// level at a time, so a resume never has to rediscover a chain.
    /// </para>
    /// <para>
    /// Every caller that promotes a split routes through here. That is
    /// deliberate and worth preserving: a gate observed at each call site is a
    /// convention, not an invariant, and it was a call site quietly not
    /// observing one that produced this defect in the first place.
    /// </para>
    /// </remarks>
    private Task<SplitResult?> PropagateSplitAsync(SplitResult? splitResult, Stack<GrainId> path)
    {
        // The captured path is not used to link (see remarks); the stack is
        // drained so it returns to the pool empty, exactly as the old Pop()
        // loop left it.
        path.Clear();
        return splitResult is null ? NullSplitTask : LinkSplitAsync(splitResult);
    }

    /// <summary>
    /// Ancestor-list overload of <see cref="PropagateSplitAsync(SplitResult?, Stack{GrainId})"/>
    /// for the batch write paths, which capture each leaf's parent path as a
    /// root-first list rather than a descent stack. The list is used only to
    /// group keys by leaf; linking re-descends (see
    /// <see cref="LinkSplitAsync"/>).
    /// </summary>
    private Task<SplitResult?> PropagateSplitAsync(SplitResult? splitResult, IReadOnlyList<GrainId> parentsRootFirst)
        => splitResult is null ? NullSplitTask : LinkSplitAsync(splitResult);

    private static readonly Task<SplitResult?> NullSplitTask = Task.FromResult<SplitResult?>(null);

    /// <summary>
    /// Links every division <paramref name="splitResult"/> describes - its
    /// primary split and each forwarded or combined one it carries - into the
    /// tree, under <see cref="_splitLinkGate"/>, and always returns
    /// <see langword="null"/> so a caller's promotion loop is a no-op
    /// (issue #3523).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why a gate, and why a fresh descent.</b> Internal nodes store no key
    /// range, and <c>AcceptSplitAsync</c> inserts whatever separator it is
    /// given. A separator delivered to a parent whose range does not cover it
    /// is therefore accepted, persisted, and routed to by nothing: its child is
    /// spliced into the sibling chain but no descent reaches it, and every key
    /// on it is lost to reads. The ancestor path a writer captured on its way
    /// down is exactly that wrong parent as soon as a concurrent
    /// <c>SetManyAsync</c> turn (the method is <c>[AlwaysInterleave]</c>) has
    /// divided a node on it or promoted the root in between. So every link
    /// runs one at a time per shard, and each re-descends from the current
    /// root, bypassing the routing cache, to the node one level above the
    /// dividing child. Under the gate nothing else links or promotes, so that
    /// parent is still the right one when it is asked to accept.
    /// </para>
    /// <para>
    /// The gate is entered only on a split, never on the per-key write path,
    /// and it is taken before <c>_promotionGate</c> everywhere, so the two
    /// cannot deadlock.
    /// </para>
    /// <para>
    /// Every caller of this overload hands over a division a leaf reported, so
    /// it is linked at leaf level regardless of
    /// <see cref="SplitResult.ChildIsLeaf"/>. Reading an unset flag as a root
    /// division would wrap the root around a leaf rather than link it.
    /// </para>
    /// </remarks>
    private Task<SplitResult?> LinkSplitAsync(SplitResult splitResult)
        => LinkSplitAsync(splitResult, 0);

    /// <summary>
    /// <see cref="LinkSplitAsync(SplitResult)"/> for a caller that knows the
    /// height of the new sibling: 0 for a leaf, the number of internal levels
    /// at and below it for an internal node, or <c>-1</c> for a division of the
    /// current root.
    /// </summary>
    private async Task<SplitResult?> LinkSplitAsync(SplitResult splitResult, int height)
    {
        await _splitLinkGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            await LinkSplitLockedAsync(splitResult, height);
        }
        finally
        {
            _inFlightChildLinks.Clear();
            _splitLinkGate.Release();
        }

        return null;
    }

    /// <summary>
    /// One outstanding link: a division, the height of its new sibling (0 for
    /// a leaf, <c>-1</c> for a sibling of the current root, as a bulk graft
    /// delivers), and the durable intent that records it, when one has been
    /// written.
    /// </summary>
    private readonly record struct LinkWork(SplitResult Split, int Height, PendingChildLink? Intent);

    private async Task LinkSplitLockedAsync(SplitResult splitResult, int height)
    {
        var work = new List<LinkWork>(1 + (splitResult.Additional?.Length ?? 0));
        foreach (var single in FlattenSplit(splitResult))
        {
            work.Add(new LinkWork(single, height, null));
        }

        // A root-level division is recorded by PendingPromotion; every other
        // one is recorded here, all in one write, before the first is linked,
        // so a fault part-way through strands records rather than siblings.
        if (height >= 0)
        {
            await RecordPendingChildLinksAsync(work);
        }

        await DrainLinkWorkLockedAsync(work);
    }

    private async Task DrainLinkWorkLockedAsync(List<LinkWork> work)
    {
        for (var next = 0; next < work.Count; next++)
        {
            var item = work[next];
            var residuals = await LinkOneLockedAsync(item);
            if (residuals is null)
            {
                continue;
            }

            // The parent divided on accepting: record every resulting division
            // and retire the one that landed in a single write, then link the
            // new ones one level up.
            var added = new List<LinkWork>(residuals.Count);
            foreach (var residual in residuals)
            {
                added.Add(new LinkWork(residual, item.Height + 1, null));
            }

            await ReplacePendingChildLinkAsync(item.Intent, added);
            work.AddRange(added);
        }
    }

    /// <summary>
    /// Links one division by fresh descent, promoting the root when the
    /// division is of the root itself, and returns the divisions the accepting
    /// parent made in turn (or <see langword="null"/>). A link that needed no
    /// further division has its intent retired here.
    /// </summary>
    private async Task<List<SplitResult>?> LinkOneLockedAsync(LinkWork item)
    {
        var split = item.Split;
        if (item.Height < 0 || RootIsLeafTyped)
        {
            if (RootIsLeafTyped && item.Height != 0)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} cannot link internal child {split.NewSiblingId} under separator '{split.PromotedKey}': the root is a leaf.");
            }

            await PromoteRootLockedAsync(split);
            await ClearPendingChildLinkAsync(item.Intent);
            return null;
        }

        var pathRootFirst = await DescendForLinkAsync(split.PromotedKey);
        var rootHeight = pathRootFirst.Count;
        if (item.Height >= rootHeight)
        {
            if (item.Height > rootHeight)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} cannot link child {split.NewSiblingId} of height {item.Height} under separator '{split.PromotedKey}': the tree is only {rootHeight} internal levels deep.");
            }

            // The current root is what divided.
            await PromoteRootLockedAsync(split);
            await ClearPendingChildLinkAsync(item.Intent);
            return null;
        }

        // pathRootFirst[i] sits at height rootHeight - i, so the parent of a
        // child at height h is at index rootHeight - h - 1.
        var parentId = pathRootFirst[rootHeight - item.Height - 1];
        var residual = await ResolveInternalGrain(parentId).AcceptSplitAsync(split.PromotedKey, split.NewSiblingId);
        InvalidateRoutingTable(parentId);

        if (residual is null)
        {
            await ClearPendingChildLinkAsync(item.Intent);
            return null;
        }

        var residuals = new List<SplitResult>(1 + (residual.Additional?.Length ?? 0));
        residuals.AddRange(FlattenSplit(residual));
        return residuals;
    }

    /// <summary>
    /// Yields the primary division of <paramref name="split"/> and every entry
    /// of its <see cref="SplitResult.Additional"/>, each as a single split with
    /// <see cref="SplitResult.Forwarded"/> cleared. Every division is linked by
    /// fresh descent, so whether it was forwarded no longer matters.
    /// </summary>
    private static IEnumerable<SplitResult> FlattenSplit(SplitResult split)
    {
        yield return split.Additional is null && !split.Forwarded
            ? split
            : split with { Additional = null, Forwarded = false };

        if (split.Additional is { } extras)
        {
            foreach (var extra in extras)
            {
                yield return extra.Additional is null && !extra.Forwarded
                    ? extra
                    : extra with { Additional = null, Forwarded = false };
            }
        }
    }

    /// <summary>
    /// Descends from the current root to the deepest internal node routing
    /// <paramref name="key"/>, reading every routing table fresh from its node
    /// rather than from the cache, and returns the internal nodes on the way,
    /// root first.
    /// </summary>
    private async Task<List<GrainId>> DescendForLinkAsync(string key)
    {
        var path = new List<GrainId>(4);
        var currentId = state.State.RootNodeId!.Value;
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            path.Add(currentId);
            var snapshot = await ResolveInternalGrain(currentId).GetRoutingTableAsync();
            var (childId, childrenAreLeaves) = snapshot.Route(key);

            // Decided by node type as well as by flag, so a ChildrenAreLeaves
            // bit that lies about internal children (issue 899) does not stop
            // the descent a level early.
            if (childrenAreLeaves && IsLeafGrainId(childId))
            {
                return path;
            }

            currentId = childId;
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} link descent for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
    }

    /// <summary>
    /// Measures how many internal levels sit at and below
    /// <paramref name="internalId"/>: 1 when its children are leaves. Used only
    /// to resume a recorded link whose child is an internal node.
    /// </summary>
    private async Task<int> MeasureInternalHeightAsync(GrainId internalId)
    {
        var currentId = internalId;
        for (var height = 1; height <= MaxTreeDescentLevels; height++)
        {
            var (childId, childrenAreLeaves) = await ResolveInternalGrain(currentId).GetLeftmostChildWithMetadataAsync();
            if (childrenAreLeaves && IsLeafGrainId(childId))
            {
                return height;
            }

            currentId = childId;
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} height measurement from {internalId} exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
    }

    private static PendingChildLink NewPendingChildLink(SplitResult split) => new()
    {
        PromotedKey = split.PromotedKey,
        ChildId = split.NewSiblingId,
        ChildIsLeaf = split.ChildIsLeaf,
        Ancestors = [],
    };

    /// <summary>
    /// Persists the intent to link every division in <paramref name="work"/>,
    /// in one write, before the first ancestor is asked to accept any of them.
    /// </summary>
    /// <remarks>
    /// A failure here is reported to the caller and the entries are withdrawn
    /// from the activation, which is the safe direction: nothing has been
    /// linked yet, so the write simply fails and the siblings are disposed of by
    /// the ordinary interrupted-split recovery rather than being stranded
    /// reachable-by-chain but unreachable-by-descent.
    /// </remarks>
    private async Task RecordPendingChildLinksAsync(List<LinkWork> work)
    {
        for (var i = 0; i < work.Count; i++)
        {
            var intent = NewPendingChildLink(work[i].Split);
            work[i] = work[i] with { Intent = intent };
            state.State.PendingChildLinks.Add(intent);
            _inFlightChildLinks.Add(intent);
        }

        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            foreach (var item in work)
            {
                RemovePendingChildLink(item.Intent!);
            }

            throw;
        }
    }

    /// <summary>
    /// Retires <paramref name="landed"/>, whose parent accepted it and divided,
    /// and records the divisions that parent made, in a single write. The
    /// entries in <paramref name="added"/> gain their intents here.
    /// </summary>
    /// <remarks>
    /// The accept has already landed, so a failure to persist is logged rather
    /// than surfaced and the in-memory records are kept: the next shard write
    /// persists them, and until then the divisions are linked from memory
    /// exactly as before. Surfacing it would abandon the parent's division,
    /// which is the loss this record exists to prevent.
    /// </remarks>
    private async Task ReplacePendingChildLinkAsync(PendingChildLink? landed, List<LinkWork> added)
    {
        if (landed is not null)
        {
            RemovePendingChildLink(landed);
        }

        for (var i = 0; i < added.Count; i++)
        {
            var intent = NewPendingChildLink(added[i].Split);
            added[i] = added[i] with { Intent = intent };
            state.State.PendingChildLinks.Add(intent);
            _inFlightChildLinks.Add(intent);
        }

        try
        {
            await WriteShardStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' linked a child whose parent then divided, but could not persist the pending link for that division; it is linked from memory and recorded on the next shard write.",
                MyShardIndex,
                TreeId);
        }
    }

    /// <summary>
    /// Retires a link that has landed.
    /// </summary>
    /// <remarks>
    /// A failure to persist the retirement is logged and swallowed rather than
    /// surfaced. The link itself has landed, so reporting the operation as
    /// failed would be wrong; the only consequence of the stale entry surviving
    /// is that a later resume re-delivers a separator the parent already holds,
    /// which <c>AcceptSplitAsync</c> recognises as a duplicate and skips.
    /// </remarks>
    private async Task ClearPendingChildLinkAsync(PendingChildLink? intent)
    {
        if (intent is null || !RemovePendingChildLink(intent))
        {
            return;
        }

        try
        {
            await WriteShardStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogDebug(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' linked child {ChildId} under separator '{PromotedKey}' but could not retire the pending-link record; a later resume will re-deliver it and the parent will skip it as a duplicate.",
                MyShardIndex,
                TreeId,
                intent.ChildId,
                intent.PromotedKey);
        }
    }

    /// <summary>
    /// Removes a pending link by reference identity. Value equality is not
    /// usable here: <see cref="PendingChildLink"/> is a record whose
    /// <see cref="PendingChildLink.Ancestors"/> member compares by reference
    /// anyway, and two concurrent splits can legitimately promote the same
    /// separator for the same child on a retry.
    /// </summary>
    private bool RemovePendingChildLink(PendingChildLink intent)
    {
        _inFlightChildLinks.Remove(intent);
        var links = state.State.PendingChildLinks;
        for (var i = 0; i < links.Count; i++)
        {
            if (ReferenceEquals(links[i], intent))
            {
                links.RemoveAt(i);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Re-delivers any child link that was recorded but not retired, which
    /// means a previous attempt failed between the two (issue #3265).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Runs from the ordinary pre-operation preamble, so a tree that is being
    /// used repairs itself on its next write without any sweep or operator
    /// action. Each entry is retired only once it has landed, so an entry that
    /// fails again is left in place for the next attempt rather than dropped -
    /// a dropped link is exactly the failure this record exists to prevent, and
    /// it is not made better by being dropped deliberately.
    /// </para>
    /// <para>
    /// It runs under <see cref="_splitLinkGate"/>, and every link records and
    /// resolves its intent without releasing that gate, so every entry seen
    /// here is genuinely stranded rather than a concurrent turn's link in
    /// flight - re-delivering one of those against its own captured topology is
    /// how a resume used to race the link it was meant to back up. Each entry
    /// re-descends like any other link (<see cref="LinkSplitAsync"/>), so the
    /// ancestors it recorded are not trusted.
    /// </para>
    /// </remarks>
    private async Task ResumePendingChildLinksAsync()
    {
        if (state.State.PendingChildLinks.Count == 0)
        {
            return;
        }

        await _splitLinkGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            var pending = state.State.PendingChildLinks.ToArray();

            foreach (var intent in pending)
            {
                try
                {
                    var split = new SplitResult
                    {
                        PromotedKey = intent.PromotedKey,
                        NewSiblingId = intent.ChildId,
                        ChildIsLeaf = intent.ChildIsLeaf,
                    };

                    var height = intent.ChildIsLeaf ? 0 : await MeasureInternalHeightAsync(intent.ChildId);
                    await DrainLinkWorkLockedAsync([new LinkWork(split, height, intent)]);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(
                        ex,
                        "Shard {ShardIndex} of tree '{TreeId}' could not re-deliver the pending link for child {ChildId} under separator '{PromotedKey}'; it stays recorded and the next operation retries it. Until it lands, that child is spliced into the sibling chain but no descent reaches it.",
                        MyShardIndex,
                        TreeId,
                        intent.ChildId,
                        intent.PromotedKey);

                    continue;
                }

                logger.LogInformation(
                    "Shard {ShardIndex} of tree '{TreeId}' completed an interrupted split: child {ChildId} is now routed under separator '{PromotedKey}'.",
                    MyShardIndex,
                    TreeId,
                    intent.ChildId,
                    intent.PromotedKey);
            }
        }
        finally
        {
            _inFlightChildLinks.Clear();
            _splitLinkGate.Release();
        }
    }

    private async Task<SplitResult?> TraverseForWriteAsync(string key, byte[] value)
    {
        var path = StackPool.Get();
        try
        {
            var leafId = await ResolveWriteLeafAsync(key, path);
            var leafGrain = ResolveLeafGrain(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            var splitResult = await leafGrain.SetAsync(key, value);

            return await PropagateSplitAsync(splitResult, path);
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
        var path = StackPool.Get();
        try
        {
            var leafId = await ResolveWriteLeafAsync(key, path);
            var leafGrain = ResolveLeafGrain(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            var splitResult = await leafGrain.SetAsync(key, value, expiresAtTicks);

            return await PropagateSplitAsync(splitResult, path);
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private async Task<GetOrSetResult> TraverseForGetOrSetAsync(string key, byte[] value)
    {
        var path = StackPool.Get();
        try
        {
            var leafId = await ResolveWriteLeafAsync(key, path);
            var leafGrain = ResolveLeafGrain(leafId);
            var result = await leafGrain.GetOrSetAsync(key, value);

            // If the key was already live, no write occurred - no splits to propagate.
            if (result.ExistingValue is not null)
            {
                return result;
            }

            // Propagate splits up the tree.
            var splitResult = await PropagateSplitAsync(result.Split, path);

            return new GetOrSetResult { Split = splitResult };
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    private async Task<CasResult> TraverseForSetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        var path = StackPool.Get();
        try
        {
            var leafId = await ResolveWriteLeafAsync(key, path);
            var leafGrain = ResolveLeafGrain(leafId);
            var result = await leafGrain.SetIfVersionAsync(key, value, expectedVersion);

            // If CAS failed, no write occurred - no splits to propagate.
            if (!result.Success)
            {
                return result;
            }

            // Propagate splits up the tree.
            var splitResult = await PropagateSplitAsync(result.Split, path);

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

    private async Task<CrdtApplyResult> TraverseForCrdtApplyAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks)
    {
        var path = StackPool.Get();
        try
        {
            var leafId = await ResolveWriteLeafAsync(key, path);
            var leafGrain = ResolveLeafGrain(leafId);
            var result = await ApplyCrdtDeltaRebindingUnboundLeafAsync(
                leafGrain, leafId, key, mode, deltaBytes, expiresAtTicks);

            // Propagate splits up the tree.
            var splitResult = await PropagateSplitAsync(result.Split, path);

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

    /// <summary>
    /// Applies a typed CRDT delta to <paramref name="leafGrain"/>, re-binding the
    /// leaf and retrying once if it turns out to be routable but unseeded.
    /// <para>
    /// A leaf's owning-tree binding is otherwise only ever written when the node
    /// is <em>created</em>, so a leaf that loses it - an interrupted
    /// <c>PurgeAsync</c> clears node state before the shard root's own state, and
    /// a split inherits the donor's binding verbatim, so an unbound donor mints an
    /// unbound sibling - stays unbound forever. Routing still delivers writes to
    /// it, but every typed CRDT apply faults resolving its <c>CrdtShape</c>, and
    /// nothing on any lifecycle path repairs it (issue #1744).
    /// </para>
    /// <para>
    /// This shard root knows the binding the leaf is missing, so it re-asserts it
    /// and retries. The repair is driven from the fault rather than from a probe,
    /// which is what lets it heal a deployment that is <em>already</em> in this
    /// state, on the very next write, with no operator action and no restart: the
    /// happy path never pays for it, because an exception filter is only evaluated
    /// once a fault is in flight.
    /// </para>
    /// <para>
    /// Deliberately narrow, so a genuine fault is never masked. Only the unbound
    /// -leaf shape is caught - <see cref="LatticeCrdtShapeNotRegisteredException"/>
    /// carrying an empty <see cref="LatticeCrdtShapeNotRegisteredException.TreeId"/>,
    /// which is raised on exactly one branch - so a real "no shape registered for
    /// this tree" fault (which carries the tree id) propagates untouched. The
    /// retry is single-shot and its own failure propagates, so a leaf whose
    /// binding cannot be restored still fails closed exactly as it does today
    /// rather than silently accepting an unbound write. The repair is logged at
    /// warning level because a leaf losing its binding is an anomaly worth
    /// surfacing even though it is now self-correcting.
    /// </para>
    /// </summary>
    private async Task<CrdtApplyResult> ApplyCrdtDeltaRebindingUnboundLeafAsync(
        IBPlusLeafGrain leafGrain,
        GrainId leafId,
        string key,
        LatticeMergeMode mode,
        byte[] deltaBytes,
        long expiresAtTicks)
    {
        try
        {
            return await leafGrain.ApplyCrdtDeltaAsync(key, mode, deltaBytes, expiresAtTicks);
        }
        catch (LatticeCrdtShapeNotRegisteredException ex) when (ex.TreeId.Length == 0)
        {
            logger.LogWarning(
                ex,
                "Leaf {LeafId} of shard {ShardIndex} of tree {TreeId} is routable but has no tree id bound, so the "
                + "typed CRDT write to key {Key} could not resolve a CrdtShape. Re-asserting the binding and "
                + "retrying once; this repairs a node left unseeded by an interrupted purge or inherited from an "
                + "unbound split donor.",
                leafId,
                MyShardIndex,
                TreeId,
                key);

            await leafGrain.SetTreeIdAsync(TreeId);
            await leafGrain.SetShardIndexAsync(MyShardIndex);

            // Single-shot. A second failure is a genuine fault (the shape really
            // is unregistered, or the binding write did not stick) and must stay
            // fail-closed, so it propagates to the caller unaltered.
            return await leafGrain.ApplyCrdtDeltaAsync(key, mode, deltaBytes, expiresAtTicks);
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
        for (var level = 0; level < MaxTreeDescentLevels; level++)
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

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} read descent for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
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
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            snapshot = await GetRoutingTableSnapshotAsync(currentId);
            (childId, childrenAreLeaves) = snapshot.Route(key);
            if (childrenAreLeaves)
            {
                return childId;
            }
            currentId = childId;
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} read descent for key '{key}' exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
    }

    private async Task<GrainId> TraverseToLeftmostLeafAsync()
    {
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else
        {
            var currentId = state.State.RootNodeId!.Value;
            GrainId? found = null;
            for (var level = 0; level < MaxTreeDescentLevels && found is null; level++)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var childId = snapshot.ChildIds[0];
                if (snapshot.ChildrenAreLeaves)
                {
                    found = childId;
                    break;
                }
                currentId = childId;
            }

            leafId = found ?? throw new InvalidOperationException(
                $"ShardRootGrain {context.GrainId} leftmost-leaf descent exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
        }

        return await DescendToEdgeLeafTypeGuardAsync(leafId, leftmost: true);
    }

    private async Task<GrainId> TraverseToRightmostLeafAsync()
    {
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else
        {
            var currentId = state.State.RootNodeId!.Value;
            GrainId? found = null;
            for (var level = 0; level < MaxTreeDescentLevels && found is null; level++)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var childId = snapshot.ChildIds[snapshot.ChildIds.Length - 1];
                if (snapshot.ChildrenAreLeaves)
                {
                    found = childId;
                    break;
                }
                currentId = childId;
            }

            leafId = found ?? throw new InvalidOperationException(
                $"ShardRootGrain {context.GrainId} rightmost-leaf descent exceeded {MaxTreeDescentLevels} levels without reaching a leaf level; tree topology may be corrupt.");
        }

        return await DescendToEdgeLeafTypeGuardAsync(leafId, leftmost: false);
    }

    /// <summary>
    /// Type-correcting edge descent for <see cref="TraverseToLeftmostLeafAsync"/>
    /// and <see cref="TraverseToRightmostLeafAsync"/>: if the flag-trusting edge
    /// walk stopped on a node that is not actually a leaf grain - a
    /// baked-inconsistent <c>RootIsLeaf</c> bit left true over an internal root,
    /// or a <c>ChildrenAreLeaves</c> flag true over internal children (issue 899)
    /// - it keeps descending the requested edge (leftmost or rightmost) by node
    /// TYPE (<see cref="IsLeafGrainId"/>) until a real leaf grain id is reached.
    /// This guarantees every caller of <see cref="GetLeftmostLeafIdAsync"/> (the
    /// scan surface, the replication snapshot producer, compaction, merge and
    /// split leaf-chain walkers) receives a leaf-typed id it can safely cast to
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>. No-op for healthy trees and for non-runtime
    /// test factories, where <see cref="IsLeafGrainId"/> is always true.
    /// </summary>
    private async ValueTask<GrainId> DescendToEdgeLeafTypeGuardAsync(GrainId nodeId, bool leftmost)
    {
        if (IsLeafGrainId(nodeId))
            return nodeId;

        var currentId = nodeId;
        for (var level = 0; level < MaxTreeDescentLevels; level++)
        {
            if (IsLeafGrainId(currentId))
                return currentId;

            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            if (snapshot.ChildIds.Length == 0)
            {
                throw new InvalidOperationException(
                    $"ShardRootGrain {context.GrainId} edge descent reached internal node {currentId} with no children.");
            }

            currentId = leftmost
                ? snapshot.ChildIds[0]
                : snapshot.ChildIds[snapshot.ChildIds.Length - 1];
        }

        throw new InvalidOperationException(
            $"ShardRootGrain {context.GrainId} edge descent from {nodeId} exceeded {MaxTreeDescentLevels} levels without reaching a leaf; tree topology may be corrupt.");
    }

    /// <summary>
    /// Links a split a caller could not propagate against an ancestor path -
    /// a division of a single-leaf root, or of the root itself - and always
    /// returns <see langword="null"/>. Routed through
    /// <see cref="LinkSplitAsync"/> so it serialises with every other link.
    /// </summary>
    /// <remarks>
    /// A leaf-level split that arrives here after an interleaved turn has
    /// already promoted the root is linked by descent like any other, rather
    /// than wrapped under a new root or - as it used to be once the live root
    /// was two or more levels deep - dropped with a warning on the reasoning
    /// that a peer promotion had absorbed it. Nothing had: the dropped sibling
    /// was spliced, pinned and unreachable, and its keys lost (issue #3523).
    /// </remarks>
    private Task<SplitResult?> PromoteRootAsync(SplitResult splitResult) => LinkSplitAsync(splitResult);

    /// <summary>
    /// Wraps the current root and <paramref name="splitResult"/>'s sibling
    /// under a new root. Only <see cref="LinkOneLockedAsync"/> calls it, under
    /// <see cref="_splitLinkGate"/>, having established that the division is of
    /// the current root.
    /// </summary>
    private async Task PromoteRootLockedAsync(SplitResult splitResult)
    {
        // The promotion gate still serialises the two-phase sequence (persist
        // intent, then complete) against ResumePendingPromotionAsync. Every
        // path takes _splitLinkGate first, so the order is fixed.
        await _promotionGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            state.State.PendingPromotion = splitResult;
            state.State.PendingPromotionRootWasLeaf = state.State.RootIsLeaf;
            await WriteShardStateAsync();

            await CompletePromotionAsync();
        }
        finally
        {
            _promotionGate.Release();
        }
    }

    /// <summary>
    /// Completes (or resumes) a root promotion whose intent has already been persisted.
    /// </summary>
    private async Task CompletePromotionAsync()
    {
        var pending = state.State.PendingPromotion!;
        var currentRootId = state.State.RootNodeId!.Value;

        // Recovery shape check. The persisted `PendingPromotion` was
        // written when some earlier turn decided to wrap a new root.
        // For a legitimate wrap-as-new-root that crashed between
        // Phase 1 (persist intent) and Phase 2 (create new root + clear
        // intent), `state.State.RootIsLeaf` is still on the pre-wrap
        // value (true for the first-ever promotion, false for higher
        // promotions) because Phase 2 is what flips it; the branch
        // below is skipped and the legacy wrap reapplies idempotently
        // via the deterministic new-root id.
        //
        // The branch fires only on the U9k step 2 race shape: turn A
        // persisted a leaf-level `PendingPromotion`
        // (`pending.ChildIsLeaf == true`) when `RootIsLeaf == true`,
        // then turn B's interleaved promotion completed before our
        // resume - leaving the live root as a fresh level-1 internal
        // node with `ChildrenAreLeaves == true`. The pending bubble
        // belongs INSIDE that promoted root rather than ABOVE it.
        // Wrapping again would seed the new root with the wrong
        // `childrenAreLeaves` bit and surface as the inverse
        // InvalidCastException SeedChildParentAsync observed on the
        // U9k step 2 ladder ("cast BPlusInternalGrain to
        // IBPlusLeafGrain"). The predicate is deliberately asymmetric
        // (only `ChildIsLeaf=true`); a symmetric
        // `ChildrenAreLeaves == ChildIsLeaf` check would false-
        // positive every legitimate depth->=1 root split whose
        // persisted intent legitimately needs to wrap.
        if (pending.ChildIsLeaf && !RootIsLeafTyped)
        {
            // The pending bubble belongs inside the live topology, at whatever
            // depth it now has. It used to be fed to the live root when that
            // root's children were leaves - discarding any division the root
            // made on accepting it - and dropped outright when the root was
            // deeper, stranding the sibling (issue #3523). Convert it to a
            // recorded child link instead, in one write, so the resume that
            // runs next re-descends and links it at the right parent.
            logger.LogWarning(
                "ShardRootGrain {ShardId} CompletePromotionAsync found a leaf-level promotion intent for child {ChildId} over an internal root; re-recording it as a pending child link to be linked by descent.",
                context.GrainId,
                pending.NewSiblingId);
            state.State.PendingPromotion = null;
            state.State.PendingChildLinks.Add(NewPendingChildLink(pending));
            await WriteShardStateAsync();
            return;
        }

        // Prefer the self-describing ChildIsLeaf flag on the persisted
        // SplitResult over the racy PendingPromotionRootWasLeaf scalar
        // (which is filled in from the live `RootIsLeaf` field at
        // PromoteRootAsync time and would have been clobbered if a
        // previous interleaved turn already flipped `RootIsLeaf` to
        // false). The ChildIsLeaf flag is stamped at split-construction
        // time by the leaf or internal grain that produced the split,
        // so it is immutable across any subsequent shard-root
        // interleaving. PendingPromotionRootWasLeaf is retained on
        // disk for backward compatibility with state persisted by a
        // pre-fix activation: if such state is resumed, ChildIsLeaf
        // would deserialise as its default `false`, and the older
        // bool is the only surviving signal of whether the new
        // sibling holds leaves.
        var childrenAreLeaves = pending.ChildIsLeaf
            ? true
            : state.State.PendingPromotionRootWasLeaf;

        // Final clamp, decided by node TYPE rather than by either flag above
        // (issue 899 / issue 1883). Both `ChildIsLeaf` and the legacy
        // `PendingPromotionRootWasLeaf` scalar describe the LEVEL the new
        // root's children sit at, and both are ultimately sampled from the
        // persisted `RootIsLeaf` bit - which a census of a pristine
        // production volume found lying (true over an internal root) on 96 of
        // 841 shard roots. The surviving root child's actual grain type cannot
        // lie, so it has the last word here. Without this clamp a lying flag
        // wraps a new root over an internal child while claiming
        // `childrenAreLeaves = true`, and the two failures compound: the
        // immediate one is BPlusInternalGrain.SeedChildParentAsync resolving
        // that internal child through IBPlusLeafGrain and throwing
        // InvalidCastException, and the durable one is that the new root is
        // PERSISTED with a ChildrenAreLeaves bit that lies about its own
        // children - minting exactly the inconsistent state that makes the
        // next promotion on this shard skip the guard above. That
        // self-perpetuating step is how the condition accumulated in
        // production. When the grain factory cannot resolve the leaf grain
        // type (a non-runtime test factory) IsLeafGrainId is always true and
        // this clamp is a no-op, exactly like every other issue-899 guard.
        if (childrenAreLeaves && !IsLeafGrainId(currentRootId))
        {
            logger.LogWarning(
                "ShardRootGrain {ShardId} CompletePromotionAsync computed childrenAreLeaves=true for a new root over "
                + "node {CurrentRootId}, which is not a leaf grain (pending.ChildIsLeaf={ChildIsLeaf}, "
                + "PendingPromotionRootWasLeaf={RootWasLeaf}, RootIsLeaf={RootIsLeaf}); clamping to false so the new "
                + "root describes its children truthfully.",
                context.GrainId,
                currentRootId,
                pending.ChildIsLeaf,
                state.State.PendingPromotionRootWasLeaf,
                state.State.RootIsLeaf);
            childrenAreLeaves = false;
        }

        var shardKey = context.GrainId.Key.ToString()!;
        var deterministicId = DeterministicGuid(
            shardKey + "/root-above/" + currentRootId);

        var newRoot = grainFactory.GetGrain<IBPlusInternalGrain>(deterministicId);
        await newRoot.SetTreeIdAsync(TreeId);
        await newRoot.InitializeAsync(
            pending.PromotedKey,
            currentRootId,
            pending.NewSiblingId,
            childrenAreLeaves);

        state.State.RootNodeId = newRoot.GetGrainId();
        state.State.RootIsLeaf = false;
        state.State.PendingPromotion = null;
        await WriteShardStateAsync();
    }
}
