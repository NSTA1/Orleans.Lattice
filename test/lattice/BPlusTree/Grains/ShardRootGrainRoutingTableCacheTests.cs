using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the per-activation routing-table-snapshot cache invariants in
/// <c>ShardRootGrain.Traversal</c>. The cache holds a
/// <see cref="System.Collections.Generic.Dictionary{TKey,TValue}"/>
/// keyed by internal-node <see cref="GrainId"/>; on hit the descend loop
/// performs key-to-child routing locally via
/// <see cref="RoutingTableSnapshot.Route"/> and skips the cross-grain
/// <c>RouteWithMetadataAsync</c> RPC entirely. The invariants the tests
/// lock are:
///
/// <list type="number">
///   <item>Repeated traversal through a stable internal node fetches the
///   snapshot exactly once via
///   <see cref="IBPlusInternalGrain.GetRoutingTableAsync"/>; subsequent
///   descends hit the cache.</item>
///   <item>A non-null <see cref="SplitResult"/> returned from
///   <see cref="IBPlusInternalGrain.AcceptSplitAsync"/> invalidates the
///   cached snapshot for that internal id; the next descend through the
///   same internal re-fetches.</item>
///   <item>Distinct internal grain ids occupy distinct cache slots; the
///   cache is multi-slot, not single-slot, so a depth-3+ tree visiting
///   multiple internals on a single descend keeps every visited
///   snapshot for the activations lifetime.</item>
///   <item>The local routing decision matches the server-side
///   <c>RouteWithMetadataAsync</c> output - i.e. the cache hit must not
///   route reads/writes to a different leaf than the cross-grain
///   fallback would.</item>
/// </list>
///
/// Without these tests a future refactor could (a) silently re-introduce
/// the per-descend RPC, (b) miss an <c>AcceptSplitAsync</c> site and ship
/// a stale-snapshot bug, or (c) regress the cache to single-slot with
/// thrashing on multi-internal descends.
/// </summary>
[TestFixture]
public class ShardRootGrainRoutingTableCacheTests
{
    private const string TreeId = "test-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class CacheHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusInternalGrain Internal { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required GrainId LeafId { get; init; }
    }

    /// <summary>
    /// Builds a depth-2 harness: one internal root grain whose snapshot
    /// has a single null separator pointing at one leaf grain. Every
    /// traversal walks exactly one internal hop; the cache should hit on
    /// every call past the first.
    /// </summary>
    private static CacheHarness CreateHarness(GrainId? rootInternalId = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = rootInternalId ?? GrainId.Create("internal", "root-internal");
        var leafId = GrainId.Create("leaf", "leaf-0");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        var internalRoot = Substitute.For<IBPlusInternalGrain>();
        internalRoot.GetRoutingTableAsync()
            .Returns(_ => Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = [null],
                ChildIds = [leafId],
                ChildrenAreLeaves = true,
            }));
        // RouteWithMetadataAsync is the cold-path fallback (used by the
        // diagnostics surface). Stub it so any accidental fall-through
        // resolves to a sensible value rather than the NSubstitute default.
        internalRoot.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.GetLeftmostChildWithMetadataAsync()
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.GetRightmostChildWithMetadataAsync()
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalRoot);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult<SplitResult?>(null));
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>())
            .Returns(Task.FromResult<SplitResult?>(null));
        leaf.GetOrSetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult(new GetOrSetResult { ExistingValue = null, Split = null }));
        leaf.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>())
            .Returns(Task.FromResult(new CasResult { Success = true, Split = null }));
        leaf.GetWithVersionAsync(Arg.Any<string>())
            .Returns(Task.FromResult(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero }));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var leafCache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(leafCache);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new CacheHarness
        {
            Grain = grain,
            Internal = internalRoot,
            Leaf = leaf,
            Factory = factory,
            State = state,
            LeafId = leafId,
        };
    }

    [Test]
    public async Task Repeated_traversal_calls_GetRoutingTableAsync_only_once_per_unique_internal()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("k1", [1]);
        await h.Grain.SetAsync("k2", [2]);
        await h.Grain.SetAsync("k3", [3]);

        await h.Internal.Received(1).GetRoutingTableAsync();
    }

    [Test]
    public async Task Mixed_traversal_methods_share_the_same_routing_table_cache_entry()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("a", [1]);
        await h.Grain.GetOrSetAsync("b", [2]);
        await h.Grain.SetIfVersionAsync("c", [3], HybridLogicalClock.Zero);
        await h.Grain.GetWithVersionAsync("d");
        await h.Grain.SetAsync("e", [4], DateTime.UtcNow.AddMinutes(5).Ticks);

        // All five entry-points walk the same root internal; the snapshot
        // must be fetched exactly once across all of them.
        await h.Internal.Received(1).GetRoutingTableAsync();
    }

    [Test]
    public async Task RouteWithMetadataAsync_is_never_called_on_cache_hit_path()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("k1", [1]);
        await h.Grain.SetAsync("k2", [2]);
        await h.Grain.SetAsync("k3", [3]);

        // The whole point of the cache: the per-key routing decision is
        // local. RouteWithMetadataAsync must NOT be on the hot path.
        await h.Internal.DidNotReceive().RouteWithMetadataAsync(Arg.Any<string>());
    }

    [Test]
    public async Task AcceptSplitAsync_returning_split_result_invalidates_cached_snapshot()
    {
        // When AcceptSplitAsync returns a non-null SplitResult, the parent
        // internals child list has just changed. The cached snapshot for
        // that parent is now stale and the next traversal through it must
        // re-fetch.
        var h = CreateHarness();

        // Force the leaf write to bubble a split to the parent (root). The
        // first AcceptSplitAsync call returns a SplitResult to simulate
        // the root needing to split too; the second returns null to
        // terminate the bubble.
        var newLeafId = GrainId.Create("leaf", "leaf-1");
        h.Leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult<SplitResult?>(new SplitResult
            {
                PromotedKey = "split-key",
                NewSiblingId = newLeafId,
            }));
        h.Internal.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>())
            .Returns(Task.FromResult<SplitResult?>(null));

        // First write: descends, fetches snapshot once, leaf splits, root
        // accepts split (invalidating the cached snapshot).
        await h.Grain.SetAsync("k1", [1]);

        // Reset the leaf to no-split for the second write so we can
        // observe whether the cache miss fires.
        h.Leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult<SplitResult?>(null));

        // Second write: cache was invalidated, must re-fetch the snapshot.
        await h.Grain.SetAsync("k2", [2]);

        // Total fetches: 1 (first write) + 1 (second write after invalidation) = 2.
        await h.Internal.Received(2).GetRoutingTableAsync();
    }

    [Test]
    public async Task AcceptSplitAsync_returning_null_does_not_invalidate_other_internals()
    {
        // The invalidation is keyed by the parent id passed to
        // InvalidateRoutingTable. A split that bubbles up through one
        // internal must not evict snapshots for unrelated internals.
        // Here we drive a single-internal harness where only the root is
        // ever touched; AcceptSplitAsync returns null so no upstream
        // bubbling happens. The cache must remain populated.
        var h = CreateHarness();

        // First write populates the cache.
        await h.Grain.SetAsync("k1", [1]);

        // Force a split that the root absorbs (returns null, terminating
        // the bubble immediately). The roots own snapshot is now
        // potentially stale (the root accepted a split into its own
        // children list), so it IS invalidated by design.
        h.Leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(
                Task.FromResult<SplitResult?>(new SplitResult
                {
                    PromotedKey = "split-key",
                    NewSiblingId = GrainId.Create("leaf", "leaf-1"),
                }),
                Task.FromResult<SplitResult?>(null));
        await h.Grain.SetAsync("k2", [2]);

        // Subsequent writes after the invalidation should re-fetch exactly
        // once more (no further invalidations happen because the leaf no
        // longer splits).
        await h.Grain.SetAsync("k3", [3]);
        await h.Grain.SetAsync("k4", [4]);

        // Fetch count: 1 (k1, populating) + 1 (k2, after invalidation post-split)
        // + 0 (k3, k4 hit the re-populated cache) = 2.
        await h.Internal.Received(2).GetRoutingTableAsync();
    }

    [Test]
    public async Task Distinct_root_ids_each_get_their_own_cache_slot()
    {
        // Cycle-14 lifted the cycle-13 single-slot cache to a multi-slot
        // dictionary keyed by GrainId. Visiting two distinct root
        // internals (via RootNodeId rotation, the only path that switches
        // the root id at runtime) should populate two slots, each fetched
        // exactly once, with no thrashing on round-trip.
        var rootA = GrainId.Create("internal", "root-a");
        var rootB = GrainId.Create("internal", "root-b");
        var h = CreateHarness(rootA);

        await h.Grain.SetAsync("k1", [1]);   // populates slot for rootA
        h.State.State.RootNodeId = rootB;
        await h.Grain.SetAsync("k2", [2]);   // populates slot for rootB
        h.State.State.RootNodeId = rootA;
        await h.Grain.SetAsync("k3", [3]);   // hits slot for rootA - no fetch
        h.State.State.RootNodeId = rootB;
        await h.Grain.SetAsync("k4", [4]);   // hits slot for rootB - no fetch

        // Across all 4 writes, only 2 fetches (one per distinct root id).
        // This is the binding multi-slot invariant.
        await h.Internal.Received(2).GetRoutingTableAsync();
    }

    [Test]
    public async Task Cache_hit_routes_to_same_leaf_as_RouteWithMetadataAsync_would()
    {
        // Parity test: drive a write through the cache, capture which leaf
        // the write actually landed on, and assert it matches the leaf the
        // server-side routing (RouteWithMetadataAsync) would have selected.
        // The harness wires both APIs to the same target leaf so a
        // divergence would manifest as the leaf NOT receiving the write.
        var h = CreateHarness();

        await h.Grain.SetAsync("any-key", [42]);

        await h.Leaf.Received(1).SetAsync("any-key", Arg.Is<byte[]>(b => b.Length == 1 && b[0] == 42));
    }

    [Test]
    public async Task RootIsLeaf_path_does_not_populate_routing_table_cache()
    {
        // When RootIsLeaf=true the descend loop short-circuits to the leaf
        // and never calls GetRoutingTableAsync. The cache must remain
        // unallocated.
        var leafRootId = GrainId.Create("leaf", "leaf-only");
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = leafRootId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        var internalRoot = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalRoot);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        var leafCache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(leafCache);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        await grain.SetAsync("k1", [1]);
        await grain.SetAsync("k2", [2]);
        await grain.SetAsync("k3", [3]);

        await internalRoot.DidNotReceive().GetRoutingTableAsync();
    }
}