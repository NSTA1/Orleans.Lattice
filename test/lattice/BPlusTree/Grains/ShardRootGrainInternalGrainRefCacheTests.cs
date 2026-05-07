using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the per-activation single-slot <see cref="IBPlusInternalGrain"/>
/// reference cache invariant in <c>ShardRootGrain.Traversal</c>. The cache
/// stores the most-recently resolved <c>(GrainId, IBPlusInternalGrain)</c>
/// pair on the activation. The invariants the tests lock are:
///
/// <list type="number">
///   <item>Repeat traversal against a tree whose root is invariant (e.g. a
///   stable depth-2 tree) resolves the internal grain reference exactly once
///   via <see cref="IGrainFactory"/>; subsequent calls hit the cache and
///   bypass the factory.</item>
///   <item>Mutating the <c>RootNodeId</c> (e.g. a root split rotating the
///   internal-root id) causes the next traversal to detect the mismatch and
///   refresh the slot.</item>
///   <item>Every traversal entry-point that walks through internal nodes
///   (writes, CAS, GetOrSet, GetWithVersion via <c>TraverseToLeafAsync</c>)
///   shares the same single-slot cache.</item>
/// </list>
///
/// Without these tests a future refactor could silently re-introduce the
/// per-call <c>grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(currentId)</c>
/// allocation on every traversal step. The deep-tree microbench captured
/// this saving as ~592 B/op (point write) and ~11 520 B/op (bulk load) at
/// the time the cache was introduced - but the microbench is not part of
/// CI gates, so a regression would ship invisibly.
/// </summary>
[TestFixture]
public class ShardRootGrainInternalGrainRefCacheTests
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
    }

    /// <summary>
    /// Builds a depth-2 tree harness: root is an internal grain; the internal
    /// grain routes every key (and the leftmost/rightmost probes) directly to
    /// a single leaf grain with <c>childrenAreLeaves=true</c>. This means
    /// every traversal walks exactly one internal-node hop (the root) and
    /// the cache should hit on every call past the first.
    /// </summary>
    private static CacheHarness CreateHarness(GrainId? rootInternalId = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = rootInternalId ?? GrainId.Create("internal", "test-internal");
        var leafId = GrainId.Create("leaf", "test-leaf");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

        var internalRoot = Substitute.For<IBPlusInternalGrain>();
        internalRoot.RouteWithMetadataAsync(Arg.Any<string>())
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.GetLeftmostChildWithMetadataAsync()
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.GetRightmostChildWithMetadataAsync()
            .Returns(Task.FromResult((leafId, true)));
        internalRoot.GetRoutingTableAsync()
            .Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[] { null },
                ChildIds = new[] { leafId },
                ChildrenAreLeaves = true,
            }));
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

        return new CacheHarness { Grain = grain, Internal = internalRoot, Leaf = leaf, Factory = factory, State = state };
    }

    [Test]
    public async Task SetAsync_resolves_internal_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("k1", [1]);
        await h.Grain.SetAsync("k2", [2]);
        await h.Grain.SetAsync("k3", [3]);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task SetAsync_with_expiry_resolves_internal_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();
        var expiresAtTicks = DateTime.UtcNow.AddMinutes(5).Ticks;

        await h.Grain.SetAsync("k1", [1], expiresAtTicks);
        await h.Grain.SetAsync("k2", [2], expiresAtTicks);
        await h.Grain.SetAsync("k3", [3], expiresAtTicks);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task GetOrSetAsync_resolves_internal_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.GetOrSetAsync("k1", [1]);
        await h.Grain.GetOrSetAsync("k2", [2]);
        await h.Grain.GetOrSetAsync("k3", [3]);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task SetIfVersionAsync_resolves_internal_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.SetIfVersionAsync("k1", [1], HybridLogicalClock.Zero);
        await h.Grain.SetIfVersionAsync("k2", [2], HybridLogicalClock.Zero);
        await h.Grain.SetIfVersionAsync("k3", [3], HybridLogicalClock.Zero);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task GetWithVersionAsync_resolves_internal_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.GetWithVersionAsync("k1");
        await h.Grain.GetWithVersionAsync("k2");
        await h.Grain.GetWithVersionAsync("k3");

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task Mixed_traversal_methods_share_the_single_slot_internal_cache()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("a", [1]);
        await h.Grain.GetOrSetAsync("b", [2]);
        await h.Grain.SetIfVersionAsync("c", [3], HybridLogicalClock.Zero);
        await h.Grain.GetWithVersionAsync("d");
        var ttl = DateTime.UtcNow.AddMinutes(5).Ticks;
        await h.Grain.SetAsync("e", [4], ttl);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task RootNodeId_rotation_invalidates_cache_and_re_resolves_internal_grain()
    {
        var rootA = GrainId.Create("internal", "internal-a");
        var rootB = GrainId.Create("internal", "internal-b");
        var h = CreateHarness(rootA);

        await h.Grain.SetAsync("k1", [1]);
        h.State.State.RootNodeId = rootB;
        await h.Grain.SetAsync("k2", [2]);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(rootA);
        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(rootB);
    }

    [Test]
    public async Task RootNodeId_rotation_back_to_previous_internal_resolves_each_root_once()
    {
        // The per-activation routing-table cache (cycle-14) is keyed by the
        // internal grain id, so each unique root is resolved exactly once
        // across its lifetime in the activation. Round-tripping rootA -> rootB
        // -> rootA must therefore resolve rootA only once total: the second
        // visit hits the cached snapshot and bypasses the factory entirely.
        // This is a behaviour upgrade over the cycle-13 single-slot reference
        // cache (which would have re-resolved rootA after the rootB visit
        // evicted its slot); the routing-table cache subsumes the reference
        // cache for the read path.
        var rootA = GrainId.Create("internal", "internal-a");
        var rootB = GrainId.Create("internal", "internal-b");
        var h = CreateHarness(rootA);

        await h.Grain.SetAsync("k1", [1]);
        h.State.State.RootNodeId = rootB;
        await h.Grain.SetAsync("k2", [2]);
        h.State.State.RootNodeId = rootA;
        await h.Grain.SetAsync("k3", [3]);

        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(rootA);
        h.Factory.Received(1).GetGrain<IBPlusInternalGrain>(rootB);
    }

    [Test]
    public async Task RootIsLeaf_path_does_not_touch_internal_cache()
    {
        // When RootIsLeaf=true, traversal short-circuits to the leaf via the
        // RootIsLeaf early-return branch and never enters the internal-grain
        // descend loop. The internal cache must remain untouched - the
        // factory should never be asked for an IBPlusInternalGrain.
        var leafRootId = GrainId.Create("leaf", "leaf-only");
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = leafRootId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

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

        factory.DidNotReceive().GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>());
    }
}