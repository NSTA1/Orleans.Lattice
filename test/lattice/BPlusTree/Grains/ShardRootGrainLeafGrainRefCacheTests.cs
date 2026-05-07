using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the per-activation single-slot <see cref="IBPlusLeafGrain"/>
/// reference cache invariant in <c>ShardRootGrain.Traversal</c>. The cache
/// stores the most-recently resolved <c>(GrainId, IBPlusLeafGrain)</c> pair
/// on the activation. The invariants the tests lock are:
///
/// <list type="number">
///   <item>Repeat traversal against the same <c>RootNodeId</c> resolves the
///   leaf grain reference exactly once via <see cref="IGrainFactory"/>; subsequent
///   calls hit the cache and bypass the factory.</item>
///   <item>Mutating the <c>RootNodeId</c> (e.g. a root split rotating the
///   leaf id) causes the next traversal to detect the mismatch and refresh
///   the slot, so reference resolution remains correct on rotation.</item>
///   <item>Every traversal entry-point that goes via the leaf grain (writes,
///   CAS, GetOrSet, GetWithVersion) shares the same single-slot cache —
///   mixing methods against the same leaf id resolves the reference once,
///   not once per method.</item>
/// </list>
///
/// Without these tests a future refactor could silently re-introduce the
/// per-call <c>grainFactory.GetGrain&lt;IBPlusLeafGrain&gt;(leafId)</c>
/// allocation (~744 B/op observed; see PR introducing the cache). The
/// microbench is not part of CI gates, so the regression would ship invisibly.
/// </summary>
[TestFixture]
public class ShardRootGrainLeafGrainRefCacheTests
{
    private const string TreeId = "test-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class CacheHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static CacheHarness CreateHarness(GrainId? rootLeafId = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootLeafId ?? GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), factory: factory);

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

        // The cycle-6 leaf-cache (ILeafCacheGrain) lives on the same partial
        // class but is exercised by reads-without-version. None of the tests
        // in this file call GetAsync/ExistsAsync, but stub the factory call
        // so an unrelated activation path stays safe.
        var leafCache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(leafCache);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new CacheHarness { Grain = grain, Leaf = leaf, Factory = factory, State = state };
    }

    [Test]
    public async Task SetAsync_resolves_leaf_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.SetAsync("k1", [1]);
        await h.Grain.SetAsync("k2", [2]);
        await h.Grain.SetAsync("k3", [3]);

        // Without the cache: factory.GetGrain<IBPlusLeafGrain>(...) would be
        // called 3 times (once per write). With the cache: only the first
        // call materialises the reference; the next two are cache hits.
        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task SetAsync_with_expiry_resolves_leaf_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();
        var expiresAtTicks = DateTime.UtcNow.AddMinutes(5).Ticks;

        await h.Grain.SetAsync("k1", [1], expiresAtTicks);
        await h.Grain.SetAsync("k2", [2], expiresAtTicks);
        await h.Grain.SetAsync("k3", [3], expiresAtTicks);

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task GetOrSetAsync_resolves_leaf_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.GetOrSetAsync("k1", [1]);
        await h.Grain.GetOrSetAsync("k2", [2]);
        await h.Grain.GetOrSetAsync("k3", [3]);

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task SetIfVersionAsync_resolves_leaf_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.SetIfVersionAsync("k1", [1], HybridLogicalClock.Zero);
        await h.Grain.SetIfVersionAsync("k2", [2], HybridLogicalClock.Zero);
        await h.Grain.SetIfVersionAsync("k3", [3], HybridLogicalClock.Zero);

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task GetWithVersionAsync_resolves_leaf_grain_reference_once_for_repeated_calls()
    {
        var h = CreateHarness();

        await h.Grain.GetWithVersionAsync("k1");
        await h.Grain.GetWithVersionAsync("k2");
        await h.Grain.GetWithVersionAsync("k3");

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task Mixed_traversal_methods_share_the_single_slot_leaf_cache()
    {
        // The cache slot is shared across every traversal helper that uses
        // ResolveLeafGrainSlow - writes, CAS, GetOrSet, GetWithVersion. When
        // the same RootNodeId is in play, mixing methods must still resolve
        // the leaf reference exactly once.
        var h = CreateHarness();

        await h.Grain.SetAsync("a", [1]);
        await h.Grain.GetOrSetAsync("b", [2]);
        await h.Grain.SetIfVersionAsync("c", [3], HybridLogicalClock.Zero);
        await h.Grain.GetWithVersionAsync("d");
        var ttl = DateTime.UtcNow.AddMinutes(5).Ticks;
        await h.Grain.SetAsync("e", [4], ttl);

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }

    [Test]
    public async Task RootNodeId_rotation_invalidates_cache_and_re_resolves_leaf_grain()
    {
        // The cache key is GrainId-equality. If RootNodeId rotates (e.g. a
        // root-split promoted the original leaf to internal and installed a
        // new leaf), the next traversal MUST detect the mismatch and call
        // factory.GetGrain again. If it didn't, every traversal after a
        // root-split would silently route to the stale (now-internal) grain.
        var leafA = GrainId.Create("leaf", "leaf-a");
        var leafB = GrainId.Create("leaf", "leaf-b");
        var h = CreateHarness(leafA);

        await h.Grain.SetAsync("k1", [1]);
        // Simulate a leaf-id rotation. The production code mutates RootNodeId
        // on root-split lifecycle hooks; here we just update the persistent
        // state directly to drive the cache-key mismatch path.
        h.State.State.RootNodeId = leafB;
        await h.Grain.SetAsync("k2", [2]);

        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(leafA);
        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(leafB);
    }

    [Test]
    public async Task RootNodeId_rotation_back_to_previous_leaf_re_resolves_after_eviction()
    {
        // The cache is single-slot: the slot is overwritten on every miss.
        // Round-tripping leafA -> leafB -> leafA must therefore resolve leafA
        // twice (initial materialisation, then again after the leafB miss
        // evicted it). This pins the eviction shape: there is no LRU or
        // multi-slot cache hiding behind the field, only the most-recent
        // resolution is retained.
        var leafA = GrainId.Create("leaf", "leaf-a");
        var leafB = GrainId.Create("leaf", "leaf-b");
        var h = CreateHarness(leafA);

        await h.Grain.SetAsync("k1", [1]);    // miss -> resolve leafA
        h.State.State.RootNodeId = leafB;
        await h.Grain.SetAsync("k2", [2]);    // miss -> resolve leafB (evicts leafA)
        h.State.State.RootNodeId = leafA;
        await h.Grain.SetAsync("k3", [3]);    // miss -> resolve leafA again

        h.Factory.Received(2).GetGrain<IBPlusLeafGrain>(leafA);
        h.Factory.Received(1).GetGrain<IBPlusLeafGrain>(leafB);
    }
}
