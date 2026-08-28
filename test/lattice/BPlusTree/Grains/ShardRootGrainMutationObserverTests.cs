using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="IMutationObserver"/> hook invoked from the
/// shard-scoped range-delete path on <see cref="ShardRootGrain"/>.
/// </summary>
[TestFixture]
public class ShardRootGrainMutationObserverTests
{
    private const string TreeId = "obs-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required RecordingMutationObserver Observer { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
    }

    private static Harness CreateHarness(int leafDeletedCount = 3)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "obs-leaf");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = leafDeletedCount, PastRange = true }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var resolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var observer = new RecordingMutationObserver();
        var grain = new ShardRootGrain(
            context, state, factory, resolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.With(observer));

        return new Harness { Grain = grain, Observer = observer, Leaf = leaf };
    }

    [Test]
    public async Task DeleteRangeAsync_publishes_single_range_mutation()
    {
        var h = CreateHarness(leafDeletedCount: 5);

        var deleted = await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(deleted, Is.EqualTo(5));
        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        var m = h.Observer.Mutations[0];
        Assert.That(m.Kind, Is.EqualTo(MutationKind.DeleteRange));
        Assert.That(m.TreeId, Is.EqualTo(TreeId));
        Assert.That(m.Key, Is.EqualTo("a"));
        Assert.That(m.EndExclusiveKey, Is.EqualTo("z"));
        Assert.That(m.IsTombstone, Is.True);
        Assert.That(m.Value, Is.Null);
    }

    [Test]
    public async Task DeleteRangeAsync_with_predicate_publishes_aggregated_matched_keys()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "pred-obs-leaf");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var predicate = LatticePredicateNode.Member("Score");

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>())
            .Returns(Task.FromResult(new RangeDeleteResult
            {
                Deleted = 2,
                PastRange = true,
                MatchedKeys = new[] { "a", "c" },
            }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());

        var resolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var observer = new RecordingMutationObserver();
        var grain = new ShardRootGrain(
            context, state, factory, resolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.With(observer));

        var deleted = await grain.DeleteRangeAsync("a", "z", predicate);

        Assert.That(deleted, Is.EqualTo(2));
        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        var m = observer.Mutations[0];
        Assert.That(m.Kind, Is.EqualTo(MutationKind.DeleteRange));
        Assert.That(m.MatchedKeys, Is.EqualTo(new[] { "a", "c" }),
            "predicate-filtered range delete must publish the aggregated matched key set");
    }

    [Test]
    public async Task DeleteRangeAsync_without_predicate_publishes_null_matched_keys()
    {
        var h = CreateHarness(leafDeletedCount: 4);

        await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].MatchedKeys, Is.Null,
            "unconditional range delete must not carry an explicit matched key set");
    }

    [Test]
    public async Task DeleteRangeAsync_publishes_even_when_zero_keys_matched()
    {
        var h = CreateHarness(leafDeletedCount: 0);

        var deleted = await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(deleted, Is.Zero);
        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].Kind, Is.EqualTo(MutationKind.DeleteRange));
    }

    [Test]
    public async Task DeleteRangeAsync_skips_publish_when_no_observers_registered()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "no-obs-leaf");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 1, PastRange = true }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());

        var resolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var grain = new ShardRootGrain(
            context, state, factory, resolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        // Should succeed with no observers; no way to assert "no publish" except
        // that the operation returns without throwing.
        var deleted = await grain.DeleteRangeAsync("a", "z");
        Assert.That(deleted, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_OriginClusterId_from_ambient_context()
    {
        var h = CreateHarness(leafDeletedCount: 2);
        try
        {
            using (LatticeOriginContext.With("cluster-peer"))
            {
                await h.Grain.DeleteRangeAsync("a", "z");
            }

            Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
            Assert.That(h.Observer.Mutations[0].Kind, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(h.Observer.Mutations[0].OriginClusterId, Is.EqualTo("cluster-peer"));
        }
        finally
        {
            LatticeOriginContext.Current = null;
        }
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_null_origin_when_context_unset()
    {
        var h = CreateHarness(leafDeletedCount: 1);
        LatticeOriginContext.Current = null;

        await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].OriginClusterId, Is.Null);
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_VectorClock_from_ambient_context()
    {
        var h = CreateHarness(leafDeletedCount: 2);
        try
        {
            var vc = new VersionVector();
            vc.Tick("cluster-peer");

            using (LatticeVectorClockContext.With(vc))
            {
                await h.Grain.DeleteRangeAsync("a", "z");
            }

            Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
            Assert.That(h.Observer.Mutations[0].Kind, Is.EqualTo(MutationKind.DeleteRange));
            VectorClockAssert.SameFrontier(h.Observer.Mutations[0].VectorClock, vc);
        }
        finally
        {
            LatticeVectorClockContext.Current = null;
        }
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_null_VectorClock_when_context_unset()
    {
        var h = CreateHarness(leafDeletedCount: 1);
        LatticeVectorClockContext.Current = null;

        await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].VectorClock, Is.Null);
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_User_category_outside_maintenance_scope()
    {
        var h = CreateHarness(leafDeletedCount: 1);

        await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].Category, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_Maintenance_category_inside_maintenance_scope()
    {
        var h = CreateHarness(leafDeletedCount: 1);

        using (LatticeMaintenanceContext.BeginScope())
        {
            await h.Grain.DeleteRangeAsync("a", "z");
        }

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].Category, Is.EqualTo(MutationCategory.Maintenance));
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_Delta_from_ambient_context()
    {
        var h = CreateHarness(leafDeletedCount: 2);
        var payload = new byte[] { 0x10, 0x20, 0x30 };
        try
        {
            using (LatticeDeltaContext.With(payload))
            {
                await h.Grain.DeleteRangeAsync("a", "z");
            }

            Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
            var m = h.Observer.Mutations[0];
            Assert.That(m.Kind, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(m.Delta, Is.EqualTo(payload));
        }
        finally
        {
            LatticeDeltaContext.Current = null;
        }
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_null_delta_when_context_unset()
    {
        var h = CreateHarness(leafDeletedCount: 1);
        LatticeDeltaContext.Current = null;

        await h.Grain.DeleteRangeAsync("a", "z");

        Assert.That(h.Observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(h.Observer.Mutations[0].Delta, Is.Null);
    }
}
