using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A leaf row rebuilt from its WAL record by the projection stores an own
/// write with a null origin, exactly as a live own write stores it, and keeps
/// a peer's id (issue #5524). Before the fix the projection copied the record's
/// own-write stamp - the local cluster id on a replicated tree, an empty string
/// on a single-cluster host - so one row had two representations depending on
/// whether it had been rebuilt from the WAL.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainProjectionOriginTests
{
    private const string TreeId = "tree-projection-origin";

    private sealed class FixedOrigin(string origin) : ILatticeOriginClusterIdResolver
    {
        public string Resolve(string treeId) => origin;
    }

    private static BPlusLeafGrain CreateLeaf(ILatticeOriginClusterIdResolver origins)
    {
        var services = new ServiceCollection().BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = TreeId } };

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 1_000_000,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            origins);
    }

    private static LatticeMutation Set(string key, string? origin) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
        OriginClusterId = origin,
    };

    private static string? RowOrigin(BPlusLeafGrain leaf, string key)
    {
        Assert.That(leaf.CacheForTest.TryPeekRow(key, out var row, out _), Is.True, $"row '{key}' was not projected");
        return row.OriginClusterId;
    }

    [Test]
    public void A_record_stamped_with_the_local_cluster_id_projects_to_a_null_origin()
    {
        var leaf = CreateLeaf(new FixedOrigin("west"));

        ((ILeafProjection)leaf).Apply(Set("own", "west"));
        ((ILeafProjection)leaf).Apply(Set("peer", "east"));

        Assert.Multiple(() =>
        {
            Assert.That(RowOrigin(leaf, "own"), Is.Null);
            Assert.That(RowOrigin(leaf, "peer"), Is.EqualTo("east"));
        });
    }

    [Test]
    public void A_single_cluster_record_stamped_empty_projects_to_a_null_origin()
    {
        var leaf = CreateLeaf(TestOriginClusterIdResolver.Default());

        ((ILeafProjection)leaf).Apply(Set("own", string.Empty));

        Assert.That(RowOrigin(leaf, "own"), Is.Null);
    }
}