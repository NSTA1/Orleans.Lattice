using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class ReportRoutingTests
{
    [TestCase(null)]
    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(2)]
    public async Task ResolveAsync_matches_canonical_routing_fallback(int? shardCount)
    {
        var factory = Substitute.For<IGrainFactory>();
        var routing = await ReportRouting.ResolveAsync(factory, "tree", new TreeRegistryEntry
        {
            PhysicalTreeId = "physical",
            ShardCount = shardCount,
        }, CancellationToken.None);

        Assert.That(routing.PhysicalTreeId, Is.EqualTo("physical"));
        Assert.That(routing.Map.GetPhysicalShardIndices().Count,
            Is.EqualTo(shardCount is > 0 ? shardCount.Value : LatticeConstants.DefaultShardCount));
        Assert.That(routing.Map.VirtualShardCount, Is.EqualTo(LatticeConstants.DefaultVirtualShardCount));
        Assert.That(factory.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task ResolveAsync_preserves_the_alias_and_map_from_one_row()
    {
        var map = ShardMap.CreateDefault(16, 2);
        var routing = await ReportRouting.ResolveAsync(Substitute.For<IGrainFactory>(),
            "logical", new TreeRegistryEntry { PhysicalTreeId = "copy", ShardMap = map },
            CancellationToken.None);

        Assert.That(routing.PhysicalTreeId, Is.EqualTo("copy"));
        Assert.That(routing.Map, Is.SameAs(map));
    }

    [Test]
    public async Task ResolveAsync_missing_row_uses_logical_identity()
    {
        var routing = await ReportRouting.ResolveAsync(
            Substitute.For<IGrainFactory>(), "tree", null, CancellationToken.None);
        Assert.That(routing.PhysicalTreeId, Is.EqualTo("tree"));
    }

    [TestCase("system")]
    [TestCase("shadow")]
    [TestCase("replication")]
    public async Task ResolveAsync_scoped_and_system_routes_keep_the_forced_entrypoint(string scope)
    {
        var tree = scope == "system" ? LatticeConstants.RegistryTreeId : "logical";
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(tree).Returns(lattice);
        var expected = new RoutingInfo("scoped-copy", ShardMap.CreateDefault(16, 1));
        lattice.GetRoutingAsync(true, Arg.Any<CancellationToken>()).Returns(expected);
        using var shadow = scope == "shadow"
            ? LatticeBootstrapShadowRouteContext.BeginScope(tree, "scoped-copy") : default;
        if (scope == "replication") ReplicationApplyScope.Enter();

        Assert.That(ReportRouting.CanReadEntry(tree), Is.False);
        var actual = await ReportRouting.ResolveAsync(factory, tree, null, CancellationToken.None);

        Assert.That(actual, Is.EqualTo(expected));
        _ = lattice.Received(1).GetRoutingAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public void ResolveAsync_cancellation_precedes_any_dispatch()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => ReportRouting.ResolveAsync(factory, "tree", null, new CancellationToken(true)),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(factory.ReceivedCalls(), Is.Empty);
    }
}
