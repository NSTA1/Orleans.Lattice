using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TagIndexReconcileGrainTests
{
    [Test]
    public async Task ComputeFingerprintAsync_folds_every_shard_of_the_current_map_not_the_cached_one()
    {
        // Issue #4180: the tree's stateless worker caches routing per activation and
        // a reshard never invalidates it. A fingerprint over the cached map leaves out
        // a shard the grow added, so a change landing there would not move it and the
        // digest gate would skip the tree.
        var (grain, _, _, grainFactory) = CreateGrain();
        var tree = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>("orders").Returns(tree);
        var cached = new ShardMap { Slots = [0, 1, 0, 1], Version = 1 };
        var current = new ShardMap { Slots = [0, 1, 2, 3], Version = 2 };
        tree.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("orders", cached)));
        tree.GetRoutingAsync(true, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("orders", current)));
        tree.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new LeafProjectionDigest { Hash = [(byte)call.Arg<int>()], Version = 1 }));

        var fingerprint = await grain.ComputeFingerprintAsync("orders", CancellationToken.None);

        Assert.That(fingerprint, Is.Not.Null);
        await tree.Received(1).GetLeafProjectionDigestAsync(3, Arg.Any<CancellationToken>());
        await tree.Received(1).GetLeafProjectionDigestAsync(2, Arg.Any<CancellationToken>());
    }
}
