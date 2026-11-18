using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

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
        var shards = StubShardRoots(grainFactory, "orders", 4);

        var fingerprint = await grain.ComputeFingerprintAsync("orders", CancellationToken.None);

        Assert.That(fingerprint, Is.Not.Null);
        await shards[3].Received(1).GetShardProjectionDigestAsync(Arg.Any<CancellationToken>());
        await shards[2].Received(1).GetShardProjectionDigestAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ComputeFingerprintAsync_reads_shard_roots_of_the_physical_tree_without_the_router_digest_hop()
    {
        // The digests are read straight off the aliased physical tree's shard roots
        // the routing named; the router's per-shard digest forwarder (one extra
        // router hop and forced registry read per shard) is never called.
        var (grain, _, _, grainFactory) = CreateGrain();
        var tree = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>("orders").Returns(tree);
        var map = new ShardMap { Slots = [0, 1, 0, 1], Version = 1 };
        tree.GetRoutingAsync(true, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("orders-copy", map)));
        var shards = StubShardRoots(grainFactory, "orders-copy", 2);

        var fingerprint = await grain.ComputeFingerprintAsync("orders", CancellationToken.None);

        Assert.That(fingerprint, Is.EqualTo(ExpectedFingerprint(2)));
        await shards[0].Received(1).GetShardProjectionDigestAsync(Arg.Any<CancellationToken>());
        await shards[1].Received(1).GetShardProjectionDigestAsync(Arg.Any<CancellationToken>());
        await tree.DidNotReceiveWithAnyArgs().GetLeafProjectionDigestAsync(default, default);
        await tree.Received(1).GetRoutingAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ComputeFingerprintAsync_returns_null_when_a_shard_digest_faults()
    {
        // A tree with digest maintenance disabled faults in the leaf and internal
        // grains; the fingerprint must surface that as ungateable (null).
        var (grain, _, _, grainFactory) = CreateGrain();
        var tree = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>("orders").Returns(tree);
        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        tree.GetRoutingAsync(true, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("orders", map)));
        var shards = StubShardRoots(grainFactory, "orders", 2);
        shards[1].GetShardProjectionDigestAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LeafProjectionDigest>(new InvalidOperationException("digest disabled")));

        var fingerprint = await grain.ComputeFingerprintAsync("orders", CancellationToken.None);

        Assert.That(fingerprint, Is.Null);
    }

    [Test]
    public async Task ComputeFingerprintAsync_returns_null_for_a_system_tree_without_any_grain_call()
    {
        var (grain, _, _, grainFactory) = CreateGrain();

        var fingerprint = await grain.ComputeFingerprintAsync(LatticeConstants.SystemTreePrefix + "trees", CancellationToken.None);

        Assert.That(fingerprint, Is.Null);
        Assert.That(grainFactory.ReceivedCalls(), Is.Empty);
    }

    private static IShardRootGrain[] StubShardRoots(IGrainFactory grainFactory, string physicalTreeId, int count)
    {
        var shards = new IShardRootGrain[count];
        for (var i = 0; i < count; i++)
        {
            var hash = (byte)i;
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetShardProjectionDigestAsync(Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new LeafProjectionDigest { Hash = [hash], Version = 1 }));
            grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{i}").Returns(shard);
            shards[i] = shard;
        }

        return shards;
    }

    // The fold the previous per-shard ILattice path produced, so persisted
    // baselines keep matching after the read moved to the shard roots.
    private static byte[] ExpectedFingerprint(int shardCount)
    {
        var hash = new System.IO.Hashing.XxHash128();
        var scratch = new byte[4];
        for (var i = 0; i < shardCount; i++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(scratch, i);
            hash.Append(scratch);
            hash.Append([(byte)i]);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(scratch, 1);
            hash.Append(scratch);
        }

        return hash.GetHashAndReset();
    }
}
