using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the structural topology a capture records: it follows the
/// tree's routing map rather than a count of shards, so it digests exactly the
/// physical shards the map names - by index, gaps included - and records the
/// map's own virtual slot count.
/// </summary>
public sealed class LatticeBackupCaptureServiceTopologyTests
{
    private static ShardMap FoldedMap()
    {
        // Physical shards {0, 2, 3} over 64 slots, shard 1's slots folded into shard 0.
        var slots = new int[64];
        for (var i = 0; i < slots.Length; i++)
        {
            var owner = i % 4;
            slots[i] = owner == 1 ? 0 : owner;
        }

        return new ShardMap { Slots = slots, Version = 3 };
    }

    private static ILattice LatticeOver(ShardMap map)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("folded", map)));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("folded", map)));
        lattice.GetLeafProjectionDigestForRangeAsync(
                Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new LeafProjectionDigest
            {
                Hash = [(byte)call.ArgAt<int>(0)],
                Version = LeafProjectionDigest.CurrentVersion,
            }));

        // The digest read refuses an index the map does not hold, as the real
        // LatticeGrain does.
        lattice.GetLeafProjectionDigestForRangeAsync(
                1, Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new ArgumentOutOfRangeException("shardIndex"));
        return lattice;
    }

    [Test]
    public async Task BuildTopologyAsync_digests_the_physical_shards_the_routing_map_names()
    {
        var lattice = LatticeOver(FoldedMap());

        var topology = await LatticeBackupCaptureService.BuildTopologyAsync(lattice, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(topology.ShardCount, Is.EqualTo(3));
            Assert.That(topology.ShardRootDigests, Is.EqualTo(new[] { "00", "02", "03" }));
        });
        await lattice.DidNotReceive().GetLeafProjectionDigestForRangeAsync(
            1, Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BuildTopologyAsync_records_the_virtual_slot_count_of_the_routing_map()
    {
        var lattice = LatticeOver(FoldedMap());

        var topology = await LatticeBackupCaptureService.BuildTopologyAsync(lattice, null, null, CancellationToken.None);

        Assert.That(topology.VirtualShardCount, Is.EqualTo(64));
    }

    [Test]
    public async Task BuildTopologyAsync_reads_a_refreshed_routing_map()
    {
        var lattice = LatticeOver(FoldedMap());

        await LatticeBackupCaptureService.BuildTopologyAsync(lattice, null, null, CancellationToken.None);

        await lattice.Received().GetRoutingAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BuildTopologyAsync_names_a_digest_placeholder_by_its_physical_shard_index()
    {
        var lattice = LatticeOver(FoldedMap());
        lattice.GetLeafProjectionDigestForRangeAsync(
                3, Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("digest maintenance disabled"));

        var topology = await LatticeBackupCaptureService.BuildTopologyAsync(lattice, null, null, CancellationToken.None);

        Assert.That(topology.ShardRootDigests, Is.EqualTo(new[] { "00", "02", "nodigest-3" }));
    }
}
