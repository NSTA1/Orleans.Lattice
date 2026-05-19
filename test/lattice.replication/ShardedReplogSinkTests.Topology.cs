using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Topology-vs-options divergence coverage for
/// <see cref="ShardedReplogSink"/>. Asserts that the doorbell
/// fan-out follows <see cref="IReplicationTopology.CurrentPeers"/>,
/// not <see cref="LatticeReplicationOptions.ReplicationPeers"/>,
/// so a host-supplied dynamic topology drives doorbell rings
/// without having to mirror membership back into options.
/// </summary>
public partial class ShardedReplogSinkTests
{
    private static (
        ShardedReplogSink Sink,
        IGrainFactory Factory,
        IReplicationShipperGrain[] Shippers) BuildSink(
            IEnumerable<string> topologyPeers,
            IEnumerable<string>? optionsPeers,
            bool doorbellEnabled = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var resolved = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplogPartitions = 1,
            ReplicationPeers = optionsPeers?.ToArray(),
            ShipDoorbellEnabled = doorbellEnabled,
        };
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        var peerArr = topologyPeers.ToArray();
        var shippers = new IReplicationShipperGrain[peerArr.Length];
        for (var i = 0; i < peerArr.Length; i++)
        {
            shippers[i] = Substitute.For<IReplicationShipperGrain>();
            factory.GetGrain<IReplicationShipperGrain>($"orders/{peerArr[i]}").Returns(shippers[i]);
        }
        var sink = new ShardedReplogSink(
            factory,
            monitor,
            new FakeReplicationTopology(peerArr),
            new LocalVectorClockCache(factory),
            NullLogger<ShardedReplogSink>.Instance);
        return (sink, factory, shippers);
    }

    [Test]
    public async Task WriteAsync_rings_peers_present_only_in_topology()
    {
        // Topology lists "site-b" but options does not - the peer
        // must still receive a doorbell ring because the topology is
        // now the canonical source for membership.
        var (sink, _, shippers) = BuildSink(
            topologyPeers: new[] { "site-b" },
            optionsPeers: Array.Empty<string>());

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Yield();
        await Task.Delay(20);

        await shippers[0].Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_peers_present_only_in_options()
    {
        // Options lists "site-b" but the topology does not - the
        // doorbell loop must NOT ring "site-b" because the topology
        // is the canonical source. The shipper-for-options-only
        // peer is unreferenced and should never be resolved.
        var (sink, factory, _) = BuildSink(
            topologyPeers: Array.Empty<string>(),
            optionsPeers: new[] { "site-b" });

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Yield();
        await Task.Delay(20);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>("orders/site-b");
    }

    [Test]
    public async Task WriteAsync_uses_topology_when_options_and_topology_diverge()
    {
        // Options says {site-b}, topology says {site-c}. The topology
        // wins: only site-c gets a doorbell ring.
        var (sink, factory, shippers) = BuildSink(
            topologyPeers: new[] { "site-c" },
            optionsPeers: new[] { "site-b" });

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Yield();
        await Task.Delay(20);

        await shippers[0].Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>("orders/site-b");
    }

    [Test]
    public async Task WriteAsync_observes_runtime_topology_add_without_options_change()
    {
        // Start with an empty topology; runtime EmitAdded brings in
        // a new peer; the next WriteAsync must ring that peer's
        // doorbell even though ReplicationPeers in options never
        // changed.
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var resolved = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplogPartitions = 1,
            ReplicationPeers = Array.Empty<string>(),
            ShipDoorbellEnabled = true,
        };
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);

        var topology = new FakeReplicationTopology();
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipper);
        var sink = new ShardedReplogSink(
            factory,
            monitor,
            topology,
            new LocalVectorClockCache(factory),
            NullLogger<ShardedReplogSink>.Instance);

        topology.EmitAdded("site-b");

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Yield();
        await Task.Delay(20);

        await shipper.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
