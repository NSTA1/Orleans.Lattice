using System.Diagnostics;
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
    /// <summary>
    /// Counts doorbell rings observed on the substituted shipper grains.
    /// <para>
    /// NSubstitute's <c>Received()</c> throws when the expectation is not yet
    /// met, so it can never be used as a polling predicate; an interlocked
    /// counter can. Doorbell fan-out is fire-and-forget on the production side
    /// (<c>_ = RingLoopAsync(...)</c>), so the ring lands after
    /// <c>WriteAsync</c> has already returned. Waiting on this counter replaces
    /// the fixed <c>Task.Delay(20)</c> these tests used to open with: a fixed
    /// sleep is unsound in both directions - dead time on an idle machine, and
    /// on a loaded CI worker it can elapse before the ring has been dispatched
    /// at all, which fails the positive assertions and, worse, lets the
    /// <c>DidNotReceive</c> assertions pass for the wrong reason.
    /// </para>
    /// </summary>
    private sealed class DoorbellCounter
    {
        private int _count;

        public void Increment() => Interlocked.Increment(ref _count);

        public int Value => Volatile.Read(ref _count);
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds, failing the test with
    /// <paramref name="because"/> if it never does inside the timeout. Fails
    /// loudly rather than falling through silently, so a barrier that never
    /// opens is reported as itself instead of as a misattributed downstream
    /// assertion failure.
    /// </summary>
    private static async Task WaitUntilAsync(Func<bool> condition, string because, int timeoutMs = 10_000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            if (stopwatch.ElapsedMilliseconds > timeoutMs)
            {
                Assert.Fail($"Timed out after {timeoutMs} ms waiting for {because}.");
            }

            await Task.Delay(5);
        }
    }

    private static (
        ShardedReplogSink Sink,
        IGrainFactory Factory,
        IReplicationShipperGrain[] Shippers,
        DoorbellCounter Doorbells) BuildSink(
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
        var doorbells = new DoorbellCounter();
        var shippers = new IReplicationShipperGrain[peerArr.Length];
        for (var i = 0; i < peerArr.Length; i++)
        {
            shippers[i] = Substitute.For<IReplicationShipperGrain>();
            shippers[i].OnDoorbellAsync(Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    doorbells.Increment();
                    return Task.CompletedTask;
                });
            factory.GetGrain<IReplicationShipperGrain>($"orders/{peerArr[i]}").Returns(shippers[i]);
        }
        var sink = new ShardedReplogSink(
            factory,
            monitor,
            new FakeReplicationTopology(peerArr),
            NullLogger<ShardedReplogSink>.Instance);
        return (sink, factory, shippers, doorbells);
    }

    [Test]
    public async Task WriteAsync_rings_peers_present_only_in_topology()
    {
        // Topology lists "site-b" but options does not - the peer
        // must still receive a doorbell ring because the topology is
        // now the canonical source for membership.
        var (sink, _, shippers, doorbells) = BuildSink(
            topologyPeers: new[] { "site-b" },
            optionsPeers: Array.Empty<string>());

        await sink.WriteAsync("orders", CancellationToken.None);
        await WaitUntilAsync(() => doorbells.Value >= 1,
            "the doorbell ring to reach the topology-only peer's shipper");

        await shippers[0].Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_peers_present_only_in_options()
    {
        // Options lists "site-b" but the topology does not - the
        // doorbell loop must NOT ring "site-b" because the topology
        // is the canonical source. The shipper-for-options-only
        // peer is unreferenced and should never be resolved.
        var (sink, factory, _, _) = BuildSink(
            topologyPeers: Array.Empty<string>(),
            optionsPeers: new[] { "site-b" });

        await sink.WriteAsync("orders", CancellationToken.None);

        // No settle window is needed or wanted here: with an empty topology the
        // production fan-out short-circuits on `peers.Count > 0` inside the
        // awaited WriteAsync, so once it returns the decision has already been
        // made and nothing further can be scheduled. The old Task.Delay(20) was
        // therefore pure dead time that also made the negative claim look
        // timing-dependent when it is in fact deterministic. Assert the stronger
        // form - no shipper at all was resolved, not merely not site-b's - which
        // keeps the original intent and additionally catches a fan-out that
        // rings some other peer it invented.
        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>("orders/site-b");
    }

    [Test]
    public async Task WriteAsync_uses_topology_when_options_and_topology_diverge()
    {
        // Options says {site-b}, topology says {site-c}. The topology
        // wins: only site-c gets a doorbell ring.
        var (sink, factory, shippers, doorbells) = BuildSink(
            topologyPeers: new[] { "site-c" },
            optionsPeers: new[] { "site-b" });

        await sink.WriteAsync("orders", CancellationToken.None);

        // Waiting for site-c's ring is also the positive control that sequences
        // the negative claim: the fan-out has demonstrably run by the time the
        // "site-b was never resolved" assertion is evaluated.
        await WaitUntilAsync(() => doorbells.Value >= 1,
            "the doorbell ring to reach site-c, the peer the topology names");

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
        var doorbells = new DoorbellCounter();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                doorbells.Increment();
                return Task.CompletedTask;
            });
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipper);
        var sink = new ShardedReplogSink(
            factory,
            monitor,
            topology,
            NullLogger<ShardedReplogSink>.Instance);

        topology.EmitAdded("site-b");

        await sink.WriteAsync("orders", CancellationToken.None);
        await WaitUntilAsync(() => doorbells.Value >= 1,
            "the doorbell ring to reach the peer added at runtime via EmitAdded");

        await shipper.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
