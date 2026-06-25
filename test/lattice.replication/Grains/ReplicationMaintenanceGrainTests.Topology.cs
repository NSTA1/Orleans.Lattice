using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Topology-vs-options divergence coverage for
/// <see cref="ReplicationMaintenanceGrain"/>. Asserts that the
/// per-cadence fall-off-log probe walks
/// <see cref="IReplicationTopology.CurrentPeers"/>, not
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/>, so a
/// host-supplied dynamic topology drives which peers are
/// protected against silent fall-off without having to mirror
/// membership back into options.
/// </summary>
public partial class ReplicationMaintenanceGrainTests
{
    [Test]
    public async Task ProcessNextPhaseAsync_probes_peers_present_only_in_topology()
    {
        // Options has no peers; topology lists "site-b". The probe
        // must walk site-b because topology is the canonical source.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = Array.Empty<string>(),
        };
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts, topology: topology);
        introspection.GetOldestAvailableHlcByOriginAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                ["site-b"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            });

        await grain.ProcessNextPhaseAsync();

        await detector.Received(1).CheckAndTriggerAsync(
            Tree, "site-b", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_peers_present_only_in_options()
    {
        // Options lists "site-b" but the topology is empty. The
        // probe must NOT walk site-b because the topology overrides
        // the options snapshot.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "site-b" },
        };
        var topology = new FakeReplicationTopology(Array.Empty<string>());
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts, topology: topology);

        await grain.ProcessNextPhaseAsync();

        // Empty topology short-circuits the probe before the WAL
        // introspection lookup; the detector is never invoked.
        await introspection.DidNotReceive().GetOldestAvailableHlcByOriginAsync(
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_uses_topology_when_options_and_topology_diverge()
    {
        // Options says {site-b}, topology says {site-c}. Topology
        // wins: only site-c is probed.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = new[] { "site-b" },
        };
        var topology = new FakeReplicationTopology(new[] { "site-c" });
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts, topology: topology);
        introspection.GetOldestAvailableHlcByOriginAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                ["site-b"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
                ["site-c"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            });

        await grain.ProcessNextPhaseAsync();

        await detector.Received(1).CheckAndTriggerAsync(
            Tree, "site-c", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
        await detector.DidNotReceive().CheckAndTriggerAsync(
            Tree, "site-b", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_observes_runtime_topology_add_without_options_change()
    {
        // Start with an empty topology, then EmitAdded("site-b").
        // The next ProcessNextPhaseAsync must walk site-b even
        // though ReplicationPeers in options never changed. Mirrors
        // the doorbell sink's runtime-add test.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicationPeers = Array.Empty<string>(),
        };
        var topology = new FakeReplicationTopology();
        var (grain, _, _, _, detector, introspection, _, _) = Create(opts, topology: topology);
        introspection.GetOldestAvailableHlcByOriginAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                ["site-b"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            });

        topology.EmitAdded("site-b");

        await grain.ProcessNextPhaseAsync();

        await detector.Received(1).CheckAndTriggerAsync(
            Tree, "site-b", Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }
}
