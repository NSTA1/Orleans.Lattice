using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos regression for the cluster-wide split admission gate
/// (<see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>). The whole point
/// of the heartbeat + time-to-live design is that a silo crashing mid-split
/// cannot wedge splitting cluster-wide: the crashed tree stops refreshing its
/// footprint, so its share lapses at expiry and is reclaimed by the next call,
/// letting splitting resume on its own. This fixture models a crash by reporting
/// footprints that saturate the ceiling and are then never refreshed, advancing
/// past the ttl window and proving a fresh grant succeeds.
/// </summary>
[TestFixture]
[Category("Chaos")]
public class ClusterSplitConcurrencyChaosTests
{
    private static ClusterSplitConcurrencyGrain CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cluster-split", "0"));
        return new ClusterSplitConcurrencyGrain(
            context,
            new FakePersistentState<ClusterSplitConcurrencyState>(),
            new LoggerFactory().CreateLogger<ClusterSplitConcurrencyGrain>());
    }

    [Test]
    public async Task Abandoned_footprints_are_reclaimed_within_the_ttl_window()
    {
        var grain = CreateGrain();
        var ttl = TimeSpan.FromMilliseconds(150);

        // Saturate the cluster ceiling with footprints that are never refreshed -
        // this models two silos that crashed mid-split.
        var a = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 1, clusterCap: 2, ttl);
        var b = await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 2, ttl);
        Assert.That(a, Is.EqualTo(1));
        Assert.That(b, Is.EqualTo(1));

        // While the footprints are live, the gate correctly refuses further splits.
        var blocked = await grain.AcquireSlotsAsync("tree-c", currentInFlight: 0, desiredNew: 1, clusterCap: 2, ttl);
        Assert.That(blocked, Is.EqualTo(0), "the ceiling must hold while the abandoned footprints are still within their ttl");

        // Advance past the ttl window without any further heartbeats.
        await Task.Delay(ttl + TimeSpan.FromMilliseconds(200));

        // The next call must reconcile the expired footprints out and admit a
        // split, so splitting resumes cluster-wide on its own.
        var recovered = await grain.AcquireSlotsAsync("tree-c", currentInFlight: 0, desiredNew: 1, clusterCap: 2, ttl);
        Assert.That(recovered, Is.EqualTo(1),
            "expired footprints must be reclaimed so a crashed split cannot wedge splitting cluster-wide");
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(1),
            "only the freshly granted split should remain live after reclamation");
    }
}
