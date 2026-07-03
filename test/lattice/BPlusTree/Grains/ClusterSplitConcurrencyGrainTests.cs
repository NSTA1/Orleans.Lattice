using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class ClusterSplitConcurrencyGrainTests
{
    private static ClusterSplitConcurrencyGrain CreateGrain(
        FakePersistentState<ClusterSplitConcurrencyState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cluster-split", "0"));
        state ??= new FakePersistentState<ClusterSplitConcurrencyState>();
        return new ClusterSplitConcurrencyGrain(
            context, state,
            new LoggerFactory().CreateLogger<ClusterSplitConcurrencyGrain>());
    }

    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(5);

    [Test]
    public async Task AcquireSlots_grants_new_splits_up_to_the_cluster_cap()
    {
        var grain = CreateGrain();

        var a = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 1, clusterCap: 2, Ttl);
        var b = await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 2, Ttl);
        var c = await grain.AcquireSlotsAsync("tree-c", currentInFlight: 0, desiredNew: 1, clusterCap: 2, Ttl);

        Assert.That(a, Is.EqualTo(1));
        Assert.That(b, Is.EqualTo(1));
        Assert.That(c, Is.EqualTo(0), "the third tree is denied once the cluster ceiling is reached");
    }

    [Test]
    public async Task AcquireSlots_grants_up_to_desired_when_headroom_allows()
    {
        var grain = CreateGrain();

        var granted = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 3, clusterCap: 2, Ttl);

        Assert.That(granted, Is.EqualTo(2), "a single call is capped at the remaining cluster headroom");
    }

    [Test]
    public async Task AcquireSlots_counts_the_callers_own_in_flight_drains()
    {
        var grain = CreateGrain();

        // The tree already has 2 splits draining and wants a third; the cluster
        // cap is 2, so its own drains already exhaust the ceiling.
        var granted = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 2, desiredNew: 1, clusterCap: 2, Ttl);

        Assert.That(granted, Is.EqualTo(0));
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(2), "reported drains still count toward the cluster total");
    }

    [Test]
    public async Task AcquireSlots_reporting_zero_in_flight_clears_the_trees_footprint()
    {
        var grain = CreateGrain();

        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: 4, Ttl);
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(2));

        // A later pass where the tree's splits have all completed reports zero
        // in-flight and requests nothing; its footprint must drop out entirely.
        var granted = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 0, clusterCap: 4, Ttl);

        Assert.That(granted, Is.EqualTo(0));
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(0), "a tree that reports no in-flight splits frees its cluster share");
    }

    [TestCase(0)]
    [TestCase(-1)]
    public async Task AcquireSlots_grants_nothing_for_non_positive_cap(int cap)
    {
        var grain = CreateGrain();
        var granted = await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: cap, Ttl);
        Assert.That(granted, Is.EqualTo(0));
    }

    [Test]
    public void AcquireSlots_rejects_a_null_tree_id()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.AcquireSlotsAsync(null!, 0, 1, 2, Ttl),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task GetClusterInFlight_sums_live_footprints_across_trees()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(0));

        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: 8, Ttl);
        await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 8, Ttl);

        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(3));
    }

    [Test]
    public async Task GetClusterInFlight_excludes_expired_footprints()
    {
        var grain = CreateGrain();
        // Report with an already-elapsed ttl so the footprint is stale at once.
        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: 8, TimeSpan.FromMilliseconds(-1));
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task AcquireSlots_reclaims_an_expired_footprint_so_a_new_grant_succeeds()
    {
        var grain = CreateGrain();

        // Saturate the single-slot ceiling with a footprint that is already stale.
        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 1, clusterCap: 1, TimeSpan.FromMilliseconds(-1));

        // The next call must reconcile the expired footprint out and admit a split.
        var granted = await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 1, Ttl);

        Assert.That(granted, Is.EqualTo(1), "an expired footprint must not permanently consume the slot");
        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(1));
    }
}
