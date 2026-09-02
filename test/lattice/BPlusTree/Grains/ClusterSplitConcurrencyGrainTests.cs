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

    // --- Readable split-activity source (the no-ceiling heartbeat path) -------

    [Test]
    public async Task ReportInFlight_publishes_a_footprint_without_granting_anything()
    {
        var grain = CreateGrain();

        await grain.ReportInFlightAsync("tree-a", inFlight: 2, Ttl);

        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(2),
            "a pure heartbeat still makes the tree's splits visible cluster-wide");
    }

    [Test]
    public async Task ReportInFlight_replaces_the_trees_previous_footprint()
    {
        var grain = CreateGrain();

        await grain.ReportInFlightAsync("tree-a", inFlight: 3, Ttl);
        await grain.ReportInFlightAsync("tree-a", inFlight: 1, Ttl);

        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(1),
            "successive heartbeats replace rather than accumulate");
    }

    [Test]
    public async Task ReportInFlight_zero_clears_the_trees_footprint()
    {
        var grain = CreateGrain();
        await grain.ReportInFlightAsync("tree-a", inFlight: 2, Ttl);

        await grain.ReportInFlightAsync("tree-a", inFlight: 0, Ttl);

        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(0),
            "the clearing heartbeat releases the tree's share as soon as its splits finish");
    }

    [Test]
    public async Task ReportInFlight_does_not_deny_a_concurrent_admission()
    {
        var grain = CreateGrain();

        // A heartbeat is not an admission request, so it neither grants nor
        // denies: the capped tree's own ceiling is what decides.
        await grain.ReportInFlightAsync("tree-a", inFlight: 5, Ttl);
        var granted = await grain.AcquireSlotsAsync("tree-b", currentInFlight: 5, desiredNew: 1, clusterCap: 5, Ttl);

        Assert.That(granted, Is.EqualTo(0), "the capped tree's own drains still exhaust its ceiling");
    }

    [Test]
    public async Task ReportInFlight_clamps_a_negative_count_to_zero()
    {
        var grain = CreateGrain();

        await grain.ReportInFlightAsync("tree-a", inFlight: -3, Ttl);

        Assert.That(await grain.GetClusterInFlightAsync(), Is.EqualTo(0));
    }

    [Test]
    public void ReportInFlight_rejects_a_null_tree_id()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.ReportInFlightAsync(null!, 1, Ttl),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ReportInFlight_never_consumes_a_capped_trees_headroom()
    {
        var grain = CreateGrain();

        // An observation-only heartbeat comes from a tree that never opted into
        // the cluster ceiling, so it must not be able to throttle - let alone
        // starve - a tree that did.
        await grain.ReportInFlightAsync("uncapped-tree", inFlight: 5, Ttl);
        var granted = await grain.AcquireSlotsAsync("capped-tree", currentInFlight: 0, desiredNew: 1, clusterCap: 5, Ttl);

        Assert.That(granted, Is.EqualTo(1),
            "an uncapped tree's observed drains must not consume admission headroom");
    }

    [Test]
    public async Task ReportInFlight_is_still_visible_to_the_readable_activity_source()
    {
        var grain = CreateGrain();

        await grain.ReportInFlightAsync("uncapped-tree", inFlight: 5, Ttl);
        await grain.AcquireSlotsAsync("capped-tree", currentInFlight: 0, desiredNew: 1, clusterCap: 5, Ttl);

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            // Excluded from admission, but the scale-in gate still needs the
            // whole cluster's truth.
            Assert.That(activity.InFlight, Is.EqualTo(6));
            Assert.That(activity.ReportingTrees, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_tree_switching_to_the_capped_path_leaves_no_duplicate_footprint()
    {
        var grain = CreateGrain();

        // The tree reports as uncapped, then an operator sets a ceiling and it
        // starts requesting admission. Its observation entry must not linger.
        await grain.ReportInFlightAsync("tree-a", inFlight: 2, Ttl);
        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 2, desiredNew: 0, clusterCap: 8, Ttl);

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(activity.InFlight, Is.EqualTo(2), "the tree must be counted once, not twice");
            Assert.That(activity.ReportingTrees, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_tree_switching_to_the_uncapped_path_releases_its_admission_share()
    {
        var grain = CreateGrain();

        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: 2, Ttl);
        Assert.That(
            await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 2, Ttl),
            Is.EqualTo(0), "the ceiling is saturated while tree-a holds admission footprints");

        // The ceiling is cleared for tree-a, so its next report is observation
        // only and its share must return to the admission ledger.
        await grain.ReportInFlightAsync("tree-a", inFlight: 2, Ttl);

        Assert.That(
            await grain.AcquireSlotsAsync("tree-b", currentInFlight: 0, desiredNew: 1, clusterCap: 2, Ttl),
            Is.EqualTo(1));
    }

    [Test]
    public async Task GetActivity_reports_nothing_in_flight_for_an_idle_cluster()
    {
        var grain = CreateGrain();

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(activity.InFlight, Is.Zero);
            Assert.That(activity.ReportingTrees, Is.Zero);
            Assert.That(activity.AnyInFlight, Is.False);
            Assert.That(activity.ObservedAt, Is.Not.EqualTo(default(DateTimeOffset)));
        });
    }

    [Test]
    public async Task GetActivity_sums_in_flight_splits_and_counts_reporting_trees()
    {
        var grain = CreateGrain();
        await grain.ReportInFlightAsync("tree-a", inFlight: 2, Ttl);
        await grain.ReportInFlightAsync("tree-b", inFlight: 1, Ttl);

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(activity.InFlight, Is.EqualTo(3));
            Assert.That(activity.ReportingTrees, Is.EqualTo(2));
            Assert.That(activity.AnyInFlight, Is.True);
        });
    }

    [Test]
    public async Task GetActivity_sees_footprints_reported_through_the_admission_path()
    {
        var grain = CreateGrain();
        await grain.AcquireSlotsAsync("tree-a", currentInFlight: 0, desiredNew: 2, clusterCap: 4, Ttl);

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(activity.InFlight, Is.EqualTo(2), "admission and heartbeat feed the same readable source");
            Assert.That(activity.ReportingTrees, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetActivity_excludes_expired_footprints()
    {
        var grain = CreateGrain();
        await grain.ReportInFlightAsync("tree-a", inFlight: 2, TimeSpan.FromMilliseconds(-1));

        var activity = await grain.GetActivityAsync();

        Assert.Multiple(() =>
        {
            // A silo lost mid-split must not pin the count above zero forever.
            Assert.That(activity.InFlight, Is.Zero);
            Assert.That(activity.ReportingTrees, Is.Zero);
            Assert.That(activity.AnyInFlight, Is.False);
        });
    }
}
