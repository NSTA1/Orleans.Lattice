using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4615: the shipper's tombstone reap watermark, which bounds the reap
/// gate's peer frontier. It is computed with the watermark the shipper ships,
/// so it is withheld while the peer is off the log, but it is never shipped and
/// a key filter does not freeze it; a change of the filter withholds it until it
/// is vouched again under the new scope. Runs the real shipper.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    [Test]
    public async Task A_peer_off_the_log_reports_no_reap_watermark_until_its_re_seed_and_acknowledgements()
    {
        const string tree = "ccv-reap-reseed";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var deleteStamp = Hlc(ticks, 40);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor);
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        var beforeFallOff = await shipper.GetReapLowWatermarkAsync();

        // The peer falls off the log: its contents were replaced.
        transport.Lineage = Guid.NewGuid();
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 2);
        var whileOff = await shipper.GetReapLowWatermarkAsync();
        var offTheLog = shipper.ReseedRequired;

        // The peer re-seeds from a later export and acknowledges the replay.
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "d", Hlc(ticks, 63)));
        await PumpAsync(shipper, ticks: 6);
        feeds[0].Append(LocalSet(tree, "e", Hlc(ticks, 64)));
        await PumpAsync(shipper, ticks: 1);
        var afterReseed = await shipper.GetReapLowWatermarkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(beforeFallOff, Is.EqualTo(floor), "precondition: the peer covered the floor");
            Assert.That(offTheLog, Is.True, "precondition: the peer was taken off the log");
            Assert.That(whileOff, Is.EqualTo(HybridLogicalClock.Zero),
                "a peer off the log may lack a delete, so no tombstone may be reaped on its account");
            Assert.That(shipper.ReseedRequired, Is.False, "precondition: the re-seed completed");
            Assert.That(afterReseed.CompareTo(deleteStamp), Is.GreaterThan(0),
                "once re-seeded and acknowledged past it, the peer no longer holds the tombstone");
        });
    }

    [Test]
    public async Task A_key_filtered_tree_reports_a_reap_watermark_it_never_ships()
    {
        const string tree = "ccv-reap-filtered";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor,
            configureOptions: o => o.KeyPrefixes = ["a"]);
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a1", Hlc(ticks, 10)));
        feeds[1].Append(LocalSet(tree, "a2", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 2);

        Assert.Multiple(async () =>
        {
            Assert.That(transport.Batches.Select(b => b.SourceFrontier), Has.All.Null,
                "a key-filtered tree ships no watermark");
            Assert.That(await shipper.GetReapLowWatermarkAsync(), Is.EqualTo(floor),
                "but its reap watermark covers every in-scope write the peer acknowledged");
        });
    }

    [Test]
    public async Task Widening_the_key_filter_withholds_the_reap_watermark_until_it_is_vouched_under_the_new_scope()
    {
        const string tree = "ccv-reap-widen";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        LatticeReplicationOptions? options = null;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor,
            configureOptions: o =>
            {
                o.KeyPrefixes = ["a"];
                options = o;
            });
        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a1", Hlc(ticks, 10)));
        feeds[1].Append(LocalSet(tree, "a2", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 2);
        var underOldScope = await shipper.GetReapLowWatermarkAsync();

        // A key that was out of scope comes into scope: a tombstone of it below
        // the old watermark was never vouched for under the new scope.
        options!.KeyPrefixes = ["a", "b"];
        await PumpAsync(shipper, ticks: 1);
        var afterWidening = await shipper.GetReapLowWatermarkAsync();

        var reFloor = Hlc(ticks, 90);
        foreach (var feed in feeds)
        {
            feed.ClockFloor = reFloor;
        }

        feeds[0].Append(LocalSet(tree, "b1", Hlc(ticks, 80)));
        feeds[1].Append(LocalSet(tree, "b2", Hlc(ticks, 81)));
        await PumpAsync(shipper, ticks: 2);
        var reVouched = await shipper.GetReapLowWatermarkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(underOldScope, Is.EqualTo(floor), "precondition: vouched under the old scope");
            Assert.That(afterWidening, Is.EqualTo(HybridLogicalClock.Zero),
                "a changed key filter withholds the reap watermark until it is vouched under the new scope");
            Assert.That(reVouched, Is.EqualTo(reFloor), "the cursor re-covers a floor read under the new scope");
        });
    }
}
