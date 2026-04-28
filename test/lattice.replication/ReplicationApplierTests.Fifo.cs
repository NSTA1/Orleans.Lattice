using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_in_order_delivery_records_no_fifo_violations()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k1", Hlc(10)));
        await applier.ApplyAsync(SetEntry("k2", Hlc(20)));
        await applier.ApplyAsync(SetEntry("k3", Hlc(30)));

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_out_of_order_delivery_records_fifo_violation_with_tree_and_origin_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        // GetAsync returns Zero throughout so HWM dedupe never short-circuits
        // — this exposes the FIFO regression rather than masking it.
        var (applier, _, apply, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(20)));
        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        // The violating entry still applies (FIFO tracking is purely
        // observability; it does not change apply behaviour).
        Assert.That(result.Applied, Is.True);
        await apply.Received(2).ApplySetAsync(
            "k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == Tree));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == RemoteCluster));
    }

    [Test]
    public async Task ApplyAsync_fifo_tracker_is_partitioned_per_origin()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        var (applier, _, _, _) = CreateApplier();

        // Origin "site-b": HLC 10 then HLC 20 — strictly increasing.
        // Origin "site-c": HLC 5 — independent of "site-b"'s tracker;
        // must not record a violation against either origin.
        await applier.ApplyAsync(SetEntry("k", Hlc(10), origin: "site-b"));
        await applier.ApplyAsync(SetEntry("k", Hlc(5), origin: "site-c"));
        await applier.ApplyAsync(SetEntry("k", Hlc(20), origin: "site-b"));

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_fifo_tracker_is_partitioned_per_tree()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        // A single applier instance routing entries across two distinct
        // trees: origin-keyed FIFO state on tree-a must not bleed into
        // tree-b.
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        var applier = new ReplicationApplier(factory, Monitor());

        await applier.ApplyAsync(SetEntry("k", Hlc(20)) with { TreeId = "tree-a" });
        // tree-b sees a "lower" HLC than tree-a's last apply, but it's a
        // different tree — must not record a violation.
        await applier.ApplyAsync(SetEntry("k", Hlc(5)) with { TreeId = "tree-b" });

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_range_delete_does_not_record_or_reset_fifo_tracker()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(20)));
        // A range delete carries HLC.Zero by design — must not register
        // a violation despite Zero < 20, and must not overwrite the prior
        // recorded HLC for this origin.
        await applier.ApplyAsync(RangeDeleteEntry("a", "z"));
        // A later in-order point apply for the same origin must still see
        // the recorded HLC=20 and therefore record no violation.
        await applier.ApplyAsync(SetEntry("k", Hlc(30)));

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_out_of_order_delete_op_records_fifo_violation()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        var (applier, _, _, _) = CreateApplier();

        // Set establishes the per-origin tracker at HLC=20; a subsequent
        // out-of-order Delete at HLC=10 from the same origin must record
        // a violation just like an out-of-order Set would.
        await applier.ApplyAsync(SetEntry("k", Hlc(20)));
        await applier.ApplyAsync(DeleteEntry("k", Hlc(10)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == RemoteCluster));
    }

    [Test]
    public async Task ApplyAsync_equal_hlc_does_not_record_fifo_violation()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        var (applier, _, _, _) = CreateApplier();

        // Two entries with identical HLC for the same (tree, origin):
        // ts == existing is not strictly less-than, so no FIFO violation
        // is recorded and the tracker is not corrupted.
        await applier.ApplyAsync(SetEntry("k", Hlc(20)));
        await applier.ApplyAsync(SetEntry("k", Hlc(20)));

        Assert.That(collector.Measurements, Is.Empty);
    }
}
