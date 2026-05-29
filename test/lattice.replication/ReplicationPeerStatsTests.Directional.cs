using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Directional (inbound twin) coverage for <see cref="ReplicationPeerStats"/>.
/// </summary>
[TestFixture]
public class ReplicationPeerStatsDirectionalTests
{
    [Test]
    public void RecordInboundSuccess_throws_on_null_arguments()
    {
        var stats = new ReplicationPeerStats();
        Assert.Multiple(() =>
        {
            Assert.That(() => stats.RecordInboundSuccess(null!, "p"), Throws.ArgumentNullException);
            Assert.That(() => stats.RecordInboundSuccess("t", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void RecordInboundError_throws_on_null_arguments()
    {
        var stats = new ReplicationPeerStats();
        Assert.Multiple(() =>
        {
            Assert.That(() => stats.RecordInboundError(null!, "p"), Throws.ArgumentNullException);
            Assert.That(() => stats.RecordInboundError("t", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Inbound_and_outbound_rows_track_independently()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordSuccess("t", "p");                  // outbound
        stats.RecordInboundError("t", "p");             // inbound
        stats.RecordInboundError("t", "p");

        var snap = stats.Snapshot();
        Assert.That(snap, Has.Count.EqualTo(2));
        var outbound = snap.Single(s => s.Direction == ReplicationContactDirection.Outbound);
        var inbound = snap.Single(s => s.Direction == ReplicationContactDirection.Inbound);
        Assert.Multiple(() =>
        {
            Assert.That(outbound.ConsecutiveErrors, Is.EqualTo(0));
            Assert.That(outbound.LastContactSeconds, Is.Not.NaN);
            Assert.That(inbound.ConsecutiveErrors, Is.EqualTo(2));
            Assert.That(inbound.LastContactSeconds, Is.NaN);
        });
    }

    [Test]
    public void RecordInboundSuccess_resets_inbound_errors_without_touching_outbound()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");
        stats.RecordInboundError("t", "p");
        stats.RecordInboundError("t", "p");

        stats.RecordInboundSuccess("t", "p");

        var snap = stats.Snapshot();
        var outbound = snap.Single(s => s.Direction == ReplicationContactDirection.Outbound);
        var inbound = snap.Single(s => s.Direction == ReplicationContactDirection.Inbound);
        Assert.That(outbound.ConsecutiveErrors, Is.EqualTo(1));
        Assert.That(inbound.ConsecutiveErrors, Is.EqualTo(0));
    }

    [Test]
    public void Entries_and_bytes_behind_are_zero_on_inbound_snapshot_rows()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordInboundSuccess("t", "p");

        var inbound = stats.Snapshot().Single(s => s.Direction == ReplicationContactDirection.Inbound);
        Assert.That(inbound.EntriesBehind, Is.Zero);
        Assert.That(inbound.BytesBehind, Is.Zero);
    }

    [Test]
    public void Default_snapshot_direction_is_outbound()
    {
        // The positional ctor must continue to default Direction to
        // Outbound so existing call sites are bit-identical.
        var snap = new ReplicationPeerSnapshot("t", "p", 0, 0, 0, double.NaN);
        Assert.That(snap.Direction, Is.EqualTo(ReplicationContactDirection.Outbound));
    }

    [Test]
    public void Last_contact_gauge_emits_direction_tag()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordSuccess("t", "p");
        stats.RecordInboundSuccess("t", "p");

        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.LastContactSecondsName);
        collector.RecordObservableInstruments();

        var withDirection = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "direction"))
            .Select(m => (string?)m.Tags.Single(t => t.Key == "direction").Value)
            .ToArray();
        Assert.That(withDirection, Has.Member("outbound"));
        Assert.That(withDirection, Has.Member("inbound"));
    }

    [Test]
    public void Consecutive_errors_gauge_emits_direction_tag()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");
        stats.RecordInboundError("t", "p");

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ConsecutiveErrorsName);
        collector.RecordObservableInstruments();

        var directions = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "direction"))
            .Select(m => (string?)m.Tags.Single(t => t.Key == "direction").Value)
            .ToArray();
        Assert.That(directions, Has.Member("outbound"));
        Assert.That(directions, Has.Member("inbound"));
    }

    [Test]
    public void Entries_behind_gauge_remains_single_series_per_peer()
    {
        // entries_behind / bytes_behind stay outbound-only by design.
        // Recording an inbound contact must NOT cause a duplicate
        // entries_behind series with direction=inbound to appear.
        var stats = new ReplicationPeerStats();
        stats.RecordBacklog("t", "p", 5, 0);
        stats.RecordInboundSuccess("t", "p");

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.EntriesBehindName);
        collector.RecordObservableInstruments();

        var rows = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == "t"))
            .ToArray();
        Assert.That(rows, Has.Length.EqualTo(1));
        Assert.That(rows[0].Tags.Any(t => t.Key == "direction"), Is.False);
    }
}
