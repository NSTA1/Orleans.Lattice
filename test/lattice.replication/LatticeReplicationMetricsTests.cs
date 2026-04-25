using System.Diagnostics.Metrics;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationMetricsTests
{
    [Test]
    public void Meter_name_is_orleans_lattice_replication()
    {
        Assert.That(LatticeReplicationMetrics.MeterName, Is.EqualTo("orleans.lattice.replication"));
        Assert.That(LatticeReplicationMetrics.Meter.Name, Is.EqualTo("orleans.lattice.replication"));
    }

    [Test]
    public void Tag_key_constants_use_canonical_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.TagTree, Is.EqualTo("tree"));
            Assert.That(LatticeReplicationMetrics.TagPeer, Is.EqualTo("peer"));
            Assert.That(LatticeReplicationMetrics.TagOutcome, Is.EqualTo("outcome"));
        });
    }

    [Test]
    public void Histogram_instruments_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ShipDuration.Name,
                Is.EqualTo("orleans.lattice.replication.ship.duration"));
            Assert.That(LatticeReplicationMetrics.ShipDuration.Unit, Is.EqualTo("ms"));
            Assert.That(LatticeReplicationMetrics.ApplyDuration.Name,
                Is.EqualTo("orleans.lattice.replication.apply.duration"));
            Assert.That(LatticeReplicationMetrics.ApplyDuration.Unit, Is.EqualTo("ms"));
        });
    }

    [Test]
    public void Observable_gauge_name_constants_match_canonical_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.EntriesBehindName,
                Is.EqualTo("orleans.lattice.replication.peer.entries_behind"));
            Assert.That(LatticeReplicationMetrics.BytesBehindName,
                Is.EqualTo("orleans.lattice.replication.peer.bytes_behind"));
            Assert.That(LatticeReplicationMetrics.ConsecutiveErrorsName,
                Is.EqualTo("orleans.lattice.replication.peer.consecutive_errors"));
            Assert.That(LatticeReplicationMetrics.LastContactSecondsName,
                Is.EqualTo("orleans.lattice.replication.peer.last_contact_seconds"));
        });
    }

    [Test]
    public void Ship_duration_histogram_records_measurements()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.ship.duration");

        LatticeReplicationMetrics.ShipDuration.Record(12.5,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, "ok"));

        var measurements = collector.Measurements;
        Assert.That(measurements, Has.Count.EqualTo(1));
        var only = measurements.Single();
        Assert.That(only.Value, Is.EqualTo(12.5));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "peer" && (string?)t.Value == "p"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "outcome" && (string?)t.Value == "ok"));
    }

    [Test]
    public void Apply_duration_histogram_records_measurements()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.apply.duration");

        LatticeReplicationMetrics.ApplyDuration.Record(3.0);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(3.0));
    }
}
