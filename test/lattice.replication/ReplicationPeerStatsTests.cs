using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationPeerStatsTests
{
    [Test]
    public void Snapshot_is_empty_when_no_state_recorded()
    {
        var stats = new ReplicationPeerStats();

        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public void RecordBacklog_throws_on_null_arguments()
    {
        var stats = new ReplicationPeerStats();

        Assert.Multiple(() =>
        {
            Assert.That(() => stats.RecordBacklog(null!, "p", 0, 0), Throws.ArgumentNullException);
            Assert.That(() => stats.RecordBacklog("t", null!, 0, 0), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void RecordSuccess_throws_on_null_arguments()
    {
        var stats = new ReplicationPeerStats();

        Assert.Multiple(() =>
        {
            Assert.That(() => stats.RecordSuccess(null!, "p"), Throws.ArgumentNullException);
            Assert.That(() => stats.RecordSuccess("t", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void RecordError_throws_on_null_arguments()
    {
        var stats = new ReplicationPeerStats();

        Assert.Multiple(() =>
        {
            Assert.That(() => stats.RecordError(null!, "p"), Throws.ArgumentNullException);
            Assert.That(() => stats.RecordError("t", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void RecordBacklog_persists_per_peer_values()
    {
        var stats = new ReplicationPeerStats();

        stats.RecordBacklog("tree-1", "peer-a", entriesBehind: 7, bytesBehind: 1024);

        var snap = stats.Snapshot().Single();
        Assert.Multiple(() =>
        {
            Assert.That(snap.Tree, Is.EqualTo("tree-1"));
            Assert.That(snap.Peer, Is.EqualTo("peer-a"));
            Assert.That(snap.EntriesBehind, Is.EqualTo(7));
            Assert.That(snap.BytesBehind, Is.EqualTo(1024));
            Assert.That(snap.ConsecutiveErrors, Is.EqualTo(0));
            Assert.That(snap.LastContactSeconds, Is.NaN);
        });
    }

    [Test]
    public void RecordError_increments_consecutive_errors()
    {
        var stats = new ReplicationPeerStats();

        stats.RecordError("t", "p");
        stats.RecordError("t", "p");
        stats.RecordError("t", "p");

        Assert.That(stats.Snapshot().Single().ConsecutiveErrors, Is.EqualTo(3));
    }

    [Test]
    public void RecordSuccess_resets_consecutive_errors_to_zero()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");
        stats.RecordError("t", "p");

        stats.RecordSuccess("t", "p");

        Assert.That(stats.Snapshot().Single().ConsecutiveErrors, Is.EqualTo(0));
    }

    [Test]
    public void Last_contact_seconds_is_NaN_until_first_success()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");

        Assert.That(stats.Snapshot().Single().LastContactSeconds, Is.NaN);
    }

    [Test]
    public void Last_contact_seconds_reflects_elapsed_time_after_success()
    {
        var clock = new FakeClock { Now = DateTimeOffset.UnixEpoch };
        var stats = new TestableStats(clock);

        stats.RecordSuccess("t", "p");
        clock.Now = DateTimeOffset.UnixEpoch.AddSeconds(42);

        Assert.That(stats.Snapshot().Single().LastContactSeconds, Is.EqualTo(42.0));
    }

    [Test]
    public void Different_tree_peer_pairs_track_independently()
    {
        var stats = new ReplicationPeerStats();

        stats.RecordBacklog("a", "p1", 1, 10);
        stats.RecordBacklog("a", "p2", 2, 20);
        stats.RecordBacklog("b", "p1", 3, 30);

        var snap = stats.Snapshot();
        Assert.That(snap, Has.Count.EqualTo(3));
        Assert.That(snap.Select(s => s.EntriesBehind).Order(), Is.EqualTo(new[] { 1L, 2L, 3L }));
    }

    [Test]
    public void Observable_gauges_emit_per_peer_measurements_with_tree_and_peer_tags()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordBacklog("tree-1", "peer-a", 5, 100);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.EntriesBehindName);

        collector.RecordObservableInstruments();

        var measurements = collector.Measurements;
        Assert.That(measurements, Has.Some.Matches<RecordedMeasurement<long>>(m =>
            m.Value == 5 &&
            m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == "tree-1") &&
            m.Tags.Any(t => t.Key == "peer" && (string?)t.Value == "peer-a")));
    }

    [Test]
    public void Bytes_behind_gauge_emits_recorded_value()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordBacklog("t", "p", 0, 4096);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BytesBehindName);

        collector.RecordObservableInstruments();

        Assert.That(collector.Measurements, Has.Some.Matches<RecordedMeasurement<long>>(m => m.Value == 4096));
    }

    [Test]
    public void Consecutive_errors_gauge_emits_recorded_value()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");
        stats.RecordError("t", "p");

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ConsecutiveErrorsName);

        collector.RecordObservableInstruments();

        Assert.That(collector.Measurements, Has.Some.Matches<RecordedMeasurement<long>>(m => m.Value == 2));
    }

    [Test]
    public void Last_contact_gauge_emits_NaN_when_no_success_recorded()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");

        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.LastContactSecondsName);

        collector.RecordObservableInstruments();

        Assert.That(collector.Measurements, Has.Some.Matches<RecordedMeasurement<double>>(m => double.IsNaN(m.Value)));
    }

    [Test]
    public void RecordError_after_RecordSuccess_increments_from_zero()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordError("t", "p");
        stats.RecordSuccess("t", "p");

        stats.RecordError("t", "p");

        Assert.That(stats.Snapshot().Single().ConsecutiveErrors, Is.EqualTo(1));
    }

    [Test]
    public void Multiple_instances_do_not_multiply_gauge_registrations()
    {
        // Construct several instances. The latest must be the only one that
        // contributes observed measurements; earlier instances must not leak
        // their state into the meter. A stricter assertion than `Has.Some` is
        // used here precisely to catch re-registration regressions.
        _ = new ReplicationPeerStats();
        _ = new ReplicationPeerStats();
        var latest = new ReplicationPeerStats();
        latest.RecordBacklog("only-tree", "only-peer", 99, 0);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.EntriesBehindName);

        collector.RecordObservableInstruments();

        var matching = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == "only-tree"))
            .ToArray();
        Assert.That(matching, Has.Length.EqualTo(1), "exactly one measurement should match the latest instance");
        Assert.That(matching[0].Value, Is.EqualTo(99));
    }

    private sealed class FakeClock
    {
        public DateTimeOffset Now;
    }

    private sealed class TestableStats(FakeClock clock) : ReplicationPeerStats
    {
        protected override DateTimeOffset GetTimestamp() => clock.Now;
    }
}
