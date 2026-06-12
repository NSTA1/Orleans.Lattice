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

    [Test]
    public void Apply_parallel_runs_histogram_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ApplyParallelRuns.Name,
                Is.EqualTo("orleans.lattice.replication.apply.parallel_runs"));
            Assert.That(LatticeReplicationMetrics.ApplyParallelRuns.Unit, Is.EqualTo("{run}"));
            Assert.That(LatticeReplicationMetrics.ApplyParallelRunsName,
                Is.EqualTo("orleans.lattice.replication.apply.parallel_runs"));
        });
    }

    [Test]
    public void Apply_parallel_runs_histogram_records_measurements()
    {
        using var collector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyParallelRunsName);

        LatticeReplicationMetrics.ApplyParallelRuns.Record(3);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(3));
    }

    [Test]
    public void Reason_tag_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.TagReason, Is.EqualTo("reason"));
            Assert.That(LatticeReplicationMetrics.ReasonDiscarded, Is.EqualTo("discarded"));
            Assert.That(LatticeReplicationMetrics.ReasonReplayed, Is.EqualTo("replayed"));
            Assert.That(LatticeReplicationMetrics.ReasonEvicted, Is.EqualTo("evicted"));
            Assert.That(LatticeReplicationMetrics.ReasonUnknown, Is.EqualTo("unknown"));
        });
    }

    [Test]
    public void Dead_letter_counters_have_expected_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.DeadLetterEnqueued.Name,
                Is.EqualTo("orleans.lattice.replication.dead_letter.enqueued"));
            Assert.That(LatticeReplicationMetrics.DeadLetterRemoved.Name,
                Is.EqualTo("orleans.lattice.replication.dead_letter.removed"));
        });
    }

    [Test]
    public void Dead_letter_enqueued_counter_records_with_tree_and_reason_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.enqueued");

        LatticeReplicationMetrics.DeadLetterEnqueued.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonUnknown));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "reason" && (string?)t.Value == "unknown"));
    }

    [Test]
    public void Dead_letter_removed_counter_records_with_tree_and_reason_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.removed");

        LatticeReplicationMetrics.DeadLetterRemoved.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonReplayed));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Tags,
            Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "replayed"));
    }

    [Test]
    public void Wal_entries_trimmed_counter_has_expected_name()
    {
        Assert.That(LatticeMetrics.WalEntriesTrimmed.Name,
            Is.EqualTo("orleans.lattice.wal.entries_trimmed"));
    }

    [Test]
    public void Wal_entries_trimmed_counter_records_with_tree_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeMetrics.MeterName,
            "orleans.lattice.wal.entries_trimmed");

        LatticeMetrics.WalEntriesTrimmed.Add(7,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree-x"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(7L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "tree-x"));
    }

    [Test]
    public void Extended_reason_tag_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ReasonSchema, Is.EqualTo("schema"));
            Assert.That(LatticeReplicationMetrics.ReasonHlcSkew, Is.EqualTo("hlc_skew"));
            Assert.That(LatticeReplicationMetrics.ReasonOversized, Is.EqualTo("oversized"));
        });
    }

    [Test]
    public void Apply_lag_histogram_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ApplyLag.Name,
                Is.EqualTo("orleans.lattice.replication.apply.lag"));
            Assert.That(LatticeReplicationMetrics.ApplyLag.Unit, Is.EqualTo("ms"));
            Assert.That(LatticeReplicationMetrics.ApplyLagName,
                Is.EqualTo(LatticeReplicationMetrics.ApplyLag.Name));
        });
    }

    [Test]
    public void Apply_lag_histogram_records_with_tree_tag()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.apply.lag");

        LatticeReplicationMetrics.ApplyLag.Record(42.0,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "tree-y"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(42.0));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "tree-y"));
    }

    [Test]
    public void Apply_lag_histogram_records_with_tree_and_peer_tags()
    {
        // The canonical applier emits both `tree` and `peer` (the
        // entry's OriginClusterId, identifying the authoring cluster).
        // The instrument-level test pins the schema so a future caller
        // dropping the `peer` argument fails here before any
        // integration test exercises the call site.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.apply.lag");

        LatticeReplicationMetrics.ApplyLag.Record(7.5,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "tree-z"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-x"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(7.5));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "tree-z"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "site-x"));
        });
    }

    [Test]
    public void Apply_duration_histogram_records_with_tree_peer_and_outcome_tags()
    {
        // Pins the documented `tree`+`peer`+`outcome` schema on the
        // apply-duration histogram. Without all three tags being
        // emitted, dashboards that filter by `peer="..."` silently
        // drop samples. The bare three-tag form below mirrors the
        // shape every call site in ReplicationApplier now uses.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.apply.duration");

        LatticeReplicationMetrics.ApplyDuration.Record(11.25,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "tree-z"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-x"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.OutcomeSuccess));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(11.25));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "tree-z"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "site-x"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeSuccess));
        });
    }

    [Test]
    public void Apply_fifo_violations_records_with_tree_and_origin_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);

        LatticeReplicationMetrics.ApplyFifoViolations.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, "site-b"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == "site-b"));
    }

    [Test]
    public void Peer_fell_off_log_counter_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.PeerFellOffLog.Name,
                Is.EqualTo("orleans.lattice.replication.peer.fell_off_log"));
            Assert.That(LatticeReplicationMetrics.PeerFellOffLog.Unit, Is.EqualTo("{event}"));
            Assert.That(LatticeReplicationMetrics.PeerFellOffLogName,
                Is.EqualTo("orleans.lattice.replication.peer.fell_off_log"));
        });
    }

    [Test]
    public void Peer_fell_off_log_counter_records_with_tree_and_origin_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.peer.fell_off_log");

        LatticeReplicationMetrics.PeerFellOffLog.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, "site-a"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == "site-a"));
    }

    [Test]
    public void Bootstrap_instruments_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapEntriesReceived.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap.entries_received"));
            Assert.That(LatticeReplicationMetrics.BootstrapEntriesReceived.Unit, Is.EqualTo("{entry}"));
            Assert.That(LatticeReplicationMetrics.BootstrapBytesReceived.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap.bytes_received"));
            Assert.That(LatticeReplicationMetrics.BootstrapBytesReceived.Unit, Is.EqualTo("By"));
            Assert.That(LatticeReplicationMetrics.BootstrapDuration.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap.duration"));
            Assert.That(LatticeReplicationMetrics.BootstrapDuration.Unit, Is.EqualTo("ms"));
        });
    }

    [Test]
    public void Bootstrap_instrument_name_constants_match_canonical_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapEntriesReceivedName,
                Is.EqualTo("orleans.lattice.replication.bootstrap.entries_received"));
            Assert.That(LatticeReplicationMetrics.BootstrapBytesReceivedName,
                Is.EqualTo("orleans.lattice.replication.bootstrap.bytes_received"));
            Assert.That(LatticeReplicationMetrics.BootstrapDurationName,
                Is.EqualTo("orleans.lattice.replication.bootstrap.duration"));
        });
    }

    [Test]
    public void Bootstrap_outcome_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapOutcomeLive, Is.EqualTo("live"));
            Assert.That(LatticeReplicationMetrics.BootstrapOutcomeFailed, Is.EqualTo("failed"));
            Assert.That(LatticeReplicationMetrics.BootstrapOutcomeTimedOut, Is.EqualTo("timed_out"));
        });
    }

    [Test]
    public void Bootstrap_entries_received_counter_records_with_tree_and_origin_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.bootstrap.entries_received");

        LatticeReplicationMetrics.BootstrapEntriesReceived.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, "site-a"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == "site-a"));
    }

    [Test]
    public void Bootstrap_bytes_received_counter_records_with_tree_and_origin_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.bootstrap.bytes_received");

        LatticeReplicationMetrics.BootstrapBytesReceived.Add(2048,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, "site-a"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(2048L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == "site-a"));
    }

    [Test]
    public void Bootstrap_duration_histogram_records_with_tree_origin_and_outcome_tags()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.bootstrap.duration");

        LatticeReplicationMetrics.BootstrapDuration.Record(42.5,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, "site-a"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome,
                LatticeReplicationMetrics.BootstrapOutcomeLive));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(42.5));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "origin" && (string?)t.Value == "site-a"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "outcome" && (string?)t.Value == "live"));
    }

    // ------------------------------------------------------------------
    // Content-hash dedup measurement counters
    // ------------------------------------------------------------------

    [Test]
    public void Ship_redundant_payload_counters_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloads.Name,
                Is.EqualTo("orleans.lattice.replication.ship.redundant_payloads"));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloads.Unit, Is.EqualTo("{entry}"));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadBytes.Name,
                Is.EqualTo("orleans.lattice.replication.ship.redundant_payload_bytes"));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadBytes.Unit, Is.EqualTo("By"));
        });
    }

    [Test]
    public void Ship_redundant_payload_name_constants_match_canonical_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadsName,
                Is.EqualTo("orleans.lattice.replication.ship.redundant_payloads"));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadsName,
                Is.EqualTo(LatticeReplicationMetrics.ShipRedundantPayloads.Name));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadBytesName,
                Is.EqualTo("orleans.lattice.replication.ship.redundant_payload_bytes"));
            Assert.That(LatticeReplicationMetrics.ShipRedundantPayloadBytesName,
                Is.EqualTo(LatticeReplicationMetrics.ShipRedundantPayloadBytes.Name));
        });
    }

    [Test]
    public void Ship_redundant_payloads_counter_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadsName);

        LatticeReplicationMetrics.ShipRedundantPayloads.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-b"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "peer" && (string?)t.Value == "site-b"));
    }

    [Test]
    public void Ship_redundant_payload_bytes_counter_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadBytesName);

        LatticeReplicationMetrics.ShipRedundantPayloadBytes.Add(64,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-b"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(64L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "t"));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "peer" && (string?)t.Value == "site-b"));
    }
}
