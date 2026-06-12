using System.Diagnostics.Metrics;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationMetricsTests
{
    [Test]
    public void Dictionary_compression_counters_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesIn.Name,
                Is.EqualTo("orleans.lattice.replication.compress.dictionary.bytes_in"));
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesIn.Unit, Is.EqualTo("By"));
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesOut.Name,
                Is.EqualTo("orleans.lattice.replication.compress.dictionary.bytes_out"));
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesOut.Unit, Is.EqualTo("By"));
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesInName,
                Is.EqualTo("orleans.lattice.replication.compress.dictionary.bytes_in"));
            Assert.That(LatticeReplicationMetrics.CompressDictionaryBytesOutName,
                Is.EqualTo("orleans.lattice.replication.compress.dictionary.bytes_out"));
        });
    }

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

    // ------------------------------------------------------------------
    // Sender-side adaptive batch sizing (ship.effective_batch_size +
    // ship.ack_latency)
    // ------------------------------------------------------------------

    [Test]
    public void Ship_effective_batch_size_histogram_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ShipEffectiveBatchSize.Name,
                Is.EqualTo("orleans.lattice.replication.ship.effective_batch_size"));
            Assert.That(LatticeReplicationMetrics.ShipEffectiveBatchSize.Unit, Is.EqualTo("{entry}"));
            Assert.That(LatticeReplicationMetrics.ShipEffectiveBatchSizeName,
                Is.EqualTo(LatticeReplicationMetrics.ShipEffectiveBatchSize.Name));
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
    public void Ship_ack_latency_histogram_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.ShipAckLatency.Name,
                Is.EqualTo("orleans.lattice.replication.ship.ack_latency"));
            Assert.That(LatticeReplicationMetrics.ShipAckLatency.Unit, Is.EqualTo("ms"));
            Assert.That(LatticeReplicationMetrics.ShipAckLatencyName,
                Is.EqualTo(LatticeReplicationMetrics.ShipAckLatency.Name));
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

    [Test]
    public void Ship_effective_batch_size_histogram_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipEffectiveBatchSizeName);

        LatticeReplicationMetrics.ShipEffectiveBatchSize.Record(128,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "tree-q"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-p"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(128));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "tree-q"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "site-p"));
        });
    }

    [Test]
    public void Ship_ack_latency_histogram_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipAckLatencyName);

        LatticeReplicationMetrics.ShipAckLatency.Record(9.5,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "tree-q"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "site-p"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(9.5));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "tree-q"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "site-p"));
        });
    }

    // ------------------------------------------------------------------
    // Anti-entropy Merkle-walk drift localisation (localise stage)
    // ------------------------------------------------------------------

    [Test]
    public void Merkle_walk_counters_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.MerkleWalkLocalised.Name,
                Is.EqualTo("orleans.lattice.replication.merkle_walk.localised"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkLocalised.Unit, Is.EqualTo("{leaf}"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkLocalisedName,
                Is.EqualTo("orleans.lattice.replication.merkle_walk.localised"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAborted.Name,
                Is.EqualTo("orleans.lattice.replication.merkle_walk.aborted"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAborted.Unit, Is.EqualTo("{walk}"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAbortedName,
                Is.EqualTo("orleans.lattice.replication.merkle_walk.aborted"));
        });
    }

    [Test]
    public void Merkle_walk_tag_and_reason_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.TagDepth, Is.EqualTo("depth"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAbortDepthCap, Is.EqualTo("depth_cap"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAbortByteBudget, Is.EqualTo("byte_budget"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAbortRemoteUnavailable, Is.EqualTo("remote_unavailable"));
            Assert.That(LatticeReplicationMetrics.MerkleWalkAbortVersionSkew, Is.EqualTo("version_skew"));
        });
    }

    [TestCase(MerkleWalkAbortReason.DepthCapExceeded, "depth_cap")]
    [TestCase(MerkleWalkAbortReason.ByteBudgetExceeded, "byte_budget")]
    [TestCase(MerkleWalkAbortReason.RemoteUnavailable, "remote_unavailable")]
    [TestCase(MerkleWalkAbortReason.VersionSkew, "version_skew")]
    public void Merkle_walk_abort_reason_tag_maps_each_reason(MerkleWalkAbortReason reason, string expected)
    {
        Assert.That(LatticeReplicationMetrics.MerkleWalkAbortReasonTag(reason), Is.EqualTo(expected));
    }

    [Test]
    public void Merkle_walk_localised_counter_records_with_tree_and_depth_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.MerkleWalkLocalisedName);

        LatticeReplicationMetrics.MerkleWalkLocalised.Add(2,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagDepth, "3"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(2L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "t"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "depth" && (string?)t.Value == "3"));
        });
    }

    [Test]
    public void Merkle_walk_aborted_counter_records_with_reason_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.MerkleWalkAbortedName);

        LatticeReplicationMetrics.MerkleWalkAborted.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.MerkleWalkAbortRemoteUnavailable));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Tags,
            Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "remote_unavailable"));
    }

    // ------------------------------------------------------------------
    // Anti-entropy targeted leaf re-replay (repair stage)
    // ------------------------------------------------------------------

    [Test]
    public void Leaf_rereplay_counters_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.LeafReReplayEntries.Name,
                Is.EqualTo("orleans.lattice.replication.leaf_rereplay.entries"));
            Assert.That(LatticeReplicationMetrics.LeafReReplayEntries.Unit, Is.EqualTo("{entry}"));
            Assert.That(LatticeReplicationMetrics.LeafReReplayEntriesName,
                Is.EqualTo("orleans.lattice.replication.leaf_rereplay.entries"));
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkipped.Name,
                Is.EqualTo("orleans.lattice.replication.leaf_rereplay.skipped"));
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkipped.Unit, Is.EqualTo("{skip}"));
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkippedName,
                Is.EqualTo("orleans.lattice.replication.leaf_rereplay.skipped"));
        });
    }

    [Test]
    public void Leaf_rereplay_skip_reason_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkipDisabled, Is.EqualTo("disabled"));
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkipRangeEmpty, Is.EqualTo("range_empty"));
            Assert.That(LatticeReplicationMetrics.LeafReReplaySkipWalTrimmed, Is.EqualTo("wal_trimmed"));
        });
    }

    [TestCase(LeafReReplaySkipReason.Disabled, "disabled")]
    [TestCase(LeafReReplaySkipReason.RangeEmpty, "range_empty")]
    [TestCase(LeafReReplaySkipReason.WalTrimmed, "wal_trimmed")]
    public void Leaf_rereplay_skip_reason_tag_maps_each_reason(LeafReReplaySkipReason reason, string expected)
    {
        Assert.That(LatticeReplicationMetrics.LeafReReplaySkipReasonTag(reason), Is.EqualTo(expected));
    }

    [Test]
    public void Leaf_rereplay_entries_counter_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.LeafReReplayEntriesName);

        LatticeReplicationMetrics.LeafReReplayEntries.Add(5,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(5L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "t"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
        });
    }

    [Test]
    public void Leaf_rereplay_skipped_counter_records_with_tree_peer_and_reason_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.LeafReReplaySkippedName);

        LatticeReplicationMetrics.LeafReReplaySkipped.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.LeafReReplaySkipWalTrimmed));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "wal_trimmed"));
        });
    }

    // ------------------------------------------------------------------
    // Anti-entropy bootstrap-snapshot fallback (GC'd-divergence repair)
    // ------------------------------------------------------------------

    [Test]
    public void Bootstrap_fallback_counters_have_expected_names_and_units()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackTriggered.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.triggered"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackTriggered.Unit, Is.EqualTo("{fallback}"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackTriggeredName,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.triggered"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackEntries.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.entries"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackEntries.Unit, Is.EqualTo("{entry}"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackEntriesName,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.entries"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipped.Name,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.skipped"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipped.Unit, Is.EqualTo("{skip}"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkippedName,
                Is.EqualTo("orleans.lattice.replication.bootstrap_fallback.skipped"));
        });
    }

    [Test]
    public void Bootstrap_fallback_skip_reason_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipDisabled, Is.EqualTo("disabled"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipRangeEmpty, Is.EqualTo("range_empty"));
            Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipEmpty, Is.EqualTo("empty"));
        });
    }

    [TestCase(BootstrapFallbackSkipReason.Disabled, "disabled")]
    [TestCase(BootstrapFallbackSkipReason.RangeEmpty, "range_empty")]
    [TestCase(BootstrapFallbackSkipReason.Empty, "empty")]
    public void Bootstrap_fallback_skip_reason_tag_maps_each_reason(
        BootstrapFallbackSkipReason reason, string expected)
    {
        Assert.That(LatticeReplicationMetrics.BootstrapFallbackSkipReasonTag(reason), Is.EqualTo(expected));
    }

    [Test]
    public void Bootstrap_fallback_triggered_counter_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapFallbackTriggeredName);

        LatticeReplicationMetrics.BootstrapFallbackTriggered.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "t"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
        });
    }

    [Test]
    public void Bootstrap_fallback_entries_counter_records_with_tree_and_peer_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        LatticeReplicationMetrics.BootstrapFallbackEntries.Add(9,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(9L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "t"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
        });
    }

    [Test]
    public void Bootstrap_fallback_skipped_counter_records_with_tree_peer_and_reason_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapFallbackSkippedName);

        LatticeReplicationMetrics.BootstrapFallbackSkipped.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.BootstrapFallbackSkipDisabled));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "disabled"));
        });
    }

    // ------------------------------------------------------------------
    // Anti-entropy remediation guards (opt-in, rate cap, circuit breaker)
    // ------------------------------------------------------------------

    [Test]
    public void Digest_remediation_skipped_counter_has_expected_name_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.DigestRemediationSkipped.Name,
                Is.EqualTo("orleans.lattice.replication.digest_remediation.skipped"));
            Assert.That(LatticeReplicationMetrics.DigestRemediationSkipped.Unit, Is.EqualTo("{skip}"));
            Assert.That(LatticeReplicationMetrics.DigestRemediationSkippedName,
                Is.EqualTo("orleans.lattice.replication.digest_remediation.skipped"));
        });
    }

    [Test]
    public void Digest_remediation_disabled_gauge_name_constant_matches_canonical_name()
    {
        Assert.That(LatticeReplicationMetrics.DigestRemediationDisabledName,
            Is.EqualTo("orleans.lattice.replication.digest_remediation.disabled"));
    }

    [Test]
    public void Digest_remediation_reason_constants_use_canonical_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.DigestRemediationReasonOptOut, Is.EqualTo("opt_out"));
            Assert.That(LatticeReplicationMetrics.DigestRemediationReasonBudgetExhausted, Is.EqualTo("budget_exhausted"));
            Assert.That(LatticeReplicationMetrics.DigestRemediationReasonCircuitOpen, Is.EqualTo("circuit_open"));
        });
    }

    [TestCase(RemediationDisabledReason.OptOut, "opt_out")]
    [TestCase(RemediationDisabledReason.BudgetExhausted, "budget_exhausted")]
    [TestCase(RemediationDisabledReason.CircuitOpen, "circuit_open")]
    public void Digest_remediation_disabled_reason_tag_maps_each_reason(
        RemediationDisabledReason reason, string expected)
    {
        Assert.That(LatticeReplicationMetrics.DigestRemediationDisabledReasonTag(reason), Is.EqualTo(expected));
    }

    [Test]
    public void Digest_remediation_skipped_counter_records_with_tree_peer_and_reason_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DigestRemediationSkippedName);

        LatticeReplicationMetrics.DigestRemediationSkipped.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "t"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, "p"),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.DigestRemediationReasonOptOut));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "t"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == "p"));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "opt_out"));
        });
    }
}
