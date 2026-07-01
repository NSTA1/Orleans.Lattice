using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Sender-side coverage for adaptive (AIMD) outbound batch sizing: with
/// the feature off the shipper is byte-identical to static sizing, a
/// healthy link stays at the configured ceiling, the receiver hint is
/// always the hard ceiling, and the effective-size / ack-latency
/// instruments emit on every acknowledged batch.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// With <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    /// off (the default dark-launch posture) the shipper sizes every
    /// batch exactly at the configured
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/>, byte-identical
    /// to the static path: no adaptive controller is ever consulted.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_with_adaptive_disabled_ships_full_configured_batch_size()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 4,
            AdaptiveBatchSizingEnabled = false,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        for (var i = 1; i <= 10; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(4),
            "with adaptive sizing off the shipper drains exactly ShipBatchSize entries");
    }

    /// <summary>
    /// With adaptive sizing on and a healthy link (the stubbed transport
    /// acks effectively instantly, so the sliding-window mean ack latency
    /// stays well below the threshold), the controller keeps the effective
    /// size pinned at the configured ceiling - the optimistic posture means
    /// a fast link never throttles itself below
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/>.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_with_adaptive_enabled_healthy_link_ships_full_configured_batch_size()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 4,
            AdaptiveBatchSizingEnabled = true,
            AdaptiveBatchLatencyThreshold = TimeSpan.FromSeconds(5),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        for (var i = 1; i <= 10; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        // Drain across several ticks so the controller observes several
        // fast acks; the effective size must never drop below the ceiling.
        await grain.PumpForTestingAsync(CancellationToken.None);
        for (var i = 11; i <= 14; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(4),
            "a healthy link keeps the adaptive size pinned at the ShipBatchSize ceiling");
    }

    /// <summary>
    /// Regression for issue #1047. With adaptive batch sizing left at its
    /// default (now enabled), a failed send drives the controller's
    /// multiplicative decrease, so the next retry ships a strictly smaller
    /// batch rather than re-shipping the identical oversized batch forever.
    /// This is the automatic recovery from a deterministic apply failure
    /// (such as a receiver phase-2 manifest-commit timeout under burst load).
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_default_adaptive_shrinks_batch_after_send_failure()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 8,
            // AdaptiveBatchSizingEnabled is intentionally left unset: the
            // default-on posture is the behaviour under test. A high latency
            // threshold keeps a healthy ack from triggering the latency-based
            // decrease so the only shrink signal is the send failure.
            AdaptiveBatchLatencyThreshold = TimeSpan.FromSeconds(5),
            ShipBackoffInitial = TimeSpan.FromMilliseconds(1),
            ShipBackoffMax = TimeSpan.FromMilliseconds(1),
            ShipBackoffJitter = 0.0,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);

        var calls = 0;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                if (calls == 1)
                {
                    throw new InvalidOperationException("apply-timeout");
                }

                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });

        for (var i = 1; i <= 8; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        // Tick 1: drains the full ceiling of 8 and the send throws, so the
        // controller halves the effective size (8 -> 4) and backs off.
        await grain.PumpForTestingAsync(CancellationToken.None);

        // Let the 1 ms backoff window elapse before retrying.
        await Task.Delay(30);

        // Tick 2: the shrunk effective size caps the drain at 4 entries.
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(4),
            "a send failure shrinks the default-on adaptive batch size so the retry ships a smaller batch");
    }

    /// <summary>
    /// The receiver flow-control hint is the hard ceiling and always wins:
    /// even with adaptive sizing enabled (and the controller's effective
    /// size at the full ShipBatchSize), an active
    /// <see cref="ReplicationAck.SuggestedBatchSize"/> below the adaptive
    /// size clamps the effective cap to the hinted value.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_receiver_hint_caps_effective_size_even_with_adaptive_enabled()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 10,
            AdaptiveBatchSizingEnabled = true,
            AdaptiveBatchLatencyThreshold = TimeSpan.FromSeconds(5),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 2,
            });

        // Tick 1: one entry, picks up the hint of 2.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.PumpForTestingAsync(CancellationToken.None);

        // Tick 2: three new entries available - the receiver hint of 2
        // must cap the drain even though the adaptive size is at the
        // configured ceiling of 10.
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        feed.Append(MakeEntry("k4", ticks: 4));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "the receiver hint is the hard ceiling and always wins over the adaptive size");
    }

    /// <summary>
    /// The two adaptive-sizing observability instruments
    /// (<see cref="LatticeReplicationMetrics.ShipEffectiveBatchSize"/> and
    /// <see cref="LatticeReplicationMetrics.ShipAckLatency"/>) emit on every
    /// acknowledged batch, tagged by tree and peer, regardless of whether
    /// adaptive sizing is enabled - they are pure observability useful even
    /// with static sizing.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_emits_effective_batch_size_and_ack_latency_metrics()
    {
        using var batchSizeCollector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipEffectiveBatchSizeName);
        using var ackLatencyCollector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipAckLatencyName);

        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 4,
        };
        var (grain, _, feed, _, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(batchSizeCollector.Measurements, Is.Not.Empty,
                "ship.effective_batch_size must emit once per acknowledged batch");
            Assert.That(ackLatencyCollector.Measurements, Is.Not.Empty,
                "ship.ack_latency must emit once per acknowledged batch");
        });

        var batchSample = batchSizeCollector.Measurements.Last();
        Assert.Multiple(() =>
        {
            Assert.That(batchSample.Value, Is.EqualTo(4),
                "the effective cap with no hint and a healthy link is the configured ShipBatchSize ceiling");
            Assert.That(batchSample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == Tree));
            Assert.That(batchSample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == Peer));
        });

        var latencySample = ackLatencyCollector.Measurements.Last();
        Assert.Multiple(() =>
        {
            Assert.That(latencySample.Value, Is.GreaterThanOrEqualTo(0.0),
                "ack latency is a non-negative millisecond reading");
            Assert.That(latencySample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == Tree));
            Assert.That(latencySample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == Peer));
        });
    }
}
