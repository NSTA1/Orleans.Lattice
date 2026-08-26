using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the per-peer capability-negotiation memoisation: an idle stream
/// that keeps pumping against a stable peer must negotiate the wire version and
/// shared dictionary exactly once and then reuse the cached plan, so it stops
/// re-recording the process-wide negotiation state and re-emitting the per-tick
/// negotiation metrics on every pump. A genuine input change - the peer
/// advertising a different capability - must re-trigger exactly one recompute,
/// and a peer that has fallen below the supported floor must keep failing fast
/// every tick (a failure is never memoised as a stale success).
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task PumpOnceAsync_wire_version_negotiation_is_memoised_across_idle_pumps()
    {
        // A stable unknown peer down-stamps to the unknown-peer floor. The
        // down-stamp outcome is recorded once per *negotiation*; with the
        // memo in place, five pumps against the unchanged peer must negotiate
        // once and reuse the cached down-stamp plan for every later ship,
        // recording the counter exactly once rather than five times.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.Zstd,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 1,
                UnknownPeerWireVersionFloor = 4,
            });

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        for (var i = 0; i < 5; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
            await grain.PumpForTestingAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.EqualTo(5),
                "every pump had an entry to ship, so negotiation was on the path each tick");
            Assert.That(counter.Measurements, Has.Count.EqualTo(1),
                "the memo must collapse the five per-tick negotiations to a single recompute");
            Assert.That(CapturedHeaderWireVersion(transport), Is.EqualTo(4),
                "later ships reuse the cached down-stamp target");
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.None),
                "later ships reuse the cached compression-dropped plan");
        });
    }

    [Test]
    public async Task PumpOnceAsync_dictionary_negotiation_is_memoised_then_reruns_on_capability_change()
    {
        // Dictionary negotiation records the ship.dictionary_negotiation
        // counter once per *negotiation*. With the memo in place a run of
        // pumps against a peer advertising a stable id set negotiates once;
        // when the peer then advertises a different id set the memo is
        // invalidated and exactly one further negotiation runs.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DictionaryNegotiationName);

        AdvertiseDictionaryIds(transport, 7u);
        for (var i = 0; i < 4; i++)
        {
            feed.Append(MakeEntry($"a{i}", ticks: i + 1));
            await grain.PumpForTestingAsync(CancellationToken.None);
        }

        var stableCount = counter.Measurements.Count;

        // Two more idle pumps against the unchanged capability must not
        // negotiate again.
        for (var i = 0; i < 2; i++)
        {
            feed.Append(MakeEntry($"b{i}", ticks: 100 + i));
            await grain.PumpForTestingAsync(CancellationToken.None);
        }

        Assert.That(counter.Measurements, Has.Count.EqualTo(stableCount),
            "a stable peer capability must not re-run dictionary negotiation");

        // The peer now advertises a different dictionary id set. The next
        // ack captures the change, and exactly one further negotiation runs
        // however many more times we pump.
        AdvertiseDictionaryIds(transport, 7u, 9u);
        for (var i = 0; i < 3; i++)
        {
            feed.Append(MakeEntry($"c{i}", ticks: 200 + i));
            await grain.PumpForTestingAsync(CancellationToken.None);
        }

        Assert.That(counter.Measurements, Has.Count.EqualTo(stableCount + 1),
            "a changed peer capability must re-trigger exactly one recompute, then re-memoise");
    }

    [Test]
    public async Task PumpOnceAsync_below_floor_peer_keeps_failing_fast_and_is_not_memoised()
    {
        // The peer advertises a wire version below the sender's minimum
        // supported floor. Once that capability is captured the shipper must
        // fail fast on every subsequent tick - a memoised *success* from the
        // first (unknown-peer) negotiation must never let a now-unsupported
        // peer receive a frame it cannot decode.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.None,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 5,
                UnknownPeerWireVersionFloor = 5,
            });

        // The peer advertises version 2, below the floor of 5. The first
        // pump negotiates the unknown peer (null capability) to the floor and
        // ships; that ship's ack captures version 2, so every later pump must
        // fail fast.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SupportedWireVersion = 2,
            });

        for (var i = 0; i < 4; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
            await grain.PumpForTestingAsync(CancellationToken.None);
        }

        Assert.That(ShipCallCount(transport), Is.EqualTo(1),
            "only the first (unknown-peer) pump ships; once the below-floor capability "
            + "is captured the shipper re-evaluates and fails fast every tick");
    }
}
