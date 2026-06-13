using NSubstitute;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Wire-version down-stamp coverage for the shipper during a rolling
/// upgrade. Verifies that a compressed last-writer-wins tree negotiating an
/// older peer keeps replicating by dropping framing compression for that
/// peer's batch (lossless), that the two genuinely un-down-encodable shapes
/// (CRDT mode, sub-floor target) pause rather than ship and surface a
/// metered signal on the
/// <see cref="LatticeReplicationMetrics.ShipWireVersionDownStamp"/> counter
/// instead of silently stalling, and that a same-version peer is a verbatim
/// no-op that neither drops compression nor records the counter.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static int ShipCallCount(IReplicationTransport transport)
        => transport.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync));

    private static int CapturedHeaderWireVersion(IReplicationTransport transport)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        return batch.EncodedEnvelope!.Value.Header.WireVersion;
    }

    private static ILatticeMergeModeResolver ModeResolver(LatticeMergeMode? mode)
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns(mode);
        return resolver;
    }

    [Test]
    public async Task PumpOnceAsync_compressed_lww_down_stamp_drops_compression_and_ships()
    {
        // A compressed last-writer-wins tree negotiating a peer below the
        // current wire version must keep replicating: the shipper drops
        // framing compression for this peer's batch (lossless) instead of
        // stalling, and records the compression_dropped outcome.
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
        feed.Append(MakeEntry("k", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.EqualTo(1),
                "a compression-only down-stamp blocker must not stall the stream");
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.None),
                "framing compression must be dropped for the down-stamped peer");
            Assert.That(CapturedHeaderWireVersion(transport), Is.EqualTo(4));
            var measurements = counter.Measurements;
            Assert.That(measurements, Has.Count.EqualTo(1));
            var m = measurements.Single();
            Assert.That(m.Value, Is.EqualTo(1));
            Assert.That(
                m.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagReason
                    && (string?)t.Value == LatticeReplicationMetrics.DownStampReasonCompressionDropped),
                Is.True,
                "the counter must be tagged reason=compression_dropped");
        });
    }

    [Test]
    public async Task PumpOnceAsync_crdt_mode_down_stamp_does_not_ship_and_records_blocked_crdt_mode()
    {
        // A CRDT-mode tree cannot be faithfully down-encoded for a
        // pre-current-version receiver: the shipper must pause (no ship,
        // backoff) and record the blocked_crdt_mode outcome rather than
        // silently stalling.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.None,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 1,
                UnknownPeerWireVersionFloor = 4,
            },
            modeResolver: ModeResolver(LatticeMergeMode.OrSet));
        feed.Append(MakeEntry("k", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.Zero,
                "a CRDT-mode down-stamp must pause rather than ship a mis-applyable frame");
            var measurements = counter.Measurements;
            Assert.That(measurements, Has.Count.EqualTo(1));
            Assert.That(
                measurements.Single().Tags.Any(t => t.Key == LatticeReplicationMetrics.TagReason
                    && (string?)t.Value == LatticeReplicationMetrics.DownStampReasonBlockedCrdtMode),
                Is.True,
                "the counter must be tagged reason=blocked_crdt_mode");
        });
    }

    [Test]
    public async Task PumpOnceAsync_sub_floor_down_stamp_does_not_ship_and_records_blocked_unsupported_version()
    {
        // A target below the down-encode floor cannot be made decodable for
        // the peer: the shipper must pause and record
        // blocked_unsupported_version.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.None,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 1,
                UnknownPeerWireVersionFloor = 3,
            });
        feed.Append(MakeEntry("k", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.Zero,
                "a sub-floor down-stamp must pause rather than ship a corrupt frame");
            var measurements = counter.Measurements;
            Assert.That(measurements, Has.Count.EqualTo(1));
            Assert.That(
                measurements.Single().Tags.Any(t => t.Key == LatticeReplicationMetrics.TagReason
                    && (string?)t.Value == LatticeReplicationMetrics.DownStampReasonBlockedUnsupportedVersion),
                Is.True,
                "the counter must be tagged reason=blocked_unsupported_version");
        });
    }

    [Test]
    public async Task PumpOnceAsync_uncompressed_lww_down_stamp_ships_without_recording_counter()
    {
        // An uncompressed last-writer-wins tree down-stamping to an
        // otherwise down-encodable target needs no degrade: it ships at the
        // negotiated version and the down-stamp counter stays silent.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.None,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 1,
                UnknownPeerWireVersionFloor = 4,
            });
        feed.Append(MakeEntry("k", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.EqualTo(1));
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.None));
            Assert.That(CapturedHeaderWireVersion(transport), Is.EqualTo(4));
            Assert.That(counter.Measurements, Is.Empty,
                "no degrade is needed, so the down-stamp counter must not fire");
        });
    }

    [Test]
    public async Task PumpOnceAsync_same_version_peer_ships_verbatim_without_dropping_compression()
    {
        // A same-version peer (no downgrade) is a true verbatim no-op: the
        // configured framing compression survives, the header carries the
        // current wire version, and the down-stamp counter never fires.
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.Zstd,
                FramingCompressionMinBatchBytes = 0,
                WireVersionNegotiationEnabled = true,
                MinimumSupportedWireVersion = 1,
                UnknownPeerWireVersionFloor = EncodedBatchHeader.CurrentWireVersion,
            });
        feed.Append(MakeEntry("k", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.EqualTo(1));
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd),
                "a same-version peer must keep the configured compression");
            Assert.That(CapturedHeaderWireVersion(transport),
                Is.EqualTo(EncodedBatchHeader.CurrentWireVersion));
            Assert.That(counter.Measurements, Is.Empty);
        });
    }
}
