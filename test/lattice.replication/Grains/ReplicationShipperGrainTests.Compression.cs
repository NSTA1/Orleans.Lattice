using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Threshold-behavior coverage for the per-batch framing-tail
/// compression decision. The shipper stamps
/// <see cref="LatticeCompression"/> on the framing header only when
/// the option is non-<see cref="LatticeCompression.None"/> *and* the
/// accumulated encoded payload meets or exceeds
/// <see cref="LatticeReplicationOptions.FramingCompressionMinBatchBytes"/>.
/// Heartbeat / small-bursty batches must remain uncompressed so the
/// per-batch fixed overhead is amortised only against payloads large
/// enough to recoup it.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static LatticeCompression CapturedHeaderCompression(IReplicationTransport transport)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        return batch.EncodedEnvelope!.Value.Header.Compression;
    }

    [Test]
    public async Task PumpOnceAsync_stamps_none_compression_when_payload_below_threshold()
    {
        // Each encoded entry contributes 4 bytes via the
        // StubWalRecordEncoder; one entry => 4 bytes total, which is
        // strictly below MinBatchBytes=1024, so the header must
        // collapse to LatticeCompression.None even though the option
        // requests Zstd.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.Zstd,
            FramingCompressionMinBatchBytes = 1024,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.None));
    }

    [Test]
    public async Task PumpOnceAsync_stamps_requested_compression_when_payload_meets_threshold()
    {
        // MinBatchBytes=0 forces the threshold check to succeed for
        // any non-empty batch; the header must carry the requested
        // compression tag verbatim.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.Zstd,
            FramingCompressionMinBatchBytes = 0,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
    }

    [Test]
    public async Task PumpOnceAsync_stamps_none_compression_when_option_is_none_regardless_of_threshold()
    {
        // Even when the accumulated payload exceeds the threshold,
        // an explicit None option must dominate; the threshold is a
        // gate on a non-None option, not an override.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.None,
            FramingCompressionMinBatchBytes = 0,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.None));
    }
}

