using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Per-peer shared-dictionary negotiation coverage for the shipper. Verifies
/// that with negotiation off the configured dictionary id is stamped verbatim
/// (byte-identical to before the feature), that an unknown or non-matching
/// peer capability falls back to dictionary-less <see cref="LatticeCompression.Zstd"/>
/// (never shipping a frame the peer cannot decode), and that the negotiation
/// adapts once the peer advertises the configured id.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static uint CapturedHeaderDictionaryId(IReplicationTransport transport)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        return batch.EncodedEnvelope!.Value.Header.DictionaryId;
    }

    private static void AdvertiseDictionaryIds(IReplicationTransport transport, params uint[] ids)
    {
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                AdvertisedDictionaryIds = ids,
            });
    }

    [Test]
    public async Task PumpOnceAsync_stamps_configured_dictionary_id_when_negotiation_disabled()
    {
        // Negotiation off: the shipper must stamp the configured dictionary
        // id verbatim regardless of what (if anything) the peer advertises -
        // the bytes on the wire are byte-identical to before the feature.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = false,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_falls_back_to_dictionary_less_zstd_when_peer_capability_unknown()
    {
        // Negotiation on, but the peer has not advertised a dictionary
        // capability (the default ack omits the slot). The shipper must
        // conservatively fall back to dictionary-less Zstd so the peer can
        // always decode the frame.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_stamps_negotiated_dictionary_id_after_peer_advertises_it()
    {
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        AdvertiseDictionaryIds(transport, 7u);

        // Tick 1: capability not yet captured -> conservative fallback. The
        // ack from this tick advertises id 7, captured for the next tick.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });

        // Tick 2: the peer advertised id 7, so the shipper now compresses
        // with the negotiated dictionary.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_falls_back_to_dictionary_less_zstd_when_peer_advertises_a_different_id()
    {
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        AdvertiseDictionaryIds(transport, 3u);

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        // The peer never advertised id 7, so the shipper must keep falling
        // back to dictionary-less Zstd rather than ship an undecodable frame.
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });
    }
}
