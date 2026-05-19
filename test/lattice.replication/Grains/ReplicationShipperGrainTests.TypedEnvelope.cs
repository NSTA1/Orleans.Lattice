using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Pins the shipper's typed-envelope hand-off: every batch handed to
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>
/// must carry both <see cref="ReplicationBatch.Payload"/> (the
/// encoded bytes, for transports that ship them verbatim) and
/// <see cref="ReplicationBatch.Envelope"/> (the pre-built typed
/// envelope, for transports that re-marshal it onto their own wire
/// without a decode-then-re-encode round-trip). This is the seam
/// that lets the gRPC streaming push transport skip the per-send
/// <c>WalRecord[]</c> allocation that would otherwise be paid purely
/// to satisfy the marshaller.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task PumpOnceAsync_populates_typed_envelope_on_batch_passed_to_transport()
    {
        var (grain, _, feed, transport, encoder, _, _) = Create();
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(captured, Is.Not.Null,
            "the shipper must have invoked the transport with a single batch");
        Assert.That(captured!.Value.Envelope, Is.Not.Null,
            "the shipper must populate the typed envelope so transports can skip a decode");
        var envelope = captured.Value.Envelope!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(envelope.WireVersion, Is.EqualTo(encoder.CurrentWireVersion),
                "envelope must carry the encoder's current wire version");
            Assert.That(envelope.TreeName, Is.EqualTo(Tree));
            Assert.That(envelope.OriginClusterId, Is.EqualTo(LocalCluster));
            Assert.That(envelope.Entries, Has.Count.EqualTo(2),
                "envelope must mirror the entries actually encoded into Payload");
            Assert.That(envelope.Entries[0].Key, Is.EqualTo("k1"));
            Assert.That(envelope.Entries[1].Key, Is.EqualTo("k2"));
        });
        Assert.That(captured.Value.Payload.IsEmpty, Is.False,
            "Payload still flows for transports that consume bytes verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_typed_envelope_matches_payload_round_trip()
    {
        // Round-trip pin: the typed envelope the shipper hands to the
        // transport must describe exactly the entries the encoder
        // wrote into Payload. The TestEncoder records the envelope it
        // last received, so this is the cheapest way to assert that
        // the same object identity flowed through both seams.
        var (grain, _, feed, transport, encoder, _, _) = Create();
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(MakeEntry("solo", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Value.Envelope, Is.Not.Null);
        Assert.That(encoder.LastEnvelope, Is.Not.Null,
            "the encoder must have observed exactly one envelope this tick");
        var envelope = captured.Value.Envelope!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(envelope.Entries, Is.SameAs(encoder.LastEnvelope!.Value.Entries),
                "the same entries collection backs both the encoded bytes and the typed slot");
            Assert.That(envelope.WireVersion, Is.EqualTo(encoder.LastEnvelope!.Value.WireVersion));
        });
    }
}
