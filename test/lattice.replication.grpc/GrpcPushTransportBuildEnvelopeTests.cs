using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Covers the three branches of <c>GrpcPushTransport.BuildEnvelope</c>:
/// (1) typed-envelope fast path - shipper supplied
/// <see cref="ReplicationBatch.Envelope"/>, so the transport ships it
/// verbatim and does not invoke
/// <see cref="IReplicationBatchEncoder.Decode(ReadOnlyMemory{byte})"/>;
/// (2) heartbeat / empty-payload shortcut - neither envelope nor bytes
/// were supplied, so the transport synthesises an empty-entries
/// envelope without touching the encoder; and (3) legacy bytes-shaped
/// fallback - the caller supplied bytes but no envelope, so the
/// transport decodes through the canonical encoder. Branch (1) is the
/// reason the per-send <c>Decode</c> allocation goes away on the
/// shipper hot path.
/// </summary>
[TestFixture]
public class GrpcPushTransportBuildEnvelopeTests
{
    private sealed class CountingDecoder : IReplicationBatchEncoder
    {
        public int DecodeCalls;
        public ReplicationBatchEnvelope DecodeReturns { get; set; }
        public string ContentType => "test/counting";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) { }
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
        {
            DecodeCalls++;
            return DecodeReturns;
        }
    }

    private static IOptionsMonitor<GrpcPushTransportOptions> OptionsFor(GrpcPushTransportOptions opts)
    {
        var monitor = Substitute.For<IOptionsMonitor<GrpcPushTransportOptions>>();
        monitor.CurrentValue.Returns(opts);
        return monitor;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptionsFor(string clusterId = "self")
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    private static IReplicationSecretProvider SecretsStub()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("secret"));
        s.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(
                new LatticeReplicationAcceptedSecrets(new[] { "secret" }, "v1")));
        s.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        return s;
    }

    private static (GrpcPushTransport transport, CountingDecoder encoder) CreateTransport()
    {
        var encoder = new CountingDecoder();
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(
            encoder,
            new OrleansBinaryWalRecordEncoder(sp.GetRequiredService<Serializer<WalRecord>>()),
            ackSerializer,
            sp.GetRequiredService<Serializer<DigestProbeRequest>>(),
            sp.GetRequiredService<Serializer<DigestProbeResponse>>(),
            sp.GetRequiredService<Serializer<ContentManifestRequest>>(),
            sp.GetRequiredService<Serializer<ContentManifestResponse>>());
        var transport = new GrpcPushTransport(
            method,
            encoder,
            OptionsFor(new GrpcPushTransportOptions { AllowPlaintextEndpoints = true }),
            SecretsStub(),
            ReplicationOptionsFor());
        return (transport, encoder);
    }

    private static ReplicationBatchEnvelope BuildEnvelope(int entries = 1)
    {
        var list = new List<WalRecord>(entries);
        for (var i = 0; i < entries; i++)
        {
            list.Add(new WalRecord
            {
                TreeId = "orders",
                Op = MutationKind.Set,
                Key = "k-" + i,
                Value = new byte[] { (byte)i },
            });
        }
        return new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "orders",
            OriginClusterId = "self",
            Entries = list,
        };
    }

    [Test]
    public void BuildEnvelope_uses_typed_slot_when_supplied_and_does_not_decode()
    {
        var (transport, encoder) = CreateTransport();
        using var _ = transport;
        var envelope = BuildEnvelope();
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "orders",
            OriginClusterId = "self",
            // Non-empty payload to prove the typed slot wins over the
            // legacy decode branch even when bytes are present.
            Payload = new byte[] { 0xAA, 0xBB, 0xCC },
            Envelope = envelope,
        };

        var actual = transport.BuildEnvelopeForTesting(batch);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.DecodeCalls, Is.Zero,
                "the typed-envelope fast path must not invoke the decoder");
            Assert.That(actual.WireVersion, Is.EqualTo(envelope.WireVersion));
            Assert.That(actual.TreeName, Is.EqualTo(envelope.TreeName));
            Assert.That(actual.OriginClusterId, Is.EqualTo(envelope.OriginClusterId));
            Assert.That(actual.Entries, Is.SameAs(envelope.Entries),
                "the entries collection is handed through by reference (zero-copy)");
        });
    }

    [Test]
    public void BuildEnvelope_returns_empty_entries_envelope_for_heartbeat_and_does_not_decode()
    {
        var (transport, encoder) = CreateTransport();
        using var _ = transport;
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "orders",
            OriginClusterId = "self",
            Payload = ReadOnlyMemory<byte>.Empty,
            // No Envelope; empty payload triggers the heartbeat path.
        };

        var actual = transport.BuildEnvelopeForTesting(batch);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.DecodeCalls, Is.Zero,
                "heartbeat envelopes are synthesised without the decoder");
            Assert.That(actual.WireVersion, Is.EqualTo(ReplicationBatchEnvelope.CurrentVersion));
            Assert.That(actual.TreeName, Is.EqualTo("orders"));
            Assert.That(actual.OriginClusterId, Is.EqualTo("self"));
            Assert.That(actual.Entries, Is.Not.Null);
            Assert.That(actual.Entries, Is.Empty);
        });
    }

    [Test]
    public void BuildEnvelope_falls_back_to_decode_when_envelope_is_null_and_payload_is_present()
    {
        var (transport, encoder) = CreateTransport();
        using var _ = transport;
        var decoded = BuildEnvelope(entries: 3);
        encoder.DecodeReturns = decoded;
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "orders",
            OriginClusterId = "self",
            Payload = new byte[] { 1, 2, 3, 4, 5 },
            // No Envelope -> legacy bytes path must decode.
        };

        var actual = transport.BuildEnvelopeForTesting(batch);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.DecodeCalls, Is.EqualTo(1),
                "the legacy bytes-shaped seam must decode exactly once per call");
            Assert.That(actual.Entries, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void BuildEnvelope_typed_slot_wins_even_when_payload_is_empty()
    {
        // The shipper may supply an envelope alongside an empty
        // bytes-buffer (e.g. for a zero-entry envelope that still
        // carries routing metadata). The typed slot must still win
        // over the heartbeat shortcut so the caller's exact wire
        // version is preserved.
        var (transport, encoder) = CreateTransport();
        using var _ = transport;
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 42,
            TreeName = "orders",
            OriginClusterId = "self",
            Entries = Array.Empty<WalRecord>(),
        };
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "orders",
            OriginClusterId = "self",
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };

        var actual = transport.BuildEnvelopeForTesting(batch);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.DecodeCalls, Is.Zero);
            Assert.That(actual.WireVersion, Is.EqualTo(42),
                "caller-supplied envelope must override the heartbeat-default wire version");
        });
    }

    [Test]
    public void BuildEnvelopeBox_routes_populated_EncodedEnvelope_through_the_framing_slot()
    {
        // Stage 4b wired the framing-only fast path: a populated
        // EncodedEnvelope is now consumed straight by the gRPC
        // marshaller via the framing slot on the envelope box, and
        // the typed envelope decode is never invoked. Pin both
        // halves: the box surfaces the framing payload, and the
        // legacy decode counter stays zero.
        var (transport, encoder) = CreateTransport();
        using var _ = transport;
        var encoded = new ReplicationBatchEncodedEnvelope
        {
            Header = new EncodedBatchHeader
            {
                Magic = EncodedBatchHeader.MagicValue,
                WireVersion = EncodedBatchHeader.CurrentWireVersion,
                OriginClusterIdHash = EncodedBatchHeader.HashClusterId("self"),
                EntryCount = 0,
            },
            EncodedEntries = ReadOnlyMemory<ArraySegment<byte>>.Empty,
        };
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "orders",
            OriginClusterId = "self",
            // No bytes / typed envelope; the framing slot is the only
            // thing the caller supplied.
            EncodedEnvelope = encoded,
        };

        var box = transport.BuildEnvelopeBoxForTesting(batch);

        Assert.Multiple(() =>
        {
            Assert.That(box.Framing, Is.Not.Null,
                "framing slot must be populated when EncodedEnvelope is supplied");
            Assert.That(box.Framing!.Value.Header, Is.EqualTo(encoded.Header));
            Assert.That(box.Framing.Value.TreeName, Is.EqualTo("orders"));
            Assert.That(box.Framing.Value.OriginClusterId, Is.EqualTo("self"));
            Assert.That(encoder.DecodeCalls, Is.Zero,
                "the framing-only slot must not fall through to the typed decode branch");
        });
    }
}
