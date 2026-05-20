using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Pins the dead-encode elimination introduced for the typed-transport
/// fast path. When the configured <see cref="IReplicationTransport"/>
/// also implements <see cref="ITypedReplicationTransport"/>, the shipper
/// must skip the per-tick
/// <see cref="IReplicationBatchEncoder.Encode(ReplicationBatchEnvelope, System.Buffers.IBufferWriter{byte})"/>
/// call that previously populated <see cref="ReplicationBatch.Payload"/>
/// purely for legacy bytes-only transports to read.
/// </summary>
[TestFixture]
public sealed class ReplicationShipperGrainTypedTransportSkipsEncodeTests
{
    private const string Tree = "tree-a";
    private const string Peer = "peer-cluster";
    private const string LocalCluster = "local-cluster";

    private sealed class CountingTypedTransport : ITypedReplicationTransport
    {
        public int TypedCalls { get; private set; }
        public int LegacyCalls { get; private set; }
        public ReplicationBatch? LastBatch { get; private set; }

        public Task<ReplicationAck> SendTypedAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            TypedCalls++;
            LastBatch = batch;
            return Task.FromResult(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
            });
        }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            LegacyCalls++;
            LastBatch = batch;
            return Task.FromResult(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
            });
        }
    }

    private sealed class CountingEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;
        public int Encodes { get; private set; }
        public int Decodes { get; private set; }

        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            Encodes++;
            writer.Write(new byte[] { 9, 9, 9 });
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
        {
            Decodes++;
            return default;
        }
    }

    private sealed class StubShardGrain : IWalShardGrain
    {
        public List<WalRecord> Entries { get; } = new();

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.FromResult((long)(Entries.Count - 1));
        }

        public Task<IReadOnlyList<long>> AppendBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            var offsets = new long[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                Entries.Add(entries[i]);
                offsets[i] = Entries.Count - 1;
            }
            return Task.FromResult<IReadOnlyList<long>>(offsets);
        }

        public Task<WalShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(WalShardPage.Empty(fromSequence));
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var entries = new WalShardSequencedEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                entries[i] = new WalShardSequencedEntry
                {
                    Sequence = seq,
                    Entry = Entries[(int)seq],
                };
            }
            return Task.FromResult(new WalShardPage
            {
                Entries = entries,
                NextSequence = endExclusive,
            });
        }

        public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

        public Task<long> GetLiveEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

#pragma warning disable LATTICE0001 // GetEntryCountAsync is an obsolete forwarder retained for one minor version.
        public Task<long> GetEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);
#pragma warning restore LATTICE0001
    }

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (ReplicationShipperGrain Grain, StubShardGrain Feed, CountingTypedTransport Transport, CountingEncoder Encoder) Create(
        IReplicationTransport? transportOverride = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor();
        var feed = new StubShardGrain();
        var typed = new CountingTypedTransport();
        var transport = transportOverride ?? typed;
        var encoder = new CountingEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(feed);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, registry, factory, fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, feed, typed, encoder);
    }

    private static WalRecord MakeEntry(string key, long ticks) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
        OriginClusterId = LocalCluster,
    };

    [Test]
    public async Task PumpOnceAsync_skips_encode_when_transport_implements_ITypedReplicationTransport()
    {
        var (grain, feed, transport, encoder) = Create();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));
        feed.Entries.Add(MakeEntry("k2", ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(transport.TypedCalls, Is.EqualTo(1),
                "shipper must route through SendTypedAsync when the transport implements the typed surface");
            Assert.That(transport.LegacyCalls, Is.EqualTo(0),
                "shipper must not also call the legacy SendAsync overload");
            Assert.That(encoder.Encodes, Is.EqualTo(0),
                "shipper must skip the dead encode into _writeBuffer when the typed transport will consume the envelope directly");
        });
    }

    [Test]
    public async Task PumpOnceAsync_leaves_payload_empty_when_transport_is_typed()
    {
        var (grain, feed, transport, _) = Create();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(transport.LastBatch, Is.Not.Null);
        Assert.That(transport.LastBatch!.Value.Payload.IsEmpty, Is.True,
            "Payload must be empty on the typed-transport path; the envelope slot is the authoritative payload");
        Assert.That(transport.LastBatch!.Value.Envelope, Is.Not.Null,
            "Envelope must still be populated so the typed transport can consume it");
        Assert.That(transport.LastBatch!.Value.Envelope!.Value.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task PumpOnceAsync_still_encodes_when_transport_is_bytes_only()
    {
        // Bytes-only transport (Substitute.For<IReplicationTransport> does
        // not also implement ITypedReplicationTransport) - the shipper
        // must still encode into Payload so the legacy seam keeps working.
        var bytesOnly = Substitute.For<IReplicationTransport>();
        ReplicationBatch? captured = null;
        bytesOnly.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });

        var (grain, feed, _, encoder) = Create(transportOverride: bytesOnly);
        feed.Entries.Add(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.Encodes, Is.EqualTo(1),
                "shipper must encode into _writeBuffer when the transport is bytes-only");
            Assert.That(captured, Is.Not.Null);
            Assert.That(captured!.Value.Payload.IsEmpty, Is.False,
                "Payload must carry encoded bytes for bytes-only transports");
            Assert.That(captured.Value.Envelope, Is.Not.Null,
                "Envelope must still be populated alongside Payload (additive contract)");
        });
    }

    [Test]
    public async Task PumpOnceAsync_typed_envelope_matches_envelope_bytes_round_trip()
    {
        // Typed-transport fast-path acceptance criterion: the typed
        // envelope the shipper hands to a typed transport must describe
        // exactly the entries
        // the encoder would have written into Payload on the bytes-only
        // path. Use the canonical Orleans-binary encoder to round-trip
        // the bytes side; assert field-for-field equality with the
        // typed-path envelope.
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        try
        {
            var serializer = services.GetRequiredService<Orleans.Serialization.Serializer<ReplicationBatchEnvelope>>();
            var canonical = new OrleansBinaryReplicationBatchEncoder(serializer);

            var typedSink = new CountingTypedTransport();
            var bytesSink = Substitute.For<IReplicationTransport>();
            ReplicationBatch? bytesBatch = null;
            bytesSink.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    bytesBatch = call.Arg<ReplicationBatch>();
                    return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
                });

            var typedGrain = BuildGrainWith(typedSink, canonical, out var typedFeed);
            typedFeed.Entries.Add(MakeEntry("k1", ticks: 1));
            typedFeed.Entries.Add(MakeEntry("k2", ticks: 2));
            await typedGrain.OnDoorbellAsync(CancellationToken.None);

            var bytesGrain = BuildGrainWith(bytesSink, canonical, out var bytesFeed);
            bytesFeed.Entries.Add(MakeEntry("k1", ticks: 1));
            bytesFeed.Entries.Add(MakeEntry("k2", ticks: 2));
            await bytesGrain.OnDoorbellAsync(CancellationToken.None);

            Assert.That(typedSink.LastBatch, Is.Not.Null);
            var typedEnvelope = typedSink.LastBatch!.Value.Envelope!.Value;

            Assert.That(bytesBatch, Is.Not.Null);
            var decoded = canonical.Decode(bytesBatch!.Value.Payload);

            Assert.Multiple(() =>
            {
                Assert.That(typedEnvelope.WireVersion, Is.EqualTo(decoded.WireVersion));
                Assert.That(typedEnvelope.TreeName, Is.EqualTo(decoded.TreeName));
                Assert.That(typedEnvelope.OriginClusterId, Is.EqualTo(decoded.OriginClusterId));
                Assert.That(typedEnvelope.Entries, Has.Count.EqualTo(decoded.Entries.Count));
                for (var i = 0; i < typedEnvelope.Entries.Count; i++)
                {
                    Assert.That(typedEnvelope.Entries[i].Key, Is.EqualTo(decoded.Entries[i].Key));
                    Assert.That(typedEnvelope.Entries[i].Timestamp, Is.EqualTo(decoded.Entries[i].Timestamp));
                    Assert.That(typedEnvelope.Entries[i].OriginClusterId, Is.EqualTo(decoded.Entries[i].OriginClusterId));
                }
            });
        }
        finally
        {
            services.Dispose();
        }
    }

    private static ReplicationShipperGrain BuildGrainWith(
        IReplicationTransport transport,
        IReplicationBatchEncoder encoder,
        out StubShardGrain feed)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor();
        feed = new StubShardGrain();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = Substitute.For<IGrainFactory>();
        var localFeed = feed;
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(localFeed);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, registry, factory, fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(Tree, Peer);
        return grain;
    }
}
