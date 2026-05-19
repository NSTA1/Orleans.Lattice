using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// End-to-end integration coverage for the typed-transport capability
/// seam: stands up a real <see cref="GrpcPushTransport"/> wired to an
/// in-process gRPC receiver via <see cref="TestServer"/>, and proves
/// that a <see cref="ReplicationBatch"/> carrying only the typed
/// <see cref="ReplicationBatchEnvelope"/> (with
/// <see cref="ReplicationBatch.Payload"/> empty - i.e. exactly the
/// shape the outbound shipper emits on its typed fast path) survives
/// a real gRPC round-trip, lands in the receiver-side applier, and
/// produces a correct ack.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TypedReplicationTransportIntegrationTests
{
    private const string TreeName = "tree";
    private const string LocalCluster = "local";
    private const string PeerCluster = "peer";

    private IHost _host = null!;
    private GrpcChannel _channel = null!;
    private IReplicationApplier _applier = null!;
    private IReplicationBatchEncoder _encoder = null!;
    private GrpcPushTransport _transport = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _applier = Substitute.For<IReplicationApplier>();
        _applier.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entry = call.Arg<WalRecord>();
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = entry.Timestamp });
            });
        _applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = call.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                var applied = false;
                foreach (var entry in batch)
                {
                    applied = true;
                    if (entry.Timestamp.CompareTo(max) > 0)
                    {
                        max = entry.Timestamp;
                    }
                }
                return Task.FromResult(new ApplyResult { Applied = applied, HighWaterMark = max });
            });

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddSingleton<IReplicationApplier>(_applier);
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new RecordingEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddLatticeReplicationGrpc();
                    // R-078 coverage is about the typed-transport wire
                    // shape, not the auth gate; relax it so the round-trip
                    // is not gated by the shared-secret authenticator.
                    services.Configure<LatticeReplicationSecurityOptions>(o => o.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeReplicationGrpc());
                });
            });

        _host = await hostBuilder.StartAsync();
        var server = _host.GetTestServer();
        _encoder = _host.Services.GetRequiredService<IReplicationBatchEncoder>();

        // Build the real GrpcPushTransport. The transport caches one
        // GrpcChannel per peer; we inject the TestServer's handler via
        // ConfigureChannel so the channel speaks to the in-process
        // receiver rather than a real socket. AllowPlaintextEndpoints
        // is on because the TestServer's BaseAddress is http://.
        var pushOptions = new GrpcPushTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = LocalCluster,
            PeerEndpoints =
            {
                [PeerCluster] = server.BaseAddress,
            },
            ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = server.CreateHandler();
            },
        };

        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(_encoder, ackSerializer);
        _transport = new GrpcPushTransport(
            method,
            _encoder,
            OptionsFor(pushOptions),
            SecretsStub(),
            OptionsFor(new LatticeReplicationOptions { ClusterId = LocalCluster }));

        // Expose the channel for assertions that do not go through
        // the transport (none today, but keeps the fixture symmetric
        // with GrpcPushTransportIntegrationTests).
        _channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _transport?.Dispose();
        _channel?.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    [SetUp]
    public void SetUp()
    {
        _applier.ClearReceivedCalls();
        if (_encoder is RecordingEncoder recording)
        {
            recording.Reset();
        }
    }

    [Test]
    public async Task GrpcPushTransport_is_assignable_to_ITypedReplicationTransport()
    {
        // The shipper's typed-transport fast path probes via
        // `transport as ITypedReplicationTransport`; pin that the
        // production registration of GrpcPushTransport still matches.
        Assert.That(_transport, Is.AssignableTo<ITypedReplicationTransport>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task SendTypedAsync_round_trips_typed_envelope_through_real_grpc_and_returns_max_hwm()
    {
        ITypedReplicationTransport typed = _transport;
        var hlcA = new HybridLogicalClock { WallClockTicks = 1000, Counter = 0 };
        var hlcB = new HybridLogicalClock { WallClockTicks = 2000, Counter = 0 };

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Entries = new[]
            {
                MakeEntry("a", new byte[] { 1 }, hlcA),
                MakeEntry("b", new byte[] { 2 }, hlcB),
            },
        };

        // This is exactly the shape the outbound shipper emits on its
        // typed fast path post R-078: typed envelope populated, payload
        // empty (no shipper-side encode into _writeBuffer).
        var batch = new ReplicationBatch
        {
            TargetClusterId = PeerCluster,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };

        var ack = await typed.SendTypedAsync(batch, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True, "the receiver applier signalled Applied=true on at least one entry");
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(hlcB), "the ack carries the pointwise-maximum HLC across both entries");
        });

        await _applier.Received(1).ApplyBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(entries =>
                entries.Count == 2
                && entries[0].Key == "a"
                && entries[1].Key == "b"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SendTypedAsync_invokes_canonical_encoder_exactly_once_per_round_trip()
    {
        // The shipper's typed fast path leaves Payload empty, so the
        // transport never asks the encoder to materialise a
        // ReplicationBatchEnvelope from a shipper-supplied byte buffer
        // (BuildEnvelope short-circuits on batch.Envelope). The only
        // encode/decode the canonical encoder sees per round-trip is
        // the pair owned by the gRPC marshaller seam: one Encode on
        // the sender's stream-buffer, one Decode on the receiver's.
        // Without R-078 the shipper would have encoded a second time
        // into its _writeBuffer; this integration test does not stand
        // up the shipper, but pinning "1 encode + 1 decode per round
        // trip" is a regression guard against any future change that
        // adds a redundant encode/decode inside the transport.
        ITypedReplicationTransport typed = _transport;
        var recording = (RecordingEncoder)_encoder;
        recording.Reset();

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Entries = new[]
            {
                MakeEntry("x", new byte[] { 9 }, new HybridLogicalClock { WallClockTicks = 5000, Counter = 0 }),
            },
        };

        var batch = new ReplicationBatch
        {
            TargetClusterId = PeerCluster,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };

        var ack = await typed.SendTypedAsync(batch, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(
                recording.EncodeCalls,
                Is.EqualTo(1),
                "the canonical encoder is invoked exactly once on the sender's gRPC marshaller; the shipper's "
                + "per-tick encode into _writeBuffer is the call R-078 eliminated, and it must not reappear here");
            Assert.That(
                recording.DecodeCalls,
                Is.EqualTo(1),
                "the canonical encoder is invoked exactly once on the receiver's gRPC marshaller to materialise the envelope");
        });
    }

    [Test]
    public async Task SendAsync_and_SendTypedAsync_produce_equivalent_ack_for_typed_envelope_batch()
    {
        // The transport's SendAsync(...) and SendTypedAsync(...) both
        // route through a shared send core. With a typed-envelope batch,
        // both entry points must produce a bit-identical ack so the
        // shipper's choice between them is purely a performance one
        // (skipping the dead encode) with no observable behaviour
        // change on the wire.
        ITypedReplicationTransport typed = _transport;
        IReplicationTransport legacy = _transport;

        var hlc = new HybridLogicalClock { WallClockTicks = 7000, Counter = 3 };
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Entries = new[] { MakeEntry("k", new byte[] { 42 }, hlc) },
        };

        var batchA = new ReplicationBatch
        {
            TargetClusterId = PeerCluster,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };
        var batchB = new ReplicationBatch
        {
            TargetClusterId = PeerCluster,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };

        var ackTyped = await typed.SendTypedAsync(batchA, CancellationToken.None);
        var ackLegacy = await legacy.SendAsync(batchB, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ackTyped.Accepted, Is.EqualTo(ackLegacy.Accepted));
            Assert.That(ackTyped.HighestAppliedHlc, Is.EqualTo(ackLegacy.HighestAppliedHlc));
        });
    }

    [Test]
    public async Task SendTypedAsync_empty_entries_round_trips_as_heartbeat()
    {
        // The shipper sends a zero-entry envelope as a heartbeat /
        // keep-alive. The typed transport must accept that shape just
        // like the bytes-shaped seam did, and return a zero-HLC ack
        // because the applier advances no HWM over an empty batch.
        ITypedReplicationTransport typed = _transport;
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Entries = Array.Empty<WalRecord>(),
        };

        var batch = new ReplicationBatch
        {
            TargetClusterId = PeerCluster,
            TreeName = TreeName,
            OriginClusterId = LocalCluster,
            Payload = ReadOnlyMemory<byte>.Empty,
            Envelope = envelope,
        };

        var ack = await typed.SendTypedAsync(batch, CancellationToken.None);

        Assert.That(
            ack.HighestAppliedHlc,
            Is.EqualTo(HybridLogicalClock.Zero),
            "a zero-entry batch is a heartbeat; the receiver advances no HWM and the ack carries the zero clock");
    }

    private static WalRecord MakeEntry(string key, byte[] value, HybridLogicalClock hlc) => new()
    {
        TreeId = TreeName,
        Op = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = hlc,
        OriginClusterId = LocalCluster,
        Mode = LatticeMergeMode.LwwRegister,
    };

    private static IOptionsMonitor<T> OptionsFor<T>(T value) where T : class
    {
        var monitor = Substitute.For<IOptionsMonitor<T>>();
        monitor.CurrentValue.Returns(value);
        monitor.Get(Arg.Any<string>()).Returns(value);
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

    /// <summary>
    /// Test encoder that delegates to the canonical Orleans serializer
    /// but records every <see cref="Decode(ReadOnlyMemory{byte})"/>
    /// call so the typed-transport round-trip can assert the receiver
    /// never decodes a shipper-supplied byte payload.
    /// </summary>
    private sealed class RecordingEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _serializer;
        private int _decodeCalls;
        private int _encodeCalls;

        public RecordingEncoder(Serializer<ReplicationBatchEnvelope> serializer)
        {
            _serializer = serializer;
        }

        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public int DecodeCalls => Volatile.Read(ref _decodeCalls);
        public int EncodeCalls => Volatile.Read(ref _encodeCalls);

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            Interlocked.Increment(ref _encodeCalls);
            _serializer.Serialize(envelope, writer);
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
        {
            Interlocked.Increment(ref _decodeCalls);
            return _serializer.Deserialize(payload.Span);
        }

        public void Reset()
        {
            Volatile.Write(ref _decodeCalls, 0);
            Volatile.Write(ref _encodeCalls, 0);
        }
    }
}
