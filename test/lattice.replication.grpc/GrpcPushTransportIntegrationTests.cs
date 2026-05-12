using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
[Category("Integration")]
public class GrpcPushTransportIntegrationTests
{
    private IHost _host = null!;
    private GrpcChannel _channel = null!;
    private IReplicationApplier _applier = null!;
    private IReplicationBatchEncoder _encoder = null!;
    private Serializer<ReplicationBatchEnvelope> _envSerializer = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _applier = Substitute.For<IReplicationApplier>();

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
                        new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddLatticeReplicationGrpcServer();
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e =>
                    {
                        e.MapLatticeReplicationGrpcService();
                    });
                });
            });

        _host = await hostBuilder.StartAsync();

        var server = _host.GetTestServer();
        _envSerializer = _host.Services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _encoder = _host.Services.GetRequiredService<IReplicationBatchEncoder>();

        var handler = server.CreateHandler();
        _channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = handler,
        });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
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
        _applier.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var entry = callInfo.Arg<WalRecord>();
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = entry.Timestamp });
            });

        // The receiver service drives the applier through ApplyBatchAsync
        // to collapse per-entry HWM round-trips. NSubstitute does not
        // call through the default-interface-method body, so we set up
        // ApplyBatchAsync to mirror the per-entry semantics: walk each
        // entry, return the pointwise-maximum HighWaterMark.
        _applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = callInfo.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                var applied = false;
                foreach (var e in batch)
                {
                    applied = true;
                    if (e.Timestamp.CompareTo(max) > 0)
                    {
                        max = e.Timestamp;
                    }
                }
                return Task.FromResult(new ApplyResult { Applied = applied, HighWaterMark = max });
            });
    }

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    private GrpcPushTransport CreateTransportTo(GrpcChannel channel)
    {
        // Build a transport with a fake options monitor that returns the
        // already-constructed channel via a custom configure callback.
        // We bypass ResolveChannel's GrpcChannel.ForAddress by exposing
        // the test channel through PeerEndpoints + a derivation: we cannot
        // inject the channel directly since ResolveChannel constructs its
        // own. So instead, we directly invoke the Push RPC through a
        // CallInvoker built on the test channel. The transport itself is
        // covered by GrpcPushTransportTests; this fixture's job is to
        // verify the receiver-side service binding is wired correctly.
        throw new NotSupportedException("Use direct CallInvoker against _channel.");
    }

    [Test]
    public async Task Push_round_trips_an_envelope_and_returns_max_hwm()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var hlcA = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var hlcB = new HybridLogicalClock { WallClockTicks = 200, Counter = 0 };

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[]
            {
                new WalRecord { TreeId = "tree", Op = MutationKind.Set, Key = "a", Value = new byte[] { 1 }, Timestamp = hlcA, OriginClusterId = "remote", Mode = LatticeMergeMode.LwwRegister },
                new WalRecord { TreeId = "tree", Op = MutationKind.Set, Key = "b", Value = new byte[] { 2 }, Timestamp = hlcB, OriginClusterId = "remote", Mode = LatticeMergeMode.LwwRegister },
            },
        };

        var box = new ReplicationBatchEnvelopeBox { Value = envelope };
        using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: default, request: box);
        var ackBox = await call.ResponseAsync;

        Assert.Multiple(() =>
        {
            Assert.That(ackBox.Value.Accepted, Is.True);
            Assert.That(ackBox.Value.HighestAppliedHlc, Is.EqualTo(hlcB));
        });
        // Service collapses per-entry calls into a single ApplyBatchAsync.
        await _applier.Received(1).ApplyBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(list => list.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Push_returns_zero_hwm_for_empty_batch_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                WireVersion = 1,
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: default, request: box);
        var ackBox = await call.ResponseAsync;

        Assert.Multiple(() =>
        {
            Assert.That(ackBox.Value.Accepted, Is.True);
            Assert.That(ackBox.Value.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}


