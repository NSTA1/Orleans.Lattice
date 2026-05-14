using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// gRPC mirror of the R-086 transport metadata pass-through contract:
/// asserts that the canonical
/// <see cref="GrpcPushTransport"/> + receiver-side service round-trips
/// <see cref="WalRecord.VectorClock"/> and
/// <see cref="WalRecord.DependencySummary"/> verbatim across the wire.
/// Captures the entries delivered to the receiver-side
/// <see cref="IReplicationApplier"/> and compares the metadata slots
/// against the producer-side originals.
/// </summary>
[TestFixture]
[Category("Integration")]
public class TransportMetadataPassthroughContractTests
{
    private IHost _host = null!;
    private GrpcChannel _channel = null!;
    private IReplicationApplier _applier = null!;
    private IReplicationBatchEncoder _encoder = null!;
    private ConcurrentQueue<WalRecord> _capturedEntries = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _capturedEntries = new ConcurrentQueue<WalRecord>();
        _applier = Substitute.For<IReplicationApplier>();
        _applier.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var entry = callInfo.Arg<WalRecord>();
                _capturedEntries.Enqueue(entry);
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = entry.Timestamp });
            });

        // The receiver-side service collapses per-entry HWM grain RPCs
        // into a single ApplyBatchAsync call per inbound push. NSubstitute
        // does not route through the default-interface-method body, so we
        // set ApplyBatchAsync up explicitly to walk every entry through
        // the same capture closure as the per-entry path. This keeps the
        // metadata-pass-through assertions pointed at the captured queue
        // regardless of whether the service decides to dispatch per-entry
        // or per-batch in the future.
        _applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = callInfo.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                var applied = false;
                foreach (var entry in batch)
                {
                    _capturedEntries.Enqueue(entry);
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
                        new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddLatticeReplicationGrpcServer();
                    // This contract fixture validates the transport metadata
                    // pass-through round-trip rather than the shared-secret
                    // authenticator; disable the receiver-side auth gate so
                    // the metadata assertions are not blocked by the gate.
                    services.Configure<LatticeReplicationSecurityOptions>(o => o.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeReplicationGrpcService());
                });
            });

        _host = await hostBuilder.StartAsync();

        var server = _host.GetTestServer();
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
        _capturedEntries.Clear();
    }

    private static VersionVector MakeVector(params (string origin, long wallClock, int counter)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (origin, wallClock, counter) in entries)
        {
            vc.Entries[origin] = new HybridLogicalClock { WallClockTicks = wallClock, Counter = counter };
        }
        return vc;
    }

    private static WalRecord MakeEntry(
        string key,
        long wallClock,
        VersionVector? vectorClock,
        VersionVector? dependencySummary)
        => new()
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1, 2, 3 },
            Timestamp = new HybridLogicalClock { WallClockTicks = wallClock, Counter = 0 },
            OriginClusterId = "remote",
            Mode = LatticeMergeMode.LwwRegister,
            VectorClock = vectorClock,
            DependencySummary = dependencySummary,
        };

    private async Task PushAsync(ReplicationBatchEnvelope envelope)
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();
        var box = new ReplicationBatchEnvelopeBox { Value = envelope };
        using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: default, request: box);
        await call.ResponseAsync;
    }

    [Test]
    public async Task GrpcPushTransport_preserves_vector_clock_and_dependency_summary()
    {
        var vectorClock = MakeVector(("site-a", 100, 0), ("site-b", 200, 1));
        var dependencySummary = MakeVector(("site-a", 100, 0), ("site-b", 200, 1));

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[] { MakeEntry("k", 100, vectorClock, dependencySummary) },
        };

        await PushAsync(envelope);

        Assert.That(_capturedEntries, Has.Count.EqualTo(1));
        var received = _capturedEntries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(received.VectorClock, Is.Not.Null);
            Assert.That(received.VectorClock!.Entries, Is.EqualTo(vectorClock.Entries));
            Assert.That(received.DependencySummary, Is.Not.Null);
            Assert.That(received.DependencySummary!.Entries, Is.EqualTo(dependencySummary.Entries));
        });
    }

    [Test]
    public async Task GrpcPushTransport_preserves_null_metadata_for_legacy_entries()
    {
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[] { MakeEntry("k", 100, vectorClock: null, dependencySummary: null) },
        };

        await PushAsync(envelope);

        var received = _capturedEntries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(received.VectorClock, Is.Null);
            Assert.That(received.DependencySummary, Is.Null);
        });
    }

    [Test]
    public async Task GrpcPushTransport_preserves_independent_vector_clock_and_dependency_summary()
    {
        var vectorClock = MakeVector(("site-a", 100, 0), ("site-b", 200, 0));
        var dependencySummary = MakeVector(("site-a", 50, 0));

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[] { MakeEntry("k", 100, vectorClock, dependencySummary) },
        };

        await PushAsync(envelope);

        var received = _capturedEntries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(received.VectorClock!.Entries, Has.Count.EqualTo(2));
            Assert.That(received.DependencySummary!.Entries, Has.Count.EqualTo(1));
            Assert.That(received.VectorClock.GetClock("site-b").WallClockTicks, Is.EqualTo(200L));
            Assert.That(received.DependencySummary.Entries.ContainsKey("site-b"), Is.False);
        });
    }

    [Test]
    public async Task GrpcPushTransport_preserves_per_entry_vector_clocks_in_a_multi_entry_batch()
    {
        var vc1 = MakeVector(("site-a", 100, 0));
        var vc2 = MakeVector(("site-a", 200, 0), ("site-b", 50, 0));
        var vc3 = MakeVector(("site-b", 75, 0));

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[]
            {
                MakeEntry("k1", 100, vc1, vc1),
                MakeEntry("k2", 200, vc2, vc2),
                MakeEntry("k3", 300, vc3, vc3),
            },
        };

        await PushAsync(envelope);

        var received = _capturedEntries.ToArray();
        Assert.That(received, Has.Length.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(received[0].VectorClock!.Entries, Is.EqualTo(vc1.Entries));
            Assert.That(received[1].VectorClock!.Entries, Is.EqualTo(vc2.Entries));
            Assert.That(received[2].VectorClock!.Entries, Is.EqualTo(vc3.Entries));
            Assert.That(received[0].DependencySummary!.Entries, Is.EqualTo(vc1.Entries));
            Assert.That(received[1].DependencySummary!.Entries, Is.EqualTo(vc2.Entries));
            Assert.That(received[2].DependencySummary!.Entries, Is.EqualTo(vc3.Entries));
        });
    }

    [Test]
    public async Task GrpcPushTransport_preserves_entry_order_in_a_multi_entry_batch()
    {
        var vc = MakeVector(("site-a", 100, 0));
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = new[]
            {
                MakeEntry("k1", 100, vc, vc),
                MakeEntry("k2", 200, vc, vc),
                MakeEntry("k3", 300, vc, vc),
                MakeEntry("k4", 400, vc, vc),
            },
        };

        await PushAsync(envelope);

        var received = _capturedEntries.ToArray();
        Assert.That(received.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2", "k3", "k4" }));
        Assert.That(
            received.Select(e => e.Timestamp.WallClockTicks),
            Is.EqualTo(new[] { 100L, 200L, 300L, 400L }));
    }

    /// <summary>
    /// Minimal in-test <see cref="IReplicationBatchEncoder"/> that
    /// frames the envelope through the Orleans serializer. Mirrors the
    /// canonical
    /// <c>OrleansBinaryReplicationBatchEncoder</c> behaviour but is
    /// declared locally so the gRPC test project (which does not have
    /// <c>InternalsVisibleTo</c> against the replication assembly) can
    /// instantiate it directly.
    /// </summary>
    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
            => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => _s.Deserialize(payload.Span);
    }
}