using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
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
using NSubstitute.ExceptionExtensions;
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
    private IGrainFactory _grainFactory = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _applier = Substitute.For<IReplicationApplier>();
        _grainFactory = Substitute.For<IGrainFactory>();

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
                    services.AddSingleton(_grainFactory);
                    // Register a dictionary provider holding one installed
                    // dictionary so the pull RPC has a held id to serve.
                    var dictionaryProvider = new AutoTrainingCompressionDictionaryProvider(
                        new CompressionDictionaryTrainingOptions { Enabled = true });
                    dictionaryProvider.TryInstall(8u, PulledDictionaryBytes);
                    services.AddSingleton<ILatticeCompressionDictionaryProvider>(dictionaryProvider);
                    services.AddLatticeReplicationGrpc();
                    // This fixture validates the wire shape rather than the
                    // shared-secret authenticator; disable the receiver-side
                    // auth gate so the metadata round-trip assertions are not
                    // blocked by the gate.
                    services.Configure<LatticeReplicationSecurityOptions>(o => o.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e =>
                    {
                        e.MapLatticeReplicationGrpc();
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

    private static readonly byte[] PulledDictionaryBytes = { 10, 20, 30, 40, 50, 60 };

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
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
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
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
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

    [Test]
    public async Task PullCompressionDictionary_round_trips_a_held_dictionary_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var box = new CompressionDictionaryPullRequestBox
        {
            Value = new CompressionDictionaryPullRequest { DictionaryId = 8u },
        };

        using var call = invoker.AsyncUnaryCall(method.PullCompressionDictionary, host: null, options: default, request: box);
        var response = await call.ResponseAsync;

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.ExchangeSupported, Is.True);
            Assert.That(response.Value.Found, Is.True);
            Assert.That(response.Value.DictionaryId, Is.EqualTo(8u));
            Assert.That(response.Value.Dictionary.ToArray(), Is.EqualTo(PulledDictionaryBytes));
            Assert.That(
                response.Value.Fingerprint,
                Is.EqualTo(CompressionDictionaryFingerprint.Compute(PulledDictionaryBytes)));
        });
    }

    [Test]
    public async Task PullCompressionDictionary_returns_not_held_for_an_unknown_id_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var box = new CompressionDictionaryPullRequestBox
        {
            Value = new CompressionDictionaryPullRequest { DictionaryId = 1234u },
        };

        using var call = invoker.AsyncUnaryCall(method.PullCompressionDictionary, host: null, options: default, request: box);
        var response = await call.ResponseAsync;

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.ExchangeSupported, Is.True);
            Assert.That(response.Value.Found, Is.False);
        });
    }

    [Test]
    public async Task ProbeMerkleWalk_round_trips_a_range_digest_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var digest = new LeafProjectionDigest
        {
            Hash = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
            EntryCount = 7,
            CheckpointOffset = 3,
            Version = LeafProjectionDigest.CurrentVersion,
        };
        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestForRangeAsync(2, "a", "m", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(digest));
        _grainFactory.GetGrain<ILattice>("tree").Returns(lattice);

        var box = new MerkleWalkProbeRequestBox
        {
            Value = new MerkleWalkProbeRequest
            {
                TreeName = "tree",
                ShardIndex = 2,
                RangeStartKey = "a",
                RangeEndKey = "m",
                Depth = 1,
            },
        };

        using var call = invoker.AsyncUnaryCall(method.ProbeMerkleWalk, host: null, options: default, request: box);
        var response = await call.ResponseAsync;

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.Available, Is.True);
            Assert.That(response.Value.Digest.EntryCount, Is.EqualTo(7));
            Assert.That(response.Value.Digest.Hash, Is.EqualTo(digest.Hash));
        });
    }

    [Test]
    public async Task ProbeMerkleWalk_reports_unavailable_when_digest_disabled_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestForRangeAsync(0, null, null, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("digest disabled"));
        _grainFactory.GetGrain<ILattice>("disabled-tree").Returns(lattice);

        var box = new MerkleWalkProbeRequestBox
        {
            Value = new MerkleWalkProbeRequest
            {
                TreeName = "disabled-tree",
                ShardIndex = 0,
                RangeStartKey = null,
                RangeEndKey = null,
                Depth = 0,
            },
        };

        using var call = invoker.AsyncUnaryCall(method.ProbeMerkleWalk, host: null, options: default, request: box);
        var response = await call.ResponseAsync;

        Assert.That(response.Value.Available, Is.False);
    }

    [Test]
    public async Task GetPeerHighWaterMark_round_trips_the_stored_clock_over_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
        var invoker = _channel.CreateCallInvoker();

        var clock = new HybridLogicalClock { WallClockTicks = 9000, Counter = 4 };
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("origin", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(clock));
        _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>("tree").Returns(hwmGrain);

        var box = new PeerHighWaterMarkRequestBox
        {
            Value = new PeerHighWaterMarkRequest { TreeName = "tree", OriginClusterId = "origin" },
        };

        using var call = invoker.AsyncUnaryCall(method.GetPeerHighWaterMark, host: null, options: default, request: box);
        var response = await call.ResponseAsync;

        Assert.That(response.Value.Clock, Is.EqualTo(clock));
    }
}


