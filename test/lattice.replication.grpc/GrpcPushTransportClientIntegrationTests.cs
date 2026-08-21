using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Exercises the client-side <see cref="GrpcPushTransport"/> RPC bodies
/// end to end against the real receiver service hosted on an in-memory
/// <see cref="TestServer"/>. The transport is pointed at the test server
/// by injecting the server's <see cref="System.Net.Http.HttpMessageHandler"/>
/// through <see cref="GrpcPushTransportOptions.ConfigureChannel"/>, so
/// <c>ResolvePeerChannel</c> builds a channel that speaks to the
/// in-memory host - no network sockets, no sleeps. This covers the
/// probe/send success paths the plain unit fixture cannot reach because
/// it stops at the guard clauses.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcPushTransportClientIntegrationTests
{
    private IHost _host = null!;
    private System.Net.Http.HttpMessageHandler _handler = null!;
    private Uri _baseAddress = null!;
    private IReplicationApplier _applier = null!;
    private IGrainFactory _grainFactory = null!;
    private TestEncoder _encoder = null!;
    private GrpcPushTransport _transport = null!;

    private static readonly byte[] PulledDictionaryBytes = { 3, 1, 4, 1, 5, 9, 2, 6 };

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    private static IReplicationSecretProvider SecretsStub()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("test-secret"));
        s.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(
                new LatticeReplicationAcceptedSecrets(new[] { "test-secret" }, "v1")));
        s.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        return s;
    }

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
                    var dictionaryProvider = new AutoTrainingCompressionDictionaryProvider(
                        new CompressionDictionaryTrainingOptions { Enabled = true });
                    dictionaryProvider.TryInstall(8u, PulledDictionaryBytes);
                    services.AddSingleton<ILatticeCompressionDictionaryProvider>(dictionaryProvider);
                    services.AddLatticeReplicationGrpc();
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
        _baseAddress = server.BaseAddress;
        _handler = server.CreateHandler();

        _encoder = new TestEncoder(_host.Services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
        _transport = CreateTransport();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _transport?.Dispose();
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
        _grainFactory.ClearReceivedCalls();
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

    private GrpcPushTransport CreateTransport()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
        var method = GrpcTestFactories.CreateMethod(_encoder, ackSerializer);

        var options = new GrpcPushTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "self",
            ConfigureChannel = (_, channelOptions) => channelOptions.HttpHandler = _handler,
        };
        options.PeerEndpoints["peer"] = _baseAddress;

        var monitor = Substitute.For<IOptionsMonitor<GrpcPushTransportOptions>>();
        monitor.CurrentValue.Returns(options);

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationMonitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "self" });

        return new GrpcPushTransport(method, _encoder, monitor, SecretsStub(), replicationMonitor);
    }

    [Test]
    public async Task SendAsync_ships_an_empty_heartbeat_batch_and_returns_zero_hwm()
    {
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "tree",
            OriginClusterId = "self",
            Payload = Array.Empty<byte>(),
        };

        var ack = await _transport.SendAsync(batch, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task SendAsync_ships_a_typed_envelope_and_returns_max_hwm()
    {
        var hlcA = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var hlcB = new HybridLogicalClock { WallClockTicks = 250, Counter = 1 };

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "self",
            Entries = new[]
            {
                new WalRecord { TreeId = "tree", Op = MutationKind.Set, Key = "a", Value = new byte[] { 1 }, Timestamp = hlcA, OriginClusterId = "self", Mode = LatticeMergeMode.LwwRegister },
                new WalRecord { TreeId = "tree", Op = MutationKind.Set, Key = "b", Value = new byte[] { 2 }, Timestamp = hlcB, OriginClusterId = "self", Mode = LatticeMergeMode.LwwRegister },
            },
        };

        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "tree",
            OriginClusterId = "self",
            Envelope = envelope,
        };

        var ack = await _transport.SendAsync(batch, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(hlcB));
        });
    }

    [Test]
    public async Task ProbeDigestAsync_returns_the_shard_digest_over_the_wire()
    {
        var digest = new LeafProjectionDigest
        {
            Hash = new byte[] { 1, 2, 3, 4 },
            EntryCount = 5,
            CheckpointOffset = 2,
            Version = LeafProjectionDigest.CurrentVersion,
        };
        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(digest));
        _grainFactory.GetGrain<ILattice>("tree").Returns(lattice);

        var response = await _transport.ProbeDigestAsync(
            "peer",
            new DigestProbeRequest { TreeName = "tree", ShardIndex = 0 },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.DigestAvailable, Is.True);
            Assert.That(response.Digest.EntryCount, Is.EqualTo(5));
            Assert.That(response.Digest.Hash, Is.EqualTo(digest.Hash));
        });
    }

    [Test]
    public async Task ExchangeContentManifestAsync_returns_a_plan_over_the_wire()
    {
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("self", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>("tree").Returns(hwmGrain);

        var request = new ContentManifestRequest
        {
            TreeName = "tree",
            OriginClusterId = "self",
            Entries = Array.Empty<ContentManifestEntry>(),
        };

        var response = await _transport.ExchangeContentManifestAsync("peer", request, CancellationToken.None);

        Assert.That(response.ExchangeSupported, Is.True);
    }

    [Test]
    public async Task PullCompressionDictionaryAsync_returns_a_held_dictionary_over_the_wire()
    {
        var response = await _transport.PullCompressionDictionaryAsync(
            "peer",
            new CompressionDictionaryPullRequest { DictionaryId = 8u },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.True);
            Assert.That(response.Found, Is.True);
            Assert.That(response.DictionaryId, Is.EqualTo(8u));
            Assert.That(response.Dictionary.ToArray(), Is.EqualTo(PulledDictionaryBytes));
        });
    }

    [Test]
    public async Task ProbeMerkleWalkAsync_returns_a_range_digest_over_the_wire()
    {
        var digest = new LeafProjectionDigest
        {
            Hash = new byte[] { 9, 9, 9, 9 },
            EntryCount = 3,
            CheckpointOffset = 1,
            Version = LeafProjectionDigest.CurrentVersion,
        };
        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestForRangeAsync(1, "a", "z", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(digest));
        _grainFactory.GetGrain<ILattice>("tree").Returns(lattice);

        var response = await _transport.ProbeMerkleWalkAsync(
            "peer",
            new MerkleWalkProbeRequest
            {
                TreeName = "tree",
                ShardIndex = 1,
                RangeStartKey = "a",
                RangeEndKey = "z",
                Depth = 1,
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.Available, Is.True);
            Assert.That(response.Digest.EntryCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task GetPeerHighWaterMarkAsync_returns_the_stored_clock_over_the_wire()
    {
        var clock = new HybridLogicalClock { WallClockTicks = 7777, Counter = 2 };
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("origin", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(clock));
        _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>("tree").Returns(hwmGrain);

        var result = await _transport.GetPeerHighWaterMarkAsync("peer", "tree", "origin", CancellationToken.None);

        Assert.That(result, Is.EqualTo(clock));
    }
}
