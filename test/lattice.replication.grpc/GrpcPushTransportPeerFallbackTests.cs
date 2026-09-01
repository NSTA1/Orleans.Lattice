using System.Buffers;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins the rolling-upgrade fallback contract of
/// <see cref="GrpcPushTransport"/>, its remaining argument guards, and the
/// lost-race arm of its per-peer channel cache.
/// <para>
/// Four of the transport's RPCs are newer than the wire protocol's first
/// release, so a peer that has not been upgraded answers them with
/// <see cref="StatusCode.Unimplemented"/> and a momentarily unreachable
/// peer answers <see cref="StatusCode.Unavailable"/>. In both cases the
/// caller must degrade to the conservative "not supported" answer rather
/// than surfacing a fault, otherwise a mixed-version mesh cannot ship at
/// all. These tests drive that path with a stub handler that returns a
/// gRPC trailers-only Unimplemented response, so no server and no socket
/// is involved.
/// </para>
/// </summary>
[TestFixture]
public class GrpcPushTransportPeerFallbackTests
{
    private const string Peer = "peer";
    private static readonly Uri PeerEndpoint = new("http://push.peer.example/");

    private sealed class StubEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "test/stub";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) { }
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => default;
    }

    /// <summary>
    /// Answers every request with a gRPC trailers-only response carrying the
    /// supplied status, which is exactly the shape an un-upgraded peer
    /// returns for an RPC it never bound.
    /// </summary>
    private sealed class TrailersOnlyStatusHandler(StatusCode status) : HttpMessageHandler
    {
        public int Calls;

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Version = new Version(2, 0),
                Content = new ByteArrayContent([]),
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            response.Headers.Add("grpc-status", ((int)status).ToString(CultureInfo.InvariantCulture));
            return Task.FromResult(response);
        }
    }

    private static IReplicationSecretProvider SecretsStub()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("test-secret"));
        s.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(
                new LatticeReplicationAcceptedSecrets(["test-secret"], "v1")));
        s.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        return s;
    }

    private static GrpcPushTransport CreateTransport(GrpcPushTransportOptions options)
    {
        var encoder = new StubEncoder();
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var method = new LatticeReplicationGrpcMethod(
            encoder,
            GrpcTestFactories.CreateWalRecordEncoder(),
            sp.GetRequiredService<Serializer<ReplicationAck>>(),
            sp.GetRequiredService<Serializer<DigestProbeRequest>>(),
            sp.GetRequiredService<Serializer<DigestProbeResponse>>(),
            sp.GetRequiredService<Serializer<ContentManifestRequest>>(),
            sp.GetRequiredService<Serializer<ContentManifestResponse>>(),
            sp.GetRequiredService<Serializer<CompressionDictionaryPullRequest>>(),
            sp.GetRequiredService<Serializer<CompressionDictionaryPullResponse>>(),
            sp.GetRequiredService<Serializer<MerkleWalkProbeRequest>>(),
            sp.GetRequiredService<Serializer<MerkleWalkProbeResponse>>(),
            sp.GetRequiredService<Serializer<PeerHighWaterMarkRequest>>(),
            sp.GetRequiredService<Serializer<PeerHighWaterMarkResponse>>());

        var monitor = Substitute.For<IOptionsMonitor<GrpcPushTransportOptions>>();
        monitor.CurrentValue.Returns(options);
        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationMonitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "self" });

        return new GrpcPushTransport(method, encoder, monitor, SecretsStub(), replicationMonitor);
    }

    private static GrpcPushTransportOptions OptionsAnsweringWith(StatusCode status, out TrailersOnlyStatusHandler handler)
    {
        var h = new TrailersOnlyStatusHandler(status);
        handler = h;
        var options = new GrpcPushTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "self",
            ConfigureChannel = (_, channelOptions) => channelOptions.HttpHandler = h,
        };
        options.PeerEndpoints[Peer] = PeerEndpoint;
        return options;
    }

    [Test]
    public async Task ExchangeContentManifestAsync_degrades_to_not_supported_for_an_un_upgraded_peer()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out var handler));

        var response = await transport.ExchangeContentManifestAsync(
            Peer,
            new ContentManifestRequest { TreeName = "tree", OriginClusterId = "self" },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response, Is.EqualTo(ContentManifestResponse.NotSupported));
            Assert.That(handler.Calls, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task ExchangeContentManifestAsync_degrades_to_not_supported_for_an_unreachable_peer()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unavailable, out _));

        var response = await transport.ExchangeContentManifestAsync(
            Peer,
            new ContentManifestRequest { TreeName = "tree", OriginClusterId = "self" },
            CancellationToken.None);

        Assert.That(response, Is.EqualTo(ContentManifestResponse.NotSupported));
    }

    [Test]
    public async Task PullCompressionDictionaryAsync_degrades_to_not_supported_for_an_un_upgraded_peer()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        var response = await transport.PullCompressionDictionaryAsync(
            Peer,
            new CompressionDictionaryPullRequest { DictionaryId = 8u },
            CancellationToken.None);

        Assert.That(response, Is.EqualTo(CompressionDictionaryPullResponse.NotSupported));
    }

    [Test]
    public async Task ProbeMerkleWalkAsync_degrades_to_unavailable_for_an_un_upgraded_peer()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        var response = await transport.ProbeMerkleWalkAsync(
            Peer,
            new MerkleWalkProbeRequest { TreeName = "tree" },
            CancellationToken.None);

        Assert.That(response, Is.EqualTo(MerkleWalkProbeResponse.Unavailable));
    }

    [Test]
    public async Task GetPeerHighWaterMarkAsync_falls_back_to_zero_for_an_un_upgraded_peer()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        var hwm = await transport.GetPeerHighWaterMarkAsync(Peer, "tree", "self", CancellationToken.None);

        // Zero re-ships every retained in-range entry and leans on the
        // receiver's per-origin dedup, which is the conservative choice.
        Assert.That(hwm, Is.EqualTo(Orleans.Lattice.HybridLogicalClock.Zero));
    }

    [Test]
    public void ProbeDigestAsync_rejects_an_empty_target_cluster_id()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        Assert.That(
            async () => await transport.ProbeDigestAsync(
                string.Empty,
                new DigestProbeRequest { TreeName = "tree" },
                CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void ProbeDigestAsync_rejects_an_empty_tree_name()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        Assert.That(
            async () => await transport.ProbeDigestAsync(
                Peer,
                new DigestProbeRequest { TreeName = string.Empty },
                CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void PullCompressionDictionaryAsync_rejects_an_empty_target_cluster_id()
    {
        using var transport = CreateTransport(OptionsAnsweringWith(StatusCode.Unimplemented, out _));

        Assert.That(
            async () => await transport.PullCompressionDictionaryAsync(
                string.Empty,
                new CompressionDictionaryPullRequest { DictionaryId = 1u },
                CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task Concurrent_first_calls_to_one_peer_settle_on_a_single_cached_channel()
    {
        // ConfigureChannel runs inside ResolvePeerChannel *before* the
        // ConcurrentDictionary.TryAdd that publishes the channel, so
        // rendezvousing both callers there forces exactly one of them down
        // the lost-race arm that disposes its redundant channel and adopts
        // the winner's. Without the barrier the race is not reproducible.
        using var barrier = new Barrier(2);
        var handler = new TrailersOnlyStatusHandler(StatusCode.Unimplemented);
        var options = new GrpcPushTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "self",
            ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = handler;
                barrier.SignalAndWait(TimeSpan.FromSeconds(30));
            },
        };
        options.PeerEndpoints[Peer] = PeerEndpoint;

        using var transport = CreateTransport(options);

        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = "self" };
        var first = Task.Run(() => transport.ExchangeContentManifestAsync(Peer, request, CancellationToken.None));
        var second = Task.Run(() => transport.ExchangeContentManifestAsync(Peer, request, CancellationToken.None));

        var results = await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            // Both callers get a usable answer: the loser adopted the winner's
            // cached channel rather than failing or leaking its own.
            Assert.That(results[0], Is.EqualTo(ContentManifestResponse.NotSupported));
            Assert.That(results[1], Is.EqualTo(ContentManifestResponse.NotSupported));
        });

        // A third call must now hit the cache, so ConfigureChannel is not
        // invoked again - if it were, the barrier would block and time out.
        var third = await transport.ExchangeContentManifestAsync(Peer, request, CancellationToken.None);
        Assert.That(third, Is.EqualTo(ContentManifestResponse.NotSupported));
    }
}
