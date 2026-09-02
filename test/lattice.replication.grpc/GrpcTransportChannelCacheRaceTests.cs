using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins the per-peer channel-cache race arm shared by
/// <see cref="GrpcRemoteSnapshotTransport"/> and
/// <see cref="GrpcSagaControlChannel"/>, and the snapshot stream's
/// cancellation translation.
/// <para>
/// Both transports build a channel optimistically and publish it with a
/// <c>ConcurrentDictionary.TryAdd</c>, so two callers racing on a cold cache
/// produce one winner and one loser. The loser must dispose its redundant
/// channel and adopt the winner's, otherwise the mesh leaks an
/// <c>HttpClient</c>-backed channel per lost race. That arm is only
/// reachable under genuine concurrency, so these tests rendezvous both
/// callers inside the host-supplied <c>ConfigureChannel</c> hook - which
/// runs immediately before the publish - making the race deterministic
/// rather than timing-dependent.
/// </para>
/// </summary>
[TestFixture]
public class GrpcTransportChannelCacheRaceTests
{
    private const string Peer = "site-a";
    private static readonly Uri PeerEndpoint = new("http://peer.example/");

    /// <summary>
    /// Answers every request with a gRPC trailers-only response carrying the
    /// supplied status, so no server or socket is needed to drive the client
    /// past channel resolution.
    /// </summary>
    private sealed class TrailersOnlyStatusHandler(StatusCode status) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
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
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("secret"));
        return secrets;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptions()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "local" });
        return monitor;
    }

    private static LatticeRemoteSnapshotGrpcMethods SnapshotMethods()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
    }

    private static LatticeSagaGrpcMethods SagaMethods()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new LatticeSagaGrpcMethods(
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
    }

    [Test]
    public async Task Snapshot_transport_concurrent_first_calls_settle_on_a_single_cached_channel()
    {
        using var barrier = new Barrier(2);
        var handler = new TrailersOnlyStatusHandler(StatusCode.Unimplemented);
        var options = new GrpcRemoteSnapshotTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "local",
            ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = handler;
                barrier.SignalAndWait(TimeSpan.FromSeconds(30));
            },
        };
        options.SenderEndpoints[Peer] = PeerEndpoint;

        var monitor = Substitute.For<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>();
        monitor.CurrentValue.Returns(options);
        using var transport = new GrpcRemoteSnapshotTransport(
            SnapshotMethods(), monitor, SecretsStub(), ReplicationOptions());

        async Task<StatusCode> CallAsync()
        {
            try
            {
                await transport.GetMetadataAsync("tree", Peer, HybridLogicalClock.Zero, CancellationToken.None);
                return StatusCode.OK;
            }
            catch (RpcException ex)
            {
                return ex.StatusCode;
            }
        }

        var results = await Task.WhenAll(Task.Run(CallAsync), Task.Run(CallAsync));

        Assert.Multiple(() =>
        {
            // Both callers reached the wire, so the loser adopted the winner's
            // channel rather than failing with ObjectDisposedException.
            Assert.That(results[0], Is.EqualTo(StatusCode.Unimplemented));
            Assert.That(results[1], Is.EqualTo(StatusCode.Unimplemented));
        });

        // A third call must hit the cache; if it re-entered ConfigureChannel
        // the lone barrier participant would block until the timeout.
        Assert.That(await CallAsync(), Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task Saga_control_channel_concurrent_first_calls_settle_on_a_single_cached_channel()
    {
        using var barrier = new Barrier(2);
        var handler = new TrailersOnlyStatusHandler(StatusCode.Unimplemented);
        var options = new GrpcSagaControlChannelOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "local",
            ConfigureChannel = (_, channelOptions) =>
            {
                channelOptions.HttpHandler = handler;
                barrier.SignalAndWait(TimeSpan.FromSeconds(30));
            },
        };
        options.PeerEndpoints[Peer] = PeerEndpoint;

        var monitor = Substitute.For<IOptionsMonitor<GrpcSagaControlChannelOptions>>();
        monitor.CurrentValue.Returns(options);
        using var channel = new GrpcSagaControlChannel(
            SagaMethods(), monitor, SecretsStub(), ReplicationOptions());

        var request = new SagaControlRequest
        {
            SagaId = "saga-1",
            TargetTree = "tree",
            CoordinatorClusterId = "local",
        };

        async Task<StatusCode> CallAsync()
        {
            try
            {
                await channel.PrepareAsync(Peer, request, CancellationToken.None);
                return StatusCode.OK;
            }
            catch (RpcException ex)
            {
                return ex.StatusCode;
            }
        }

        var results = await Task.WhenAll(Task.Run(CallAsync), Task.Run(CallAsync));

        Assert.Multiple(() =>
        {
            Assert.That(results[0], Is.EqualTo(StatusCode.Unimplemented));
            Assert.That(results[1], Is.EqualTo(StatusCode.Unimplemented));
        });

        Assert.That(await CallAsync(), Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task Snapshot_stream_translates_a_cancelled_rpc_into_operation_cancelled()
    {
        // The binding-agnostic IRemoteSnapshotTransport contract is that a
        // caller-initiated cancellation surfaces as OperationCanceledException,
        // so a receiver's drain loop does not have to special-case gRPC's
        // StatusCode.Cancelled. The cancellation has to land while the call is
        // already in flight: a token cancelled up front never reaches the
        // stream, so it would not exercise the translation at all.
        using var cts = new CancellationTokenSource();
        var callStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var handler = new BlockingHandler(callStarted);
        var options = new GrpcRemoteSnapshotTransportOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = "local",
            ConfigureChannel = (_, channelOptions) => channelOptions.HttpHandler = handler,
        };
        options.SenderEndpoints[Peer] = PeerEndpoint;

        var monitor = Substitute.For<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>();
        monitor.CurrentValue.Returns(options);
        using var transport = new GrpcRemoteSnapshotTransport(
            SnapshotMethods(), monitor, SecretsStub(), ReplicationOptions());

        await using var enumerator = transport
            .RequestSnapshotAsync("tree", Peer, HybridLogicalClock.Zero, cts.Token)
            .GetAsyncEnumerator(CancellationToken.None);

        var pending = enumerator.MoveNextAsync().AsTask();
        await callStarted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await cts.CancelAsync();

        Assert.That(async () => await pending, Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Holds the request open until the caller's token fires, so a test can
    /// cancel a call that is genuinely in flight.
    /// </summary>
    private sealed class BlockingHandler(TaskCompletionSource started) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException("unreachable");
        }
    }
}
