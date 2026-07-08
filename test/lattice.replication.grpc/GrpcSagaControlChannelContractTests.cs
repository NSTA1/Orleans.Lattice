using System.Buffers;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Wire-level round-trip tests for the saga control channel. Stands up
/// an in-process ASP.NET Core TestServer hosting the participant-side
/// <see cref="LatticeSagaGrpcService"/> in front of a recording stub
/// handler, and drives the four saga RPCs over the TestServer's
/// in-memory HTTP/2 channel with Box marshalling. Also pins the
/// peer-authorization gate end-to-end: a saga call whose origin is not a
/// configured replication peer is rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcSagaControlChannelContractTests
{
    private const string AuthorizedPeer = "site-a";
    private const string Tree = "tree";

    private sealed record Harness(
        IHost Host,
        GrpcChannel Channel,
        LatticeSagaGrpcMethods Methods,
        EchoSagaHandler Handler) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            Channel.Dispose();
            await Host.StopAsync();
            Host.Dispose();
        }
    }

    private static async Task<Harness> StartAsync()
    {
        var handler = new EchoSagaHandler();

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    // Push/snapshot-side deps so the unified binding can construct.
                    services.AddSingleton<IReplicationApplier>(Substitute.For<IReplicationApplier>());
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new StubBatchEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddSingleton(Substitute.For<ISnapshotProvider>());
                    // Real participant handler is host-supplied; TryAdd in the
                    // binding defers to this registration.
                    services.AddSingleton<ILatticeSagaControlHandler>(handler);
                    services.AddLatticeReplicationGrpc(o =>
                    {
                        // The peer map is the authorized-peer set consulted by
                        // the default PeerMapSagaPeerAuthorizer. A dummy https
                        // endpoint is enough; the authorizer only checks keys.
                        o.Peers[AuthorizedPeer] = new Uri("https://site-a.example/");
                    });
                    // Isolate the peer-authorization gate from the shared-secret
                    // interceptor, which is covered separately.
                    services.Configure<LatticeReplicationSecurityOptions>(x => x.RequireAuthentication = false);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeReplicationGrpc());
                });
            });

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });
        var methods = host.Services.GetRequiredService<LatticeSagaGrpcMethods>();
        return new Harness(host, channel, methods, handler);
    }

    private static SagaControlRequest Request(string coordinator, string sagaId = "saga-1")
        => new()
        {
            SagaId = sagaId,
            TargetTree = Tree,
            ManifestId = "m1",
            CoordinatorClusterId = coordinator,
        };

    private static async Task<SagaControlResponse> InvokeAsync(
        GrpcChannel channel,
        Method<SagaControlRequestBox, SagaControlResponseBox> method,
        SagaControlRequest request)
    {
        var invoker = channel.CreateCallInvoker();
        using var call = invoker.AsyncUnaryCall(
            method, host: null, options: new CallOptions(), request: new SagaControlRequestBox { Value = request });
        var response = await call.ResponseAsync;
        return response.Value;
    }

    [Test]
    public async Task Prepare_round_trips_and_returns_handler_vote()
    {
        await using var h = await StartAsync();

        var response = await InvokeAsync(h.Channel, h.Methods.Prepare, Request(AuthorizedPeer));

        Assert.Multiple(() =>
        {
            Assert.That(response.SagaId, Is.EqualTo("saga-1"));
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(response.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(h.Handler.PrepareCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Commit_abort_getStatus_round_trip_through_handler()
    {
        await using var h = await StartAsync();

        var commit = await InvokeAsync(h.Channel, h.Methods.Commit, Request(AuthorizedPeer));
        var abort = await InvokeAsync(h.Channel, h.Methods.Abort, Request(AuthorizedPeer));
        var status = await InvokeAsync(h.Channel, h.Methods.GetStatus, Request(AuthorizedPeer));

        Assert.Multiple(() =>
        {
            Assert.That(commit.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(abort.Phase, Is.EqualTo(SagaPhase.Aborted));
            Assert.That(status.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(h.Handler.CommitCalls, Is.EqualTo(1));
            Assert.That(h.Handler.AbortCalls, Is.EqualTo(1));
            Assert.That(h.Handler.GetStatusCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Unauthorized_origin_is_rejected_before_handler()
    {
        await using var h = await StartAsync();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await InvokeAsync(h.Channel, h.Methods.Prepare, Request("rogue-cluster")));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(h.Handler.PrepareCalls, Is.EqualTo(0));
        });
    }

    private sealed class EchoSagaHandler : ILatticeSagaControlHandler
    {
        public int PrepareCalls;
        public int CommitCalls;
        public int AbortCalls;
        public int GetStatusCalls;

        public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            PrepareCalls++;
            return Task.FromResult(new SagaControlResponse
            {
                SagaId = request.SagaId,
                Phase = SagaPhase.Prepared,
                Vote = SagaVote.Commit,
            });
        }

        public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            CommitCalls++;
            return Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Committed });
        }

        public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            AbortCalls++;
            return Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Aborted });
        }

        public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
        {
            GetStatusCalls++;
            return Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Prepared });
        }
    }

    private sealed class StubBatchEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public StubBatchEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }
}
