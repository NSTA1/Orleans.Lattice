using System.Buffers;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Exercises the client-side <see cref="GrpcSagaControlChannel"/> RPC
/// bodies end to end against the real participant-side
/// <see cref="LatticeSagaGrpcService"/> hosted on an in-memory
/// <see cref="TestServer"/>. Unlike the contract suite (which drives a
/// raw <c>CallInvoker</c>), this fixture drives the production channel
/// itself by injecting the server's HTTP handler through
/// <see cref="GrpcSagaControlChannelOptions.ConfigureChannel"/>, so
/// <c>ResolvePeerChannel</c> and the shared <c>InvokeAsync</c> body for
/// all four saga verbs are covered.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcSagaControlChannelClientIntegrationTests
{
    private const string Peer = "site-a";
    private const string Tree = "tree";

    private IHost _host = null!;
    private System.Net.Http.HttpMessageHandler _handler = null!;
    private Uri _baseAddress = null!;
    private EchoSagaHandler _sagaHandler = null!;
    private GrpcSagaControlChannel _channel = null!;

    private sealed class StubEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public StubEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _sagaHandler = new EchoSagaHandler();

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    services.AddSingleton<IReplicationApplier>(Substitute.For<IReplicationApplier>());
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new StubEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddSingleton(Substitute.For<ISnapshotProvider>());
                    services.AddSingleton<ILatticeSagaControlHandler>(_sagaHandler);
                    services.AddLatticeReplicationGrpc(o => o.Peers[Peer] = new Uri("https://site-a.example/"));
                    services.Configure<LatticeReplicationSecurityOptions>(x => x.RequireAuthentication = false);
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
        _channel = CreateChannel();
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

    private GrpcSagaControlChannel CreateChannel()
    {
        var methods = _host.Services.GetRequiredService<LatticeSagaGrpcMethods>();

        var options = new GrpcSagaControlChannelOptions
        {
            AllowPlaintextEndpoints = true,
            LocalClusterId = Peer,
            ConfigureChannel = (_, channelOptions) => channelOptions.HttpHandler = _handler,
        };
        options.PeerEndpoints[Peer] = _baseAddress;

        var monitor = Substitute.For<IOptionsMonitor<GrpcSagaControlChannelOptions>>();
        monitor.CurrentValue.Returns(options);

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationMonitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "local" });

        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));

        return new GrpcSagaControlChannel(methods, monitor, secrets, replicationMonitor);
    }

    private static SagaControlRequest Request()
        => new()
        {
            SagaId = "saga-1",
            TargetTree = Tree,
            ManifestId = "m1",
            CoordinatorClusterId = Peer,
        };

    [Test]
    public async Task PrepareAsync_round_trips_and_returns_handler_vote()
    {
        var response = await _channel.PrepareAsync(Peer, Request(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.SagaId, Is.EqualTo("saga-1"));
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(response.Vote, Is.EqualTo(SagaVote.Commit));
        });
    }

    [Test]
    public async Task CommitAsync_round_trips_committed_phase()
    {
        var response = await _channel.CommitAsync(Peer, Request(), CancellationToken.None);
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Committed));
    }

    [Test]
    public async Task AbortAsync_round_trips_aborted_phase()
    {
        var response = await _channel.AbortAsync(Peer, Request(), CancellationToken.None);
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Aborted));
    }

    [Test]
    public async Task GetStatusAsync_round_trips_status_phase()
    {
        var response = await _channel.GetStatusAsync(Peer, Request(), CancellationToken.None);
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared));
    }

    private sealed class EchoSagaHandler : ILatticeSagaControlHandler
    {
        public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Prepared, Vote = SagaVote.Commit });

        public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Committed });

        public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Aborted });

        public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SagaControlResponse { SagaId = request.SagaId, Phase = SagaPhase.Prepared });
    }
}
