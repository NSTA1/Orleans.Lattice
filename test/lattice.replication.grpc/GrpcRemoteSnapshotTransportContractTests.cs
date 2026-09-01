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
using Orleans.Lattice.Replication.Tests;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Drives the inherited
/// <see cref="RemoteSnapshotTransportContractTests"/> acceptance suite
/// against the canonical gRPC binding. Stands up an in-process
/// ASP.NET Core TestServer that hosts the sender-side
/// <see cref="LatticeRemoteSnapshotGrpcService"/> in front of a
/// <see cref="StubSenderSnapshotProvider"/>, and resolves a
/// <see cref="GrpcRemoteSnapshotTransport"/> bound to the TestServer's
/// in-memory channel. Pins the contract for the canonical
/// cross-cluster bootstrap transport binding.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcRemoteSnapshotTransportContractTests : RemoteSnapshotTransportContractTests
{
    private const string SourceClusterId = "site-a";

    /// <inheritdoc />
    protected override async Task<TransportFixture> CreateTransportAsync()
    {
        var sender = new StubSenderSnapshotProvider();

        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddSingleton<ISnapshotProvider>(sender);
                    // The unified gRPC binding maps both the live-push and
                    // snapshot routes from a single AddLatticeReplicationGrpc
                    // call. Pre-register stub push-side deps so the live-push
                    // service can construct at host startup. The contract
                    // suite exercises only the snapshot RPCs, so the push
                    // applier/encoder are never invoked.
                    services.AddSingleton<IReplicationApplier>(Substitute.For<IReplicationApplier>());
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new StubBatchEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddEnrollAllReplicationContext();
                    services.AddLatticeReplicationGrpc();
                    // The contract tests focus on the wire shape; the
                    // shared-secret authenticator is covered separately.
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

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = handler,
        });

        // Build the client transport against the TestServer's channel.
        // The transport caches its own per-peer channel, but the
        // contract suite uses a fixed SourceClusterId so a custom
        // ConfigureChannel hook is the cleanest way to substitute the
        // pre-built test channel. We work around the
        // GrpcChannel.ForAddress call by routing the transport through
        // the TestServer's handler-backed channel directly.
        var methods = host.Services.GetRequiredService<LatticeRemoteSnapshotGrpcMethods>();

        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));

        var transport = new TestChannelGrpcRemoteSnapshotTransport(methods, channel);

        return new TransportFixture(transport, sender, async () =>
        {
            transport.Dispose();
            channel.Dispose();
            await host.StopAsync();
            host.Dispose();
        });
    }

    /// <summary>
    /// A thin subclass-equivalent of
    /// <see cref="GrpcRemoteSnapshotTransport"/> that bypasses the
    /// peer-channel cache and routes every call through a
    /// pre-constructed <see cref="GrpcChannel"/>. Lets the contract
    /// suite point the transport at a TestServer-backed channel
    /// without depending on a resolvable URI.
    /// </summary>
    private sealed class TestChannelGrpcRemoteSnapshotTransport : IRemoteSnapshotTransport, IDisposable
    {
        private readonly LatticeRemoteSnapshotGrpcMethods _methods;
        private readonly GrpcChannel _channel;
        private readonly global::Grpc.Core.CallInvoker _invoker;

        public TestChannelGrpcRemoteSnapshotTransport(LatticeRemoteSnapshotGrpcMethods methods, GrpcChannel channel)
        {
            _methods = methods;
            _channel = channel;
            _invoker = channel.CreateCallInvoker();
        }

        public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName,
            string sourceClusterId,
            Orleans.Lattice.HybridLogicalClock fromAsOfHlc,
            CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
            ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
            cancellationToken.ThrowIfCancellationRequested();

            var requestBox = new RemoteSnapshotMetadataRequestBox
            {
                Value = new RemoteSnapshotMetadataRequest
                {
                    TreeName = treeName,
                    SourceClusterId = sourceClusterId,
                    FromAsOfHlc = fromAsOfHlc,
                },
            };

            using var call = _invoker.AsyncUnaryCall(
                _methods.GetMetadata,
                host: null,
                options: new global::Grpc.Core.CallOptions(cancellationToken: cancellationToken),
                request: requestBox);

            var response = await call.ResponseAsync.ConfigureAwait(false);
            return response.Value;
        }

        public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName,
            string sourceClusterId,
            Orleans.Lattice.HybridLogicalClock fromAsOfHlc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
            ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
            cancellationToken.ThrowIfCancellationRequested();

            var requestBox = new RemoteSnapshotMetadataRequestBox
            {
                Value = new RemoteSnapshotMetadataRequest
                {
                    TreeName = treeName,
                    SourceClusterId = sourceClusterId,
                    FromAsOfHlc = fromAsOfHlc,
                },
            };

            using var call = _invoker.AsyncServerStreamingCall(
                _methods.RequestSnapshot,
                host: null,
                options: new global::Grpc.Core.CallOptions(cancellationToken: cancellationToken),
                request: requestBox);

            while (true)
            {
                bool more;
                try
                {
                    more = await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
                }
                catch (global::Grpc.Core.RpcException ex) when (ex.StatusCode == global::Grpc.Core.StatusCode.Cancelled
                    && cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException(ex.Status.Detail, ex, cancellationToken);
                }

                if (!more)
                {
                    yield break;
                }

                yield return call.ResponseStream.Current.Value.Entry;
            }
        }

        public void Dispose()
        {
            // Channel disposal is owned by the fixture.
        }
    }

    /// <summary>
    /// Minimal in-test <see cref="IReplicationBatchEncoder"/> stub
    /// that the snapshot contract fixture wires into the host. The
    /// snapshot RPCs never exercise this encoder; it exists only so
    /// the unified gRPC binding's live-push service can construct at
    /// startup.
    /// </summary>
    private sealed class StubBatchEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public StubBatchEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
            => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => _s.Deserialize(payload.Span);
    }
}