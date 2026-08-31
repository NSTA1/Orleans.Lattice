using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Receiver-side end-to-end auth coverage for the shared-secret
/// authenticator. Spins up the gRPC server with
/// <c>RequireAuthentication = true</c> and asserts:
/// (a) inbound calls without the secret header are rejected as
/// <see cref="StatusCode.Unauthenticated"/>;
/// (b) inbound calls with the wrong secret are rejected as
/// <see cref="StatusCode.PermissionDenied"/>;
/// (c) inbound calls with the correct secret reach the applier and
/// succeed.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcReplicationAuthIntegrationTests
{
    private const string Secret = "test-shared-secret-value-1234567890";

    private IHost _host = null!;
    private GrpcChannel _channel = null!;
    private IReplicationApplier _applier = null!;
    private IReplicationBatchEncoder _encoder = null!;

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "application/x-orleans-lattice-replog+binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    private sealed class FixedSecretSource : ILatticeReplicationSecretSource
    {
        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
            => new(Secret);
        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
            => new(new LatticeReplicationAcceptedSecrets(new[] { Secret }, "v1"));
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _applier = Substitute.For<IReplicationApplier>();
        _applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = callInfo.Arg<IReadOnlyList<WalRecord>>();
                var max = HybridLogicalClock.Zero;
                var applied = false;
                foreach (var e in batch)
                {
                    applied = true;
                    if (e.Timestamp.CompareTo(max) > 0) max = e.Timestamp;
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
                    services.AddSingleton(Substitute.For<IGrainFactory>());
                    services.AddEnrollAllReplicationContext();
                    services.AddLatticeReplicationGrpc();

                    // Replace the default env-var source with a fixed in-memory
                    // source so this fixture is hermetic.
                    services.AddSingleton<ILatticeReplicationSecretSource, FixedSecretSource>();
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
        var handler = server.CreateHandler();
        _channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions { HttpHandler = handler });
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

    private static ReplicationBatchEnvelopeBox MinimalBox()
        => new()
        {
            Value = new ReplicationBatchEnvelope
            {
                WireVersion = 1,
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

    private LatticeReplicationGrpcMethod ResolveMethod()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        return GrpcTestFactories.CreateMethod(_encoder, ackSerializer);
    }

    [Test]
    public void Push_without_secret_header_is_rejected_as_Unauthenticated()
    {
        var method = ResolveMethod();
        var invoker = _channel.CreateCallInvoker();

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: default, request: MinimalBox());
            await call.ResponseAsync;
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public void Push_with_wrong_secret_is_rejected_as_PermissionDenied()
    {
        var method = ResolveMethod();
        var invoker = _channel.CreateCallInvoker();

        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.SecretHeader, "wrong-secret" },
        };

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: new CallOptions(headers), request: MinimalBox());
            await call.ResponseAsync;
        });

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Push_with_correct_secret_succeeds()
    {
        var method = ResolveMethod();
        var invoker = _channel.CreateCallInvoker();

        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.SecretHeader, Secret },
        };

        using var call = invoker.AsyncUnaryCall(method.Push, host: null, options: new CallOptions(headers), request: MinimalBox());
        var ackBox = await call.ResponseAsync;
        Assert.That(ackBox.Value.Accepted, Is.True);
    }
}
