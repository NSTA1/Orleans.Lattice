using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Wire-level assertion that the sender-side CallCredentials populate
/// both the shared-secret and origin-cluster-id headers on the actual
/// gRPC call. Closes the test gap that previously only verified the
/// secret header end-to-end and exercised the origin header only via
/// the helper unit tests.
/// </summary>
[TestFixture]
[Category("Integration")]
public class GrpcReplicationHeaderCaptureTests
{
    private const string Secret = "header-capture-secret-1234567890";

    private IHost _host = null!;
    private GrpcChannel _channel = null!;
    private IReplicationBatchEncoder _encoder = null!;
    private CapturedHeaders _captured = null!;

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

    private sealed class CapturedHeaders
    {
        public string? Secret;
        public string? Origin;
    }

    private sealed class CapturingInterceptor : Interceptor
    {
        private readonly CapturedHeaders _captured;
        public CapturingInterceptor(CapturedHeaders captured) { _captured = captured; }

        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            foreach (var h in context.RequestHeaders)
            {
                if (string.Equals(h.Key, LatticeReplicationGrpcMetadataNames.SecretHeader, StringComparison.OrdinalIgnoreCase))
                    _captured.Secret = h.Value;
                else if (string.Equals(h.Key, LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, StringComparison.OrdinalIgnoreCase))
                    _captured.Origin = h.Value;
            }
            return continuation(request, context);
        }
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _captured = new CapturedHeaders();
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var captured = _captured;
        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddSingleton<IReplicationApplier>(applier);
                    services.AddSingleton<IReplicationBatchEncoder>(sp =>
                        new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()));
                    services.AddRouting();
                    services.AddSingleton<CapturedHeaders>(captured);
                    services.AddSingleton<CapturingInterceptor>();
                    services.AddLatticeReplicationGrpcServer();
                    // Add the capturing interceptor after the auth interceptor so
                    // it only runs on calls that already passed auth.
                    services.Configure<global::Grpc.AspNetCore.Server.GrpcServiceOptions>(o =>
                        o.Interceptors.Add<CapturingInterceptor>());
                    services.AddSingleton<ILatticeReplicationSecretSource, FixedSecretSource>();
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
        _channel = GrpcChannel.ForAddress(
            server.BaseAddress,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                // TestServer is plaintext; call credentials only flow
                // over plaintext when this opt-in is set.
                UnsafeUseInsecureChannelCallCredentials = true,
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

    private static ReplicationBatchEnvelopeBox MinimalBox() => new()
    {
        Value = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = Array.Empty<WalRecord>(),
        },
    };

    [Test]
    public async Task Sender_call_credentials_send_both_secret_and_origin_headers_on_the_wire()
    {
        var ackSerializer = _host.Services.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(_encoder, ackSerializer);

        // Use the same call-credentials path the production sender uses.
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>(Secret));
        var callCreds = GrpcChannelHardening.BuildCallCredentials(secrets, "peer-z", "site-z");
        var invoker = _channel.CreateCallInvoker();

        using var call = invoker.AsyncUnaryCall(
            method.Push,
            host: null,
            options: new CallOptions().WithCredentials(callCreds),
            request: MinimalBox());
        var ackBox = await call.ResponseAsync;

        Assert.That(ackBox.Value.Accepted, Is.True);
        Assert.That(_captured.Secret, Is.EqualTo(Secret));
        Assert.That(_captured.Origin, Is.EqualTo("site-z"));
    }
}
