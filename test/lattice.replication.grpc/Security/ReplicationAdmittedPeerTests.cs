using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Detectors for the admitted-peer seam of the replication gRPC host: every method
/// the host binds passes through it, and a method it does not register is refused.
/// </summary>
[TestFixture]
public class ReplicationAdmittedPeerTests
{
    private const string Secret = "secret-for-site-a";

    [Test]
    public void Every_method_bound_on_the_replication_host_passes_through_the_admitted_peer_seam()
    {
        // Enumerates every gRPC service this package can map - any type carrying
        // [BindServiceMethod] - and binds it as Grpc.AspNetCore does at startup. A
        // new method, or a new cross-cluster service, fails here until it is
        // registered with the seam, and a registration no bound method uses fails too.
        var bound = BoundMethodNames();

        Assert.Multiple(() =>
        {
            Assert.That(bound, Is.Not.Empty, "the scan found no bound method, so it proves nothing");
            Assert.That(
                bound.Where(m => !ReplicationAdmittedPeer.TryClassify(m, out _)),
                Is.Empty,
                "bound methods the admitted-peer seam does not register");
            Assert.That(
                ReplicationAdmittedPeer.RegisteredMethods.Except(bound, StringComparer.Ordinal),
                Is.Empty,
                "registrations no bound method uses");
        });
    }

    [Test]
    public void Every_origin_consuming_verb_is_registered_as_bound_to_its_caller()
    {
        Assert.Multiple(() =>
        {
            AssertAdmission(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.PushMethodName, ReplicationPeerAdmission.OriginBound);
            AssertAdmission(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.ExchangeContentManifestMethodName, ReplicationPeerAdmission.OriginBound);
            AssertAdmission(LatticeReplicationGrpcMethod.ServiceName, LatticeReplicationGrpcMethod.GetPeerHighWaterMarkMethodName, ReplicationPeerAdmission.OriginBound);
            foreach (var saga in new[]
            {
                LatticeSagaGrpcMethods.PrepareMethodName, LatticeSagaGrpcMethods.CommitMethodName,
                LatticeSagaGrpcMethods.AbortMethodName, LatticeSagaGrpcMethods.GetStatusMethodName,
                LatticeSagaGrpcMethods.GetDecisionMethodName,
            })
            {
                AssertAdmission(LatticeSagaGrpcMethods.ServiceName, saga, ReplicationPeerAdmission.ControlPeer);
            }
        });
    }

    [Test]
    public void The_interceptor_refuses_a_replication_method_the_seam_does_not_register()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<string?>(Secret));
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationSecurityOptions { RequireAuthentication = false });
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            secrets, options, NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);
        var reached = false;

        var rpc = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler<object, string>(
            new object(),
            new StubServerCallContext("/" + LatticeSagaGrpcMethods.ServiceName + "/GrantIntent"),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult("ok");
            }));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
            Assert.That(reached, Is.False);
        });
    }

    [Test]
    public void CredentialBoundOrigin_is_null_unless_the_interceptor_bound_the_stamped_origin()
    {
        var unbound = new StubServerCallContext("/x/y", "site-a");
        var mismatched = new StubServerCallContext("/x/y", "site-a");
        mismatched.UserState[LatticeReplicationGrpcAuthInterceptor.AuthenticatedPeerKey] = "site-b";
        var bound = new StubServerCallContext("/x/y", "site-a");
        bound.UserState[LatticeReplicationGrpcAuthInterceptor.AuthenticatedPeerKey] = "site-a";
        var unstamped = new StubServerCallContext("/x/y");
        unstamped.UserState[LatticeReplicationGrpcAuthInterceptor.AuthenticatedPeerKey] = "site-a";

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationAdmittedPeer.CredentialBoundOrigin(unbound), Is.Null);
            Assert.That(ReplicationAdmittedPeer.CredentialBoundOrigin(mismatched), Is.Null);
            Assert.That(ReplicationAdmittedPeer.CredentialBoundOrigin(unstamped), Is.Null);
            Assert.That(ReplicationAdmittedPeer.CredentialBoundOrigin(bound), Is.EqualTo("site-a"));
        });
    }

    private static void AssertAdmission(string service, string method, ReplicationPeerAdmission expected)
    {
        var name = "/" + service + "/" + method;
        Assert.That(ReplicationAdmittedPeer.TryClassify(name, out var admission), Is.True, name);
        Assert.That(admission, Is.EqualTo(expected), name);
    }

    private static List<string> BoundMethodNames()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var savedPush = LatticeReplicationGrpcMethodHolder.Current;
        var savedSnapshot = LatticeRemoteSnapshotGrpcMethodsHolder.Current;
        var savedSaga = LatticeSagaGrpcMethodsHolder.Current;
        LatticeReplicationGrpcMethodHolder.Current = GrpcTestFactories.CreateMethod(
            Substitute.For<IReplicationBatchEncoder>(), sp.GetRequiredService<Serializer<ReplicationAck>>());
        LatticeRemoteSnapshotGrpcMethodsHolder.Current = new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
        LatticeSagaGrpcMethodsHolder.Current = new LatticeSagaGrpcMethods(
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
        try
        {
            var binder = new RecordingBinder();
            foreach (var type in typeof(LatticeReplicationGrpcAuthInterceptor).Assembly.GetTypes())
            {
                var attribute = type.GetCustomAttribute<BindServiceMethodAttribute>(inherit: false);
                if (attribute is null)
                {
                    continue;
                }

                var bind = attribute.BindType.GetMethod(
                    attribute.BindMethodName,
                    BindingFlags.Public | BindingFlags.Static)
                    ?? throw new InvalidOperationException($"{type} names a missing bind method.");
                bind.Invoke(null, [binder, null]);
            }

            return binder.Names;
        }
        finally
        {
            LatticeReplicationGrpcMethodHolder.Current = savedPush;
            LatticeRemoteSnapshotGrpcMethodsHolder.Current = savedSnapshot;
            LatticeSagaGrpcMethodsHolder.Current = savedSaga;
        }
    }

    private sealed class RecordingBinder : ServiceBinderBase
    {
        public List<string> Names { get; } = [];

        public override void AddMethod<TRequest, TResponse>(Method<TRequest, TResponse> method, UnaryServerMethod<TRequest, TResponse>? handler)
            => Names.Add(method.FullName);

        public override void AddMethod<TRequest, TResponse>(Method<TRequest, TResponse> method, ClientStreamingServerMethod<TRequest, TResponse>? handler)
            => Names.Add(method.FullName);

        public override void AddMethod<TRequest, TResponse>(Method<TRequest, TResponse> method, ServerStreamingServerMethod<TRequest, TResponse>? handler)
            => Names.Add(method.FullName);

        public override void AddMethod<TRequest, TResponse>(Method<TRequest, TResponse> method, DuplexStreamingServerMethod<TRequest, TResponse>? handler)
            => Names.Add(method.FullName);
    }

    private sealed class StubServerCallContext(string method, string? origin = null) : ServerCallContext
    {
        private readonly global::Grpc.Core.Metadata _headers = origin is null
            ? new global::Grpc.Core.Metadata()
            : new global::Grpc.Core.Metadata { { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, origin } };

        protected override string MethodCore => method;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => "ipv4:127.0.0.1:0";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore => _headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
