using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests.PeerStatus;

/// <summary>
/// Unit tests for the peer-status gRPC registration (<c>AddLatticeReplicationStatusApiGrpc</c>,
/// <c>MapLatticeReplicationStatusApiGrpc</c>), the method definitions, and the
/// static <see cref="LatticeReplicationStatusGrpcServiceBase.BindService"/> hook.
/// Mutates the process-wide method holder, so it is not parallelizable.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LatticeReplicationStatusGrpcRegistrationTests
{
    private ServiceProvider _serializers = null!;
    private LatticeReplicationStatusGrpcMethods? _priorHolder;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializers.Dispose();

    [SetUp]
    public void SetUp() => _priorHolder = LatticeReplicationStatusGrpcMethodsHolder.Current;

    [TearDown]
    public void TearDown() => LatticeReplicationStatusGrpcMethodsHolder.Current = _priorHolder;

    [Test]
    public void AddLatticeReplicationStatusApiGrpc_without_the_control_binding_throws()
    {
        Assert.That(
            () => new ServiceCollection().AddSerializer().AddLatticeReplicationStatusApiGrpc(),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeReplicationStatusApiGrpc_null_services_throws()
    {
        Assert.That(
            () => LatticeReplicationApiGrpcServiceCollectionExtensions.AddLatticeReplicationStatusApiGrpc(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationStatusApiGrpc_resolves_the_service_and_populates_the_holder()
    {
        LatticeReplicationStatusGrpcMethodsHolder.Current = null;
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSerializer()
            .AddSingleton(Substitute.For<ILatticeReplicationStatus>())
            .AddLatticeReplicationApiGrpc()
            .AddLatticeReplicationStatusApiGrpc()
            .BuildServiceProvider();

        var service = provider.GetRequiredService<LatticeReplicationStatusGrpcServiceBase>();

        Assert.Multiple(() =>
        {
            Assert.That(service, Is.InstanceOf<LatticeReplicationStatusGrpcService>());
            Assert.That(LatticeReplicationStatusGrpcMethodsHolder.Current, Is.SameAs(
                provider.GetRequiredService<LatticeReplicationStatusGrpcMethods>()));
        });
    }

    [Test]
    public void AddLatticeReplicationStatusApiGrpc_is_idempotent_and_reuses_the_control_binding_security()
    {
        var services = new ServiceCollection().AddSerializer().AddLatticeReplicationApiGrpc();
        var interceptors = services.Count(d => d.ServiceType == typeof(LatticeReplicationApiGrpcAuthInterceptor));
        var authorizers = services.Count(d => d.ServiceType == typeof(ILatticeReplicationApiAuthorizer));

        services.AddLatticeReplicationStatusApiGrpc();
        var afterFirst = services.Count;
        services.AddLatticeReplicationStatusApiGrpc();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count, Is.EqualTo(afterFirst));
            Assert.That(services.Count(d => d.ServiceType == typeof(LatticeReplicationApiGrpcAuthInterceptor)), Is.EqualTo(interceptors));
            Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeReplicationApiAuthorizer)), Is.EqualTo(authorizers));
        });
    }

    [Test]
    public void MapLatticeReplicationStatusApiGrpc_null_endpoints_throws()
    {
        Assert.That(
            () => LatticeReplicationApiGrpcServiceCollectionExtensions.MapLatticeReplicationStatusApiGrpc(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Methods_define_one_unary_rpc_on_the_status_service()
    {
        var methods = LatticeReplicationStatusGrpcMethods.FromServiceProvider(_serializers);

        Assert.Multiple(() =>
        {
            Assert.That(methods.GetPeerStatus.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(methods.GetPeerStatus.ServiceName, Is.EqualTo("orleans.lattice.api.replication.status"));
            Assert.That(methods.GetPeerStatus.Name, Is.EqualTo("GetPeerStatus"));
            Assert.That(
                methods.GetPeerStatus.ServiceName,
                Is.Not.EqualTo(LatticeReplicationGrpcMethods.ServiceName),
                "the control service must stay exactly as it is");
        });
    }

    [Test]
    public void Methods_reject_null_serializers()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeReplicationStatusGrpcMethods.FromServiceProvider(null!), Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeReplicationStatusGrpcMethods(null!, _serializers.GetRequiredService<Serializer<ReplicationPeerStatusPage>>()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new LatticeReplicationStatusGrpcMethods(_serializers.GetRequiredService<Serializer<ReplicationPeerStatusQuery>>(), null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void BindService_null_binder_throws()
    {
        Assert.That(() => LatticeReplicationStatusGrpcServiceBase.BindService(null!, null), Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_uninitialised_holder_throws_invalid_operation()
    {
        LatticeReplicationStatusGrpcMethodsHolder.Current = null;

        Assert.That(
            () => LatticeReplicationStatusGrpcServiceBase.BindService(new RecordingServiceBinder(), null),
            Throws.InvalidOperationException);
    }

    [Test]
    public void BindService_metadata_pass_registers_one_null_handler()
    {
        LatticeReplicationStatusGrpcMethodsHolder.Current = LatticeReplicationStatusGrpcMethods.FromServiceProvider(_serializers);
        var binder = new RecordingServiceBinder();

        LatticeReplicationStatusGrpcServiceBase.BindService(binder, null);

        Assert.Multiple(() =>
        {
            Assert.That(binder.MethodNames, Is.EqualTo(new[] { "GetPeerStatus" }));
            Assert.That(binder.NullHandlerCount, Is.EqualTo(1));
            Assert.That(binder.HandlerCount, Is.Zero);
        });
    }

    [Test]
    public void BindService_instance_pass_registers_one_bound_handler()
    {
        LatticeReplicationStatusGrpcMethodsHolder.Current = LatticeReplicationStatusGrpcMethods.FromServiceProvider(_serializers);
        var binder = new RecordingServiceBinder();

        LatticeReplicationStatusGrpcServiceBase.BindService(binder, new StubService());

        Assert.Multiple(() =>
        {
            Assert.That(binder.MethodNames, Is.EqualTo(new[] { "GetPeerStatus" }));
            Assert.That(binder.HandlerCount, Is.EqualTo(1));
            Assert.That(binder.NullHandlerCount, Is.Zero);
        });
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public int NullHandlerCount { get; private set; }

        public int HandlerCount { get; private set; }

        public List<string> MethodNames { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            MethodNames.Add(method.Name);
            if (handler is null)
            {
                NullHandlerCount++;
            }
            else
            {
                HandlerCount++;
            }
        }
    }

    private sealed class StubService : LatticeReplicationStatusGrpcServiceBase
    {
        public override Task<ReplicationPeerStatusPage> GetPeerStatus(ReplicationPeerStatusQuery request, ServerCallContext context) =>
            Task.FromResult(ReplicationPeerStatusPage.Empty("west"));
    }
}
