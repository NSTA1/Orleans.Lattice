using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for the static <see cref="LatticeReplicationGrpcServiceBase.BindService"/>
/// binding hook that <c>Grpc.AspNetCore</c> reflects against at startup. Drives it
/// against a recording <see cref="ServiceBinderBase"/> so the metadata pass
/// (null instance) and the per-request instance pass are asserted without a gRPC
/// host. Mutates the process-wide method holder, so it is not parallelizable.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LatticeReplicationGrpcServiceBaseBindServiceTests
{
    private ServiceProvider _services = null!;
    private LatticeReplicationGrpcMethods _methods = null!;
    private LatticeReplicationGrpcMethods? _priorHolder;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeReplicationGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _priorHolder = LatticeReplicationGrpcMethodsHolder.Current;
        LatticeReplicationGrpcMethodsHolder.Current = _methods;
    }

    [TearDown]
    public void TearDown() => LatticeReplicationGrpcMethodsHolder.Current = _priorHolder;

    [Test]
    public void BindService_null_binder_throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeReplicationGrpcServiceBase.BindService(null!, null));
    }

    [Test]
    public void BindService_uninitialised_holder_throws_invalid_operation()
    {
        LatticeReplicationGrpcMethodsHolder.Current = null;
        var binder = new RecordingServiceBinder();

        Assert.Throws<InvalidOperationException>(
            () => LatticeReplicationGrpcServiceBase.BindService(binder, null));
    }

    [Test]
    public void BindService_metadata_pass_registers_four_null_handler_methods()
    {
        var binder = new RecordingServiceBinder();

        LatticeReplicationGrpcServiceBase.BindService(binder, null);

        Assert.Multiple(() =>
        {
            Assert.That(binder.NullHandlerCount, Is.EqualTo(4));
            Assert.That(binder.HandlerCount, Is.EqualTo(0));
            Assert.That(binder.MethodNames, Is.EquivalentTo(new[]
            {
                "EnableReplication", "DisableReplication", "GetReplicationConfig", "GetAuthScheme",
            }));
        });
    }

    [Test]
    public void BindService_instance_pass_registers_four_bound_handlers()
    {
        var binder = new RecordingServiceBinder();

        LatticeReplicationGrpcServiceBase.BindService(binder, new StubService());

        Assert.Multiple(() =>
        {
            Assert.That(binder.HandlerCount, Is.EqualTo(4));
            Assert.That(binder.NullHandlerCount, Is.EqualTo(0));
            Assert.That(binder.MethodNames, Is.EquivalentTo(new[]
            {
                "EnableReplication", "DisableReplication", "GetReplicationConfig", "GetAuthScheme",
            }));
        });
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public int NullHandlerCount { get; private set; }

        public int HandlerCount { get; private set; }

        public List<string> MethodNames { get; } = new();

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

    private sealed class StubService : LatticeReplicationGrpcServiceBase
    {
        public override Task<ReplicationEnableResponse> EnableReplication(
            ReplicationEnableRequestMessage request, ServerCallContext context) =>
            Task.FromResult(new ReplicationEnableResponse { TreeId = string.Empty });

        public override Task<ReplicationDisableResponse> DisableReplication(
            ReplicationDisableRequestMessage request, ServerCallContext context) =>
            Task.FromResult(new ReplicationDisableResponse { TreeId = string.Empty });

        public override Task<ReplicationConfigResponse> GetReplicationConfig(
            ReplicationGetConfigRequest request, ServerCallContext context) =>
            Task.FromResult(new ReplicationConfigResponse());

        public override Task<AuthSchemeAdvertisement> GetAuthScheme(
            AuthSchemeAdvertisementRequest request, ServerCallContext context) =>
            Task.FromResult(new AuthSchemeAdvertisement());
    }
}
