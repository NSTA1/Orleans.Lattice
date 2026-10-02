using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// The accept-then-poll storage-usage refresh RPCs (#4126) of the tree-admin gRPC
/// binding, without a cluster: each RPC adapts onto
/// <see cref="ILatticeStorageUsageOperations"/>, a host without the operations
/// surface answers <see cref="StatusCode.Unimplemented"/>, the interceptor names
/// every new RPC with its own appended operation, the wire messages round-trip, and
/// the client sends each request to the right RPC.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcStorageUsageOperationsTests
{
    private ServiceProvider _serializerProvider = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private LatticeTreeAdminGrpcService CreateService(ILatticeStorageUsageOperations? operations)
    {
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeTreeAdminGrpcService(
            _methods,
            Substitute.For<ILatticeTreeAdmin>(),
            bridge,
            Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>(),
            Options.Create(new LatticeTreeAdminApiGrpcOptions()),
            NullLogger<LatticeTreeAdminGrpcService>.Instance,
            operations);
    }

    private static FakeServerCallContext Context(string methodName) =>
        new("/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + methodName);

    private static LatticeOperationStatus Status() => new()
    {
        OperationId = "refresh-1",
        Kind = StorageUsageRefreshOperation.Kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [] },
        State = LatticeOperationState.Running,
        Phase = StorageUsageRefreshOperation.MeasuringPhase,
        CompletedUnits = 2,
        TotalUnits = 5,
        UnitName = StorageUsageRefreshOperation.TreesUnit,
    };

    [Test]
    public async Task Each_rpc_adapts_onto_the_operations_surface()
    {
        var operations = Substitute.For<ILatticeStorageUsageOperations>();
        var handle = new LatticeOperationHandle
        {
            OperationId = "refresh-1",
            Kind = StorageUsageRefreshOperation.Kind,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [] },
            Created = true,
        };
        var running = Status();
        var page = new LatticeOperationPage { Operations = [running] };
        var listRequest = new LatticeOperationListRequest();
        operations.StartStorageUsageRefreshAsync("refresh-1", Arg.Any<CancellationToken>()).Returns(handle);
        operations.GetOperationStatusAsync("refresh-1", Arg.Any<CancellationToken>()).Returns(running);
        operations.ListOperationsAsync(listRequest, Arg.Any<CancellationToken>()).Returns(page);
        operations.CancelOperationAsync("refresh-1", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);
        var service = CreateService(operations);

        var started = await service.StartStorageUsageRefresh(
            new TreeAdminStorageUsageRefreshRequest { OperationId = "refresh-1" },
            Context(LatticeTreeAdminGrpcMethods.StartStorageUsageRefreshMethodName));
        var status = await service.GetStorageUsageRefreshStatus(
            new TreeAdminStorageUsageOperationRequest { OperationId = "refresh-1" },
            Context(LatticeTreeAdminGrpcMethods.GetStorageUsageRefreshStatusMethodName));
        var listed = await service.ListStorageUsageRefreshes(listRequest, Context(LatticeTreeAdminGrpcMethods.ListStorageUsageRefreshesMethodName));
        var cancelled = await service.CancelStorageUsageRefresh(
            new TreeAdminStorageUsageOperationRequest { OperationId = "refresh-1" },
            Context(LatticeTreeAdminGrpcMethods.CancelStorageUsageRefreshMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.SameAs(handle));
            Assert.That(status.Status, Is.SameAs(running));
            Assert.That(listed, Is.SameAs(page));
            Assert.That(cancelled.Status, Is.Null);
        });
    }

    [Test]
    public void A_host_without_the_operations_surface_answers_unimplemented()
    {
        var ex = Assert.ThrowsAsync<RpcException>(() => CreateService(null).StartStorageUsageRefresh(
            new TreeAdminStorageUsageRefreshRequest(),
            Context(LatticeTreeAdminGrpcMethods.StartStorageUsageRefreshMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_caller_without_telemetry_is_refused_with_permission_denied()
    {
        var operations = Substitute.For<ILatticeStorageUsageOperations>();
        operations.StartStorageUsageRefreshAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no telemetry"));

        var ex = Assert.ThrowsAsync<RpcException>(() => CreateService(operations).StartStorageUsageRefresh(
            new TreeAdminStorageUsageRefreshRequest(),
            Context(LatticeTreeAdminGrpcMethods.StartStorageUsageRefreshMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [TestCase(LatticeTreeAdminGrpcMethods.StartStorageUsageRefreshMethodName, LatticeTreeAdminApiOperation.StartStorageUsageRefresh)]
    [TestCase(LatticeTreeAdminGrpcMethods.GetStorageUsageRefreshStatusMethodName, LatticeTreeAdminApiOperation.GetStorageUsageRefreshStatus)]
    [TestCase(LatticeTreeAdminGrpcMethods.ListStorageUsageRefreshesMethodName, LatticeTreeAdminApiOperation.ListStorageUsageRefreshes)]
    [TestCase(LatticeTreeAdminGrpcMethods.CancelStorageUsageRefreshMethodName, LatticeTreeAdminApiOperation.CancelStorageUsageRefresh)]
    public void The_interceptor_names_each_new_rpc_with_no_tree_target(string methodName, LatticeTreeAdminApiOperation expected)
    {
        var (operation, target) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + methodName,
            new TreeAdminStorageUsageOperationRequest { OperationId = "refresh-1" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(expected));
            Assert.That(target, Is.Null, "A cluster-wide refresh and an operation id name no tree.");
            Assert.That((int)expected, Is.GreaterThan((int)LatticeTreeAdminApiOperation.CreateView), "Appended after the shipped values.");
        });
    }

    [Test]
    public void The_new_wire_messages_round_trip()
    {
        var serializer = _serializerProvider.GetRequiredService<Serializer>();
        var start = new TreeAdminStorageUsageRefreshRequest { OperationId = "refresh-1" };
        var request = new TreeAdminStorageUsageOperationRequest { OperationId = "refresh-1" };
        var response = new TreeAdminStorageUsageOperationStatusResponse { Status = Status() };

        Assert.Multiple(() =>
        {
            Assert.That(serializer.Deserialize<TreeAdminStorageUsageRefreshRequest>(serializer.SerializeToArray(start)), Is.EqualTo(start));
            Assert.That(serializer.Deserialize<TreeAdminStorageUsageOperationRequest>(serializer.SerializeToArray(request)), Is.EqualTo(request));
            Assert.That(
                serializer.Deserialize<TreeAdminStorageUsageOperationStatusResponse>(serializer.SerializeToArray(response)).Status!.TotalUnits,
                Is.EqualTo(5));
        });
    }

    [Test]
    public async Task The_client_sends_each_refresh_call_to_its_rpc()
    {
        var startInvoker = new UnaryResponseCallInvoker(new LatticeOperationHandle
        {
            OperationId = "refresh-1",
            Kind = StorageUsageRefreshOperation.Kind,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [] },
        });
        var statusInvoker = new UnaryResponseCallInvoker(new TreeAdminStorageUsageOperationStatusResponse { Status = Status() });
        var listInvoker = new UnaryResponseCallInvoker(new LatticeOperationPage());
        var cancelInvoker = new UnaryResponseCallInvoker(new TreeAdminStorageUsageOperationStatusResponse());

        var handle = await LatticeTreeAdminApiGrpcClient.Create(startInvoker, _serializerProvider).StartStorageUsageRefreshAsync("refresh-1");
        var status = await LatticeTreeAdminApiGrpcClient.Create(statusInvoker, _serializerProvider).GetStorageUsageRefreshStatusAsync("refresh-1");
        await LatticeTreeAdminApiGrpcClient.Create(listInvoker, _serializerProvider).ListStorageUsageRefreshesAsync(new LatticeOperationListRequest());
        var cancelled = await LatticeTreeAdminApiGrpcClient.Create(cancelInvoker, _serializerProvider).CancelStorageUsageRefreshAsync("refresh-1");

        Assert.Multiple(() =>
        {
            Assert.That(startInvoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.StartStorageUsageRefreshMethodName));
            Assert.That(startInvoker.LastRequest, Is.EqualTo(new TreeAdminStorageUsageRefreshRequest { OperationId = "refresh-1" }));
            Assert.That(handle.OperationId, Is.EqualTo("refresh-1"));
            Assert.That(statusInvoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.GetStorageUsageRefreshStatusMethodName));
            Assert.That(status!.CompletedUnits, Is.EqualTo(2));
            Assert.That(listInvoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.ListStorageUsageRefreshesMethodName));
            Assert.That(cancelInvoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.CancelStorageUsageRefreshMethodName));
            Assert.That(cancelled, Is.Null);
        });
    }

    [Test]
    public void The_client_refuses_missing_arguments()
    {
        var client = LatticeTreeAdminApiGrpcClient.Create(
            new UnaryResponseCallInvoker(new TreeAdminStorageUsageOperationStatusResponse()), _serializerProvider);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await client.GetStorageUsageRefreshStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.CancelStorageUsageRefreshAsync(""), Throws.ArgumentException);
            Assert.That(() => client.ListStorageUsageRefreshesAsync(null!), Throws.ArgumentNullException);
        });
    }
}
