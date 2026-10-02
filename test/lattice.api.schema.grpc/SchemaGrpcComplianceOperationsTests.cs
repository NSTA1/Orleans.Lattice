using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// The accept-then-poll compliance-scan RPCs (#4126) of the schema gRPC binding,
/// without a cluster: each RPC adapts onto <see cref="ILatticeSchemaComplianceOperations"/>,
/// a host without the operations surface answers <see cref="StatusCode.Unimplemented"/>,
/// the interceptor names every new RPC (appending its operations after the shipped
/// values) and targets a start at its tree, the wire messages round-trip, and the
/// client sends each request to the right RPC.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcComplianceOperationsTests
{
    private const string Tree = "orders";

    private ServiceProvider _serializerProvider = null!;
    private LatticeSchemaGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeSchemaGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private LatticeSchemaGrpcService CreateService(ILatticeSchemaComplianceOperations? operations)
    {
        var bridge = Substitute.For<ILatticeSchemaApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeSchemaGrpcService(
            _methods,
            Substitute.For<ILatticeSchemaControl>(),
            bridge,
            Substitute.For<ILatticeSchemaApiAuthSchemeSource>(),
            Options.Create(new LatticeSchemaApiGrpcOptions()),
            NullLogger<LatticeSchemaGrpcService>.Instance,
            operations);
    }

    private static FakeServerCallContext Context(string methodName) => new(SchemaGrpcTestDoubles.FullMethod(methodName));

    private static LatticeOperationHandle Handle(string id = "scan-1") => new()
    {
        OperationId = id,
        Kind = SchemaComplianceScanOperation.Kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [Tree] },
        Created = true,
    };

    private static LatticeOperationStatus Status(string id = "scan-1") => new()
    {
        OperationId = id,
        Kind = SchemaComplianceScanOperation.Kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [Tree] },
        State = LatticeOperationState.Running,
        Phase = SchemaComplianceScanOperation.ScanningPhase,
        CompletedUnits = 3,
        TotalUnits = 9,
        UnitName = SchemaComplianceScanOperation.EntriesUnit,
    };

    [Test]
    public async Task StartComplianceScan_starts_the_scan_and_returns_its_handle()
    {
        var operations = Substitute.For<ILatticeSchemaComplianceOperations>();
        var expected = Handle();
        operations.StartComplianceScanAsync(Tree, "scan-1", Arg.Any<CancellationToken>()).Returns(expected);

        var handle = await CreateService(operations).StartComplianceScan(
            new SchemaComplianceScanStartRequest { TreeId = Tree, OperationId = "scan-1" },
            Context(LatticeSchemaGrpcMethods.StartComplianceScanMethodName));

        Assert.That(handle, Is.SameAs(expected));
    }

    [Test]
    public async Task Status_list_and_cancel_adapt_onto_the_operations_surface()
    {
        var operations = Substitute.For<ILatticeSchemaComplianceOperations>();
        var running = Status();
        var page = new LatticeOperationPage { Operations = [Status()], NextPageToken = "next" };
        var listRequest = new LatticeOperationListRequest { PageSize = 5 };
        operations.GetOperationStatusAsync("scan-1", Arg.Any<CancellationToken>()).Returns(running);
        operations.GetOperationStatusAsync("missing", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);
        operations.ListOperationsAsync(listRequest, Arg.Any<CancellationToken>()).Returns(page);
        operations.CancelOperationAsync("scan-1", Arg.Any<CancellationToken>()).Returns(Status() with { CancelRequested = true });
        var service = CreateService(operations);

        var status = await service.GetComplianceScanStatus(
            new SchemaComplianceOperationRequest { OperationId = "scan-1" },
            Context(LatticeSchemaGrpcMethods.GetComplianceScanStatusMethodName));
        var missing = await service.GetComplianceScanStatus(
            new SchemaComplianceOperationRequest { OperationId = "missing" },
            Context(LatticeSchemaGrpcMethods.GetComplianceScanStatusMethodName));
        var listed = await service.ListComplianceScans(listRequest, Context(LatticeSchemaGrpcMethods.ListComplianceScansMethodName));
        var cancelled = await service.CancelComplianceScan(
            new SchemaComplianceOperationRequest { OperationId = "scan-1" },
            Context(LatticeSchemaGrpcMethods.CancelComplianceScanMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(status.Status, Is.SameAs(running));
            Assert.That(missing.Status, Is.Null);
            Assert.That(listed, Is.SameAs(page));
            Assert.That(cancelled.Status!.CancelRequested, Is.True);
        });
    }

    [Test]
    public void A_host_without_the_operations_surface_answers_unimplemented()
    {
        var service = CreateService(null);

        var ex = Assert.ThrowsAsync<RpcException>(() => service.StartComplianceScan(
            new SchemaComplianceScanStartRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.StartComplianceScanMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_denied_start_maps_to_permission_denied_and_a_malformed_id_to_invalid_argument()
    {
        var operations = Substitute.For<ILatticeSchemaComplianceOperations>();
        operations.StartComplianceScanAsync(Tree, "denied", Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no read"));
        operations.StartComplianceScanAsync(Tree, "bad id", Arg.Any<CancellationToken>())
            .ThrowsAsync(new ArgumentException("malformed"));
        var service = CreateService(operations);

        var denied = Assert.ThrowsAsync<RpcException>(() => service.StartComplianceScan(
            new SchemaComplianceScanStartRequest { TreeId = Tree, OperationId = "denied" },
            Context(LatticeSchemaGrpcMethods.StartComplianceScanMethodName)));
        var malformed = Assert.ThrowsAsync<RpcException>(() => service.StartComplianceScan(
            new SchemaComplianceScanStartRequest { TreeId = Tree, OperationId = "bad id" },
            Context(LatticeSchemaGrpcMethods.StartComplianceScanMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(denied!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(malformed!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
        });
    }

    [Test]
    public void The_interceptor_names_each_new_rpc_and_targets_a_start_at_its_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                    SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.StartComplianceScanMethodName),
                    new SchemaComplianceScanStartRequest { TreeId = Tree }),
                Is.EqualTo((LatticeSchemaApiOperation.StartComplianceScan, (string?)Tree)));
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                    SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.GetComplianceScanStatusMethodName),
                    new SchemaComplianceOperationRequest { OperationId = "scan-1" }),
                Is.EqualTo((LatticeSchemaApiOperation.GetComplianceScanStatus, (string?)null)));
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                    SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.ListComplianceScansMethodName),
                    new LatticeOperationListRequest()),
                Is.EqualTo((LatticeSchemaApiOperation.ListComplianceScans, (string?)null)));
            Assert.That(
                LatticeSchemaApiGrpcAuthInterceptor.DescribeCall(
                    SchemaGrpcTestDoubles.FullMethod(LatticeSchemaGrpcMethods.CancelComplianceScanMethodName),
                    new SchemaComplianceOperationRequest { OperationId = "scan-1" }),
                Is.EqualTo((LatticeSchemaApiOperation.CancelComplianceScan, (string?)null)));
        });
    }

    [Test]
    public void The_new_operations_are_appended_after_the_shipped_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeSchemaApiOperation.ProbeCapabilities, Is.EqualTo(14));
            Assert.That((int)LatticeSchemaApiOperation.Unknown, Is.EqualTo(15));
            Assert.That((int)LatticeSchemaApiOperation.StartComplianceScan, Is.GreaterThan((int)LatticeSchemaApiOperation.Unknown));
        });
    }

    [Test]
    public void The_new_wire_messages_round_trip()
    {
        var serializer = _serializerProvider.GetRequiredService<Serializer>();
        var start = new SchemaComplianceScanStartRequest { TreeId = Tree, OperationId = "scan-1" };
        var request = new SchemaComplianceOperationRequest { OperationId = "scan-1" };
        var response = new SchemaComplianceOperationStatusResponse { Status = Status() };

        Assert.Multiple(() =>
        {
            Assert.That(serializer.Deserialize<SchemaComplianceScanStartRequest>(serializer.SerializeToArray(start)), Is.EqualTo(start));
            Assert.That(serializer.Deserialize<SchemaComplianceOperationRequest>(serializer.SerializeToArray(request)), Is.EqualTo(request));
            Assert.That(
                serializer.Deserialize<SchemaComplianceOperationStatusResponse>(serializer.SerializeToArray(response)).Status!.CompletedUnits,
                Is.EqualTo(3));
        });
    }

    [Test]
    public async Task The_client_sends_each_compliance_operation_call_to_its_rpc()
    {
        var startInvoker = FakeCallInvoker.ForUnary(Handle());
        var statusInvoker = FakeCallInvoker.ForUnary(new SchemaComplianceOperationStatusResponse { Status = Status() });
        var listInvoker = FakeCallInvoker.ForUnary(new LatticeOperationPage());

        var handle = await LatticeSchemaApiGrpcClient.Create(startInvoker, _serializerProvider).StartComplianceScanAsync(Tree, "scan-1");
        var status = await LatticeSchemaApiGrpcClient.Create(statusInvoker, _serializerProvider).GetComplianceScanStatusAsync("scan-1");
        await LatticeSchemaApiGrpcClient.Create(listInvoker, _serializerProvider).ListComplianceScansAsync(new LatticeOperationListRequest());
        var cancelInvoker = FakeCallInvoker.ForUnary(new SchemaComplianceOperationStatusResponse());
        var cancelled = await LatticeSchemaApiGrpcClient.Create(cancelInvoker, _serializerProvider).CancelComplianceScanAsync("scan-1");

        Assert.Multiple(() =>
        {
            Assert.That(startInvoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.StartComplianceScanMethodName));
            Assert.That(startInvoker.LastRequest, Is.EqualTo(new SchemaComplianceScanStartRequest { TreeId = Tree, OperationId = "scan-1" }));
            Assert.That(handle.OperationId, Is.EqualTo("scan-1"));
            Assert.That(statusInvoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.GetComplianceScanStatusMethodName));
            Assert.That(status!.TotalUnits, Is.EqualTo(9));
            Assert.That(listInvoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.ListComplianceScansMethodName));
            Assert.That(cancelInvoker.LastMethodName, Is.EqualTo(LatticeSchemaGrpcMethods.CancelComplianceScanMethodName));
            Assert.That(cancelled, Is.Null);
        });
    }

    [Test]
    public void The_client_refuses_missing_arguments()
    {
        var client = LatticeSchemaApiGrpcClient.Create(FakeCallInvoker.ForUnary(new SchemaComplianceOperationStatusResponse()), _serializerProvider);

        Assert.Multiple(() =>
        {
            Assert.That(() => client.StartComplianceScanAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.GetComplianceScanStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.CancelComplianceScanAsync(""), Throws.ArgumentException);
            Assert.That(() => client.ListComplianceScansAsync(null!), Throws.ArgumentNullException);
        });
    }
}
