using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the accept-then-poll tree-administration RPCs (#4124): the
/// service forwards each start, status, list and cancel RPC to
/// <see cref="ILatticeTreeAdminOperations"/> with the wire's tracking id and fails
/// <see cref="StatusCode.Unimplemented"/> when no operations facade is registered,
/// and the client shapes each request onto the matching RPC.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminGrpcOperationsUnitTests
{
    private ServiceProvider _services = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static LatticeOperationHandle Handle(string id, string kind) =>
        new() { OperationId = id, Kind = kind, Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] }, Created = true };

    private static LatticeOperationStatus Status(string id) => new()
    {
        OperationId = id,
        Kind = TreeAdminOperationKinds.WalMove,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = LatticeOperationState.Running,
        Phase = TreeAdminOperationPhases.Copying,
        CompletedUnits = 4,
        TotalUnits = 10,
    };

    private LatticeTreeAdminGrpcService CreateService(ILatticeTreeAdminOperations? operations) =>
        new(
            _methods,
            Substitute.For<ILatticeTreeAdmin>(),
            Substitute.For<ILatticeTreeAdminApiCredentialBridge>(),
            Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>(),
            Options.Create(new LatticeTreeAdminApiGrpcOptions()),
            NullLogger<LatticeTreeAdminGrpcService>.Instance,
            operations: operations);

    private static FakeServerCallContext Context(string method) =>
        new($"/{LatticeTreeAdminGrpcMethods.ServiceName}/{method}");

    [Test]
    public async Task Start_rpcs_forward_the_tracking_id_to_the_operations_facade()
    {
        var operations = Substitute.For<ILatticeTreeAdminOperations>();
        operations.StartViewRebuildAsync("by-region", "op-v", Arg.Any<CancellationToken>()).Returns(Handle("op-v", TreeAdminOperationKinds.ViewRebuild));
        operations.StartViewReconcileAsync("by-region", "op-r", Arg.Any<CancellationToken>()).Returns(Handle("op-r", TreeAdminOperationKinds.ViewReconcile));
        operations.StartTagIndexReconcileAsync("by-tag", "op-t", Arg.Any<CancellationToken>()).Returns(Handle("op-t", TreeAdminOperationKinds.TagIndexReconcile));
        operations.StartWalMoveAsync("orders", 2, "secondary", Arg.Any<TreeWalMoveOptions?>(), "op-w", Arg.Any<CancellationToken>()).Returns(Handle("op-w", TreeAdminOperationKinds.WalMove));
        operations.StartOrphanedLeavesAuditAsync("orders", "op-a", Arg.Any<CancellationToken>()).Returns(Handle("op-a", TreeAdminOperationKinds.OrphanedLeavesAudit));
        operations.StartOrphanedLeavesRepairAsync("orders", "op-p", Arg.Any<CancellationToken>()).Returns(Handle("op-p", TreeAdminOperationKinds.OrphanedLeavesRepair));
        var service = CreateService(operations);

        var handles = new[]
        {
            await service.StartViewRebuild(new TreeAdminViewRequest { ViewName = "by-region", TrackingOperationId = "op-v" }, Context("StartViewRebuild")),
            await service.StartViewReconcile(new TreeAdminViewRequest { ViewName = "by-region", TrackingOperationId = "op-r" }, Context("StartViewReconcile")),
            await service.StartTagIndexReconcile(new TreeAdminTagIndexRequest { IndexName = "by-tag", TrackingOperationId = "op-t" }, Context("StartTagIndexReconcile")),
            await service.StartWalMove(new TreeAdminWalMoveExecuteRequest { TreeId = "orders", Partition = 2, TargetProviderKey = "secondary", TrackingOperationId = "op-w" }, Context("StartWalMove")),
            await service.StartOrphanedLeavesAudit(new TreeAdminOrphanedLeafRequest { TreeId = "orders", TrackingOperationId = "op-a" }, Context("StartOrphanedLeavesAudit")),
            await service.StartOrphanedLeavesRepair(new TreeAdminOrphanedLeafRequest { TreeId = "orders", TrackingOperationId = "op-p" }, Context("StartOrphanedLeavesRepair")),
        };

        Assert.That(handles.Select(h => h.OperationId), Is.EqualTo(new[] { "op-v", "op-r", "op-t", "op-w", "op-a", "op-p" }));
    }

    [Test]
    public async Task Status_list_and_cancel_rpcs_wrap_the_facade_results()
    {
        var operations = Substitute.For<ILatticeTreeAdminOperations>();
        operations.GetOperationStatusAsync("op-1", Arg.Any<CancellationToken>()).Returns(Status("op-1"));
        operations.CancelOperationAsync("missing", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);
        var page = new LatticeOperationPage { Operations = [Status("op-1")], NextPageToken = "next" };
        operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>()).Returns(page);
        var service = CreateService(operations);

        var status = await service.GetTreeAdminOperationStatus(new TreeAdminOperationRequest { OperationId = "op-1" }, Context("GetTreeAdminOperationStatus"));
        var cancelled = await service.CancelTreeAdminOperation(new TreeAdminOperationRequest { OperationId = "missing" }, Context("CancelTreeAdminOperation"));
        var listed = await service.ListTreeAdminOperations(new LatticeOperationListRequest(), Context("ListTreeAdminOperations"));

        Assert.Multiple(() =>
        {
            Assert.That(status.Status!.CompletedUnits, Is.EqualTo(4));
            Assert.That(cancelled.Status, Is.Null, "Not found travels as a null status, never an error.");
            Assert.That(listed, Is.SameAs(page));
        });
    }

    [Test]
    public void Operation_rpcs_without_an_operations_facade_fail_unimplemented()
    {
        var service = CreateService(null);

        var error = Assert.ThrowsAsync<RpcException>(async () => await service.GetTreeAdminOperationStatus(
            new TreeAdminOperationRequest { OperationId = "op-1" }, Context("GetTreeAdminOperationStatus")));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_refused_start_maps_onto_the_shared_fault_statuses()
    {
        var operations = Substitute.For<ILatticeTreeAdminOperations>();
        operations.StartWalMoveAsync(default!, default, default!, default, default, default)
            .ReturnsForAnyArgs<Task<LatticeOperationHandle>>(_ => throw new LatticeAuthorizationDeniedException("no lifecycle grant"));
        var service = CreateService(operations);

        var error = Assert.ThrowsAsync<RpcException>(async () => await service.StartWalMove(
            new TreeAdminWalMoveExecuteRequest { TreeId = "orders", TargetProviderKey = "secondary" }, Context("StartWalMove")));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task Client_start_verbs_carry_the_tracking_id_on_the_matching_rpc()
    {
        var handle = Handle("op-1", TreeAdminOperationKinds.WalMove);
        var invoker = new UnaryResponseCallInvoker(handle);
        var client = new LatticeTreeAdminApiGrpcClient(invoker, _methods);
        var options = new TreeWalMoveOptions { CopyPageSize = 64 };

        var returned = await client.StartWalMoveAsync("orders", 3, "secondary", options, "op-1");
        var request = (TreeAdminWalMoveExecuteRequest)invoker.LastRequest!;

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(handle));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.StartWalMoveMethodName));
            Assert.That(request.TrackingOperationId, Is.EqualTo("op-1"));
            Assert.That(request.Partition, Is.EqualTo(3));
            Assert.That(request.Options, Is.EqualTo(options));
        });

        await client.StartViewRebuildAsync("v", "a");
        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.StartViewRebuildMethodName));
        await client.StartViewReconcileAsync("v", "b");
        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.StartViewReconcileMethodName));
        await client.StartTagIndexReconcileAsync("i", "c");
        Assert.That(((TreeAdminTagIndexRequest)invoker.LastRequest!).TrackingOperationId, Is.EqualTo("c"));
        await client.StartOrphanedLeavesAuditAsync("orders", "d");
        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.StartOrphanedLeavesAuditMethodName));
        await client.StartOrphanedLeavesRepairAsync("orders", "e");
        Assert.That(((TreeAdminOrphanedLeafRequest)invoker.LastRequest!).TrackingOperationId, Is.EqualTo("e"));
    }

    [Test]
    public async Task Client_status_and_cancel_unwrap_the_wire_response()
    {
        var invoker = new UnaryResponseCallInvoker(new TreeAdminOperationStatusResponse { Status = Status("op-1") });
        var client = new LatticeTreeAdminApiGrpcClient(invoker, _methods);

        var status = await client.GetTreeAdminOperationStatusAsync("op-1");
        Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.GetTreeAdminOperationStatusMethodName));
        var cancelled = await client.CancelTreeAdminOperationAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(status!.TotalUnits, Is.EqualTo(10));
            Assert.That(cancelled!.OperationId, Is.EqualTo("op-1"));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.CancelTreeAdminOperationMethodName));
        });
    }

    [Test]
    public async Task Client_list_sends_the_page_request()
    {
        var page = new LatticeOperationPage { Operations = [], NextPageToken = null };
        var invoker = new UnaryResponseCallInvoker(page);
        var client = new LatticeTreeAdminApiGrpcClient(invoker, _methods);
        var request = new LatticeOperationListRequest { PageSize = 5, PageToken = "t" };

        Assert.That(await client.ListTreeAdminOperationsAsync(request), Is.SameAs(page));
        Assert.That(invoker.LastRequest, Is.SameAs(request));
    }

    [Test]
    public void Client_verbs_validate_their_arguments()
    {
        var client = new LatticeTreeAdminApiGrpcClient(new UnaryResponseCallInvoker(new object()), _methods);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await client.StartViewRebuildAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.StartViewReconcileAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.StartTagIndexReconcileAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.StartWalMoveAsync("orders", 0, ""), Throws.ArgumentException);
            Assert.That(async () => await client.StartOrphanedLeavesAuditAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.StartOrphanedLeavesRepairAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.GetTreeAdminOperationStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.CancelTreeAdminOperationAsync(""), Throws.ArgumentException);
            Assert.That(async () => await client.ListTreeAdminOperationsAsync(null!), Throws.ArgumentNullException);
        });
    }
}
