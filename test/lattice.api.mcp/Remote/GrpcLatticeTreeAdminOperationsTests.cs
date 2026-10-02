using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for <see cref="GrpcLatticeTreeAdminOperations"/>, the remote-mode
/// adapter that forwards the accept-then-poll tree-administration verbs to a
/// control-API endpoint over gRPC (#4124). Deterministic over a
/// <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTreeAdminOperationsTests
{
    private static GrpcLatticeTreeAdminOperations Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TreeAdminClient(invoker));

    private static LatticeOperationHandle Handle() =>
        new() { OperationId = "op-1", Kind = TreeAdminOperationKinds.WalMove, Scope = new LatticeOperationScope { TenantId = "default" }, Created = true };

    private static LatticeOperationStatus Status() => new()
    {
        OperationId = "op-1",
        Kind = TreeAdminOperationKinds.WalMove,
        Scope = new LatticeOperationScope { TenantId = "default" },
        State = LatticeOperationState.Succeeded,
        Phase = "Completed",
    };

    [Test]
    public async Task Start_verbs_forward_their_arguments_and_tracking_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle());
        var adapter = Adapter(invoker);

        await adapter.StartWalMoveAsync("orders", 2, "secondary", null, "op-1");
        var move = (TreeAdminWalMoveExecuteRequest)invoker.LastRequest!;
        await adapter.StartViewRebuildAsync("v", "a");
        var rebuild = (TreeAdminViewRequest)invoker.LastRequest!;
        await adapter.StartViewReconcileAsync("v", "b");
        await adapter.StartTagIndexReconcileAsync("i", "c");
        var tag = (TreeAdminTagIndexRequest)invoker.LastRequest!;
        await adapter.StartOrphanedLeavesAuditAsync("orders", "d");
        await adapter.StartOrphanedLeavesRepairAsync("orders", "e");
        var repair = (TreeAdminOrphanedLeafRequest)invoker.LastRequest!;

        Assert.Multiple(() =>
        {
            Assert.That(move.Partition, Is.EqualTo(2));
            Assert.That(move.TrackingOperationId, Is.EqualTo("op-1"));
            Assert.That(rebuild.TrackingOperationId, Is.EqualTo("a"));
            Assert.That(tag.IndexName, Is.EqualTo("i"));
            Assert.That(repair.TrackingOperationId, Is.EqualTo("e"));
        });
    }

    [Test]
    public async Task Status_list_and_cancel_unwrap_the_wire_responses()
    {
        var page = new LatticeOperationPage { Operations = [Status()] };
        var invoker = new FakeCallInvoker(request => request is LatticeOperationListRequest
            ? page
            : new TreeAdminOperationStatusResponse { Status = Status() });
        var adapter = Adapter(invoker);

        var status = await adapter.GetOperationStatusAsync("op-1");
        var cancelled = await adapter.CancelOperationAsync("op-1");
        var listed = await adapter.ListOperationsAsync(new LatticeOperationListRequest());

        Assert.Multiple(() =>
        {
            Assert.That(status!.OperationId, Is.EqualTo("op-1"));
            Assert.That(cancelled!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(listed.Operations, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void The_adapter_rejects_a_null_client() =>
        Assert.That(() => new GrpcLatticeTreeAdminOperations(null!), Throws.ArgumentNullException);
}
