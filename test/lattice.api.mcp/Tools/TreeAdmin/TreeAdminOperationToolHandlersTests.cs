using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for <see cref="TreeAdminOperationToolHandlers"/> (#4124): each start
/// tool forwards to <see cref="ILatticeTreeAdminOperations"/> (an empty id meaning
/// "generate one") and returns the handle with the status tool to poll; status and
/// cancel report not-found as <c>found=false</c>; listing maps every status.
/// </summary>
[TestFixture]
public sealed class TreeAdminOperationToolHandlersTests
{
    private static LatticeOperationHandle Handle(string kind) =>
        new() { OperationId = "op-1", Kind = kind, Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] }, Created = true };

    private static LatticeOperationStatus Status() => new()
    {
        OperationId = "op-1",
        Kind = TreeAdminOperationKinds.ViewRebuild,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = LatticeOperationState.Running,
        Phase = TreeAdminOperationPhases.Projecting,
        PhaseIndex = 1,
        PhaseCount = 3,
        CompletedUnits = 40,
        TotalUnits = 100,
        UnitName = TreeAdminOperationUnits.Keys,
        Result = new Dictionary<string, string> { ["viewName"] = "v" },
    };

    [Test]
    public async Task Start_tools_forward_to_the_facade_and_name_the_status_tool()
    {
        var ops = Substitute.For<ILatticeTreeAdminOperations>();
        ops.StartViewRebuildAsync("v", null, Arg.Any<CancellationToken>()).Returns(Handle(TreeAdminOperationKinds.ViewRebuild));
        ops.StartViewReconcileAsync("v", "id", Arg.Any<CancellationToken>()).Returns(Handle(TreeAdminOperationKinds.ViewReconcile));
        ops.StartTagIndexReconcileAsync("i", null, Arg.Any<CancellationToken>()).Returns(Handle(TreeAdminOperationKinds.TagIndexReconcile));
        ops.StartWalMoveAsync("orders", 1, "secondary", Arg.Is<TreeWalMoveOptions>(o => o.CopyPageSize == 32 && o.DisableVerifyAfterCopy), null, Arg.Any<CancellationToken>())
            .Returns(Handle(TreeAdminOperationKinds.WalMove));
        ops.StartOrphanedLeavesAuditAsync("orders", null, Arg.Any<CancellationToken>()).Returns(Handle(TreeAdminOperationKinds.OrphanedLeavesAudit));
        ops.StartOrphanedLeavesRepairAsync("orders", null, Arg.Any<CancellationToken>()).Returns(Handle(TreeAdminOperationKinds.OrphanedLeavesRepair));

        var handles = new[]
        {
            await TreeAdminOperationToolHandlers.StartViewRebuildAsync(ops, "v", ""),
            await TreeAdminOperationToolHandlers.StartViewReconcileAsync(ops, "v", "id"),
            await TreeAdminOperationToolHandlers.StartTagIndexReconcileAsync(ops, "i"),
            await TreeAdminOperationToolHandlers.StartWalMoveAsync(ops, "orders", 1, "secondary", copyPageSize: 32, disableVerifyAfterCopy: true),
            await TreeAdminOperationToolHandlers.StartOrphanedLeavesAuditAsync(ops, "orders"),
            await TreeAdminOperationToolHandlers.StartOrphanedLeavesRepairAsync(ops, "orders"),
        };

        Assert.Multiple(() =>
        {
            Assert.That(handles.Select(h => h.Kind), Is.EqualTo(new[]
            {
                TreeAdminOperationKinds.ViewRebuild,
                TreeAdminOperationKinds.ViewReconcile,
                TreeAdminOperationKinds.TagIndexReconcile,
                TreeAdminOperationKinds.WalMove,
                TreeAdminOperationKinds.OrphanedLeavesAudit,
                TreeAdminOperationKinds.OrphanedLeavesRepair,
            }));
            Assert.That(handles, Has.All.Matches<McpTreeAdminOperationHandle>(h =>
                h.StatusTool == "lattice_treeadmin_operation_status" && h.Created && h.TreeIds.SequenceEqual(new[] { "orders" })));
        });
    }

    [Test]
    public async Task Status_maps_the_progress_and_reports_not_found()
    {
        var ops = Substitute.For<ILatticeTreeAdminOperations>();
        ops.GetOperationStatusAsync("op-1", Arg.Any<CancellationToken>()).Returns(Status());
        ops.GetOperationStatusAsync("gone", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);

        var found = await TreeAdminOperationToolHandlers.GetOperationStatusAsync(ops, "op-1");
        var missing = await TreeAdminOperationToolHandlers.GetOperationStatusAsync(ops, "gone");

        Assert.Multiple(() =>
        {
            Assert.That(found.Found, Is.True);
            Assert.That(found.Operation!.State, Is.EqualTo("Running"));
            Assert.That(found.Operation.CompletedUnits, Is.EqualTo(40));
            Assert.That(found.Operation.TotalUnits, Is.EqualTo(100));
            Assert.That(found.Operation.UnitName, Is.EqualTo("keys"));
            Assert.That(found.Operation.PhaseIndex, Is.EqualTo(1));
            Assert.That(found.Operation.Result["viewName"], Is.EqualTo("v"));
            Assert.That(missing.Found, Is.False);
            Assert.That(missing.Operation, Is.Null);
            Assert.That(missing.OperationId, Is.EqualTo("gone"));
        });
    }

    [Test]
    public async Task Cancel_forwards_and_list_pages_with_the_token()
    {
        var ops = Substitute.For<ILatticeTreeAdminOperations>();
        ops.CancelOperationAsync("op-1", Arg.Any<CancellationToken>()).Returns(Status() with { CancelRequested = true });
        ops.ListOperationsAsync(Arg.Is<LatticeOperationListRequest>(r => r.PageToken == "t" && r.PageSize == 5), Arg.Any<CancellationToken>())
            .Returns(new LatticeOperationPage { Operations = [Status()], NextPageToken = "next" });

        var cancelled = await TreeAdminOperationToolHandlers.CancelOperationAsync(ops, "op-1");
        var page = await TreeAdminOperationToolHandlers.ListOperationsAsync(ops, "t", 5);

        Assert.Multiple(() =>
        {
            Assert.That(cancelled.Operation!.CancelRequested, Is.True);
            Assert.That(page.Operations, Has.Count.EqualTo(1));
            Assert.That(page.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public void Every_tool_rejects_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminOperationToolHandlers.StartViewRebuildAsync(null!, "v"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.StartViewReconcileAsync(null!, "v"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.StartTagIndexReconcileAsync(null!, "i"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.StartWalMoveAsync(null!, "t", 0, "k"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.StartOrphanedLeavesAuditAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.StartOrphanedLeavesRepairAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.GetOperationStatusAsync(null!, "o"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.ListOperationsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationToolHandlers.CancelOperationAsync(null!, "o"), Throws.ArgumentNullException);
        });
    }
}
