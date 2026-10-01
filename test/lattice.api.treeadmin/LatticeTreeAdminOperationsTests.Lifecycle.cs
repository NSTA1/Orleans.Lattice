using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// The orphaned-leaf operations, the shared status, list and cancel verbs, and the
/// deprecated blocking wrappers of <see cref="LatticeTreeAdmin"/>'s accept-then-poll
/// surface (#4124).
/// </summary>
public sealed partial class LatticeTreeAdminOperationsTests
{
    private ILattice WireTree(int shards, params OrphanedLeafRepairReport[] batches)
    {
        var tree = Substitute.For<ILattice>();
        tree.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo(SourceTree, ShardMap.CreateDefault(shards * 16, shards))));
        tree.RepairOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(batches[0], batches[1..]);
        tree.InspectOrphanedLeavesAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(batches[0], batches[1..]);
        _factory.GetGrain<ILattice>(SourceTree, null).Returns(tree);
        return tree;
    }

    private static OrphanedLeafRepairReport Batch(string? resumeFrom, int leaves, params OrphanedLeafDisposition[] findings) => new()
    {
        LeavesWalked = leaves,
        ResumeFrom = resumeFrom,
        Findings = findings.Select(d => new OrphanedLeafFinding { ShardIndex = 0, LeafId = "leaf", Disposition = d }).ToList(),
        Gaps = [],
    };

    [Test]
    public async Task StartOrphanedLeavesRepairAsync_drives_every_batch_and_totals_the_findings()
    {
        var tree = WireTree(
            2,
            Batch(OrphanedLeafPassCursor.Encode(1, null), 5, OrphanedLeafDisposition.Repaired),
            Batch(null, 4, OrphanedLeafDisposition.Repaired, OrphanedLeafDisposition.RefusedUnverifiedKeys));
        var facade = Create();

        var handle = await facade.StartOrphanedLeavesRepairAsync(SourceTree, "repair-1");
        var grain = await UntilCompletedAsync("repair-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(TreeAdminOperationKinds.OrphanedLeavesRepair));
            Assert.That(grain.Completion!.Result[TreeAdminOperationResultKeys.LeavesWalked], Is.EqualTo("9"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.OrphanedLeaves], Is.EqualTo("3"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.Repaired], Is.EqualTo("2"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.Refused], Is.EqualTo("1"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.Gaps], Is.EqualTo("0"));
            Assert.That(
                grain.Reports.Where(r => r.Phase == TreeAdminOperationPhases.Walking).Select(r => r.CompletedUnits).Distinct(),
                Is.EqualTo(new long[] { 0, 1, 2 }),
                "Each batch reports the shards fully walked before it, then the last unit.");
            Assert.That(grain.Reports[^1].TotalUnits, Is.EqualTo(2));
        });
        await tree.Received(1).RepairOrphanedLeavesAsync(null, Arg.Any<CancellationToken>());
        await tree.Received(1).RepairOrphanedLeavesAsync(OrphanedLeafPassCursor.Encode(1, null), Arg.Any<CancellationToken>());
        await tree.DidNotReceiveWithAnyArgs().InspectOrphanedLeavesAsync(default, default);
    }

    [Test]
    public async Task StartOrphanedLeavesAuditAsync_needs_only_read_and_never_repairs()
    {
        var tree = WireTree(1, Batch(null, 3, OrphanedLeafDisposition.Repairable), Batch(null, 0));
        var facade = Create([LatticeOperation.Read]);

        await facade.StartOrphanedLeavesAuditAsync(SourceTree, "audit-1");
        var grain = await UntilCompletedAsync("audit-1");

        Assert.That(grain.Completion!.Result[TreeAdminOperationResultKeys.Repairable], Is.EqualTo("1"));
        await tree.DidNotReceiveWithAnyArgs().RepairOrphanedLeavesAsync(default, default);
    }

    [Test]
    public void StartOrphanedLeavesRepairAsync_requires_the_lifecycle_grant()
    {
        WireTree(1, Batch(null, 0), Batch(null, 0));
        Assert.That(
            async () => await Create([LatticeOperation.Read, LatticeOperation.Admin]).StartOrphanedLeavesRepairAsync(SourceTree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void ShardsWalked_counts_the_shards_below_the_cursor()
    {
        int[] shards = [0, 2, 5, 9];

        Assert.Multiple(() =>
        {
            Assert.That(LatticeTreeAdmin.ShardsWalked(shards, null), Is.Zero);
            Assert.That(LatticeTreeAdmin.ShardsWalked(shards, OrphanedLeafPassCursor.Encode(0, "k")), Is.Zero);
            Assert.That(LatticeTreeAdmin.ShardsWalked(shards, OrphanedLeafPassCursor.Encode(5, null)), Is.EqualTo(2));
            Assert.That(LatticeTreeAdmin.ShardsWalked(shards, OrphanedLeafPassCursor.Encode(42, null)), Is.EqualTo(4));
        });
    }

    private void SeedRecord(string operationId, string kind, string treeId, bool terminal = false)
    {
        OperationFor(operationId).Record = new LatticeOperationRecord
        {
            OperationId = operationId,
            Kind = kind,
            TenantId = Tenant,
            TreeIds = [treeId],
            State = terminal ? Orleans.Lattice.Operations.LatticeOperationState.Succeeded : Orleans.Lattice.Operations.LatticeOperationState.Running,
            Phase = TreeAdminOperationPhases.Copying,
            CompletedUnits = 7,
            TotalUnits = 10,
        };
    }

    [Test]
    public async Task GetOperationStatusAsync_maps_a_visible_operation_and_hides_the_rest()
    {
        SeedRecord("mine", TreeAdminOperationKinds.WalMove, SourceTree);
        SeedRecord("foreign-kind", "backup.capture", SourceTree);

        var visible = await Create([LatticeOperation.Read]).GetOperationStatusAsync("mine");
        var unreadable = await Create([LatticeOperation.Admin]).GetOperationStatusAsync("mine");
        var otherKind = await Create().GetOperationStatusAsync("foreign-kind");
        var unknown = await Create().GetOperationStatusAsync("unknown");

        Assert.Multiple(() =>
        {
            Assert.That(visible!.CompletedUnits, Is.EqualTo(7));
            Assert.That(visible.TotalUnits, Is.EqualTo(10));
            Assert.That(visible.State, Is.EqualTo(Orleans.Lattice.Api.Operations.LatticeOperationState.Running));
            Assert.That(unreadable, Is.Null, "Not found, never forbidden, without the read grant.");
            Assert.That(otherKind, Is.Null, "Another facade's kinds are not this facade's to show.");
            Assert.That(unknown, Is.Null);
        });
    }

    [Test]
    public async Task ListOperationsAsync_lists_only_the_visible_tree_admin_operations()
    {
        SeedRecord("a", TreeAdminOperationKinds.ViewRebuild, SourceTree);
        SeedRecord("b", TreeAdminOperationKinds.WalMove, "other-tree");
        _index.ListAsync(TreeAdminOperationKinds.Prefix, null, LatticeOperationListRequest.DefaultPageSize)
            .Returns(new LatticeOperationIndexPage(["a", "b"], "next"));
        var facade = Create([LatticeOperation.Read]);

        var page = await facade.ListOperationsAsync(new LatticeOperationListRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Operations.Select(o => o.OperationId), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(page.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task CancelOperationAsync_needs_the_grant_that_starting_needed()
    {
        SeedRecord("move", TreeAdminOperationKinds.WalMove, SourceTree);

        Assert.That(async () => await Create([LatticeOperation.Read, LatticeOperation.Admin]).CancelOperationAsync("move"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(OperationFor("move").Record!.CancelRequested, Is.False);

        var cancelled = await Create().CancelOperationAsync("move");
        var hidden = await Create([LatticeOperation.Admin]).CancelOperationAsync("move");

        Assert.Multiple(() =>
        {
            Assert.That(cancelled!.CancelRequested, Is.True);
            Assert.That(hidden, Is.Null, "An operation the caller cannot read is not found.");
        });
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_starts_nothing()
    {
        var maintainer = WireMaintainer();
        var facade = Create();

        var first = await facade.StartViewRebuildAsync(ViewName, "same");
        await UntilCompletedAsync("same");
        var second = await facade.StartViewRebuildAsync(ViewName, "same");

        Assert.Multiple(() =>
        {
            Assert.That(first.Created, Is.True);
            Assert.That(second.Created, Is.False);
        });
        await maintainer.ReceivedWithAnyArgs(1).RebuildTrackedAsync(default!, default);
    }

#pragma warning disable LATTICE0002 // The deprecated wrappers are exercised on purpose.
    [Test]
    public async Task Deprecated_RebuildViewAsync_wraps_a_tracked_operation()
    {
        var maintainer = WireMaintainer();
        var facade = Create();

        var status = await facade.RebuildViewAsync(ViewName);

        Assert.Multiple(() =>
        {
            Assert.That(status.ViewName, Is.EqualTo(ViewName));
            Assert.That(_operations.Values.Single().BeginRequest!.Kind, Is.EqualTo(TreeAdminOperationKinds.ViewRebuild),
                "The blocking verb now runs as a tracked operation, so it is listed and survives as one.");
        });
        await maintainer.ReceivedWithAnyArgs(1).RebuildTrackedAsync(default!, default);
        await maintainer.DidNotReceiveWithAnyArgs().RebuildAsync(default);
    }

    [Test]
    public async Task Deprecated_ReconcileViewAsync_returns_the_operations_drift_verdict()
    {
        var maintainer = WireMaintainer();
        maintainer.ReconcileTrackedAsync(Arg.Any<LatticeOperationTicket>(), Arg.Any<CancellationToken>()).Returns(true);

        var result = await Create().ReconcileViewAsync(ViewName);

        Assert.Multiple(() =>
        {
            Assert.That(result.DriftRepaired, Is.True);
            Assert.That(result.SourceTreeId, Is.EqualTo(SourceTree));
        });
        await maintainer.DidNotReceiveWithAnyArgs().ReconcileAsync(default);
    }

    [Test]
    public void Deprecated_ExecuteWalMoveAsync_rethrows_the_engines_own_exception()
    {
        var admin = Substitute.For<ILatticeAdminTrackedGrain>();
        admin.ExecuteWalMoveTrackedAsync(default!, default, default!, default, default!, default)
            .ReturnsForAnyArgs<Task<WalMoveReceipt>>(_ => throw new ArgumentOutOfRangeException("partition"));
        _factory.GetGrain<ILatticeAdminTrackedGrain>(LatticeConstants.AdminGrainKey, null).Returns(admin);

        Assert.That(async () => await Create().ExecuteWalMoveAsync(SourceTree, 9, "secondary"),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(_operations.Values.Single().Completion!.State, Is.EqualTo(Orleans.Lattice.Operations.LatticeOperationState.Failed));
    }

    [Test]
    public async Task Deprecated_ReconcileTagIndexAsync_wraps_a_tracked_sweep()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(IndexTree).Returns(new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry());
        _factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var coordinator = Substitute.For<ITagIndexReconcileGrain>();
        coordinator.RunTrackedSweepAsync(Arg.Any<LatticeOperationTicket>(), Arg.Any<CancellationToken>())
            .Returns(new TagReconcileReport(2, 5, 6, 1));
        _factory.GetGrain<ITagIndexReconcileGrain>(IndexName, null).Returns(coordinator);

        var report = await Create().ReconcileTagIndexAsync(IndexName);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(IndexTree));
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(1));
        });
        await coordinator.DidNotReceive().RunSweepAsync();
    }
#pragma warning restore LATTICE0002
}
