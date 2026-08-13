using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminLifecycleToolHandlers"/>, the thin adapter
/// methods behind the tree-administration lifecycle tools. Every test drives a
/// handler with a substituted <see cref="ILatticeTreeAdmin"/> facade and proves the
/// handler forwards the tool-call arguments verbatim and returns the facade result
/// unchanged - it re-implements no authorization, registration, or configuration
/// logic. Covers each of the seven lifecycle operations, the flat-parameter to
/// <see cref="TreeConfigurationUpdate"/> assembly in the config setter, and the
/// null-facade guards. Deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class TreeAdminLifecycleToolHandlersTests
{
    private static ILatticeTreeAdmin TreeAdmin() => Substitute.For<ILatticeTreeAdmin>();

    [Test]
    public async Task CreateTreeAsync_forwards_the_sizing_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeCreationResult { TreeId = "orders", Created = true, ShardCount = 8 };
        admin.CreateTreeAsync("orders", 8, 64, 32, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.CreateTreeAsync(admin, "orders", 8, 64, 32, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).CreateTreeAsync("orders", 8, 64, 32, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateTreeAsync_defaults_all_sizing_to_null()
    {
        var admin = TreeAdmin();
        admin.CreateTreeAsync("orders", null, null, null, Arg.Any<CancellationToken>())
            .Returns(new TreeCreationResult { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.CreateTreeAsync(admin, "orders");

        await admin.Received(1).CreateTreeAsync("orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CheckTreeExistsAsync_forwards_the_tree_id_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeExistenceResult { TreeId = "orders", Exists = true };
        admin.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.CheckTreeExistsAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeAliasAsync_forwards_both_ids_and_returns_the_resolution()
    {
        var admin = TreeAdmin();
        var expected = new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true };
        admin.SetTreeAliasAsync("orders", "phys", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.SetTreeAliasAsync(admin, "orders", "phys", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).SetTreeAliasAsync("orders", "phys", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveTreeAliasAsync_forwards_the_tree_id_and_returns_the_resolution()
    {
        var admin = TreeAdmin();
        var expected = new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true };
        admin.ResolveTreeAliasAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.ResolveTreeAliasAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ResolveTreeAliasAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeConfigAsync_forwards_the_tree_id_and_returns_the_report()
    {
        var admin = TreeAdmin();
        var expected = new TreeConfigurationReport { TreeId = "orders", Exists = true };
        admin.GetTreeConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetTreeConfigAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetTreeConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeConfigAsync_assembles_the_update_from_the_flat_parameters()
    {
        var admin = TreeAdmin();
        var expected = new TreeConfigurationReport { TreeId = "orders", Exists = true };
        admin.SetTreeConfigAsync("orders", Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(
            admin,
            "orders",
            applyPublishEvents: true,
            publishEvents: false,
            applyMaintainProjectionDigest: true,
            maintainProjectionDigest: true,
            applyHistoryRetention: true,
            historyRetentionMode: HistoryRetentionMode.FullValue,
            historyRetentionWindowTicks: 500,
            CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).SetTreeConfigAsync(
            "orders",
            Arg.Is<TreeConfigurationUpdate>(u =>
                u.ApplyPublishEvents == true &&
                u.PublishEvents == false &&
                u.ApplyMaintainProjectionDigest == true &&
                u.MaintainProjectionDigest == true &&
                u.ApplyHistoryRetention == true &&
                u.HistoryRetentionMode == HistoryRetentionMode.FullValue &&
                u.HistoryRetentionWindowTicks == 500),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeConfigAsync_defaults_leave_every_override_unchanged()
    {
        var admin = TreeAdmin();
        admin.SetTreeConfigAsync("orders", Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(new TreeConfigurationReport { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(admin, "orders");

        await admin.Received(1).SetTreeConfigAsync(
            "orders",
            Arg.Is<TreeConfigurationUpdate>(u =>
                !u.ApplyPublishEvents &&
                !u.ApplyMaintainProjectionDigest &&
                !u.ApplyHistoryRetention),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetShardMapAsync_forwards_the_tree_id_and_returns_the_view()
    {
        var admin = TreeAdmin();
        var expected = new TreeShardMapView { TreeId = "orders", PhysicalShardIndices = ImmutableArray<int>.Empty };
        admin.GetShardMapAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetShardMapAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetShardMapAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeDeletionStatusAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true };
        admin.GetTreeDeletionStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetTreeDeletionStatusAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetTreeDeletionStatusAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteTreeAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true, CanRecover = true };
        admin.DeleteTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.DeleteTreeAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).DeleteTreeAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RecoverTreeAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = false };
        admin.RecoverTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.RecoverTreeAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).RecoverTreeAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTreeAsync_forwards_the_confirmation_flag_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true, PurgeComplete = true };
        admin.PurgeTreeAsync("orders", true, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.PurgeTreeAsync(admin, "orders", confirm: true, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).PurgeTreeAsync("orders", true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTreeAsync_defaults_confirmation_to_false()
    {
        var admin = TreeAdmin();
        admin.PurgeTreeAsync("orders", false, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.PurgeTreeAsync(admin, "orders");

        await admin.Received(1).PurgeTreeAsync("orders", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BeginBulkLoadAsync_forwards_the_ids_and_returns_the_session()
    {
        var admin = TreeAdmin();
        var expected = new TreeBulkLoadSession { TreeId = "orders", OperationId = "op-1" };
        admin.BeginBulkLoadAsync("orders", "op-1", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.BeginBulkLoadAsync(admin, "orders", "op-1", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).BeginBulkLoadAsync("orders", "op-1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendBulkLoadAsync_projects_the_dto_entries_and_returns_the_ack()
    {
        var admin = TreeAdmin();
        var expected = new TreeBulkLoadChunkAck { TreeId = "orders", OperationId = "op-1", ChunkIndex = 3, AcceptedEntryCount = 2, NextChunkIndex = 4 };
        admin.AppendBulkLoadAsync("orders", "op-1", 3, Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var dtos = new List<DataEntryDto>
        {
            new() { Key = "a", Value = [1] },
            new() { Key = "b", Value = [2, 3] },
        };

        var result = await TreeAdminLifecycleToolHandlers.AppendBulkLoadAsync(admin, "orders", "op-1", 3, dtos, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).AppendBulkLoadAsync(
            "orders", "op-1", 3,
            Arg.Is<IReadOnlyList<DataEntry>>(e =>
                e.Count == 2 && e[0].Key == "a" && e[1].Key == "b"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AppendBulkLoadAsync_maps_a_null_entry_list_to_an_empty_projection()
    {
        var admin = TreeAdmin();
        admin.AppendBulkLoadAsync("orders", "op-1", 0, Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(new TreeBulkLoadChunkAck { TreeId = "orders", OperationId = "op-1", ChunkIndex = 0, AcceptedEntryCount = 0, NextChunkIndex = 1 });

        await TreeAdminLifecycleToolHandlers.AppendBulkLoadAsync(admin, "orders", "op-1", 0);

        await admin.Received(1).AppendBulkLoadAsync(
            "orders", "op-1", 0,
            Arg.Is<IReadOnlyList<DataEntry>>(e => e.Count == 0),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CommitBulkLoadAsync_forwards_the_ids_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeBulkLoadResult { TreeId = "orders", OperationId = "op-1", TotalLiveKeys = 5 };
        admin.CommitBulkLoadAsync("orders", "op-1", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.CommitBulkLoadAsync(admin, "orders", "op-1", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).CommitBulkLoadAsync("orders", "op-1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RestoreTreeAsync_forwards_the_ids_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = ["bk-1"],
            EntriesApplied = 3,
        };
        admin.RestoreTreeAsync("orders", "bk-1", "op-1", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.RestoreTreeAsync(admin, "orders", "bk-1", "op-1", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).RestoreTreeAsync("orders", "bk-1", "op-1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RestoreTreeAsync_defaults_the_operation_id_to_null()
    {
        var admin = TreeAdmin();
        admin.RestoreTreeAsync("orders", "bk-1", null, Arg.Any<CancellationToken>())
            .Returns(new TreeRestoreResult
            {
                BackupId = "bk-1",
                TargetTreeId = "orders",
                Mode = TreeRestoreMode.ShadowCutover,
                OperationId = "derived",
                ManifestChain = [],
                EntriesApplied = 0,
            });

        await TreeAdminLifecycleToolHandlers.RestoreTreeAsync(admin, "orders", "bk-1");

        await admin.Received(1).RestoreTreeAsync("orders", "bk-1", null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RestoreTreeSetAsync_wraps_the_member_results()
    {
        var admin = TreeAdmin();
        var members = new[]
        {
            new TreeRestoreResult { BackupId = "bk-a", TargetTreeId = "a", Mode = TreeRestoreMode.ShadowCutover, OperationId = "op", ManifestChain = [], EntriesApplied = 1 },
            new TreeRestoreResult { BackupId = "bk-b", TargetTreeId = "b", Mode = TreeRestoreMode.ShadowCutover, OperationId = "op", ManifestChain = [], EntriesApplied = 2 },
        };
        admin.RestoreTreeSetAsync("nightly", Arg.Any<CancellationToken>()).Returns(members);

        var result = await TreeAdminLifecycleToolHandlers.RestoreTreeSetAsync(admin, "nightly", CancellationToken.None);

        Assert.That(result.Results, Is.EqualTo(members));
        await admin.Received(1).RestoreTreeSetAsync("nightly", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RevertTreeRestoreAsync_reconstructs_the_result_and_forwards_it()
    {
        var admin = TreeAdmin();

        var result = await TreeAdminLifecycleToolHandlers.RevertTreeRestoreAsync(
            admin, "orders", "bk-1", "op-1", "phys-new", "phys-old", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(TreeRestoreMode.ShadowCutover));
            Assert.That(result.ShadowPhysicalTreeId, Is.EqualTo("phys-new"));
            Assert.That(result.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
        });
        await admin.Received(1).RevertTreeRestoreAsync(
            Arg.Is<TreeRestoreResult>(r => r.TargetTreeId == "orders" && r.PreviousPhysicalTreeId == "phys-old"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReshardTreeAsync_forwards_the_target_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeReshardStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentPhysicalShardCount = 4,
            VirtualShardCount = 4096,
            MapVersion = 3,
            RequestedShardCount = 4,
        };
        admin.ReshardTreeAsync("orders", 4, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.ReshardTreeAsync(admin, "orders", 4, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ReshardTreeAsync("orders", 4, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetReshardStatusAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeReshardStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentPhysicalShardCount = 2,
            VirtualShardCount = 4096,
            MapVersion = 1,
        };
        admin.GetReshardStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetReshardStatusAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetReshardStatusAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResizeTreeAsync_forwards_the_capacity_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentMaxLeafKeys = 256,
            CurrentMaxInternalChildren = 128,
            RequestedMaxLeafKeys = 256,
            RequestedMaxInternalChildren = 128,
        };
        admin.ResizeTreeAsync("orders", 256, 128, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.ResizeTreeAsync(admin, "orders", 256, 128, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ResizeTreeAsync("orders", 256, 128, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UndoTreeResizeAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentMaxLeafKeys = 64,
            CurrentMaxInternalChildren = 32,
        };
        admin.UndoTreeResizeAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.UndoTreeResizeAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).UndoTreeResizeAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetResizeStatusAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentMaxLeafKeys = 128,
            CurrentMaxInternalChildren = 128,
        };
        admin.GetResizeStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetResizeStatusAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetResizeStatusAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SnapshotTreeAsync_forwards_the_destination_and_mode_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeSnapshotStatus
        {
            TreeId = "orders",
            InProgress = true,
            RequestedDestinationTreeId = "orders-snap",
            RequestedMode = TreeSnapshotMode.Online,
        };
        admin.SnapshotTreeAsync("orders", "orders-snap", TreeSnapshotMode.Online, 128, 64, Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.SnapshotTreeAsync(
            admin, "orders", "orders-snap", TreeSnapshotMode.Online, 128, 64, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).SnapshotTreeAsync(
            "orders", "orders-snap", TreeSnapshotMode.Online, 128, 64, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSnapshotStatusAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeSnapshotStatus
        {
            TreeId = "orders",
            InProgress = false,
        };
        admin.GetSnapshotStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetSnapshotStatusAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetSnapshotStatusAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminLifecycleToolHandlers.CreateTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.CheckTreeExistsAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.SetTreeAliasAsync(null!, "t", "p"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.ResolveTreeAliasAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetTreeConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetShardMapAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetTreeDeletionStatusAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.DeleteTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.RecoverTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.PurgeTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.BeginBulkLoadAsync(null!, "t", "op"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.AppendBulkLoadAsync(null!, "t", "op", 0), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.CommitBulkLoadAsync(null!, "t", "op"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.RestoreTreeAsync(null!, "t", "bk"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.RestoreTreeSetAsync(null!, "set"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.RevertTreeRestoreAsync(null!, "t", "bk", "op", "s", "p"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.ReshardTreeAsync(null!, "t", 4), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetReshardStatusAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.ResizeTreeAsync(null!, "t", 256, 128), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.UndoTreeResizeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetResizeStatusAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.SnapshotTreeAsync(null!, "t", "d", TreeSnapshotMode.Offline), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetSnapshotStatusAsync(null!, "t"), Throws.ArgumentNullException);
        });
    }
}
