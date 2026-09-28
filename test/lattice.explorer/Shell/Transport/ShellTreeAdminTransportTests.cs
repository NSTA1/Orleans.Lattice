using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTreeAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTreeAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTreeAdmin>
{
    private const string Service = "/orleans.lattice.api.treeadmin/";

    private static readonly TreeRestoreResult Restore = new()
    {
        BackupId = "b1",
        TargetTreeId = "orders",
        Mode = TreeRestoreMode.ShadowCutover,
        OperationId = "op-1",
        ManifestChain = ["b1"],
        EntriesApplied = 3,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeTreeAdmin>> Calls() =>
    [
        new("ProbeCapabilitiesAsync", Service + "ProbeCapabilities", (f, ct) => f.ProbeCapabilitiesAsync("orders", ct)),
        new("GetShardHotnessAsync", Service + "GetShardHotness", (f, ct) => f.GetShardHotnessAsync("orders", ct)),
        new("GetDiagnosticsAsync", Service + "GetDiagnostics", (f, ct) => f.GetDiagnosticsAsync("orders", true, ct)),
        new("InspectShardMapAsync", Service + "InspectShardMap", (f, ct) => f.InspectShardMapAsync("orders", ct)),
        new("GetProjectionDigestAsync", Service + "GetProjectionDigest", (f, ct) => f.GetProjectionDigestAsync("orders", 1, ct)),
        new("GetTreeStatsAsync", Service + "GetTreeStats", (f, ct) => f.GetTreeStatsAsync("orders", ct)),
        new("GetStorageUsageAsync", Service + "GetStorageUsage", (f, ct) => f.GetStorageUsageAsync(true, ct)),
        new("CreateTreeAsync", Service + "CreateTree", (f, ct) => f.CreateTreeAsync("orders", 4, 128, 64, ct)),
        new("CheckTreeExistsAsync", Service + "CheckTreeExists", (f, ct) => f.CheckTreeExistsAsync("orders", ct)),
        new("SetTreeAliasAsync", Service + "SetTreeAlias", (f, ct) => f.SetTreeAliasAsync("orders", "orders-v2", ct)),
        new("ResolveTreeAliasAsync", Service + "ResolveTreeAlias", (f, ct) => f.ResolveTreeAliasAsync("orders", ct)),
        new("GetTreeConfigAsync", Service + "GetTreeConfig", (f, ct) => f.GetTreeConfigAsync("orders", ct)),
        new("SetTreeConfigAsync", Service + "SetTreeConfig", (f, ct) => f.SetTreeConfigAsync("orders", new TreeConfigurationUpdate { ApplyPublishEvents = true, PublishEvents = true }, ct)),
        new("GetShardMapAsync", Service + "GetShardMap", (f, ct) => f.GetShardMapAsync("orders", ct)),
        new("DeleteTreeAsync", Service + "DeleteTree", (f, ct) => f.DeleteTreeAsync("orders", ct)),
        new("RecoverTreeAsync", Service + "RecoverTree", (f, ct) => f.RecoverTreeAsync("orders", ct)),
        new("PurgeTreeAsync", Service + "PurgeTree", (f, ct) => f.PurgeTreeAsync("orders", true, ct)),
        new("GetTreeDeletionStatusAsync", Service + "GetTreeDeletionStatus", (f, ct) => f.GetTreeDeletionStatusAsync("orders", ct)),
        new("BeginBulkLoadAsync", Service + "BeginBulkLoad", (f, ct) => f.BeginBulkLoadAsync("orders", "load-1", ct)),
        new("AppendBulkLoadAsync", Service + "AppendBulkLoad", (f, ct) => f.AppendBulkLoadAsync("orders", "load-1", 0, [new DataEntry { Key = "k" }], ct)),
        new("CommitBulkLoadAsync", Service + "CommitBulkLoad", (f, ct) => f.CommitBulkLoadAsync("orders", "load-1", ct)),
        new("RestoreTreeAsync", Service + "RestoreTree", (f, ct) => f.RestoreTreeAsync("orders", "b1", "op-1", ct)),
        new("RestoreTreeSetAsync", Service + "RestoreTreeSet", (f, ct) => f.RestoreTreeSetAsync("set-1", ct)),
        new("RevertTreeRestoreAsync", Service + "RevertTreeRestore", (f, ct) => f.RevertTreeRestoreAsync(Restore, ct)),
        new("ReshardTreeAsync", Service + "ReshardTree", (f, ct) => f.ReshardTreeAsync("orders", 8, ct)),
        new("GetReshardStatusAsync", Service + "GetReshardStatus", (f, ct) => f.GetReshardStatusAsync("orders", ct)),
        new("ResizeTreeAsync", Service + "ResizeTree", (f, ct) => f.ResizeTreeAsync("orders", 256, 64, ct)),
        new("UndoTreeResizeAsync", Service + "UndoTreeResize", (f, ct) => f.UndoTreeResizeAsync("orders", ct)),
        new("GetResizeStatusAsync", Service + "GetResizeStatus", (f, ct) => f.GetResizeStatusAsync("orders", ct)),
        new("SnapshotTreeAsync", Service + "SnapshotTree", (f, ct) => f.SnapshotTreeAsync("orders", "orders-copy", TreeSnapshotMode.Online, 128, 64, ct)),
        new("GetSnapshotStatusAsync", Service + "GetSnapshotStatus", (f, ct) => f.GetSnapshotStatusAsync("orders", ct)),
        new("GetWalPlacementAsync", Service + "GetWalPlacement", (f, ct) => f.GetWalPlacementAsync("orders", ct)),
        new("AuditWalPlacementAsync", Service + "AuditWalPlacement", (f, ct) => f.AuditWalPlacementAsync("orders", ct)),
        new("AuditOrphanedLeavesAsync", Service + "AuditOrphanedLeaves", (f, ct) => f.AuditOrphanedLeavesAsync("orders", "r1", ct)),
        new("SurveyOrphanedLeavesAsync", Service + "AuditOrphanedLeaves", (f, ct) => f.SurveyOrphanedLeavesAsync("orders", "r1", ct)),
        new("RepairOrphanedLeavesAsync", Service + "RepairOrphanedLeaves", (f, ct) => f.RepairOrphanedLeavesAsync("orders", "r1", ct)),
        new("PlanWalMoveAsync", Service + "PlanWalMove", (f, ct) => f.PlanWalMoveAsync("orders", 0, "cold", ct)),
        new("ExecuteWalMoveAsync", Service + "ExecuteWalMove", (f, ct) => f.ExecuteWalMoveAsync("orders", 0, "cold", null, ct)),
        new("ReclaimMovedWalSourceAsync", Service + "ReclaimMovedWalSource", (f, ct) => f.ReclaimMovedWalSourceAsync("orders", 0, "hot", ct)),
        new("ListViewsAsync", Service + "ListViews", (f, ct) => f.ListViewsAsync(ct)),
        new("CreateViewAsync", Service + "CreateView", (f, ct) => f.CreateViewAsync("by-customer", "orders", "customer-index", [1, 2], ct)),
        new("GetViewStatusAsync", Service + "GetViewStatus", (f, ct) => f.GetViewStatusAsync("by-customer", ct)),
        new("RebuildViewAsync", Service + "RebuildView", (f, ct) => f.RebuildViewAsync("by-customer", ct)),
        new("ReconcileViewAsync", Service + "ReconcileView", (f, ct) => f.ReconcileViewAsync("by-customer", ct)),
        new("DropViewAsync", Service + "DropView", (f, ct) => f.DropViewAsync("by-customer", ct)),
        new("ListTagIndexesAsync", Service + "ListTagIndexes", (f, ct) => f.ListTagIndexesAsync(ct)),
        new("GetTagIndexStatusAsync", Service + "GetTagIndexStatus", (f, ct) => f.GetTagIndexStatusAsync("colour", ct)),
        new("ReconcileTagIndexAsync", Service + "ReconcileTagIndex", (f, ct) => f.ReconcileTagIndexAsync("colour", ct)),
        new("TriggerShardCompactionAsync", Service + "TriggerShardCompaction", (f, ct) => f.TriggerShardCompactionAsync("orders", 1, ct)),
        new("GetHistoryRetentionAsync", Service + "GetHistoryRetention", (f, ct) => f.GetHistoryRetentionAsync("orders", ct)),
        new("SetHistoryRetentionAsync", Service + "SetHistoryRetention", (f, ct) => f.SetHistoryRetentionAsync("orders", TreeHistoryRetentionMode.FullValue, TimeSpan.FromDays(7), ct)),
    ];

    [Test]
    public void An_unknown_tree_maps_to_key_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTreeAdmin>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.NotFound, "Tree 'orders' is not registered.");

        Assert.That(
            () => admin.GetTreeStatsAsync("orders"),
            Throws.InstanceOf<KeyNotFoundException>().With.Message.EqualTo("Tree 'orders' is not registered."));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTreeAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.ProbeCapabilitiesAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SetTreeAliasAsync("orders", string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SetTreeConfigAsync("orders", null!), Throws.ArgumentNullException);
            Assert.That(() => admin.AppendBulkLoadAsync("orders", "load-1", 0, null!), Throws.ArgumentNullException);
            Assert.That(() => admin.RestoreTreeAsync("orders", string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RestoreTreeSetAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RevertTreeRestoreAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.SnapshotTreeAsync("orders", string.Empty, TreeSnapshotMode.Offline), Throws.ArgumentException);
            Assert.That(() => admin.PlanWalMoveAsync("orders", 0, string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.CreateViewAsync("v", "orders", "p", null!), Throws.ArgumentNullException);
            Assert.That(() => admin.GetViewStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.GetTagIndexStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SetHistoryRetentionAsync(string.Empty, null, null), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
