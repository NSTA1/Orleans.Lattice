using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

// Still calls the deprecated blocking tree-administration verbs (LATTICE0002); the Explorer moves to
// ILatticeTreeAdminOperations in the second #4124 change, which removes this suppression.
#pragma warning disable LATTICE0002

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTreeAdmin"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeTreeAdminApiGrpcClient"/>, which mirrors the facade verb
/// for verb. Faults map through <see cref="ShellTransportFaults"/>; the binding
/// reports an unknown tree as <c>NotFound</c>, which arrives as the
/// <see cref="KeyNotFoundException"/> the facade throws.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed partial class ShellTreeAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTreeAdminApiGrpcClient>(channel, LatticeTreeAdminApiGrpcClient.Create), ILatticeTreeAdmin
{
    /// <inheritdoc />
    public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.ProbeCapabilitiesAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeHotnessReport> GetShardHotnessAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetShardHotnessAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(string treeId, bool deep = false, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Deep: deep),
            static (client, state, ct) => client.GetDiagnosticsAsync(state.TreeId, state.Deep, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<ShardMapInspection> InspectShardMapAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.InspectShardMapAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<ShardProjectionDigestReport> GetProjectionDigestAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ShardIndex: shardIndex),
            static (client, state, ct) => client.GetProjectionDigestAsync(state.TreeId, state.ShardIndex, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeStatsReport> GetTreeStatsAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetTreeStatsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<ClusterStorageUsageSummary> GetStorageUsageAsync(bool deep = false, CancellationToken cancellationToken = default)
    {
        return CallAsync(
            deep,
            static (client, state, ct) => client.GetStorageUsageAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeCreationResult> CreateTreeAsync(string treeId, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ShardCount: shardCount, MaxLeafKeys: maxLeafKeys, MaxInternalChildren: maxInternalChildren),
            static (client, state, ct) => client.CreateTreeAsync(state.TreeId, state.ShardCount, state.MaxLeafKeys, state.MaxInternalChildren, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeExistenceResult> CheckTreeExistsAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.CheckTreeExistsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeAliasResolution> SetTreeAliasAsync(string treeId, string physicalTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(physicalTreeId);
        return CallAsync(
            (TreeId: treeId, PhysicalTreeId: physicalTreeId),
            static (client, state, ct) => client.SetTreeAliasAsync(state.TreeId, state.PhysicalTreeId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeAliasResolution> ResolveTreeAliasAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.ResolveTreeAliasAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeConfigurationReport> GetTreeConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetTreeConfigAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeConfigurationReport> SetTreeConfigAsync(string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(update);
        return CallAsync(
            (TreeId: treeId, Update: update),
            static (client, state, ct) => client.SetTreeConfigAsync(state.TreeId, state.Update, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeShardMapView> GetShardMapAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetShardMapAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeDeletionStatus> DeleteTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.DeleteTreeAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeDeletionStatus> RecoverTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.RecoverTreeAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeDeletionStatus> PurgeTreeAsync(string treeId, bool confirm, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Confirm: confirm),
            static (client, state, ct) => client.PurgeTreeAsync(state.TreeId, state.Confirm, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetTreeDeletionStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeBulkLoadSession> BeginBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.BeginBulkLoadAsync(state.TreeId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(string treeId, string operationId, long chunkIndex, IReadOnlyList<DataEntry> entries, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(entries);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId, ChunkIndex: chunkIndex, Entries: entries),
            static (client, state, ct) => client.AppendBulkLoadAsync(state.TreeId, state.OperationId, state.ChunkIndex, state.Entries, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeBulkLoadResult> CommitBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.CommitBulkLoadAsync(state.TreeId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeRestoreResult> RestoreTreeAsync(string treeId, string backupId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return CallAsync(
            (TreeId: treeId, BackupId: backupId, OperationId: operationId),
            static (client, state, ct) => client.RestoreTreeAsync(state.TreeId, state.BackupId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(string setId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        return CallAsync(
            setId,
            static (client, state, ct) => client.RestoreTreeSetAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task RevertTreeRestoreAsync(TreeRestoreResult restore, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        return CallAsync(
            restore,
            static (client, state, ct) => client.RevertTreeRestoreAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeReshardStatus> ReshardTreeAsync(string treeId, int targetShardCount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, TargetShardCount: targetShardCount),
            static (client, state, ct) => client.ReshardTreeAsync(state.TreeId, state.TargetShardCount, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeReshardStatus> GetReshardStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetReshardStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeResizeStatus> ResizeTreeAsync(string treeId, int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, NewMaxLeafKeys: newMaxLeafKeys, NewMaxInternalChildren: newMaxInternalChildren),
            static (client, state, ct) => client.ResizeTreeAsync(state.TreeId, state.NewMaxLeafKeys, state.NewMaxInternalChildren, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeResizeStatus> UndoTreeResizeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.UndoTreeResizeAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeResizeStatus> GetResizeStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetResizeStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeSnapshotStatus> SnapshotTreeAsync(string treeId, string destinationTreeId, TreeSnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(destinationTreeId);
        return CallAsync(
            (TreeId: treeId, DestinationTreeId: destinationTreeId, Mode: mode, MaxLeafKeys: maxLeafKeys, MaxInternalChildren: maxInternalChildren),
            static (client, state, ct) => client.SnapshotTreeAsync(state.TreeId, state.DestinationTreeId, state.Mode, state.MaxLeafKeys, state.MaxInternalChildren, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeSnapshotStatus> GetSnapshotStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetSnapshotStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeWalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetWalPlacementAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeWalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.AuditWalPlacementAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeOrphanedLeafReport> AuditOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ResumeFrom: resumeFrom),
            static (client, state, ct) => client.AuditOrphanedLeavesAsync(state.TreeId, state.ResumeFrom, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeOrphanedLeafReport> SurveyOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ResumeFrom: resumeFrom),
            static (client, state, ct) => client.SurveyOrphanedLeavesAsync(state.TreeId, state.ResumeFrom, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeOrphanedLeafReport> RepairOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ResumeFrom: resumeFrom),
            static (client, state, ct) => client.RepairOrphanedLeavesAsync(state.TreeId, state.ResumeFrom, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeWalMovePlan> PlanWalMoveAsync(string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        return CallAsync(
            (TreeId: treeId, Partition: partition, TargetProviderKey: targetProviderKey),
            static (client, state, ct) => client.PlanWalMoveAsync(state.TreeId, state.Partition, state.TargetProviderKey, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeWalMoveReceipt> ExecuteWalMoveAsync(string treeId, int partition, string targetProviderKey, TreeWalMoveOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        return CallAsync(
            (TreeId: treeId, Partition: partition, TargetProviderKey: targetProviderKey, Options: options),
#pragma warning disable LATTICE0002 // The interface still declares the deprecated verb; this forwards it (see ShellTreeAdminTransport.Operations).
            static (client, state, ct) => client.ExecuteWalMoveAsync(state.TreeId, state.Partition, state.TargetProviderKey, state.Options, ct),
#pragma warning restore LATTICE0002
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(sourceProviderKey);
        return CallAsync(
            (TreeId: treeId, Partition: partition, SourceProviderKey: sourceProviderKey),
            static (client, state, ct) => client.ReclaimMovedWalSourceAsync(state.TreeId, state.Partition, state.SourceProviderKey, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeViewCatalog> ListViewsAsync(CancellationToken cancellationToken = default)
    {
        return CallAsync(
            (object?)null,
            static (client, _, ct) => client.ListViewsAsync(ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeViewStatus> CreateViewAsync(string viewName, string sourceTreeId, string providerKey, byte[] payload, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentException.ThrowIfNullOrEmpty(providerKey);
        ArgumentNullException.ThrowIfNull(payload);
        return CallAsync(
            (ViewName: viewName, SourceTreeId: sourceTreeId, ProviderKey: providerKey, Payload: payload),
            static (client, state, ct) => client.CreateViewAsync(state.ViewName, state.SourceTreeId, state.ProviderKey, state.Payload, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeViewStatus> GetViewStatusAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            viewName,
            static (client, state, ct) => client.GetViewStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeViewStatus> RebuildViewAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            viewName,
#pragma warning disable LATTICE0002 // The interface still declares the deprecated verb; this forwards it (see ShellTreeAdminTransport.Operations).
            static (client, state, ct) => client.RebuildViewAsync(state, ct),
#pragma warning restore LATTICE0002
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeViewReconcileResult> ReconcileViewAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            viewName,
#pragma warning disable LATTICE0002 // The interface still declares the deprecated verb; this forwards it (see ShellTreeAdminTransport.Operations).
            static (client, state, ct) => client.ReconcileViewAsync(state, ct),
#pragma warning restore LATTICE0002
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task DropViewAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            viewName,
            static (client, state, ct) => client.DropViewAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeTagIndexCatalog> ListTagIndexesAsync(CancellationToken cancellationToken = default)
    {
        return CallAsync(
            (object?)null,
            static (client, _, ct) => client.ListTagIndexesAsync(ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeTagIndexStatus> GetTagIndexStatusAsync(string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return CallAsync(
            indexName,
            static (client, state, ct) => client.GetTagIndexStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeTagReconcileReport> ReconcileTagIndexAsync(string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return CallAsync(
            indexName,
#pragma warning disable LATTICE0002 // The interface still declares the deprecated verb; this forwards it (see ShellTreeAdminTransport.Operations).
            static (client, state, ct) => client.ReconcileTagIndexAsync(state, ct),
#pragma warning restore LATTICE0002
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, ShardIndex: shardIndex),
            static (client, state, ct) => client.TriggerShardCompactionAsync(state.TreeId, state.ShardIndex, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeHistoryRetention> GetHistoryRetentionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            treeId,
            static (client, state, ct) => client.GetHistoryRetentionAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeHistoryRetention> SetHistoryRetentionAsync(string treeId, TreeHistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Mode: mode, Window: window),
            static (client, state, ct) => client.SetHistoryRetentionAsync(state.TreeId, state.Mode, state.Window, ct),
            null,
            cancellationToken);
    }
}
