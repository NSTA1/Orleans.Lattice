using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the tree-administration control facade
/// (<see cref="ILatticeTreeAdmin"/>) by delegating to the tree-administration-API
/// gRPC client (<see cref="LatticeTreeAdminApiGrpcClient"/>), so the
/// topology-agnostic tree-administration tool module works unchanged against a
/// cluster reached over gRPC. Cancellation flows through every call.
/// </summary>
/// <remarks>
/// The gRPC client projects the wire messages back onto the abstractions DTOs, so
/// this adapter is a pure pass-through that adds no authorization of its own: the
/// caller credential is stamped onto the outbound request by the
/// credential-forwarding interceptor and the remote cluster re-runs the facade's
/// own fail-closed access gate. As the tree-administration facade grows operations
/// beyond capability probing, each is added here as a one-line delegation, and the
/// underlying routing invoker can adopt region-targeting without changing this
/// adapter.
/// </remarks>
internal sealed class GrpcLatticeTreeAdmin : ILatticeTreeAdmin
{
    private readonly LatticeTreeAdminApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied tree-administration-API gRPC client.</summary>
    public GrpcLatticeTreeAdmin(LatticeTreeAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.ProbeCapabilitiesAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeHotnessReport> GetShardHotnessAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetShardHotnessAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        string treeId,
        bool deep = false,
        CancellationToken cancellationToken = default)
        => _client.GetDiagnosticsAsync(treeId, deep, cancellationToken);

    /// <inheritdoc />
    public Task<ShardMapInspection> InspectShardMapAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.InspectShardMapAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default)
        => _client.GetProjectionDigestAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task<TreeStatsReport> GetTreeStatsAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetTreeStatsAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        bool deep = false,
        CancellationToken cancellationToken = default)
        => _client.GetStorageUsageAsync(deep, cancellationToken);

    /// <inheritdoc />
    public Task<TreeCreationResult> CreateTreeAsync(
        string treeId,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
        => _client.CreateTreeAsync(treeId, shardCount, maxLeafKeys, maxInternalChildren, cancellationToken);

    /// <inheritdoc />
    public Task<TreeExistenceResult> CheckTreeExistsAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.CheckTreeExistsAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeAliasResolution> SetTreeAliasAsync(
        string treeId,
        string physicalTreeId,
        CancellationToken cancellationToken = default)
        => _client.SetTreeAliasAsync(treeId, physicalTreeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeAliasResolution> ResolveTreeAliasAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.ResolveTreeAliasAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeConfigurationReport> GetTreeConfigAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetTreeConfigAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeConfigurationReport> SetTreeConfigAsync(
        string treeId,
        TreeConfigurationUpdate update,
        CancellationToken cancellationToken = default)
        => _client.SetTreeConfigAsync(treeId, update, cancellationToken);

    /// <inheritdoc />
    public Task<TreeShardMapView> GetShardMapAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetShardMapAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeDeletionStatus> DeleteTreeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.DeleteTreeAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeDeletionStatus> RecoverTreeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.RecoverTreeAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeDeletionStatus> PurgeTreeAsync(
        string treeId,
        bool confirm,
        CancellationToken cancellationToken = default)
        => _client.PurgeTreeAsync(treeId, confirm, cancellationToken);

    /// <inheritdoc />
    public Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetTreeDeletionStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeBulkLoadSession> BeginBulkLoadAsync(
        string treeId,
        string operationId,
        CancellationToken cancellationToken = default)
        => _client.BeginBulkLoadAsync(treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(
        string treeId,
        string operationId,
        long chunkIndex,
        IReadOnlyList<DataEntry> entries,
        CancellationToken cancellationToken = default)
        => _client.AppendBulkLoadAsync(treeId, operationId, chunkIndex, entries, cancellationToken);

    /// <inheritdoc />
    public Task<TreeBulkLoadResult> CommitBulkLoadAsync(
        string treeId,
        string operationId,
        CancellationToken cancellationToken = default)
        => _client.CommitBulkLoadAsync(treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeRestoreResult> RestoreTreeAsync(
        string treeId,
        string backupId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
        => _client.RestoreTreeAsync(treeId, backupId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(
        string setId,
        CancellationToken cancellationToken = default)
        => _client.RestoreTreeSetAsync(setId, cancellationToken);

    /// <inheritdoc />
    public Task RevertTreeRestoreAsync(
        TreeRestoreResult restore,
        CancellationToken cancellationToken = default)
        => _client.RevertTreeRestoreAsync(restore, cancellationToken);

    /// <inheritdoc />
    public Task<TreeReshardStatus> ReshardTreeAsync(
        string treeId,
        int targetShardCount,
        CancellationToken cancellationToken = default)
        => _client.ReshardTreeAsync(treeId, targetShardCount, cancellationToken);

    /// <inheritdoc />
    public Task<TreeReshardStatus> GetReshardStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetReshardStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeResizeStatus> ResizeTreeAsync(
        string treeId,
        int newMaxLeafKeys,
        int newMaxInternalChildren,
        CancellationToken cancellationToken = default)
        => _client.ResizeTreeAsync(treeId, newMaxLeafKeys, newMaxInternalChildren, cancellationToken);

    /// <inheritdoc />
    public Task<TreeResizeStatus> UndoTreeResizeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.UndoTreeResizeAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeResizeStatus> GetResizeStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetResizeStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeSnapshotStatus> SnapshotTreeAsync(
        string treeId,
        string destinationTreeId,
        TreeSnapshotMode mode,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
        => _client.SnapshotTreeAsync(treeId, destinationTreeId, mode, maxLeafKeys, maxInternalChildren, cancellationToken);

    /// <inheritdoc />
    public Task<TreeSnapshotStatus> GetSnapshotStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetSnapshotStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeWalPlacement> GetWalPlacementAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.GetWalPlacementAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeWalPlacementAudit> AuditWalPlacementAsync(
        string treeId,
        CancellationToken cancellationToken = default)
        => _client.AuditWalPlacementAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<TreeWalMovePlan> PlanWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        CancellationToken cancellationToken = default)
        => _client.PlanWalMoveAsync(treeId, partition, targetProviderKey, cancellationToken);

    /// <inheritdoc />
    public Task<TreeWalMoveReceipt> ExecuteWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        CancellationToken cancellationToken = default)
        => _client.ExecuteWalMoveAsync(treeId, partition, targetProviderKey, options, cancellationToken);

    /// <inheritdoc />
    public Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(
        string treeId,
        int partition,
        string sourceProviderKey,
        CancellationToken cancellationToken = default)
        => _client.ReclaimMovedWalSourceAsync(treeId, partition, sourceProviderKey, cancellationToken);

    /// <inheritdoc />
    public Task<TreeViewCatalog> ListViewsAsync(CancellationToken cancellationToken = default)
        => _client.ListViewsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<TreeViewStatus> GetViewStatusAsync(string viewName, CancellationToken cancellationToken = default)
        => _client.GetViewStatusAsync(viewName, cancellationToken);

    /// <inheritdoc />
    public Task<TreeViewStatus> RebuildViewAsync(string viewName, CancellationToken cancellationToken = default)
        => _client.RebuildViewAsync(viewName, cancellationToken);

    /// <inheritdoc />
    public Task<TreeViewReconcileResult> ReconcileViewAsync(string viewName, CancellationToken cancellationToken = default)
        => _client.ReconcileViewAsync(viewName, cancellationToken);

    /// <inheritdoc />
    public Task DropViewAsync(string viewName, CancellationToken cancellationToken = default)
        => _client.DropViewAsync(viewName, cancellationToken);
}
