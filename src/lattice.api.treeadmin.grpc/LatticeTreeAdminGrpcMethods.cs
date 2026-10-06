using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Operations;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definitions for the
/// tree-administration control API. Each method is a unary RPC over an
/// Orleans-serialized, code-first contract. Constructed from DI-resolved
/// serializers so both the public client invoker and the server-side binder wire
/// up identical marshallers.
/// </summary>
/// <remarks>
/// This foundation contract is a minimal set of RPCs over the transport-agnostic
/// <see cref="ILatticeTreeAdmin"/> facade: the capability probe
/// (<c>ProbeCapabilities</c>) and unauthenticated discovery (<c>GetAuthScheme</c>).
/// The whole-tree lifecycle operations append
/// RPC here. Contract-versioning policy: fields on the wire messages are
/// additive-only (new <c>[Id(n)]</c>); aliases and field numbers are never
/// renumbered, so a newer response decodes cleanly under an older client, and new
/// RPCs are added without renaming or renumbering the existing ones.
/// </remarks>
internal sealed class LatticeTreeAdminGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.treeadmin";

    /// <summary>The unary capability-probe RPC method name.</summary>
    public const string ProbeCapabilitiesMethodName = "ProbeCapabilities";

    /// <summary>The unary, unauthenticated auth-scheme advertisement RPC method name.</summary>
    public const string GetAuthSchemeMethodName = "GetAuthScheme";

    /// <summary>The unary shard-hotness RPC method name.</summary>
    public const string GetShardHotnessMethodName = "GetShardHotness";

    /// <summary>The unary shard-diagnostics RPC method name.</summary>
    public const string GetDiagnosticsMethodName = "GetDiagnostics";

    /// <summary>The unary shard-map inspection RPC method name.</summary>
    public const string InspectShardMapMethodName = "InspectShardMap";

    /// <summary>The unary projection-digest RPC method name.</summary>
    public const string GetProjectionDigestMethodName = "GetProjectionDigest";

    /// <summary>The unary tree-statistics RPC method name.</summary>
    public const string GetTreeStatsMethodName = "GetTreeStats";

    /// <summary>The unary cluster-wide storage-usage RPC method name.</summary>
    public const string GetStorageUsageMethodName = "GetStorageUsage";

    /// <summary>The unary explicit tree-creation RPC method name.</summary>
    public const string CreateTreeMethodName = "CreateTree";

    /// <summary>The unary tree-existence RPC method name.</summary>
    public const string CheckTreeExistsMethodName = "CheckTreeExists";

    /// <summary>The unary set-alias RPC method name.</summary>
    public const string SetTreeAliasMethodName = "SetTreeAlias";

    /// <summary>The unary resolve-alias RPC method name.</summary>
    public const string ResolveTreeAliasMethodName = "ResolveTreeAlias";

    /// <summary>The unary get-config RPC method name.</summary>
    public const string GetTreeConfigMethodName = "GetTreeConfig";

    /// <summary>The unary set-config RPC method name.</summary>
    public const string SetTreeConfigMethodName = "SetTreeConfig";

    /// <summary>The unary registry-persisted shard-map RPC method name.</summary>
    public const string GetShardMapMethodName = "GetShardMap";

    /// <summary>The unary tree soft-delete RPC method name.</summary>
    public const string DeleteTreeMethodName = "DeleteTree";

    /// <summary>The unary tree recover RPC method name.</summary>
    public const string RecoverTreeMethodName = "RecoverTree";

    /// <summary>The unary tree hard-purge RPC method name.</summary>
    public const string PurgeTreeMethodName = "PurgeTree";

    /// <summary>The unary tree deletion-status RPC method name.</summary>
    public const string GetTreeDeletionStatusMethodName = "GetTreeDeletionStatus";

    /// <summary>The unary bulk-load begin (session-open) RPC method name.</summary>
    public const string BeginBulkLoadMethodName = "BeginBulkLoad";

    /// <summary>The unary bulk-load append (chunk-graft) RPC method name.</summary>
    public const string AppendBulkLoadMethodName = "AppendBulkLoad";

    /// <summary>The unary bulk-load commit (session-close) RPC method name.</summary>
    public const string CommitBulkLoadMethodName = "CommitBulkLoad";

    /// <summary>The unary restore-into-tree RPC method name.</summary>
    public const string RestoreTreeMethodName = "RestoreTree";

    /// <summary>The unary restore-set RPC method name.</summary>
    public const string RestoreTreeSetMethodName = "RestoreTreeSet";

    /// <summary>The unary revert-restore RPC method name.</summary>
    public const string RevertTreeRestoreMethodName = "RevertTreeRestore";

    /// <summary>The unary online-reshard trigger RPC method name.</summary>
    public const string ReshardTreeMethodName = "ReshardTree";

    /// <summary>The unary read-only reshard-status RPC method name.</summary>
    public const string GetReshardStatusMethodName = "GetReshardStatus";

    /// <summary>The unary online-resize trigger RPC method name.</summary>
    public const string ResizeTreeMethodName = "ResizeTree";

    /// <summary>The unary undo-resize RPC method name.</summary>
    public const string UndoTreeResizeMethodName = "UndoTreeResize";

    /// <summary>The unary read-only resize-status RPC method name.</summary>
    public const string GetResizeStatusMethodName = "GetResizeStatus";

    /// <summary>The unary snapshot-capture trigger RPC method name.</summary>
    public const string SnapshotTreeMethodName = "SnapshotTree";

    /// <summary>The unary read-only snapshot-status RPC method name.</summary>
    public const string GetSnapshotStatusMethodName = "GetSnapshotStatus";

    /// <summary>The unary read-only WAL placement inspection RPC method name.</summary>
    public const string GetWalPlacementMethodName = "GetWalPlacement";

    /// <summary>The unary read-only WAL placement audit RPC method name.</summary>
    public const string AuditWalPlacementMethodName = "AuditWalPlacement";

    /// <summary>The unary read-only orphaned-leaf audit RPC method name.</summary>
    public const string AuditOrphanedLeavesMethodName = "AuditOrphanedLeaves";

    /// <summary>The unary mutating orphaned-leaf repair RPC method name.</summary>
    public const string RepairOrphanedLeavesMethodName = "RepairOrphanedLeaves";

    /// <summary>The unary read-only WAL move plan RPC method name.</summary>
    public const string PlanWalMoveMethodName = "PlanWalMove";

    /// <summary>The unary WAL move execute trigger RPC method name.</summary>
    /// <summary>The unary WAL move reclaim RPC method name.</summary>
    public const string ReclaimMovedWalSourceMethodName = "ReclaimMovedWalSource";

    /// <summary>The unary read-only runtime materialised-view listing RPC method name.</summary>
    public const string ListViewsMethodName = "ListViews";

    /// <summary>The unary runtime materialised-view creation RPC method name.</summary>
    public const string CreateViewMethodName = "CreateView";

    /// <summary>The unary read-only materialised-view status RPC method name.</summary>
    public const string GetViewStatusMethodName = "GetViewStatus";

    /// <summary>The unary materialised-view rebuild trigger RPC method name.</summary>
    /// <summary>The unary materialised-view reconcile trigger RPC method name.</summary>
    /// <summary>The unary materialised-view drop RPC method name.</summary>
    public const string DropViewMethodName = "DropView";

    /// <summary>The unary read-only tag-index listing RPC method name.</summary>
    public const string ListTagIndexesMethodName = "ListTagIndexes";

    /// <summary>The unary read-only tag-index status RPC method name.</summary>
    public const string GetTagIndexStatusMethodName = "GetTagIndexStatus";

    /// <summary>The unary tag-index reconcile trigger RPC method name.</summary>
    /// <summary>The unary shard tombstone-compaction trigger RPC method name.</summary>
    public const string TriggerShardCompactionMethodName = "TriggerShardCompaction";

    /// <summary>The unary read-only durable-history retention read RPC method name.</summary>
    public const string GetHistoryRetentionMethodName = "GetHistoryRetention";

    /// <summary>The unary durable-history retention set RPC method name.</summary>
    public const string SetHistoryRetentionMethodName = "SetHistoryRetention";
    /// <summary>The unary accept-then-poll view-rebuild start RPC method name.</summary>
    public const string StartViewRebuildMethodName = "StartViewRebuild";

    /// <summary>The unary accept-then-poll view-reconcile start RPC method name.</summary>
    public const string StartViewReconcileMethodName = "StartViewReconcile";

    /// <summary>The unary accept-then-poll tag-index reconcile start RPC method name.</summary>
    public const string StartTagIndexReconcileMethodName = "StartTagIndexReconcile";

    /// <summary>The unary accept-then-poll WAL move start RPC method name.</summary>
    public const string StartWalMoveMethodName = "StartWalMove";

    /// <summary>The unary accept-then-poll orphaned-leaf audit start RPC method name.</summary>
    public const string StartOrphanedLeavesAuditMethodName = "StartOrphanedLeavesAudit";

    /// <summary>The unary accept-then-poll orphaned-leaf repair start RPC method name.</summary>
    public const string StartOrphanedLeavesRepairMethodName = "StartOrphanedLeavesRepair";

    /// <summary>The unary tree-administration operation status RPC method name.</summary>
    public const string GetTreeAdminOperationStatusMethodName = "GetTreeAdminOperationStatus";

    /// <summary>The unary tree-administration operation listing RPC method name.</summary>
    public const string ListTreeAdminOperationsMethodName = "ListTreeAdminOperations";

    /// <summary>The unary tree-administration operation cancellation RPC method name.</summary>
    public const string CancelTreeAdminOperationMethodName = "CancelTreeAdminOperation";

    /// <summary>The unary accept-then-poll storage-usage refresh start RPC method name.</summary>
    public const string StartStorageUsageRefreshMethodName = "StartStorageUsageRefresh";

    /// <summary>The unary storage-usage refresh status RPC method name.</summary>
    public const string GetStorageUsageRefreshStatusMethodName = "GetStorageUsageRefreshStatus";

    /// <summary>The unary storage-usage refresh listing RPC method name.</summary>
    public const string ListStorageUsageRefreshesMethodName = "ListStorageUsageRefreshes";

    /// <summary>The unary storage-usage refresh cancellation RPC method name.</summary>
    public const string CancelStorageUsageRefreshMethodName = "CancelStorageUsageRefresh";

    /// <summary>The unary read-only WAL reclamation (floor-holder) RPC method name.</summary>
    public const string GetWalReclamationMethodName = "GetWalReclamation";

    /// <summary>Initialises the method definitions from DI-resolved serializers.</summary>
    public LatticeTreeAdminGrpcMethods(
        Serializer<TreeAdminTreeRequest> treeRequestSerializer,
        Serializer<LatticeTreeAdminCapabilities> capabilitiesSerializer,
        Serializer<AuthSchemeAdvertisementRequest> authSchemeRequestSerializer,
        Serializer<AuthSchemeAdvertisement> authSchemeAdvertisementSerializer,
        Serializer<TreeAdminShardRequest> shardRequestSerializer,
        Serializer<TreeAdminOrphanedLeafRequest> orphanedLeafRequestSerializer,
        Serializer<TreeAdminDiagnosticsRequest> diagnosticsRequestSerializer,
        Serializer<TreeAdminStorageUsageRequest> storageUsageRequestSerializer,
        Serializer<TreeHotnessReport> hotnessReportSerializer,
        Serializer<TreeAdminDiagnosticReport> diagnosticReportSerializer,
        Serializer<ShardMapInspection> shardMapInspectionSerializer,
        Serializer<ShardProjectionDigestReport> projectionDigestSerializer,
        Serializer<TreeStatsReport> treeStatsSerializer,
        Serializer<ClusterStorageUsageSummary> storageUsageSummarySerializer,
        Serializer<TreeAdminCreateRequest> createRequestSerializer,
        Serializer<TreeAdminSetAliasRequest> setAliasRequestSerializer,
        Serializer<TreeAdminSetConfigRequest> setConfigRequestSerializer,
        Serializer<TreeCreationResult> creationResultSerializer,
        Serializer<TreeExistenceResult> existenceResultSerializer,
        Serializer<TreeAliasResolution> aliasResolutionSerializer,
        Serializer<TreeConfigurationReport> configurationReportSerializer,
        Serializer<TreeShardMapView> shardMapViewSerializer,
        Serializer<TreeAdminPurgeRequest> purgeRequestSerializer,
        Serializer<TreeDeletionStatus> deletionStatusSerializer,
        Serializer<TreeAdminBulkLoadSessionRequest> bulkLoadSessionRequestSerializer,
        Serializer<TreeAdminBulkLoadAppendRequest> bulkLoadAppendRequestSerializer,
        Serializer<TreeBulkLoadSession> bulkLoadSessionSerializer,
        Serializer<TreeBulkLoadChunkAck> bulkLoadChunkAckSerializer,
        Serializer<TreeBulkLoadResult> bulkLoadResultSerializer,
        Serializer<TreeAdminRestoreRequest> restoreRequestSerializer,
        Serializer<TreeAdminRestoreSetRequest> restoreSetRequestSerializer,
        Serializer<TreeRestoreResult> restoreResultSerializer,
        Serializer<TreeRestoreSetResult> restoreSetResultSerializer,
        Serializer<TreeAdminReshardRequest> reshardRequestSerializer,
        Serializer<TreeReshardStatus> reshardStatusSerializer,
        Serializer<TreeAdminResizeRequest> resizeRequestSerializer,
        Serializer<TreeResizeStatus> resizeStatusSerializer,
        Serializer<TreeAdminSnapshotRequest> snapshotRequestSerializer,
        Serializer<TreeSnapshotStatus> snapshotStatusSerializer,
        Serializer<TreeAdminWalMovePlanRequest> walMovePlanRequestSerializer,
        Serializer<TreeAdminWalMoveExecuteRequest> walMoveExecuteRequestSerializer,
        Serializer<TreeAdminWalReclaimRequest> walReclaimRequestSerializer,
        Serializer<TreeWalPlacement> walPlacementSerializer,
        Serializer<TreeWalPlacementAudit> walPlacementAuditSerializer,
        Serializer<TreeOrphanedLeafReport> orphanedLeafReportSerializer,
        Serializer<TreeWalMovePlan> walMovePlanSerializer,
        Serializer<TreeWalMoveReceipt> walMoveReceiptSerializer,
        Serializer<TreeAdminViewRequest> viewRequestSerializer,
        Serializer<TreeAdminCreateViewRequest> createViewRequestSerializer,
        Serializer<TreeAdminViewListRequest> viewListRequestSerializer,
        Serializer<TreeViewCatalog> viewCatalogSerializer,
        Serializer<TreeViewStatus> viewStatusSerializer,
        Serializer<TreeViewReconcileResult> viewReconcileResultSerializer,
        Serializer<TreeAdminTagIndexRequest> tagIndexRequestSerializer,
        Serializer<TreeAdminTagIndexListRequest> tagIndexListRequestSerializer,
        Serializer<TreeTagIndexCatalog> tagIndexCatalogSerializer,
        Serializer<TreeTagIndexStatus> tagIndexStatusSerializer,
        Serializer<TreeTagReconcileReport> tagReconcileReportSerializer,
        Serializer<TreeAdminSetRetentionRequest> setRetentionRequestSerializer,
        Serializer<TreeHistoryRetention> historyRetentionSerializer,
        Serializer<TreeCompactionTriggerResult> compactionTriggerResultSerializer,
        Serializer<TreeAdminStorageUsageRefreshRequest> storageUsageRefreshRequestSerializer,
        Serializer<LatticeOperationHandle> operationHandleSerializer,
        Serializer<TreeAdminStorageUsageOperationRequest> storageUsageOperationRequestSerializer,
        Serializer<TreeAdminStorageUsageOperationStatusResponse> storageUsageOperationStatusResponseSerializer,
        Serializer<TreeAdminOperationRequest> operationRequestSerializer,
        Serializer<TreeAdminOperationStatusResponse> operationStatusResponseSerializer,
        Serializer<LatticeOperationListRequest> operationListRequestSerializer,
        Serializer<LatticeOperationPage> operationPageSerializer,
        Serializer<TreeWalReclamationReport> walReclamationSerializer)
    {
        ArgumentNullException.ThrowIfNull(treeRequestSerializer);
        ArgumentNullException.ThrowIfNull(capabilitiesSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeRequestSerializer);
        ArgumentNullException.ThrowIfNull(authSchemeAdvertisementSerializer);
        ArgumentNullException.ThrowIfNull(shardRequestSerializer);
        ArgumentNullException.ThrowIfNull(orphanedLeafRequestSerializer);
        ArgumentNullException.ThrowIfNull(diagnosticsRequestSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageRequestSerializer);
        ArgumentNullException.ThrowIfNull(hotnessReportSerializer);
        ArgumentNullException.ThrowIfNull(diagnosticReportSerializer);
        ArgumentNullException.ThrowIfNull(shardMapInspectionSerializer);
        ArgumentNullException.ThrowIfNull(projectionDigestSerializer);
        ArgumentNullException.ThrowIfNull(treeStatsSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageSummarySerializer);
        ArgumentNullException.ThrowIfNull(createRequestSerializer);
        ArgumentNullException.ThrowIfNull(setAliasRequestSerializer);
        ArgumentNullException.ThrowIfNull(setConfigRequestSerializer);
        ArgumentNullException.ThrowIfNull(creationResultSerializer);
        ArgumentNullException.ThrowIfNull(existenceResultSerializer);
        ArgumentNullException.ThrowIfNull(aliasResolutionSerializer);
        ArgumentNullException.ThrowIfNull(configurationReportSerializer);
        ArgumentNullException.ThrowIfNull(shardMapViewSerializer);
        ArgumentNullException.ThrowIfNull(purgeRequestSerializer);
        ArgumentNullException.ThrowIfNull(deletionStatusSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadSessionRequestSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadAppendRequestSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadSessionSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadChunkAckSerializer);
        ArgumentNullException.ThrowIfNull(bulkLoadResultSerializer);
        ArgumentNullException.ThrowIfNull(restoreRequestSerializer);
        ArgumentNullException.ThrowIfNull(restoreSetRequestSerializer);
        ArgumentNullException.ThrowIfNull(restoreResultSerializer);
        ArgumentNullException.ThrowIfNull(restoreSetResultSerializer);
        ArgumentNullException.ThrowIfNull(reshardRequestSerializer);
        ArgumentNullException.ThrowIfNull(reshardStatusSerializer);
        ArgumentNullException.ThrowIfNull(resizeRequestSerializer);
        ArgumentNullException.ThrowIfNull(resizeStatusSerializer);
        ArgumentNullException.ThrowIfNull(snapshotRequestSerializer);
        ArgumentNullException.ThrowIfNull(snapshotStatusSerializer);
        ArgumentNullException.ThrowIfNull(walMovePlanRequestSerializer);
        ArgumentNullException.ThrowIfNull(walMoveExecuteRequestSerializer);
        ArgumentNullException.ThrowIfNull(walReclaimRequestSerializer);
        ArgumentNullException.ThrowIfNull(walPlacementSerializer);
        ArgumentNullException.ThrowIfNull(walPlacementAuditSerializer);
        ArgumentNullException.ThrowIfNull(orphanedLeafReportSerializer);
        ArgumentNullException.ThrowIfNull(walMovePlanSerializer);
        ArgumentNullException.ThrowIfNull(walMoveReceiptSerializer);
        ArgumentNullException.ThrowIfNull(viewRequestSerializer);
        ArgumentNullException.ThrowIfNull(createViewRequestSerializer);
        ArgumentNullException.ThrowIfNull(viewListRequestSerializer);
        ArgumentNullException.ThrowIfNull(viewCatalogSerializer);
        ArgumentNullException.ThrowIfNull(viewStatusSerializer);
        ArgumentNullException.ThrowIfNull(viewReconcileResultSerializer);
        ArgumentNullException.ThrowIfNull(tagIndexRequestSerializer);
        ArgumentNullException.ThrowIfNull(tagIndexListRequestSerializer);
        ArgumentNullException.ThrowIfNull(tagIndexCatalogSerializer);
        ArgumentNullException.ThrowIfNull(tagIndexStatusSerializer);
        ArgumentNullException.ThrowIfNull(tagReconcileReportSerializer);
        ArgumentNullException.ThrowIfNull(setRetentionRequestSerializer);
        ArgumentNullException.ThrowIfNull(historyRetentionSerializer);
        ArgumentNullException.ThrowIfNull(compactionTriggerResultSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageRefreshRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationHandleSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageOperationRequestSerializer);
        ArgumentNullException.ThrowIfNull(storageUsageOperationStatusResponseSerializer);
        ArgumentNullException.ThrowIfNull(operationListRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationPageSerializer);

        ProbeCapabilities = new Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeCapabilitiesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(capabilitiesSerializer));

        GetAuthScheme = new Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetAuthSchemeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(authSchemeAdvertisementSerializer));

        GetShardHotness = new Method<TreeAdminTreeRequest, TreeHotnessReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetShardHotnessMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(hotnessReportSerializer));

        GetDiagnostics = new Method<TreeAdminDiagnosticsRequest, TreeAdminDiagnosticReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetDiagnosticsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(diagnosticsRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(diagnosticReportSerializer));

        InspectShardMap = new Method<TreeAdminTreeRequest, ShardMapInspection>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: InspectShardMapMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardMapInspectionSerializer));

        GetProjectionDigest = new Method<TreeAdminShardRequest, ShardProjectionDigestReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetProjectionDigestMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(projectionDigestSerializer));

        GetTreeStats = new Method<TreeAdminTreeRequest, TreeStatsReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeStatsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeStatsSerializer));

        GetStorageUsage = new Method<TreeAdminStorageUsageRequest, ClusterStorageUsageSummary>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetStorageUsageMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(storageUsageRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(storageUsageSummarySerializer));

        CreateTree = new Method<TreeAdminCreateRequest, TreeCreationResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(createRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(creationResultSerializer));

        CheckTreeExists = new Method<TreeAdminTreeRequest, TreeExistenceResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CheckTreeExistsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(existenceResultSerializer));

        SetTreeAlias = new Method<TreeAdminSetAliasRequest, TreeAliasResolution>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetTreeAliasMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(setAliasRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(aliasResolutionSerializer));

        ResolveTreeAlias = new Method<TreeAdminTreeRequest, TreeAliasResolution>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ResolveTreeAliasMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(aliasResolutionSerializer));

        GetTreeConfig = new Method<TreeAdminTreeRequest, TreeConfigurationReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeConfigMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(configurationReportSerializer));

        SetTreeConfig = new Method<TreeAdminSetConfigRequest, TreeConfigurationReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetTreeConfigMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(setConfigRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(configurationReportSerializer));

        GetShardMap = new Method<TreeAdminTreeRequest, TreeShardMapView>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetShardMapMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardMapViewSerializer));

        DeleteTree = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DeleteTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        RecoverTree = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RecoverTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        PurgeTree = new Method<TreeAdminPurgeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PurgeTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(purgeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        GetTreeDeletionStatus = new Method<TreeAdminTreeRequest, TreeDeletionStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTreeDeletionStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(deletionStatusSerializer));

        BeginBulkLoad = new Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadSession>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: BeginBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionSerializer));

        AppendBulkLoad = new Method<TreeAdminBulkLoadAppendRequest, TreeBulkLoadChunkAck>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: AppendBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadAppendRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadChunkAckSerializer));

        CommitBulkLoad = new Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CommitBulkLoadMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadSessionRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(bulkLoadResultSerializer));

        RestoreTree = new Method<TreeAdminRestoreRequest, TreeRestoreResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RestoreTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreResultSerializer));

        RestoreTreeSet = new Method<TreeAdminRestoreSetRequest, TreeRestoreSetResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RestoreTreeSetMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreSetRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreSetResultSerializer));

        RevertTreeRestore = new Method<TreeRestoreResult, TreeRestoreResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RevertTreeRestoreMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreResultSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(restoreResultSerializer));

        ReshardTree = new Method<TreeAdminReshardRequest, TreeReshardStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ReshardTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(reshardRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(reshardStatusSerializer));

        GetReshardStatus = new Method<TreeAdminTreeRequest, TreeReshardStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetReshardStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(reshardStatusSerializer));

        ResizeTree = new Method<TreeAdminResizeRequest, TreeResizeStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ResizeTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(resizeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(resizeStatusSerializer));

        UndoTreeResize = new Method<TreeAdminTreeRequest, TreeResizeStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: UndoTreeResizeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(resizeStatusSerializer));

        GetResizeStatus = new Method<TreeAdminTreeRequest, TreeResizeStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetResizeStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(resizeStatusSerializer));

        SnapshotTree = new Method<TreeAdminSnapshotRequest, TreeSnapshotStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SnapshotTreeMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(snapshotRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(snapshotStatusSerializer));

        GetSnapshotStatus = new Method<TreeAdminTreeRequest, TreeSnapshotStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetSnapshotStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(snapshotStatusSerializer));

        GetWalPlacement = new Method<TreeAdminTreeRequest, TreeWalPlacement>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetWalPlacementMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walPlacementSerializer));

        AuditWalPlacement = new Method<TreeAdminTreeRequest, TreeWalPlacementAudit>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: AuditWalPlacementMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walPlacementAuditSerializer));

        AuditOrphanedLeaves = new Method<TreeAdminOrphanedLeafRequest, TreeOrphanedLeafReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: AuditOrphanedLeavesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafReportSerializer));

        RepairOrphanedLeaves = new Method<TreeAdminOrphanedLeafRequest, TreeOrphanedLeafReport>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: RepairOrphanedLeavesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafReportSerializer));

        PlanWalMove = new Method<TreeAdminWalMovePlanRequest, TreeWalMovePlan>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PlanWalMoveMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walMovePlanRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walMovePlanSerializer));


        ReclaimMovedWalSource = new Method<TreeAdminWalReclaimRequest, TreeWalMoveReceipt>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ReclaimMovedWalSourceMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walReclaimRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(walMoveReceiptSerializer));

        ListViews = new Method<TreeAdminViewListRequest, TreeViewCatalog>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListViewsMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewListRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewCatalogSerializer));

        CreateView = new Method<TreeAdminCreateViewRequest, TreeViewStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: CreateViewMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(createViewRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewStatusSerializer));

        GetViewStatus = new Method<TreeAdminViewRequest, TreeViewStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetViewStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewStatusSerializer));



        DropView = new Method<TreeAdminViewRequest, TreeAdminViewRequest>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: DropViewMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(viewRequestSerializer));

        ListTagIndexes = new Method<TreeAdminTagIndexListRequest, TreeTagIndexCatalog>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ListTagIndexesMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(tagIndexListRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(tagIndexCatalogSerializer));

        GetTagIndexStatus = new Method<TreeAdminTagIndexRequest, TreeTagIndexStatus>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetTagIndexStatusMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(tagIndexRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(tagIndexStatusSerializer));


        TriggerShardCompaction = new Method<TreeAdminShardRequest, TreeCompactionTriggerResult>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: TriggerShardCompactionMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(shardRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(compactionTriggerResultSerializer));

        GetHistoryRetention = new Method<TreeAdminTreeRequest, TreeHistoryRetention>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetHistoryRetentionMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(historyRetentionSerializer));

        SetHistoryRetention = new Method<TreeAdminSetRetentionRequest, TreeHistoryRetention>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: SetHistoryRetentionMethodName,
            requestMarshaller: LatticeTreeAdminGrpcMarshallers.Create(setRetentionRequestSerializer),
            responseMarshaller: LatticeTreeAdminGrpcMarshallers.Create(historyRetentionSerializer));
        ArgumentNullException.ThrowIfNull(operationHandleSerializer);
        ArgumentNullException.ThrowIfNull(operationRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationStatusResponseSerializer);
        ArgumentNullException.ThrowIfNull(operationListRequestSerializer);
        ArgumentNullException.ThrowIfNull(operationPageSerializer);

        var storageUsageOperationRequestMarshaller = LatticeTreeAdminGrpcMarshallers.Create(storageUsageOperationRequestSerializer);
        var storageUsageOperationStatusMarshaller = LatticeTreeAdminGrpcMarshallers.Create(storageUsageOperationStatusResponseSerializer);

        StartStorageUsageRefresh = new Method<TreeAdminStorageUsageRefreshRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartStorageUsageRefreshMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(storageUsageRefreshRequestSerializer),
            LatticeTreeAdminGrpcMarshallers.Create(operationHandleSerializer));

        GetStorageUsageRefreshStatus = new Method<TreeAdminStorageUsageOperationRequest, TreeAdminStorageUsageOperationStatusResponse>(
            MethodType.Unary, ServiceName, GetStorageUsageRefreshStatusMethodName,
            storageUsageOperationRequestMarshaller, storageUsageOperationStatusMarshaller);

        ListStorageUsageRefreshes = new Method<LatticeOperationListRequest, LatticeOperationPage>(
            MethodType.Unary, ServiceName, ListStorageUsageRefreshesMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(operationListRequestSerializer),
            LatticeTreeAdminGrpcMarshallers.Create(operationPageSerializer));

        CancelStorageUsageRefresh = new Method<TreeAdminStorageUsageOperationRequest, TreeAdminStorageUsageOperationStatusResponse>(
            MethodType.Unary, ServiceName, CancelStorageUsageRefreshMethodName,
            storageUsageOperationRequestMarshaller, storageUsageOperationStatusMarshaller);

        var handleMarshaller = LatticeTreeAdminGrpcMarshallers.Create(operationHandleSerializer);
        var operationRequestMarshaller = LatticeTreeAdminGrpcMarshallers.Create(operationRequestSerializer);
        var operationStatusMarshaller = LatticeTreeAdminGrpcMarshallers.Create(operationStatusResponseSerializer);

        StartViewRebuild = new Method<TreeAdminViewRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartViewRebuildMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(viewRequestSerializer), handleMarshaller);

        StartViewReconcile = new Method<TreeAdminViewRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartViewReconcileMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(viewRequestSerializer), handleMarshaller);

        StartTagIndexReconcile = new Method<TreeAdminTagIndexRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartTagIndexReconcileMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(tagIndexRequestSerializer), handleMarshaller);

        StartWalMove = new Method<TreeAdminWalMoveExecuteRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartWalMoveMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(walMoveExecuteRequestSerializer), handleMarshaller);

        StartOrphanedLeavesAudit = new Method<TreeAdminOrphanedLeafRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartOrphanedLeavesAuditMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafRequestSerializer), handleMarshaller);

        StartOrphanedLeavesRepair = new Method<TreeAdminOrphanedLeafRequest, LatticeOperationHandle>(
            MethodType.Unary, ServiceName, StartOrphanedLeavesRepairMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(orphanedLeafRequestSerializer), handleMarshaller);

        GetTreeAdminOperationStatus = new Method<TreeAdminOperationRequest, TreeAdminOperationStatusResponse>(
            MethodType.Unary, ServiceName, GetTreeAdminOperationStatusMethodName,
            operationRequestMarshaller, operationStatusMarshaller);

        ListTreeAdminOperations = new Method<LatticeOperationListRequest, LatticeOperationPage>(
            MethodType.Unary, ServiceName, ListTreeAdminOperationsMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(operationListRequestSerializer),
            LatticeTreeAdminGrpcMarshallers.Create(operationPageSerializer));

        CancelTreeAdminOperation = new Method<TreeAdminOperationRequest, TreeAdminOperationStatusResponse>(
            MethodType.Unary, ServiceName, CancelTreeAdminOperationMethodName,
            operationRequestMarshaller, operationStatusMarshaller);

        ArgumentNullException.ThrowIfNull(walReclamationSerializer);
        GetWalReclamation = new Method<TreeAdminTreeRequest, TreeWalReclamationReport>(
            MethodType.Unary, ServiceName, GetWalReclamationMethodName,
            LatticeTreeAdminGrpcMarshallers.Create(treeRequestSerializer),
            LatticeTreeAdminGrpcMarshallers.Create(walReclamationSerializer));
    }
    /// <summary>The unary <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    public Method<TreeAdminTreeRequest, LatticeTreeAdminCapabilities> ProbeCapabilities { get; }

    /// <summary>The unary, unauthenticated <c>GetAuthScheme</c> advertisement RPC.</summary>
    public Method<AuthSchemeAdvertisementRequest, AuthSchemeAdvertisement> GetAuthScheme { get; }

    /// <summary>The unary <c>GetShardHotness</c> read-only hotness RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeHotnessReport> GetShardHotness { get; }

    /// <summary>The unary <c>GetDiagnostics</c> read-only diagnostics RPC.</summary>
    public Method<TreeAdminDiagnosticsRequest, TreeAdminDiagnosticReport> GetDiagnostics { get; }

    /// <summary>The unary <c>InspectShardMap</c> read-only topology RPC.</summary>
    public Method<TreeAdminTreeRequest, ShardMapInspection> InspectShardMap { get; }

    /// <summary>The unary <c>GetProjectionDigest</c> read-only digest RPC.</summary>
    public Method<TreeAdminShardRequest, ShardProjectionDigestReport> GetProjectionDigest { get; }

    /// <summary>The unary <c>GetTreeStats</c> read-only statistics RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeStatsReport> GetTreeStats { get; }

    /// <summary>The unary <c>GetStorageUsage</c> read-only cluster-storage RPC.</summary>
    public Method<TreeAdminStorageUsageRequest, ClusterStorageUsageSummary> GetStorageUsage { get; }

    /// <summary>The unary <c>CreateTree</c> explicit-creation lifecycle RPC.</summary>
    public Method<TreeAdminCreateRequest, TreeCreationResult> CreateTree { get; }

    /// <summary>The unary <c>CheckTreeExists</c> read-only existence RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeExistenceResult> CheckTreeExists { get; }

    /// <summary>The unary <c>SetTreeAlias</c> alias-assignment lifecycle RPC.</summary>
    public Method<TreeAdminSetAliasRequest, TreeAliasResolution> SetTreeAlias { get; }

    /// <summary>The unary <c>ResolveTreeAlias</c> read-only alias-resolution RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeAliasResolution> ResolveTreeAlias { get; }

    /// <summary>The unary <c>GetTreeConfig</c> read-only configuration RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeConfigurationReport> GetTreeConfig { get; }

    /// <summary>The unary <c>SetTreeConfig</c> configuration-update lifecycle RPC.</summary>
    public Method<TreeAdminSetConfigRequest, TreeConfigurationReport> SetTreeConfig { get; }

    /// <summary>The unary <c>GetShardMap</c> read-only registry-persisted shard-map RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeShardMapView> GetShardMap { get; }

    /// <summary>The unary <c>DeleteTree</c> soft-delete lifecycle RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> DeleteTree { get; }

    /// <summary>The unary <c>RecoverTree</c> recovery lifecycle RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> RecoverTree { get; }

    /// <summary>The unary <c>PurgeTree</c> irreversible hard-purge lifecycle RPC.</summary>
    public Method<TreeAdminPurgeRequest, TreeDeletionStatus> PurgeTree { get; }

    /// <summary>The unary <c>GetTreeDeletionStatus</c> read-only deletion-status RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeDeletionStatus> GetTreeDeletionStatus { get; }

    /// <summary>The unary <c>BeginBulkLoad</c> session-open RPC.</summary>
    public Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadSession> BeginBulkLoad { get; }

    /// <summary>The unary <c>AppendBulkLoad</c> chunk-graft RPC.</summary>
    public Method<TreeAdminBulkLoadAppendRequest, TreeBulkLoadChunkAck> AppendBulkLoad { get; }

    /// <summary>The unary <c>CommitBulkLoad</c> session-close RPC.</summary>
    public Method<TreeAdminBulkLoadSessionRequest, TreeBulkLoadResult> CommitBulkLoad { get; }

    /// <summary>The unary <c>RestoreTree</c> restore-into-tree RPC.</summary>
    public Method<TreeAdminRestoreRequest, TreeRestoreResult> RestoreTree { get; }

    /// <summary>The unary <c>RestoreTreeSet</c> restore-set RPC.</summary>
    public Method<TreeAdminRestoreSetRequest, TreeRestoreSetResult> RestoreTreeSet { get; }

    /// <summary>The unary <c>RevertTreeRestore</c> revert-restore RPC. The request result is echoed back as the completion ack.</summary>
    public Method<TreeRestoreResult, TreeRestoreResult> RevertTreeRestore { get; }

    /// <summary>The unary <c>ReshardTree</c> online-reshard trigger RPC.</summary>
    public Method<TreeAdminReshardRequest, TreeReshardStatus> ReshardTree { get; }

    /// <summary>The unary <c>GetReshardStatus</c> read-only reshard-status RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeReshardStatus> GetReshardStatus { get; }

    /// <summary>The unary <c>ResizeTree</c> online-resize trigger RPC.</summary>
    public Method<TreeAdminResizeRequest, TreeResizeStatus> ResizeTree { get; }

    /// <summary>The unary <c>UndoTreeResize</c> undo-resize RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeResizeStatus> UndoTreeResize { get; }

    /// <summary>The unary <c>GetResizeStatus</c> read-only resize-status RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeResizeStatus> GetResizeStatus { get; }

    /// <summary>The unary <c>SnapshotTree</c> snapshot-capture trigger RPC.</summary>
    public Method<TreeAdminSnapshotRequest, TreeSnapshotStatus> SnapshotTree { get; }

    /// <summary>The unary <c>GetSnapshotStatus</c> read-only snapshot-status RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeSnapshotStatus> GetSnapshotStatus { get; }

    /// <summary>The unary <c>GetWalPlacement</c> read-only WAL placement inspection RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeWalPlacement> GetWalPlacement { get; }

    /// <summary>The unary <c>AuditWalPlacement</c> read-only WAL placement audit RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeWalPlacementAudit> AuditWalPlacement { get; }

    /// <summary>The unary <c>AuditOrphanedLeaves</c> read-only orphaned-leaf audit RPC.</summary>
    public Method<TreeAdminOrphanedLeafRequest, TreeOrphanedLeafReport> AuditOrphanedLeaves { get; }

    /// <summary>The unary <c>RepairOrphanedLeaves</c> mutating orphaned-leaf repair RPC.</summary>
    public Method<TreeAdminOrphanedLeafRequest, TreeOrphanedLeafReport> RepairOrphanedLeaves { get; }

    /// <summary>The unary <c>PlanWalMove</c> read-only WAL move plan RPC.</summary>
    public Method<TreeAdminWalMovePlanRequest, TreeWalMovePlan> PlanWalMove { get; }

    /// <summary>The unary <c>ReclaimMovedWalSource</c> WAL move reclaim RPC.</summary>
    public Method<TreeAdminWalReclaimRequest, TreeWalMoveReceipt> ReclaimMovedWalSource { get; }

    /// <summary>The unary <c>ListViews</c> read-only runtime materialised-view listing RPC.</summary>
    public Method<TreeAdminViewListRequest, TreeViewCatalog> ListViews { get; }

    /// <summary>The unary <c>CreateView</c> runtime materialised-view creation RPC.</summary>
    public Method<TreeAdminCreateViewRequest, TreeViewStatus> CreateView { get; }

    /// <summary>The unary <c>GetViewStatus</c> read-only materialised-view status RPC.</summary>
    public Method<TreeAdminViewRequest, TreeViewStatus> GetViewStatus { get; }

    /// <summary>The unary <c>DropView</c> materialised-view drop RPC. The request is echoed back as the completion ack.</summary>
    public Method<TreeAdminViewRequest, TreeAdminViewRequest> DropView { get; }

    /// <summary>The unary <c>ListTagIndexes</c> read-only tag-index listing RPC.</summary>
    public Method<TreeAdminTagIndexListRequest, TreeTagIndexCatalog> ListTagIndexes { get; }

    /// <summary>The unary <c>GetTagIndexStatus</c> read-only tag-index status RPC.</summary>
    public Method<TreeAdminTagIndexRequest, TreeTagIndexStatus> GetTagIndexStatus { get; }

    /// <summary>The unary <c>TriggerShardCompaction</c> tombstone-compaction trigger RPC.</summary>
    public Method<TreeAdminShardRequest, TreeCompactionTriggerResult> TriggerShardCompaction { get; }

    /// <summary>The unary <c>GetHistoryRetention</c> read-only retention read RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeHistoryRetention> GetHistoryRetention { get; }

    /// <summary>The unary <c>SetHistoryRetention</c> retention set RPC.</summary>
    public Method<TreeAdminSetRetentionRequest, TreeHistoryRetention> SetHistoryRetention { get; }
    /// <summary>The unary <c>StartViewRebuild</c> accept-then-poll view-rebuild RPC.</summary>
    public Method<TreeAdminViewRequest, LatticeOperationHandle> StartViewRebuild { get; }

    /// <summary>The unary <c>StartViewReconcile</c> accept-then-poll view-reconcile RPC.</summary>
    public Method<TreeAdminViewRequest, LatticeOperationHandle> StartViewReconcile { get; }

    /// <summary>The unary <c>StartTagIndexReconcile</c> accept-then-poll tag-index reconcile RPC.</summary>
    public Method<TreeAdminTagIndexRequest, LatticeOperationHandle> StartTagIndexReconcile { get; }

    /// <summary>The unary <c>StartWalMove</c> accept-then-poll WAL move RPC.</summary>
    public Method<TreeAdminWalMoveExecuteRequest, LatticeOperationHandle> StartWalMove { get; }

    /// <summary>The unary <c>StartOrphanedLeavesAudit</c> accept-then-poll orphaned-leaf audit RPC.</summary>
    public Method<TreeAdminOrphanedLeafRequest, LatticeOperationHandle> StartOrphanedLeavesAudit { get; }

    /// <summary>The unary <c>StartOrphanedLeavesRepair</c> accept-then-poll orphaned-leaf repair RPC.</summary>
    public Method<TreeAdminOrphanedLeafRequest, LatticeOperationHandle> StartOrphanedLeavesRepair { get; }

    /// <summary>The unary <c>GetTreeAdminOperationStatus</c> RPC.</summary>
    public Method<TreeAdminOperationRequest, TreeAdminOperationStatusResponse> GetTreeAdminOperationStatus { get; }

    /// <summary>The unary <c>ListTreeAdminOperations</c> RPC.</summary>
    public Method<LatticeOperationListRequest, LatticeOperationPage> ListTreeAdminOperations { get; }

    /// <summary>The unary <c>CancelTreeAdminOperation</c> RPC.</summary>
    public Method<TreeAdminOperationRequest, TreeAdminOperationStatusResponse> CancelTreeAdminOperation { get; }

    /// <summary>The unary <c>StartStorageUsageRefresh</c> accept-then-poll cluster-storage RPC.</summary>
    public Method<TreeAdminStorageUsageRefreshRequest, LatticeOperationHandle> StartStorageUsageRefresh { get; }

    /// <summary>The unary <c>GetStorageUsageRefreshStatus</c> RPC.</summary>
    public Method<TreeAdminStorageUsageOperationRequest, TreeAdminStorageUsageOperationStatusResponse> GetStorageUsageRefreshStatus { get; }

    /// <summary>The unary <c>ListStorageUsageRefreshes</c> RPC.</summary>
    public Method<LatticeOperationListRequest, LatticeOperationPage> ListStorageUsageRefreshes { get; }

    /// <summary>The unary <c>CancelStorageUsageRefresh</c> RPC.</summary>
    public Method<TreeAdminStorageUsageOperationRequest, TreeAdminStorageUsageOperationStatusResponse> CancelStorageUsageRefresh { get; }

    /// <summary>The unary <c>GetWalReclamation</c> read-only WAL floor-holder RPC.</summary>
    public Method<TreeAdminTreeRequest, TreeWalReclamationReport> GetWalReclamation { get; }

    /// <summary>
    /// Builds the method definitions from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client so both ends wire identical marshallers.
    /// </summary>
    public static LatticeTreeAdminGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTreeAdminGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<TreeAdminTreeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeTreeAdminCapabilities>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisementRequest>>(),
            serializerProvider.GetRequiredService<Serializer<AuthSchemeAdvertisement>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminShardRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminOrphanedLeafRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminDiagnosticsRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminStorageUsageRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeHotnessReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminDiagnosticReport>>(),
            serializerProvider.GetRequiredService<Serializer<ShardMapInspection>>(),
            serializerProvider.GetRequiredService<Serializer<ShardProjectionDigestReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeStatsReport>>(),
            serializerProvider.GetRequiredService<Serializer<ClusterStorageUsageSummary>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminCreateRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSetAliasRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSetConfigRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeCreationResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeExistenceResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAliasResolution>>(),
            serializerProvider.GetRequiredService<Serializer<TreeConfigurationReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeShardMapView>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminPurgeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeDeletionStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminBulkLoadSessionRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminBulkLoadAppendRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadSession>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadChunkAck>>(),
            serializerProvider.GetRequiredService<Serializer<TreeBulkLoadResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminRestoreRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminRestoreSetRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeRestoreResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeRestoreSetResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminReshardRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeReshardStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminResizeRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeResizeStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSnapshotRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeSnapshotStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminWalMovePlanRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminWalMoveExecuteRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminWalReclaimRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeWalPlacement>>(),
            serializerProvider.GetRequiredService<Serializer<TreeWalPlacementAudit>>(),
            serializerProvider.GetRequiredService<Serializer<TreeOrphanedLeafReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeWalMovePlan>>(),
            serializerProvider.GetRequiredService<Serializer<TreeWalMoveReceipt>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminViewRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminCreateViewRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminViewListRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeViewCatalog>>(),
            serializerProvider.GetRequiredService<Serializer<TreeViewStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeViewReconcileResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminTagIndexRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminTagIndexListRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeTagIndexCatalog>>(),
            serializerProvider.GetRequiredService<Serializer<TreeTagIndexStatus>>(),
            serializerProvider.GetRequiredService<Serializer<TreeTagReconcileReport>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminSetRetentionRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeHistoryRetention>>(),
            serializerProvider.GetRequiredService<Serializer<TreeCompactionTriggerResult>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminStorageUsageRefreshRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationHandle>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminStorageUsageOperationRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminStorageUsageOperationStatusResponse>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminOperationRequest>>(),
            serializerProvider.GetRequiredService<Serializer<TreeAdminOperationStatusResponse>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationListRequest>>(),
            serializerProvider.GetRequiredService<Serializer<LatticeOperationPage>>(),
            serializerProvider.GetRequiredService<Serializer<TreeWalReclamationReport>>());
    }
}

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeTreeAdminGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed: subsequent registrations
/// replace the prior instance, matching the "last-host-wins" semantics
/// integration-test fixtures rely on.
/// </summary>
internal static class LatticeTreeAdminGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeTreeAdminGrpcMethods? Current { get; set; }
}
