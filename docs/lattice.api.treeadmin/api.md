# Public API

The package implements the shared `ILatticeTreeAdmin`, `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` and `ILatticeWalReclamation` contracts. Their model types live in `Orleans.Lattice.Api.Abstractions`; the declarations below include that shared tree-administration family. The [facade guide](README.md) explains scope, authorization and request behavior.

## Registration and authority

`AddLatticeTreeAdminApi` registers the in-process facades and options. Call it after `AddLatticeSchemaApi` on the same silo builder: registration checks for `ILatticeSchemaControl` and fails immediately if it is absent. It composes that facade for schema-policy methods. `LatticeApiTreeAdminOptions` currently exposes no tunable properties. Transport bindings register/map their own endpoints and authorization; they do not replace the core facade's access gate.

Read and mutation operations are not one undifferentiated admin grant: the surface distinguishes tree lifecycle, schema management, telemetry and view/source-tree administration. See the [exact method guide](README.md#facade-method-signatures) and [operation guide](operations.md) before granting cluster authority.

## Immediate calls and tracked operations

Tree configuration, statistics, schema and WAL-reclamation diagnostics remain direct request/response calls. Long-running view rebuild/reconcile, tag-index repair, WAL move, orphaned-leaf audit and storage-usage refresh use accept-then-poll operations with an idempotency id, progress/status and result data. Cancellation of a start request is not cancellation of the recorded operation; use the operation cancellation contract.

Use `StorageUsageRefreshResults.TryReadSummary` for cluster totals and the public operation result helpers for other results. A WAL move does not reclaim the old provider's tail automatically: reclamation is a separate irreversible finalization, and `ILatticeWalReclamation` diagnoses the durable pin/leaf state that controls trimming.

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Operations](operations.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TreeAdmin.LatticeApiTreeAdminOptions`

[Source](../../src/lattice.api.treeadmin/LatticeApiTreeAdminOptions.cs) (line 14).

`public sealed class LatticeApiTreeAdminOptions`


### `Orleans.Lattice.Api.TreeAdmin.LatticeApiTreeAdminServiceCollectionExtensions`

[Source](../../src/lattice.api.treeadmin/LatticeApiTreeAdminServiceCollectionExtensions.cs) (line 13).

`public static class LatticeApiTreeAdminServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeTreeAdminApi( this ISiloBuilder builder, Action<LatticeApiTreeAdminOptions>? configure = null)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TreeAdmin.ApiTreeAdminTypeAliases`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ApiTreeAdminTypeAliases.cs) (line 16).

`public static class ApiTreeAdminTypeAliases`

- `public const string AliasPrefix`
- `public const string LatticeTreeAdminCapabilities`
- `public const string ShardHotnessSnapshot`
- `public const string TreeHotnessReport`
- `public const string ShardDiagnosticSnapshot`
- `public const string TreeAdminDiagnosticReport`
- `public const string ShardMapInspection`
- `public const string ShardProjectionDigestReport`
- `public const string TreeStatsReport`
- `public const string TreeStorageUsageSnapshot`
- `public const string ClusterStorageUsageSummary`
- `public const string TreeCreationResult`
- `public const string TreeExistenceResult`
- `public const string TreeAliasResolution`
- `public const string TreeConfigurationReport`
- `public const string TreeConfigurationUpdate`
- `public const string TreeShardMapView`
- `public const string TreeDeletionStatus`
- `public const string TreeBulkLoadSession`
- `public const string TreeBulkLoadChunkAck`
- `public const string TreeBulkLoadResult`
- `public const string TreeRestoreMode`
- `public const string TreeRestoreResult`
- `public const string TreeRestoreSetResult`
- `public const string TreeReshardStatus`
- `public const string TreeResizeStatus`
- `public const string TreeSnapshotMode`
- `public const string TreeSnapshotStatus`
- `public const string TreeWalPlacement`
- `public const string TreeWalPartitionPlacement`
- `public const string TreeWalPlacementAudit`
- `public const string TreeWalMovePlan`
- `public const string TreeWalMoveReceipt`
- `public const string TreeWalMoveOutcome`
- `public const string TreeWalMoveOptions`
- `public const string TreeViewStatus`
- `public const string TreeViewInfo`
- `public const string TreeViewCatalog`
- `public const string TreeViewReconcileResult`
- `public const string TreeTagIndexInfo`
- `public const string TreeTagIndexCatalog`
- `public const string TreeTagIndexStatus`
- `public const string TreeTagReconcileReport`
- `public const string TreeHistoryRetentionMode`
- `public const string TreeHistoryRetention`
- `public const string TreeCompactionTriggerResult`
- `public const string TreeOrphanedLeafDisposition`
- `public const string TreeOrphanedLeafFinding`
- `public const string TreeOrphanedLeafReport`
- `public const string TreeOrphanedLeafGap`
- `public const string TreeOrphanedLeafGapReason`
- `public const string TreeResizePhase`
- `public const string TreeSnapshotPhase`
- `public const string TreeWalReclamationReport`
- `public const string TreeWalFloorHolder`
- `public const string TreeWalFloorHolderState`

### `Orleans.Lattice.Api.TreeAdmin.BulkLoadOrderException`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/BulkLoadOrderException.cs) (line 12).

`public sealed class BulkLoadOrderException : Exception`

- `public BulkLoadOrderException(string treeId, long chunkIndex, string offendingKey, string precedingKey)`
- `public string TreeId { get; }`
- `public long ChunkIndex { get; }`
- `public string OffendingKey { get; }`
- `public string PrecedingKey { get; }`

### `Orleans.Lattice.Api.TreeAdmin.ClusterStorageUsageSummary`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ClusterStorageUsageSummary.cs) (line 12).

`public sealed record ClusterStorageUsageSummary`

- `public int TreeCount { get; init; }`
- `public long WalRetainedBytes { get; init; }`
- `public long SnapshotBytes { get; init; }`
- `public long LeafStateBytes { get; init; }`
- `public long TotalBytes { get; init; }`
- `public bool Partial { get; init; }`
- `public bool Deep { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`
- `public ImmutableArray<TreeStorageUsageSnapshot> Trees { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.ILatticeStorageUsageOperations`

[Source](../../src/lattice.api.abstractions/TreeAdmin/ILatticeStorageUsageOperations.cs) (line 25).

`public interface ILatticeStorageUsageOperations : ILatticeOperations`

- `Task<LatticeOperationHandle> StartStorageUsageRefreshAsync( string? operationId = null, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin`

[Source](../../src/lattice.api.abstractions/TreeAdmin/ILatticeTreeAdmin.cs) (line 43).

`public interface ILatticeTreeAdmin`

- `Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeHotnessReport> GetShardHotnessAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync( string treeId, bool deep = false, CancellationToken cancellationToken = default)`
- `Task<ShardMapInspection> InspectShardMapAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<ShardProjectionDigestReport> GetProjectionDigestAsync( string treeId, int shardIndex, CancellationToken cancellationToken = default)`
- `Task<TreeStatsReport> GetTreeStatsAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<ClusterStorageUsageSummary> GetStorageUsageAsync( bool deep = false, CancellationToken cancellationToken = default)`
- `Task<TreeCreationResult> CreateTreeAsync( string treeId, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)`
- `Task<TreeExistenceResult> CheckTreeExistsAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeAliasResolution> SetTreeAliasAsync( string treeId, string physicalTreeId, CancellationToken cancellationToken = default)`
- `Task<TreeAliasResolution> ResolveTreeAliasAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeConfigurationReport> GetTreeConfigAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeConfigurationReport> SetTreeConfigAsync( string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)`
- `Task<TreeShardMapView> GetShardMapAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> DeleteTreeAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> RecoverTreeAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> PurgeTreeAsync( string treeId, bool confirm, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> GetTreeDeletionStatusAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeBulkLoadSession> BeginBulkLoadAsync( string treeId, string operationId, CancellationToken cancellationToken = default)`
- `Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync( string treeId, string operationId, long chunkIndex, IReadOnlyList<DataEntry> entries, CancellationToken cancellationToken = default)`
- `Task<TreeBulkLoadResult> CommitBulkLoadAsync( string treeId, string operationId, CancellationToken cancellationToken = default)`
- `Task<TreeRestoreResult> RestoreTreeAsync( string treeId, string backupId, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync( string setId, CancellationToken cancellationToken = default)`
- `Task RevertTreeRestoreAsync( TreeRestoreResult restore, CancellationToken cancellationToken = default)`
- `Task<TreeReshardStatus> ReshardTreeAsync( string treeId, int targetShardCount, CancellationToken cancellationToken = default)`
- `Task<TreeReshardStatus> GetReshardStatusAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeResizeStatus> ResizeTreeAsync( string treeId, int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)`
- `Task<TreeResizeStatus> UndoTreeResizeAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeResizeStatus> GetResizeStatusAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeSnapshotStatus> SnapshotTreeAsync( string treeId, string destinationTreeId, TreeSnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)`
- `Task<TreeSnapshotStatus> GetSnapshotStatusAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeWalPlacement> GetWalPlacementAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeWalPlacementAudit> AuditWalPlacementAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeOrphanedLeafReport> AuditOrphanedLeavesAsync( string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `Task<TreeOrphanedLeafReport> SurveyOrphanedLeavesAsync( string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `Task<TreeOrphanedLeafReport> RepairOrphanedLeavesAsync( string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `Task<TreeWalMovePlan> PlanWalMoveAsync( string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)`
- `Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync( string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)`
- `Task<TreeViewCatalog> ListViewsAsync( CancellationToken cancellationToken = default)`
- `Task<TreeViewStatus> CreateViewAsync( string viewName, string sourceTreeId, string providerKey, byte[] payload, CancellationToken cancellationToken = default)`
- `Task<TreeViewStatus> GetViewStatusAsync( string viewName, CancellationToken cancellationToken = default)`
- `Task DropViewAsync( string viewName, CancellationToken cancellationToken = default)`
- `Task<TreeTagIndexCatalog> ListTagIndexesAsync( CancellationToken cancellationToken = default)`
- `Task<TreeTagIndexStatus> GetTagIndexStatusAsync( string indexName, CancellationToken cancellationToken = default)`
- `Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync( string treeId, int shardIndex, CancellationToken cancellationToken = default)`
- `Task<TreeHistoryRetention> GetHistoryRetentionAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeHistoryRetention> SetHistoryRetentionAsync( string treeId, TreeHistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdminOperations`

[Source](../../src/lattice.api.abstractions/TreeAdmin/ILatticeTreeAdminOperations.cs) (line 37).

`public interface ILatticeTreeAdminOperations : ILatticeOperations`

- `Task<LatticeOperationHandle> StartViewRebuildAsync( string viewName, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartViewReconcileAsync( string viewName, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartTagIndexReconcileAsync( string indexName, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartWalMoveAsync( string treeId, int partition, string targetProviderKey, TreeWalMoveOptions? options = null, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync( string treeId, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync( string treeId, string? operationId = null, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TreeAdmin.ILatticeWalReclamation`

[Source](../../src/lattice.api.abstractions/TreeAdmin/ILatticeWalReclamation.cs) (line 8).

`public interface ILatticeWalReclamation`

- `Task<TreeWalReclamationReport> GetWalReclamationAsync(string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TreeAdmin.LatticeTreeAdminCapabilities`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/LatticeTreeAdminCapabilities.cs) (line 35).

`public sealed record LatticeTreeAdminCapabilities`

- `public required string TreeId { get; init; }`
- `public bool CanAdministerTree { get; init; }`
- `public bool CanManageTreeLifecycle { get; init; }`
- `public bool CanViewDiagnostics { get; init; }`
- `public required LatticeSchemaCapabilities Schema { get; init; }`
- `public bool CanBulkLoad { get; init; }`
- `public bool CanRestore { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.ShardDiagnosticSnapshot`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ShardDiagnosticSnapshot.cs) (line 9).

`public sealed record ShardDiagnosticSnapshot`

- `public int ShardIndex { get; init; }`
- `public int Depth { get; init; }`
- `public bool RootIsLeaf { get; init; }`
- `public long LiveKeys { get; init; }`
- `public long Tombstones { get; init; }`
- `public double TombstoneRatio { get; init; }`
- `public double OpsPerSecond { get; init; }`
- `public long Reads { get; init; }`
- `public long Writes { get; init; }`
- `public double WindowSeconds { get; init; }`
- `public bool SplitInProgress { get; init; }`
- `public bool BulkOperationPending { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.ShardHotnessSnapshot`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ShardHotnessSnapshot.cs) (line 10).

`public sealed record ShardHotnessSnapshot`

- `public int ShardIndex { get; init; }`
- `public long Reads { get; init; }`
- `public long Writes { get; init; }`
- `public double OpsPerSecond { get; init; }`
- `public double WindowSeconds { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.ShardMapInspection`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ShardMapInspection.cs) (line 12).

`public sealed record ShardMapInspection`

- `public required string TreeId { get; init; }`
- `public required string PhysicalTreeId { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public int PhysicalShardCount { get; init; }`
- `public long MapVersion { get; init; }`
- `public ImmutableArray<int> PhysicalShardIndices { get; init; }`
- `public ImmutableArray<int> SlotCounts { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.ShardProjectionDigestReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/ShardProjectionDigestReport.cs) (line 11).

`public sealed record ShardProjectionDigestReport`

- `public required string TreeId { get; init; }`
- `public int ShardIndex { get; init; }`
- `public required string HashHex { get; init; }`
- `public long EntryCount { get; init; }`
- `public long CheckpointOffset { get; init; }`
- `public long Version { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.StorageUsageRefreshOperation`

[Source](../../src/lattice.api.abstractions/TreeAdmin/StorageUsageRefreshOperation.cs) (line 9).

`public static class StorageUsageRefreshOperation`

- `public const string Kind`
- `public const string MeasuringPhase`
- `public const string TreesUnit`

### `Orleans.Lattice.Api.TreeAdmin.StorageUsageRefreshResults`

[Source](../../src/lattice.api.abstractions/TreeAdmin/StorageUsageRefreshResults.cs) (line 13).

`public static class StorageUsageRefreshResults`

- `public const string TreeCountKey`
- `public const string WalRetainedBytesKey`
- `public const string SnapshotBytesKey`
- `public const string LeafStateBytesKey`
- `public const string TotalBytesKey`
- `public const string PartialKey`
- `public const string SampledAtKey`
- `public static IReadOnlyDictionary<string, string> ToResultMap(ClusterStorageUsageSummary summary)`
- `public static bool TryReadSummary(IReadOnlyDictionary<string, string> result, out ClusterStorageUsageSummary? summary)`

### `Orleans.Lattice.Api.TreeAdmin.TreeAdminDiagnosticReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeAdminDiagnosticReport.cs) (line 12).

`public sealed record TreeAdminDiagnosticReport`

- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public long TotalLiveKeys { get; init; }`
- `public long TotalTombstones { get; init; }`
- `public bool Deep { get; init; }`
- `public int RecentSplitCount { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`
- `public ImmutableArray<ShardDiagnosticSnapshot> Shards { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationKinds`

[Source](../../src/lattice.api.abstractions/TreeAdmin/TreeAdminOperationKinds.cs) (line 9).

`public static class TreeAdminOperationKinds`

- `public const string Prefix`
- `public const string ViewRebuild`
- `public const string ViewReconcile`
- `public const string TagIndexReconcile`
- `public const string WalMove`
- `public const string OrphanedLeavesAudit`
- `public const string OrphanedLeavesRepair`

### `Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationPhases`

[Source](../../src/lattice.api.abstractions/TreeAdmin/TreeAdminOperationPhases.cs) (line 11).

`public static class TreeAdminOperationPhases`

- `public const string Scanning`
- `public const string Projecting`
- `public const string Digesting`
- `public const string Comparing`
- `public const string Swapping`
- `public const string Probing`
- `public const string Repairing`
- `public const string Copying`
- `public const string Verifying`
- `public const string Flipping`
- `public const string Walking`

### `Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationResultKeys`

[Source](../../src/lattice.api.abstractions/TreeAdmin/TreeAdminOperationResultKeys.cs) (line 9).

`public static class TreeAdminOperationResultKeys`

- `public const string ViewName`
- `public const string SourceTreeId`
- `public const string DriftRepaired`
- `public const string IndexName`
- `public const string TreesCovered`
- `public const string KeysScanned`
- `public const string MembershipRowsScanned`
- `public const string OrphanRowsRemoved`
- `public const string TreeId`
- `public const string Partition`
- `public const string FromProviderKey`
- `public const string ToProviderKey`
- `public const string Outcome`
- `public const string PreviousPlacementVersion`
- `public const string NewPlacementVersion`
- `public const string CopiedFromOffset`
- `public const string CopiedThroughOffset`
- `public const string SourceHighestOffset`
- `public const string TargetHighestOffset`
- `public const string SourceRetained`
- `public const string LeavesWalked`
- `public const string OrphanedLeaves`
- `public const string Repaired`
- `public const string Repairable`
- `public const string Refused`
- `public const string Gaps`

### `Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationUnits`

[Source](../../src/lattice.api.abstractions/TreeAdmin/TreeAdminOperationUnits.cs) (line 9).

`public static class TreeAdminOperationUnits`

- `public const string Keys`
- `public const string Trees`
- `public const string Entries`
- `public const string Shards`

### `Orleans.Lattice.Api.TreeAdmin.TreeAliasResolution`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeAliasResolution.cs) (line 15).

`public sealed record TreeAliasResolution`

- `public required string TreeId { get; init; }`
- `public required string PhysicalTreeId { get; init; }`
- `public bool IsAliased { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeBulkLoadChunkAck`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeBulkLoadChunkAck.cs) (line 9).

`public sealed record TreeBulkLoadChunkAck`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`
- `public required long ChunkIndex { get; init; }`
- `public required int AcceptedEntryCount { get; init; }`
- `public required long NextChunkIndex { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeBulkLoadResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeBulkLoadResult.cs) (line 11).

`public sealed record TreeBulkLoadResult`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`
- `public required long TotalLiveKeys { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeBulkLoadSession`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeBulkLoadSession.cs) (line 19).

`public sealed record TreeBulkLoadSession`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeCompactionTriggerResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeCompactionTriggerResult.cs) (line 10).

`public sealed record TreeCompactionTriggerResult`

- `public required string TreeId { get; init; }`
- `public int ShardIndex { get; init; }`
- `public bool Accepted { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeConfigurationReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeConfigurationReport.cs) (line 16).

`public sealed record TreeConfigurationReport`

- `public required string TreeId { get; init; }`
- `public bool Exists { get; init; }`
- `public string? PhysicalTreeId { get; init; }`
- `public int? ShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`
- `public bool? PublishEvents { get; init; }`
- `public bool? MaintainProjectionDigest { get; init; }`
- `public bool ProjectionDigestPermanentlyDisabled { get; init; }`
- `public HistoryRetentionMode? HistoryRetentionMode { get; init; }`
- `public long? HistoryRetentionWindowTicks { get; init; }`
- `public long? WalMaxRetainedBytes { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeConfigurationUpdate`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeConfigurationUpdate.cs) (line 21).

`public sealed record TreeConfigurationUpdate`

- `public bool ApplyPublishEvents { get; init; }`
- `public bool? PublishEvents { get; init; }`
- `public bool ApplyMaintainProjectionDigest { get; init; }`
- `public bool? MaintainProjectionDigest { get; init; }`
- `public bool ApplyHistoryRetention { get; init; }`
- `public HistoryRetentionMode? HistoryRetentionMode { get; init; }`
- `public long? HistoryRetentionWindowTicks { get; init; }`
- `public bool ApplyWalMaxRetainedBytes { get; init; }`
- `public long? WalMaxRetainedBytes { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeCreationResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeCreationResult.cs) (line 18).

`public sealed record TreeCreationResult`

- `public required string TreeId { get; init; }`
- `public bool Created { get; init; }`
- `public int ShardCount { get; init; }`
- `public int MaxLeafKeys { get; init; }`
- `public int MaxInternalChildren { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeDeletionStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeDeletionStatus.cs) (line 11).

`public sealed record TreeDeletionStatus`

- `public required string TreeId { get; init; }`
- `public bool IsDeleted { get; init; }`
- `public DateTimeOffset? DeletedAtUtc { get; init; }`
- `public DateTimeOffset? RecoveryDeadlineUtc { get; init; }`
- `public bool PurgeInProgress { get; init; }`
- `public bool PurgeComplete { get; init; }`
- `public bool CanRecover { get; init; }`
- `public int PurgedShardCount { get; init; }`
- `public int PurgeShardCount { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeExistenceResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeExistenceResult.cs) (line 7).

`public sealed record TreeExistenceResult`

- `public required string TreeId { get; init; }`
- `public bool Exists { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeHistoryRetention`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeHistoryRetention.cs) (line 11).

`public sealed record TreeHistoryRetention`

- `public required string TreeId { get; init; }`
- `public TreeHistoryRetentionMode Mode { get; init; }`
- `public TimeSpan Window { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeHistoryRetentionMode`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeHistoryRetentionMode.cs) (line 9).

`public enum TreeHistoryRetentionMode`

- `MetadataOnly = 0`
- `FullValue = 1`
- `Hybrid = 2`

### `Orleans.Lattice.Api.TreeAdmin.TreeHotnessReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeHotnessReport.cs) (line 11).

`public sealed record TreeHotnessReport`

- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`
- `public long TotalReads { get; init; }`
- `public long TotalWrites { get; init; }`
- `public double TotalOpsPerSecond { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`
- `public ImmutableArray<ShardHotnessSnapshot> Shards { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeNotEmptyException`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeNotEmptyException.cs) (line 12).

`public sealed class TreeNotEmptyException : Exception`

- `public TreeNotEmptyException(string treeId)`
- `public TreeNotEmptyException(string treeId, string message)`
- `public string TreeId { get; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeOrphanedLeafDisposition`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeOrphanedLeafDisposition.cs) (line 12).

`public enum TreeOrphanedLeafDisposition`

- `Repaired = 0`
- `Repairable = 1`
- `RefusedUnverifiedKeys = 2`
- `RefusedKeyCountExceeded = 3`
- `RefusedBlockingState = 4`
- `RefusedChainRace = 5`
- `RefusedRoutingContradiction = 6`

### `Orleans.Lattice.Api.TreeAdmin.TreeOrphanedLeafFinding`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeOrphanedLeafFinding.cs) (line 8).

`public sealed record TreeOrphanedLeafFinding`

- `public int ShardIndex { get; init; }`
- `public required string LeafId { get; init; }`
- `public string? LowKeyInclusive { get; init; }`
- `public string? HighKeyExclusive { get; init; }`
- `public int KeyCount { get; init; }`
- `public int VerifiedKeyCount { get; init; }`
- `public TreeOrphanedLeafDisposition Disposition { get; init; }`
- `public string? UnverifiedKey { get; init; }`
- `public int? SurveyVerifiedKeyCount { get; init; }`
- `public int? SurveyMissingKeyCount { get; init; }`
- `public int? SurveyRoutingContradictionKeyCount { get; init; }`
- `public bool IsRefusal`

### `Orleans.Lattice.Api.TreeAdmin.TreeOrphanedLeafGap`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeOrphanedLeafGap.cs) (line 14).

`public sealed record TreeOrphanedLeafGap`

- `public int ShardIndex { get; init; }`
- `public TreeOrphanedLeafGapReason Reason { get; init; }`
- `public string? LeafId { get; init; }`
- `public string? KeyHint { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeOrphanedLeafGapReason`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeOrphanedLeafGapReason.cs) (line 15).

`public enum TreeOrphanedLeafGapReason`

- `ShardSplitInProgress = 0`
- `ShardPassAlreadyRunning = 1`
- `ChainTruncated = 2`
- `ChainTruncatedUnrecoverable = 3`
- `WalkBudgetExhaustedWithoutResumePosition = 4`
- `LeafBoundsUndecidable = 5`
- `EntryLeafUnreachable = 6`

### `Orleans.Lattice.Api.TreeAdmin.TreeOrphanedLeafReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeOrphanedLeafReport.cs) (line 33).

`public sealed record TreeOrphanedLeafReport`

- `public required string TreeId { get; init; }`
- `public bool DryRun { get; init; }`
- `public int LeavesWalked { get; init; }`
- `public ImmutableArray<TreeOrphanedLeafFinding> Findings { get; init; }`
- `public string? ResumeFrom { get; init; }`
- `public bool IsComplete`
- `public ImmutableArray<TreeOrphanedLeafGap> Gaps { get; init; }`
- `public bool Survey { get; init; }`
- `public int OrphanedLeafCount`
- `public int RepairableCount { get; }`
- `public long? SurveyMissingKeyCount { get; }`
- `public bool VerdictComplete`
- `public int RepairedCount { get; }`
- `public int RefusedCount { get; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeReshardStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeReshardStatus.cs) (line 19).

`public sealed record TreeReshardStatus`

- `public required string TreeId { get; init; }`
- `public bool InProgress { get; init; }`
- `public int CurrentPhysicalShardCount { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public long MapVersion { get; init; }`
- `public int? RequestedShardCount { get; init; }`
- `public int? TargetShardCount { get; init; }`
- `public int? StartPhysicalShardCount { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeResizePhase`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeResizePhase.cs) (line 7).

`public enum TreeResizePhase`

- `Copy = 0`
- `Swap = 1`
- `RejectOldShards = 2`
- `RetireOldCopy = 3`
- `Undo = 4`

### `Orleans.Lattice.Api.TreeAdmin.TreeResizeStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeResizeStatus.cs) (line 20).

`public sealed record TreeResizeStatus`

- `public required string TreeId { get; init; }`
- `public bool InProgress { get; init; }`
- `public int CurrentMaxLeafKeys { get; init; }`
- `public int CurrentMaxInternalChildren { get; init; }`
- `public int? RequestedMaxLeafKeys { get; init; }`
- `public int? RequestedMaxInternalChildren { get; init; }`
- `public bool UndoRequested { get; init; }`
- `public TreeResizePhase? Phase { get; init; }`
- `public int CompletedUnits { get; init; }`
- `public int? TotalUnits { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeRestoreMode`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeRestoreMode.cs) (line 8).

`public enum TreeRestoreMode`

- `InPlace = 0`
- `ShadowCutover = 1`

### `Orleans.Lattice.Api.TreeAdmin.TreeRestoreResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeRestoreResult.cs) (line 15).

`public sealed record TreeRestoreResult`

- `public required string BackupId { get; init; }`
- `public required string TargetTreeId { get; init; }`
- `public required TreeRestoreMode Mode { get; init; }`
- `public required string OperationId { get; init; }`
- `public required IReadOnlyList<string> ManifestChain { get; init; }`
- `public required long EntriesApplied { get; init; }`
- `public string? ShadowPhysicalTreeId { get; init; }`
- `public string? PreviousPhysicalTreeId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeRestoreSetResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeRestoreSetResult.cs) (line 11).

`public sealed record TreeRestoreSetResult`

- `public required IReadOnlyList<TreeRestoreResult> Results { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeShardMapView`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeShardMapView.cs) (line 20).

`public sealed record TreeShardMapView`

- `public required string TreeId { get; init; }`
- `public bool HasCustomMap { get; init; }`
- `public long MapVersion { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public int PhysicalShardCount { get; init; }`
- `public ImmutableArray<int> PhysicalShardIndices { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeSnapshotMode`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeSnapshotMode.cs) (line 8).

`public enum TreeSnapshotMode`

- `Offline = 0`
- `Online = 1`

### `Orleans.Lattice.Api.TreeAdmin.TreeSnapshotPhase`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeSnapshotPhase.cs) (line 7).

`public enum TreeSnapshotPhase`

- `LockSource = 0`
- `BeginForwarding = 1`
- `Copy = 2`
- `UnlockSource = 3`

### `Orleans.Lattice.Api.TreeAdmin.TreeSnapshotStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeSnapshotStatus.cs) (line 18).

`public sealed record TreeSnapshotStatus`

- `public required string TreeId { get; init; }`
- `public bool InProgress { get; init; }`
- `public string? RequestedDestinationTreeId { get; init; }`
- `public TreeSnapshotMode? RequestedMode { get; init; }`
- `public TreeSnapshotPhase? Phase { get; init; }`
- `public int CopiedShardCount { get; init; }`
- `public int? ShardCount { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeStatsReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeStatsReport.cs) (line 10).

`public sealed record TreeStatsReport`

- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public long TotalLiveKeys { get; init; }`
- `public long TotalTombstones { get; init; }`
- `public long LeafStateBytes { get; init; }`
- `public long SnapshotBytes { get; init; }`
- `public long WalRetainedBytes { get; init; }`
- `public long TotalBytes { get; init; }`
- `public bool PartialStorage { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeStorageUsageSnapshot`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeStorageUsageSnapshot.cs) (line 9).

`public sealed record TreeStorageUsageSnapshot`

- `public required string TreeId { get; init; }`
- `public long WalRetainedBytes { get; init; }`
- `public long SnapshotBytes { get; init; }`
- `public long LeafStateBytes { get; init; }`
- `public long TotalBytes { get; init; }`
- `public bool Partial { get; init; }`
- `public long LiveKeys { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeTagIndexCatalog`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeTagIndexCatalog.cs) (line 10).

`public sealed record TreeTagIndexCatalog`

- `public ImmutableArray<TreeTagIndexInfo> Indexes { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeTagIndexInfo`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeTagIndexInfo.cs) (line 10).

`public sealed record TreeTagIndexInfo`

- `public required string IndexName { get; init; }`
- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`
- `public ImmutableArray<string> CoveredTrees { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeTagIndexStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeTagIndexStatus.cs) (line 10).

`public sealed record TreeTagIndexStatus`

- `public required string IndexName { get; init; }`
- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`
- `public ImmutableArray<string> CoveredTrees { get; init; }`
- `public bool ReconcileIdle { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeTagReconcileReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeTagReconcileReport.cs) (line 9).

`public sealed record TreeTagReconcileReport`

- `public required string IndexName { get; init; }`
- `public required string TreeId { get; init; }`
- `public int TreesCovered { get; init; }`
- `public int KeysScanned { get; init; }`
- `public int MembershipRowsScanned { get; init; }`
- `public int OrphanRowsRemoved { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeViewCatalog`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeViewCatalog.cs) (line 10).

`public sealed record TreeViewCatalog`

- `public ImmutableArray<TreeViewInfo> Views { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeViewInfo`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeViewInfo.cs) (line 15).

`public sealed record TreeViewInfo`

- `public required string ViewName { get; init; }`
- `public required string SourceTreeId { get; init; }`
- `public bool IsAggregation { get; init; }`
- `public bool Accumulative { get; init; }`
- `public string? ProviderKey { get; init; }`
- `public string? ProjectionVersion { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeViewReconcileResult`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeViewReconcileResult.cs) (line 9).

`public sealed record TreeViewReconcileResult`

- `public required string ViewName { get; init; }`
- `public required string SourceTreeId { get; init; }`
- `public bool DriftRepaired { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeViewStatus`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeViewStatus.cs) (line 16).

`public sealed record TreeViewStatus`

- `public required string ViewName { get; init; }`
- `public required string SourceTreeId { get; init; }`
- `public bool IsAggregation { get; init; }`
- `public long ApplyLag { get; init; }`
- `public string ActiveTreeId { get; init; }`
- `public string? ProviderKey { get; init; }`
- `public string? ProjectionVersion { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalFloorHolder`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalFloorHolder.cs) (line 7).

`public sealed record TreeWalFloorHolder`

- `public required string ConsumerId { get; init; }`
- `public string? LeafId { get; init; }`
- `public int Partition { get; init; }`
- `public long PinOffset { get; init; }`
- `public long? PersistedCheckpoint { get; init; }`
- `public TreeWalFloorHolderState State { get; init; }`
- `public bool HoldsOffsetFloor`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalFloorHolderState`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalFloorHolderState.cs) (line 15).

`public enum TreeWalFloorHolderState`

- `CheckpointedUncovered = 0`
- `NeverCheckpointed = 1`
- `NoDurableState = 2`
- `Unreadable = 3`
- `Orphaned = 4`
- `CheckpointedCoverageUnknown = 5`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalMoveOptions`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalMoveOptions.cs) (line 11).

`public readonly record struct TreeWalMoveOptions`

- `public double QuiesceLeaseSeconds { get; init; }`
- `public int CopyPageSize { get; init; }`
- `public bool DisableVerifyAfterCopy { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalMoveOutcome`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalMoveOutcome.cs) (line 7).

`public enum TreeWalMoveOutcome`

- `Moved = 0`
- `AlreadyAtTarget = 1`
- `SourceReclaimed = 2`
- `NoOp = 3`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalMovePlan`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalMovePlan.cs) (line 10).

`public sealed record TreeWalMovePlan`

- `public required string TreeId { get; init; }`
- `public int Partition { get; init; }`
- `public string FromProviderKey { get; init; }`
- `public string ToProviderKey { get; init; }`
- `public long PlacementVersion { get; init; }`
- `public long SourceLowestOffset { get; init; }`
- `public long SourceHighestOffset { get; init; }`
- `public long EntriesToCopy { get; init; }`
- `public bool TargetResolvableOnThisSilo { get; init; }`
- `public bool AlreadyAtTarget { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalMoveReceipt`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalMoveReceipt.cs) (line 16).

`public sealed record TreeWalMoveReceipt`

- `public required string TreeId { get; init; }`
- `public int Partition { get; init; }`
- `public string FromProviderKey { get; init; }`
- `public string ToProviderKey { get; init; }`
- `public long PreviousPlacementVersion { get; init; }`
- `public long NewPlacementVersion { get; init; }`
- `public long CopiedFromOffset { get; init; }`
- `public long CopiedThroughOffset { get; init; }`
- `public long SourceHighestOffset { get; init; }`
- `public long TargetHighestOffset { get; init; }`
- `public bool SourceRetained { get; init; }`
- `public TreeWalMoveOutcome Outcome { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalPartitionPlacement`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalPartitionPlacement.cs) (line 9).

`public readonly record struct TreeWalPartitionPlacement`

- `public int Partition { get; init; }`
- `public string ProviderKey { get; init; }`
- `public bool ResolvableOnThisSilo { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalPlacement`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalPlacement.cs) (line 11).

`public sealed record TreeWalPlacement`

- `public required string TreeId { get; init; }`
- `public long Version { get; init; }`
- `public string DefaultProviderKey { get; init; }`
- `public ImmutableArray<TreeWalPartitionPlacement> Partitions { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalPlacementAudit`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalPlacementAudit.cs) (line 12).

`public sealed record TreeWalPlacementAudit`

- `public required string TreeId { get; init; }`
- `public long Version { get; init; }`
- `public int PartitionCount { get; init; }`
- `public ImmutableArray<TreeWalPartitionPlacement> Partitions { get; init; }`
- `public bool AllResolvableOnThisSilo { get; init; }`
- `public ImmutableArray<string> KnownProviderKeys { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.TreeWalReclamationReport`

[Source](../../src/lattice.api.abstractions/TreeAdmin/Model/TreeWalReclamationReport.cs) (line 26).

`public sealed record TreeWalReclamationReport`

- `public required string TreeId { get; init; }`
- `public bool PinStoreReadable { get; init; }`
- `public int PinCount { get; init; }`
- `public int PinsWithoutOffset { get; init; }`
- `public TreeWalFloorHolder? FloorHolder { get; init; }`
- `public bool IsWedged`
