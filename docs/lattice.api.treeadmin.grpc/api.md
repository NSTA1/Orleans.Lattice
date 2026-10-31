---
agent_spec: "docs/agents/api/treeadmin.json"
---

# Orleans.Lattice.Api.TreeAdmin.Grpc API reference

The public surface of this package is the typed client, the server-side options, the registration extensions, the authorization and identity seams (with the operation enum and the authorization-context struct), the auth-scheme discovery records, and the wire request records. The gRPC service, the method definitions, the marshallers, the authorization interceptor, the default header credential bridge, and the options-backed auth-scheme source are internal; [Architecture](architecture.md) describes them by behaviour, and [Configuration](configuration.md) covers the options.

The facade contracts this binding projects - `ILatticeTreeAdmin`, `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` and `ILatticeWalReclamation` - and the result records the RPCs return are published by `Orleans.Lattice.Api.Abstractions`. They are documented with the [facade's public API](../lattice.api.treeadmin/api.md) and its [method guide](../lattice.api.treeadmin/README.md#facade-method-signatures), and are not restated here.

## Registration

`LatticeTreeAdminApiGrpcServiceCollectionExtensions` holds the two server-side entry points:

- `AddLatticeTreeAdminApiGrpc(this IServiceCollection services, Action<LatticeTreeAdminApiGrpcOptions>? configure = null)` registers the binding: the options, the method definitions, the server-side service, the default-deny `ILatticeTreeAdminApiAuthorizer`, the header-reading `ILatticeTreeAdminApiCredentialBridge`, the options-backed `ILatticeTreeAdminApiAuthSchemeSource`, and the authorization interceptor. Throws `ArgumentNullException` when `services` is `null`.
- `MapLatticeTreeAdminApiGrpc(this IEndpointRouteBuilder endpoints)` maps the service's routes. The host must already have called `AddLatticeTreeAdminApiGrpc` and must expose `ILatticeTreeAdmin` (through `AddLatticeTreeAdminApi`) in the same service provider. Throws `ArgumentNullException` when `endpoints` is `null`.

Which registrations a host may pre-empt, and why the registration call must run once, are covered in [Configuration](configuration.md#registration).

## Client

`LatticeTreeAdminApiGrpcClient` is the public typed client. Build it with `LatticeTreeAdminApiGrpcClient.Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`; the serializer provider must have Orleans serialization registered (`AddSerializer()`), because the per-message marshallers are resolved from it. `Create` throws `ArgumentNullException` when either argument is `null`.

- **One method per RPC.** Every RPC has one client method, except `AuditOrphanedLeaves`, which backs both `AuditOrphanedLeavesAsync` and `SurveyOrphanedLeavesAsync` (the survey sets the request's `Survey` flag). The full signature list is in the [package README](README.md#client-method-signatures) and in the [declaration reference](#public-declaration-reference) below.
- **Unwrapped responses.** `GetAuthSchemeAsync` returns the advertisement's `Schemes`; `RestoreTreeSetAsync` returns the `Results` of the facade's `TreeRestoreSetResult`; the operation and storage-usage-refresh status and cancel methods return the response's `Status`, which is `null` when the operation is not visible to the caller. `DropViewAsync` and `RevertTreeRestoreAsync` return a bare `Task` and discard the echoed acknowledgement.
- **Local argument checks.** The client refuses some malformed arguments before sending - among them a `null` or empty tree, view, index, set, backup or operation id, a negative shard index or bulk-load chunk index, a bulk-load operation id containing `/`, and a restore operation id that is supplied but empty - with the same `ArgumentException` family a local facade caller sees. Everything else is decided by the server.
- **No metadata of its own.** Each call carries only the caller's cancellation token. The credential header and the active-tenant header the server reads (see [Configuration](configuration.md#client-transport)) must be attached by the `CallInvoker` the caller supplies, as must the address, TLS, deadlines and retries.
- **Errors.** A server refusal arrives as an `RpcException` carrying the mapped status; see [Status mapping](README.md#status-mapping).

## Service and RPCs

The service name is `orleans.lattice.api.treeadmin`, so each method's full path is `/orleans.lattice.api.treeadmin/<RPC>`. All 61 RPCs are unary. Each row names the request record, the response record, the facade member the service calls, and the client method that drives it:

| RPC | Request | Response | Facade member | Client method |
|---|---|---|---|---|
| `ProbeCapabilities` | `TreeAdminTreeRequest` | `LatticeTreeAdminCapabilities` | `ILatticeTreeAdmin.ProbeCapabilitiesAsync` | `ProbeCapabilitiesAsync` |
| `GetAuthScheme` | `AuthSchemeAdvertisementRequest` | `AuthSchemeAdvertisement` | binding-local: `ILatticeTreeAdminApiAuthSchemeSource.GetAdvertisement` | `GetAuthSchemeAsync` |
| `GetShardHotness` | `TreeAdminTreeRequest` | `TreeHotnessReport` | `ILatticeTreeAdmin.GetShardHotnessAsync` | `GetShardHotnessAsync` |
| `GetDiagnostics` | `TreeAdminDiagnosticsRequest` | `TreeAdminDiagnosticReport` | `ILatticeTreeAdmin.GetDiagnosticsAsync` | `GetDiagnosticsAsync` |
| `InspectShardMap` | `TreeAdminTreeRequest` | `ShardMapInspection` | `ILatticeTreeAdmin.InspectShardMapAsync` | `InspectShardMapAsync` |
| `GetProjectionDigest` | `TreeAdminShardRequest` | `ShardProjectionDigestReport` | `ILatticeTreeAdmin.GetProjectionDigestAsync` | `GetProjectionDigestAsync` |
| `GetTreeStats` | `TreeAdminTreeRequest` | `TreeStatsReport` | `ILatticeTreeAdmin.GetTreeStatsAsync` | `GetTreeStatsAsync` |
| `GetStorageUsage` | `TreeAdminStorageUsageRequest` | `ClusterStorageUsageSummary` | `ILatticeTreeAdmin.GetStorageUsageAsync` | `GetStorageUsageAsync` |
| `CreateTree` | `TreeAdminCreateRequest` | `TreeCreationResult` | `ILatticeTreeAdmin.CreateTreeAsync` | `CreateTreeAsync` |
| `CheckTreeExists` | `TreeAdminTreeRequest` | `TreeExistenceResult` | `ILatticeTreeAdmin.CheckTreeExistsAsync` | `CheckTreeExistsAsync` |
| `SetTreeAlias` | `TreeAdminSetAliasRequest` | `TreeAliasResolution` | `ILatticeTreeAdmin.SetTreeAliasAsync` | `SetTreeAliasAsync` |
| `ResolveTreeAlias` | `TreeAdminTreeRequest` | `TreeAliasResolution` | `ILatticeTreeAdmin.ResolveTreeAliasAsync` | `ResolveTreeAliasAsync` |
| `GetTreeConfig` | `TreeAdminTreeRequest` | `TreeConfigurationReport` | `ILatticeTreeAdmin.GetTreeConfigAsync` | `GetTreeConfigAsync` |
| `SetTreeConfig` | `TreeAdminSetConfigRequest` | `TreeConfigurationReport` | `ILatticeTreeAdmin.SetTreeConfigAsync` | `SetTreeConfigAsync` |
| `GetShardMap` | `TreeAdminTreeRequest` | `TreeShardMapView` | `ILatticeTreeAdmin.GetShardMapAsync` | `GetShardMapAsync` |
| `DeleteTree` | `TreeAdminTreeRequest` | `TreeDeletionStatus` | `ILatticeTreeAdmin.DeleteTreeAsync` | `DeleteTreeAsync` |
| `RecoverTree` | `TreeAdminTreeRequest` | `TreeDeletionStatus` | `ILatticeTreeAdmin.RecoverTreeAsync` | `RecoverTreeAsync` |
| `PurgeTree` | `TreeAdminPurgeRequest` | `TreeDeletionStatus` | `ILatticeTreeAdmin.PurgeTreeAsync` | `PurgeTreeAsync` |
| `GetTreeDeletionStatus` | `TreeAdminTreeRequest` | `TreeDeletionStatus` | `ILatticeTreeAdmin.GetTreeDeletionStatusAsync` | `GetTreeDeletionStatusAsync` |
| `BeginBulkLoad` | `TreeAdminBulkLoadSessionRequest` | `TreeBulkLoadSession` | `ILatticeTreeAdmin.BeginBulkLoadAsync` | `BeginBulkLoadAsync` |
| `AppendBulkLoad` | `TreeAdminBulkLoadAppendRequest` | `TreeBulkLoadChunkAck` | `ILatticeTreeAdmin.AppendBulkLoadAsync` | `AppendBulkLoadAsync` |
| `CommitBulkLoad` | `TreeAdminBulkLoadSessionRequest` | `TreeBulkLoadResult` | `ILatticeTreeAdmin.CommitBulkLoadAsync` | `CommitBulkLoadAsync` |
| `RestoreTree` | `TreeAdminRestoreRequest` | `TreeRestoreResult` | `ILatticeTreeAdmin.RestoreTreeAsync` | `RestoreTreeAsync` |
| `RestoreTreeSet` | `TreeAdminRestoreSetRequest` | `TreeRestoreSetResult` | `ILatticeTreeAdmin.RestoreTreeSetAsync` | `RestoreTreeSetAsync` |
| `RevertTreeRestore` | `TreeRestoreResult` | `TreeRestoreResult` | `ILatticeTreeAdmin.RevertTreeRestoreAsync` | `RevertTreeRestoreAsync` |
| `ReshardTree` | `TreeAdminReshardRequest` | `TreeReshardStatus` | `ILatticeTreeAdmin.ReshardTreeAsync` | `ReshardTreeAsync` |
| `GetReshardStatus` | `TreeAdminTreeRequest` | `TreeReshardStatus` | `ILatticeTreeAdmin.GetReshardStatusAsync` | `GetReshardStatusAsync` |
| `ResizeTree` | `TreeAdminResizeRequest` | `TreeResizeStatus` | `ILatticeTreeAdmin.ResizeTreeAsync` | `ResizeTreeAsync` |
| `UndoTreeResize` | `TreeAdminTreeRequest` | `TreeResizeStatus` | `ILatticeTreeAdmin.UndoTreeResizeAsync` | `UndoTreeResizeAsync` |
| `GetResizeStatus` | `TreeAdminTreeRequest` | `TreeResizeStatus` | `ILatticeTreeAdmin.GetResizeStatusAsync` | `GetResizeStatusAsync` |
| `SnapshotTree` | `TreeAdminSnapshotRequest` | `TreeSnapshotStatus` | `ILatticeTreeAdmin.SnapshotTreeAsync` | `SnapshotTreeAsync` |
| `GetSnapshotStatus` | `TreeAdminTreeRequest` | `TreeSnapshotStatus` | `ILatticeTreeAdmin.GetSnapshotStatusAsync` | `GetSnapshotStatusAsync` |
| `GetWalPlacement` | `TreeAdminTreeRequest` | `TreeWalPlacement` | `ILatticeTreeAdmin.GetWalPlacementAsync` | `GetWalPlacementAsync` |
| `AuditWalPlacement` | `TreeAdminTreeRequest` | `TreeWalPlacementAudit` | `ILatticeTreeAdmin.AuditWalPlacementAsync` | `AuditWalPlacementAsync` |
| `AuditOrphanedLeaves` | `TreeAdminOrphanedLeafRequest` | `TreeOrphanedLeafReport` | `ILatticeTreeAdmin.AuditOrphanedLeavesAsync`, or `ILatticeTreeAdmin.SurveyOrphanedLeavesAsync` when `Survey` is set | `AuditOrphanedLeavesAsync`, `SurveyOrphanedLeavesAsync` |
| `RepairOrphanedLeaves` | `TreeAdminOrphanedLeafRequest` | `TreeOrphanedLeafReport` | `ILatticeTreeAdmin.RepairOrphanedLeavesAsync` | `RepairOrphanedLeavesAsync` |
| `PlanWalMove` | `TreeAdminWalMovePlanRequest` | `TreeWalMovePlan` | `ILatticeTreeAdmin.PlanWalMoveAsync` | `PlanWalMoveAsync` |
| `ReclaimMovedWalSource` | `TreeAdminWalReclaimRequest` | `TreeWalMoveReceipt` | `ILatticeTreeAdmin.ReclaimMovedWalSourceAsync` | `ReclaimMovedWalSourceAsync` |
| `ListViews` | `TreeAdminViewListRequest` | `TreeViewCatalog` | `ILatticeTreeAdmin.ListViewsAsync` | `ListViewsAsync` |
| `ListViewProviders` | `TreeAdminTreeRequest` | `TreeViewProviderCatalog` | `ILatticeTreeAdmin.ListViewProvidersAsync` | `ListViewProvidersAsync` |
| `CreateView` | `TreeAdminCreateViewRequest` | `TreeViewStatus` | `ILatticeTreeAdmin.CreateViewAsync` | `CreateViewAsync` |
| `GetViewStatus` | `TreeAdminViewRequest` | `TreeViewStatus` | `ILatticeTreeAdmin.GetViewStatusAsync` | `GetViewStatusAsync` |
| `DropView` | `TreeAdminViewRequest` | `TreeAdminViewRequest` | `ILatticeTreeAdmin.DropViewAsync` | `DropViewAsync` |
| `ListTagIndexes` | `TreeAdminTagIndexListRequest` | `TreeTagIndexCatalog` | `ILatticeTreeAdmin.ListTagIndexesAsync` | `ListTagIndexesAsync` |
| `GetTagIndexStatus` | `TreeAdminTagIndexRequest` | `TreeTagIndexStatus` | `ILatticeTreeAdmin.GetTagIndexStatusAsync` | `GetTagIndexStatusAsync` |
| `TriggerShardCompaction` | `TreeAdminShardRequest` | `TreeCompactionTriggerResult` | `ILatticeTreeAdmin.TriggerShardCompactionAsync` | `TriggerShardCompactionAsync` |
| `GetHistoryRetention` | `TreeAdminTreeRequest` | `TreeHistoryRetention` | `ILatticeTreeAdmin.GetHistoryRetentionAsync` | `GetHistoryRetentionAsync` |
| `SetHistoryRetention` | `TreeAdminSetRetentionRequest` | `TreeHistoryRetention` | `ILatticeTreeAdmin.SetHistoryRetentionAsync` | `SetHistoryRetentionAsync` |
| `StartViewRebuild` | `TreeAdminViewRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartViewRebuildAsync` | `StartViewRebuildAsync` |
| `StartViewReconcile` | `TreeAdminViewRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartViewReconcileAsync` | `StartViewReconcileAsync` |
| `StartTagIndexReconcile` | `TreeAdminTagIndexRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartTagIndexReconcileAsync` | `StartTagIndexReconcileAsync` |
| `StartWalMove` | `TreeAdminWalMoveExecuteRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartWalMoveAsync` | `StartWalMoveAsync` |
| `StartOrphanedLeavesAudit` | `TreeAdminOrphanedLeafRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartOrphanedLeavesAuditAsync` | `StartOrphanedLeavesAuditAsync` |
| `StartOrphanedLeavesRepair` | `TreeAdminOrphanedLeafRequest` | `LatticeOperationHandle` | `ILatticeTreeAdminOperations.StartOrphanedLeavesRepairAsync` | `StartOrphanedLeavesRepairAsync` |
| `GetTreeAdminOperationStatus` | `TreeAdminOperationRequest` | `TreeAdminOperationStatusResponse` | `ILatticeTreeAdminOperations.GetOperationStatusAsync` | `GetTreeAdminOperationStatusAsync` |
| `ListTreeAdminOperations` | `LatticeOperationListRequest` | `LatticeOperationPage` | `ILatticeTreeAdminOperations.ListOperationsAsync` | `ListTreeAdminOperationsAsync` |
| `CancelTreeAdminOperation` | `TreeAdminOperationRequest` | `TreeAdminOperationStatusResponse` | `ILatticeTreeAdminOperations.CancelOperationAsync` | `CancelTreeAdminOperationAsync` |
| `StartStorageUsageRefresh` | `TreeAdminStorageUsageRefreshRequest` | `LatticeOperationHandle` | `ILatticeStorageUsageOperations.StartStorageUsageRefreshAsync` | `StartStorageUsageRefreshAsync` |
| `GetStorageUsageRefreshStatus` | `TreeAdminStorageUsageOperationRequest` | `TreeAdminStorageUsageOperationStatusResponse` | `ILatticeStorageUsageOperations.GetOperationStatusAsync` | `GetStorageUsageRefreshStatusAsync` |
| `ListStorageUsageRefreshes` | `LatticeOperationListRequest` | `LatticeOperationPage` | `ILatticeStorageUsageOperations.ListOperationsAsync` | `ListStorageUsageRefreshesAsync` |
| `CancelStorageUsageRefresh` | `TreeAdminStorageUsageOperationRequest` | `TreeAdminStorageUsageOperationStatusResponse` | `ILatticeStorageUsageOperations.CancelOperationAsync` | `CancelStorageUsageRefreshAsync` |
| `GetWalReclamation` | `TreeAdminTreeRequest` | `TreeWalReclamationReport` | `ILatticeWalReclamation.GetWalReclamationAsync` | `GetWalReclamationAsync` |

Mapping notes:

- `AuditOrphanedLeaves` calls `SurveyOrphanedLeavesAsync` when the request's `Survey` flag is set and `AuditOrphanedLeavesAsync` otherwise.
- `RevertTreeRestore` takes the facade's own `TreeRestoreResult` as its request and echoes it back, and `DropView` echoes its `TreeAdminViewRequest`, because both facade methods return a bare `Task`. `RestoreTreeSet` wraps the facade's member results in `TreeRestoreSetResult`.
- `ListTreeAdminOperations` and `ListStorageUsageRefreshes` carry the shared `LatticeOperationListRequest` directly; the status and cancel RPCs wrap the facade's nullable `LatticeOperationStatus` in `TreeAdminOperationStatusResponse` or `TreeAdminStorageUsageOperationStatusResponse`.
- The accept-then-poll operation RPCs, the storage-usage refresh RPCs and `GetWalReclamation` call optional facades. A host that registers no `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations` or `ILatticeWalReclamation` answers the matching RPCs `Unimplemented`; `AddLatticeTreeAdminApi` registers all three.
- `GetAuthScheme` calls no facade. It returns the registered `ILatticeTreeAdminApiAuthSchemeSource` advertisement and is the one RPC the authorization interceptor exempts.

The request records' members and the RPCs that use each one are listed under [Wire message records](README.md#wire-message-records) in the package README.

## Authorization seams

- `ILatticeTreeAdminApiAuthorizer` - the per-call transport gate, with the shipped `DenyTreeAdminApiAuthorizer` (the registered default) and `AllowAllTreeAdminApiAuthorizer` (opt-in).
- `LatticeTreeAdminApiAuthorizationContext` - the call handed to the authorizer: `Call`, `Operation` and `TargetId`.
- `LatticeTreeAdminApiOperation` - names 20 RPCs individually. Every other RPC, and any unrecognised method, is presented as `Unknown`.
- `ILatticeTreeAdminApiCredentialBridge` - lifts the caller's credential from the call.
- `ILatticeTreeAdminApiAuthSchemeSource` - supplies the `GetAuthScheme` advertisement, built from `AuthSchemeDescriptor` entries inside an `AuthSchemeAdvertisement`.

The README's [Authorization surface](README.md#authorization-surface) explains which RPCs map to which operation and how `TargetId` is decoded; [Architecture](architecture.md#authorization) describes how the seams combine with the facade's own gate.

## Wire compatibility

The request records and the auth-scheme records are `[GenerateSerializer]` records whose stable aliases carry the `oitg.` prefix; the constants live in `GrpcTreeAdminTypeAliases`. The facade's result records keep their own `oit.` aliases from `Orleans.Lattice.Api.Abstractions`, and the shared operation records keep their `oio.` aliases. Never rename an alias or renumber an `[Id]`: both are the wire format.

## Related

- [Package README](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.TreeAdmin` public API](../lattice.api.treeadmin/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TreeAdmin.Grpc.AllowAllTreeAdminApiAuthorizer`

[Source](../../src/lattice.api.treeadmin.grpc/Security/ILatticeTreeAdminApiAuthorizer.cs) (line 54).

`public sealed class AllowAllTreeAdminApiAuthorizer : ILatticeTreeAdminApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.AuthSchemeAdvertisement`

[Source](../../src/lattice.api.treeadmin.grpc/Model/AuthSchemeAdvertisement.cs) (line 12).

`public sealed record AuthSchemeAdvertisement`

- `public IReadOnlyList<AuthSchemeDescriptor> Schemes { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.AuthSchemeAdvertisementRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/AuthSchemeAdvertisementRequest.cs) (line 11).

`public sealed record AuthSchemeAdvertisementRequest`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.AuthSchemeDescriptor`

[Source](../../src/lattice.api.treeadmin.grpc/Model/AuthSchemeDescriptor.cs) (line 20).

`public sealed record AuthSchemeDescriptor`

- `public required string SchemeId { get; init; }`
- `public string DisplayName { get; init; }`
- `public IReadOnlyDictionary<string, string> Parameters { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.DenyTreeAdminApiAuthorizer`

[Source](../../src/lattice.api.treeadmin.grpc/Security/ILatticeTreeAdminApiAuthorizer.cs) (line 40).

`public sealed class DenyTreeAdminApiAuthorizer : ILatticeTreeAdminApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.GrpcTreeAdminTypeAliases`

[Source](../../src/lattice.api.treeadmin.grpc/Model/GrpcTreeAdminTypeAliases.cs) (line 16).

`public static class GrpcTreeAdminTypeAliases`

- `public const string AliasPrefix`
- `public const string TreeAdminTreeRequest`
- `public const string TreeAdminShardRequest`
- `public const string TreeAdminOrphanedLeafRequest`
- `public const string TreeAdminDiagnosticsRequest`
- `public const string TreeAdminStorageUsageRequest`
- `public const string AuthSchemeAdvertisementRequest`
- `public const string AuthSchemeDescriptor`
- `public const string AuthSchemeAdvertisement`
- `public const string TreeAdminCreateRequest`
- `public const string TreeAdminSetAliasRequest`
- `public const string TreeAdminSetConfigRequest`
- `public const string TreeAdminPurgeRequest`
- `public const string TreeAdminBulkLoadSessionRequest`
- `public const string TreeAdminBulkLoadAppendRequest`
- `public const string TreeAdminRestoreRequest`
- `public const string TreeAdminRestoreSetRequest`
- `public const string TreeAdminReshardRequest`
- `public const string TreeAdminResizeRequest`
- `public const string TreeAdminSnapshotRequest`
- `public const string TreeAdminWalMovePlanRequest`
- `public const string TreeAdminWalMoveExecuteRequest`
- `public const string TreeAdminWalReclaimRequest`
- `public const string TreeAdminViewRequest`
- `public const string TreeAdminViewListRequest`
- `public const string TreeAdminCreateViewRequest`
- `public const string TreeAdminTagIndexRequest`
- `public const string TreeAdminTagIndexListRequest`
- `public const string TreeAdminSetRetentionRequest`
- `public const string TreeAdminOperationRequest`
- `public const string TreeAdminOperationStatusResponse`
- `public const string TreeAdminStorageUsageRefreshRequest`
- `public const string TreeAdminStorageUsageOperationRequest`
- `public const string TreeAdminStorageUsageOperationStatusResponse`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.ILatticeTreeAdminApiAuthSchemeSource`

[Source](../../src/lattice.api.treeadmin.grpc/Security/ILatticeTreeAdminApiAuthSchemeSource.cs) (line 15).

`public interface ILatticeTreeAdminApiAuthSchemeSource`

- `AuthSchemeAdvertisement GetAdvertisement()`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.ILatticeTreeAdminApiAuthorizer`

[Source](../../src/lattice.api.treeadmin.grpc/Security/ILatticeTreeAdminApiAuthorizer.cs) (line 16).

`public interface ILatticeTreeAdminApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.ILatticeTreeAdminApiCredentialBridge`

[Source](../../src/lattice.api.treeadmin.grpc/ILatticeTreeAdminApiCredentialBridge.cs) (line 37).

`public interface ILatticeTreeAdminApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiAuthorizationContext`

[Source](../../src/lattice.api.treeadmin.grpc/Security/LatticeTreeAdminApiAuthorizationContext.cs) (line 91).

`public readonly struct LatticeTreeAdminApiAuthorizationContext`

- `public LatticeTreeAdminApiAuthorizationContext(ServerCallContext call, LatticeTreeAdminApiOperation operation, string? targetId)`
- `public ServerCallContext Call { get; }`
- `public LatticeTreeAdminApiOperation Operation { get; }`
- `public string? TargetId { get; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient`

[Source](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcClient.cs) (line 30).

`public sealed class LatticeTreeAdminApiGrpcClient`

- `public static LatticeTreeAdminApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)`
- `public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)`
- `public Task<TreeHotnessReport> GetShardHotnessAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(string treeId, bool deep = false, CancellationToken cancellationToken = default)`
- `public Task<ShardMapInspection> InspectShardMapAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<ShardProjectionDigestReport> GetProjectionDigestAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)`
- `public Task<TreeStatsReport> GetTreeStatsAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<ClusterStorageUsageSummary> GetStorageUsageAsync(bool deep = false, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(string? operationId = null, CancellationToken cancellationToken = default)`
- `public async Task<LatticeOperationStatus?> GetStorageUsageRefreshStatusAsync(string operationId, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationPage> ListStorageUsageRefreshesAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)`
- `public async Task<LatticeOperationStatus?> CancelStorageUsageRefreshAsync(string operationId, CancellationToken cancellationToken = default)`
- `public Task<TreeCreationResult> CreateTreeAsync(string treeId, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)`
- `public Task<TreeExistenceResult> CheckTreeExistsAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeAliasResolution> SetTreeAliasAsync(string treeId, string physicalTreeId, CancellationToken cancellationToken = default)`
- `public Task<TreeAliasResolution> ResolveTreeAliasAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeConfigurationReport> GetTreeConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeConfigurationReport> SetTreeConfigAsync(string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)`
- `public Task<TreeShardMapView> GetShardMapAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeDeletionStatus> DeleteTreeAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeDeletionStatus> RecoverTreeAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeDeletionStatus> PurgeTreeAsync(string treeId, bool confirm, CancellationToken cancellationToken = default)`
- `public Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeBulkLoadSession> BeginBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)`
- `public Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(string treeId, string operationId, long chunkIndex, IReadOnlyList<DataEntry> entries, CancellationToken cancellationToken = default)`
- `public Task<TreeBulkLoadResult> CommitBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)`
- `public Task<TreeRestoreResult> RestoreTreeAsync(string treeId, string backupId, string? operationId = null, CancellationToken cancellationToken = default)`
- `public async Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(string setId, CancellationToken cancellationToken = default)`
- `public async Task RevertTreeRestoreAsync(TreeRestoreResult restore, CancellationToken cancellationToken = default)`
- `public Task<TreeReshardStatus> ReshardTreeAsync(string treeId, int targetShardCount, CancellationToken cancellationToken = default)`
- `public Task<TreeReshardStatus> GetReshardStatusAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeResizeStatus> ResizeTreeAsync(string treeId, int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)`
- `public Task<TreeResizeStatus> UndoTreeResizeAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeResizeStatus> GetResizeStatusAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeSnapshotStatus> SnapshotTreeAsync(string treeId, string destinationTreeId, TreeSnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)`
- `public Task<TreeSnapshotStatus> GetSnapshotStatusAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeWalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeWalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeWalReclamationReport> GetWalReclamationAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeOrphanedLeafReport> AuditOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `public Task<TreeOrphanedLeafReport> SurveyOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `public Task<TreeOrphanedLeafReport> RepairOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)`
- `public Task<TreeWalMovePlan> PlanWalMoveAsync(string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)`
- `public Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)`
- `public Task<TreeViewCatalog> ListViewsAsync(CancellationToken cancellationToken = default)`
- `public Task<TreeViewProviderCatalog> ListViewProvidersAsync(string sourceTreeId, CancellationToken cancellationToken = default)`
- `public Task<TreeViewStatus> CreateViewAsync(string viewName, string sourceTreeId, string providerKey, byte[] payload, CancellationToken cancellationToken = default)`
- `public Task<TreeViewStatus> GetViewStatusAsync(string viewName, CancellationToken cancellationToken = default)`
- `public async Task DropViewAsync(string viewName, CancellationToken cancellationToken = default)`
- `public Task<TreeTagIndexCatalog> ListTagIndexesAsync(CancellationToken cancellationToken = default)`
- `public Task<TreeTagIndexStatus> GetTagIndexStatusAsync(string indexName, CancellationToken cancellationToken = default)`
- `public Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)`
- `public Task<TreeHistoryRetention> GetHistoryRetentionAsync(string treeId, CancellationToken cancellationToken = default)`
- `public Task<TreeHistoryRetention> SetHistoryRetentionAsync(string treeId, TreeHistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartViewRebuildAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartViewReconcileAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartTagIndexReconcileAsync(string indexName, string? operationId = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartWalMoveAsync(string treeId, int partition, string targetProviderKey, TreeWalMoveOptions? options = null, string? operationId = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)`
- `public async Task<LatticeOperationStatus?> GetTreeAdminOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)`
- `public Task<LatticeOperationPage> ListTreeAdminOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)`
- `public async Task<LatticeOperationStatus?> CancelTreeAdminOperationAsync(string operationId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcOptions`

[Source](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcOptions.cs) (line 7).

`public sealed class LatticeTreeAdminApiGrpcOptions`

- `public bool RequireAuthorization { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string ActiveTenantHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes { get; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.treeadmin.grpc/LatticeTreeAdminApiGrpcServiceCollectionExtensions.cs) (line 34).

`public static class LatticeTreeAdminApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeTreeAdminApiGrpc(this IServiceCollection services, Action<LatticeTreeAdminApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeTreeAdminApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiOperation`

[Source](../../src/lattice.api.treeadmin.grpc/Security/LatticeTreeAdminApiAuthorizationContext.cs) (line 12).

`public enum LatticeTreeAdminApiOperation`

- `ProbeCapabilities = 0`
- `GetShardHotness = 1`
- `GetDiagnostics = 2`
- `InspectShardMap = 3`
- `GetProjectionDigest = 4`
- `GetTreeStats = 5`
- `GetStorageUsage = 6`
- `CreateTree = 7`
- `CheckTreeExists = 8`
- `SetTreeAlias = 9`
- `ResolveTreeAlias = 10`
- `GetTreeConfig = 11`
- `SetTreeConfig = 12`
- `GetShardMap = 13`
- `Unknown = 14`
- `CreateView = 15`
- `StartStorageUsageRefresh = 16`
- `GetStorageUsageRefreshStatus = 17`
- `ListStorageUsageRefreshes = 18`
- `CancelStorageUsageRefresh = 19`
- `GetWalReclamation = 20`
- `ListViewProviders = 21`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminBulkLoadAppendRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminBulkLoadAppendRequest.cs) (line 17).

`public sealed record TreeAdminBulkLoadAppendRequest`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`
- `public long ChunkIndex { get; init; }`
- `public IReadOnlyList<DataEntry> Entries { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminBulkLoadSessionRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminBulkLoadSessionRequest.cs) (line 13).

`public sealed record TreeAdminBulkLoadSessionRequest`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminCreateRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminCreateRequest.cs) (line 13).

`public sealed record TreeAdminCreateRequest`

- `public required string TreeId { get; init; }`
- `public int? ShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminCreateViewRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminCreateViewRequest.cs) (line 10).

`public sealed record TreeAdminCreateViewRequest`

- `public required string ViewName { get; init; }`
- `public required string SourceTreeId { get; init; }`
- `public required string ProviderKey { get; init; }`
- `public byte[] Payload { get; init; }`
- `public bool Equals(TreeAdminCreateViewRequest? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminDiagnosticsRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminDiagnosticsRequest.cs) (line 12).

`public sealed record TreeAdminDiagnosticsRequest`

- `public required string TreeId { get; init; }`
- `public bool Deep { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminOperationRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminOperationRequest.cs) (line 10).

`public sealed record TreeAdminOperationRequest`

- `public required string OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminOperationStatusResponse`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminOperationStatusResponse.cs) (line 13).

`public sealed record TreeAdminOperationStatusResponse`

- `public LatticeOperationStatus? Status { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminOrphanedLeafRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminOrphanedLeafRequest.cs) (line 16).

`public sealed record TreeAdminOrphanedLeafRequest`

- `public required string TreeId { get; init; }`
- `public string? ResumeFrom { get; init; }`
- `public bool Survey { get; init; }`
- `public string? TrackingOperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminPurgeRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminPurgeRequest.cs) (line 13).

`public sealed record TreeAdminPurgeRequest`

- `public required string TreeId { get; init; }`
- `public bool Confirm { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminReshardRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminReshardRequest.cs) (line 12).

`public sealed record TreeAdminReshardRequest`

- `public required string TreeId { get; init; }`
- `public int TargetShardCount { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminResizeRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminResizeRequest.cs) (line 13).

`public sealed record TreeAdminResizeRequest`

- `public required string TreeId { get; init; }`
- `public int NewMaxLeafKeys { get; init; }`
- `public int NewMaxInternalChildren { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminRestoreRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminRestoreRequest.cs) (line 12).

`public sealed record TreeAdminRestoreRequest`

- `public required string TreeId { get; init; }`
- `public required string BackupId { get; init; }`
- `public string? OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminRestoreSetRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminRestoreSetRequest.cs) (line 10).

`public sealed record TreeAdminRestoreSetRequest`

- `public required string SetId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminSetAliasRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminSetAliasRequest.cs) (line 11).

`public sealed record TreeAdminSetAliasRequest`

- `public required string TreeId { get; init; }`
- `public required string PhysicalTreeId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminSetConfigRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminSetConfigRequest.cs) (line 12).

`public sealed record TreeAdminSetConfigRequest`

- `public required string TreeId { get; init; }`
- `public required TreeConfigurationUpdate Update { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminSetRetentionRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminSetRetentionRequest.cs) (line 15).

`public sealed record TreeAdminSetRetentionRequest`

- `public required string TreeId { get; init; }`
- `public TreeHistoryRetentionMode? Mode { get; init; }`
- `public TimeSpan? Window { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminShardRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminShardRequest.cs) (line 11).

`public sealed record TreeAdminShardRequest`

- `public required string TreeId { get; init; }`
- `public int ShardIndex { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminSnapshotRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminSnapshotRequest.cs) (line 13).

`public sealed record TreeAdminSnapshotRequest`

- `public required string TreeId { get; init; }`
- `public required string DestinationTreeId { get; init; }`
- `public TreeSnapshotMode Mode { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminStorageUsageOperationRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminStorageUsageOperationRequest.cs) (line 10).

`public sealed record TreeAdminStorageUsageOperationRequest`

- `public required string OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminStorageUsageOperationStatusResponse`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminStorageUsageOperationStatusResponse.cs) (line 13).

`public sealed record TreeAdminStorageUsageOperationStatusResponse`

- `public LatticeOperationStatus? Status { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminStorageUsageRefreshRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminStorageUsageRefreshRequest.cs) (line 11).

`public sealed record TreeAdminStorageUsageRefreshRequest`

- `public string? OperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminStorageUsageRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminStorageUsageRequest.cs) (line 12).

`public sealed record TreeAdminStorageUsageRequest`

- `public bool Deep { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminTagIndexListRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminTagIndexListRequest.cs) (line 11).

`public sealed record TreeAdminTagIndexListRequest`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminTagIndexRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminTagIndexRequest.cs) (line 14).

`public sealed record TreeAdminTagIndexRequest`

- `public required string IndexName { get; init; }`
- `public string? TrackingOperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminTreeRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminTreeRequest.cs) (line 12).

`public sealed record TreeAdminTreeRequest`

- `public required string TreeId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminViewListRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminViewListRequest.cs) (line 12).

`public sealed record TreeAdminViewListRequest`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminViewRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminViewRequest.cs) (line 14).

`public sealed record TreeAdminViewRequest`

- `public required string ViewName { get; init; }`
- `public string? TrackingOperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminWalMoveExecuteRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminWalMoveExecuteRequest.cs) (line 12).

`public sealed record TreeAdminWalMoveExecuteRequest`

- `public required string TreeId { get; init; }`
- `public int Partition { get; init; }`
- `public required string TargetProviderKey { get; init; }`
- `public TreeWalMoveOptions? Options { get; init; }`
- `public string? TrackingOperationId { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminWalMovePlanRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminWalMovePlanRequest.cs) (line 12).

`public sealed record TreeAdminWalMovePlanRequest`

- `public required string TreeId { get; init; }`
- `public int Partition { get; init; }`
- `public required string TargetProviderKey { get; init; }`

### `Orleans.Lattice.Api.TreeAdmin.Grpc.TreeAdminWalReclaimRequest`

[Source](../../src/lattice.api.treeadmin.grpc/Model/TreeAdminWalReclaimRequest.cs) (line 13).

`public sealed record TreeAdminWalReclaimRequest`

- `public required string TreeId { get; init; }`
- `public int Partition { get; init; }`
- `public required string SourceProviderKey { get; init; }`
