# Orleans.Lattice.Api.TreeAdmin.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.TreeAdmin](../lattice.api.treeadmin/README.md) - projects the whole-tree administration control facade onto a gRPC service and a public typed client, marshalled with the Orleans binary serializer over code-first request and response records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.TreeAdmin.Grpc` is the remote transport for the cluster's tree-administration control plane. A host references it when a dashboard, a CLI, or an operations tool needs to create, inspect, reconfigure, reshard, resize, snapshot, restore, bulk-load, or retire whole trees, manage their materialised views and tag indexes, and audit or move their WAL placement over the network rather than in-process.

It provides:

- **A code-first gRPC service.** One unary RPC per facade operation - the capability probe, diagnostics and inspection reads, tree lifecycle, alias and configuration, deletion and recovery, bulk load, restore, reshard, resize, snapshot, WAL placement and movement, orphaned-leaf audit / survey / repair, view and tag-index management, shard compaction, history retention, and the unauthenticated auth-scheme discovery call - bound from C# definitions rather than a `.proto`. The orphaned-leaf survey is the one operation without an RPC of its own: `SurveyOrphanedLeavesAsync` rides the `AuditOrphanedLeaves` RPC with the request's `Survey` flag set, so the service binds 51 unary RPCs - 50 facade operations plus `GetAuthScheme`.
- **A public typed client.** `LatticeTreeAdminApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Every wire message is a `[GenerateSerializer]` record serialized with the Orleans binary serializer. Each RPC wraps the facade operation's arguments in this package's own request record (for example `TreeAdminCreateRequest`), and its response reuses the facade's result record from `Orleans.Lattice.Api.Abstractions`. The exceptions follow the facade's shape: `RevertTreeRestore` sends the facade's own `TreeRestoreResult` as its request and echoes it back, `DropView` echoes its `TreeAdminViewRequest` (both facade verbs return a bare `Task`), `RestoreTreeSet` wraps the member results in the facade's `TreeRestoreSetResult`, and `GetAuthScheme` uses this package's own `AuthSchemeAdvertisementRequest` / `AuthSchemeAdvertisement` records. The gRPC contract therefore adapts over the facade DTOs rather than being wire-identical to them, and client and server stay in lock-step by construction.
- **Layered authorization.** A per-call `ILatticeTreeAdminApiAuthorizer` seam gates every protected RPC at the edge, and the composed facade re-authorizes the resolved caller through the core access gate. The edge gate defaults to deny (`DenyTreeAdminApiAuthorizer`, with `RequireAuthorization` defaulting to `true`). The facade gate denies by default only when `Orleans.Lattice.Auth` is registered, whose `LatticeAuthOptions.DefaultEffect` defaults to `Deny`; without that add-on the core no-op access gate allows every call, so the edge gate is the only barrier.

The package has no external broker and no `.proto` file to maintain.

## Core properties

- **Public client, internal service.** Callers consume `LatticeTreeAdminApiGrpcClient`; the service, marshallers, method definitions, and interceptor are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`. Build a client with `LatticeTreeAdminApiGrpcClient.Create(callInvoker, serializerProvider)`, passing a service provider that has Orleans serialization registered (`AddSerializer()`).
- **Fail-closed.** Unconfigured, the binding denies every protected call: the default `DenyTreeAdminApiAuthorizer` is registered via `TryAdd` and `LatticeTreeAdminApiGrpcOptions.RequireAuthorization` defaults to `true`, so a host must register a real `ILatticeTreeAdminApiAuthorizer` (or turn enforcement off behind an outer boundary) before any protected call succeeds. Whole-tree administration creates, reconfigures, and destroys trees, so the closed default is deliberate.
- **Composed authorization.** The transport authorizer decides whether a call may run at all; the credential the identity bridge lifts from the request header then feeds the composed facade's own fail-closed authorization. Neither replaces the other.
- **Discoverable sign-in.** An unauthenticated `GetAuthScheme` RPC lets a client discover how to authenticate before it holds a credential.

## Service and RPCs

The gRPC service name is `orleans.lattice.api.treeadmin`. Every RPC is unary. The client method that drives each RPC:

| Area | Client methods |
|---|---|
| Capability and discovery | `ProbeCapabilitiesAsync`, `GetAuthSchemeAsync` (unauthenticated) |
| Inspection and diagnostics | `GetShardHotnessAsync`, `GetDiagnosticsAsync`, `InspectShardMapAsync`, `GetProjectionDigestAsync`, `GetTreeStatsAsync`, `GetStorageUsageAsync`, `GetShardMapAsync` |
| Lifecycle and configuration | `CreateTreeAsync`, `CheckTreeExistsAsync`, `SetTreeAliasAsync`, `ResolveTreeAliasAsync`, `GetTreeConfigAsync`, `SetTreeConfigAsync` |
| Deletion and recovery | `DeleteTreeAsync`, `RecoverTreeAsync`, `PurgeTreeAsync`, `GetTreeDeletionStatusAsync` |
| Bulk load | `BeginBulkLoadAsync`, `AppendBulkLoadAsync`, `CommitBulkLoadAsync` |
| Restore | `RestoreTreeAsync`, `RestoreTreeSetAsync`, `RevertTreeRestoreAsync` |
| Reshard and resize | `ReshardTreeAsync`, `GetReshardStatusAsync`, `ResizeTreeAsync`, `UndoTreeResizeAsync`, `GetResizeStatusAsync` |
| Snapshot | `SnapshotTreeAsync`, `GetSnapshotStatusAsync` |
| WAL placement | `GetWalPlacementAsync`, `AuditWalPlacementAsync`, `PlanWalMoveAsync`, `ExecuteWalMoveAsync`, `ReclaimMovedWalSourceAsync` |
| Orphaned leaves | `AuditOrphanedLeavesAsync`, `SurveyOrphanedLeavesAsync`, `RepairOrphanedLeavesAsync` |
| Views | `ListViewsAsync`, `CreateViewAsync`, `GetViewStatusAsync`, `RebuildViewAsync`, `ReconcileViewAsync`, `DropViewAsync` |
| Tag indexes | `ListTagIndexesAsync`, `GetTagIndexStatusAsync`, `ReconcileTagIndexAsync` |
| Compaction and retention | `TriggerShardCompactionAsync`, `GetHistoryRetentionAsync`, `SetHistoryRetentionAsync` |

### Client method signatures

| Method | Signature |
|---|---|
| `ProbeCapabilitiesAsync` | `Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetAuthSchemeAsync` | `Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)` |
| `GetShardHotnessAsync` | `Task<TreeHotnessReport> GetShardHotnessAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetDiagnosticsAsync` | `Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(string treeId, bool deep = false, CancellationToken cancellationToken = default)` |
| `InspectShardMapAsync` | `Task<ShardMapInspection> InspectShardMapAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetProjectionDigestAsync` | `Task<ShardProjectionDigestReport> GetProjectionDigestAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)` |
| `GetTreeStatsAsync` | `Task<TreeStatsReport> GetTreeStatsAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetStorageUsageAsync` | `Task<ClusterStorageUsageSummary> GetStorageUsageAsync(bool deep = false, CancellationToken cancellationToken = default)` |
| `CreateTreeAsync` | `Task<TreeCreationResult> CreateTreeAsync(string treeId, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)` |
| `CheckTreeExistsAsync` | `Task<TreeExistenceResult> CheckTreeExistsAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SetTreeAliasAsync` | `Task<TreeAliasResolution> SetTreeAliasAsync(string treeId, string physicalTreeId, CancellationToken cancellationToken = default)` |
| `ResolveTreeAliasAsync` | `Task<TreeAliasResolution> ResolveTreeAliasAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetTreeConfigAsync` | `Task<TreeConfigurationReport> GetTreeConfigAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SetTreeConfigAsync` | `Task<TreeConfigurationReport> SetTreeConfigAsync(string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)` |
| `GetShardMapAsync` | `Task<TreeShardMapView> GetShardMapAsync(string treeId, CancellationToken cancellationToken = default)` |
| `DeleteTreeAsync` | `Task<TreeDeletionStatus> DeleteTreeAsync(string treeId, CancellationToken cancellationToken = default)` |
| `RecoverTreeAsync` | `Task<TreeDeletionStatus> RecoverTreeAsync(string treeId, CancellationToken cancellationToken = default)` |
| `PurgeTreeAsync` | `Task<TreeDeletionStatus> PurgeTreeAsync(string treeId, bool confirm, CancellationToken cancellationToken = default)` |
| `GetTreeDeletionStatusAsync` | `Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `BeginBulkLoadAsync` | `Task<TreeBulkLoadSession> BeginBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)` |
| `AppendBulkLoadAsync` | `Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(string treeId, string operationId, long chunkIndex, IReadOnlyList<DataEntry> entries, CancellationToken cancellationToken = default)` |
| `CommitBulkLoadAsync` | `Task<TreeBulkLoadResult> CommitBulkLoadAsync(string treeId, string operationId, CancellationToken cancellationToken = default)` |
| `RestoreTreeAsync` | `Task<TreeRestoreResult> RestoreTreeAsync(string treeId, string backupId, string? operationId = null, CancellationToken cancellationToken = default)` |
| `RestoreTreeSetAsync` | `Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(string setId, CancellationToken cancellationToken = default)` |
| `RevertTreeRestoreAsync` | `Task RevertTreeRestoreAsync(TreeRestoreResult restore, CancellationToken cancellationToken = default)` |
| `ReshardTreeAsync` | `Task<TreeReshardStatus> ReshardTreeAsync(string treeId, int targetShardCount, CancellationToken cancellationToken = default)` |
| `GetReshardStatusAsync` | `Task<TreeReshardStatus> GetReshardStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ResizeTreeAsync` | `Task<TreeResizeStatus> ResizeTreeAsync(string treeId, int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)` |
| `UndoTreeResizeAsync` | `Task<TreeResizeStatus> UndoTreeResizeAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetResizeStatusAsync` | `Task<TreeResizeStatus> GetResizeStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SnapshotTreeAsync` | `Task<TreeSnapshotStatus> SnapshotTreeAsync(string treeId, string destinationTreeId, TreeSnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)` |
| `GetSnapshotStatusAsync` | `Task<TreeSnapshotStatus> GetSnapshotStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetWalPlacementAsync` | `Task<TreeWalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)` |
| `AuditWalPlacementAsync` | `Task<TreeWalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default)` |
| `PlanWalMoveAsync` | `Task<TreeWalMovePlan> PlanWalMoveAsync(string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default)` |
| `ExecuteWalMoveAsync` | `Task<TreeWalMoveReceipt> ExecuteWalMoveAsync(string treeId, int partition, string targetProviderKey, TreeWalMoveOptions? options = null, CancellationToken cancellationToken = default)` |
| `ReclaimMovedWalSourceAsync` | `Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default)` |
| `AuditOrphanedLeavesAsync` | `Task<TreeOrphanedLeafReport> AuditOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)` |
| `SurveyOrphanedLeavesAsync` | `Task<TreeOrphanedLeafReport> SurveyOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)` |
| `RepairOrphanedLeavesAsync` | `Task<TreeOrphanedLeafReport> RepairOrphanedLeavesAsync(string treeId, string? resumeFrom = null, CancellationToken cancellationToken = default)` |
| `ListViewsAsync` | `Task<TreeViewCatalog> ListViewsAsync(CancellationToken cancellationToken = default)` |
| `CreateViewAsync` | `Task<TreeViewStatus> CreateViewAsync(string viewName, string sourceTreeId, string providerKey, byte[] payload, CancellationToken cancellationToken = default)` |
| `GetViewStatusAsync` | `Task<TreeViewStatus> GetViewStatusAsync(string viewName, CancellationToken cancellationToken = default)` |
| `RebuildViewAsync` | `Task<TreeViewStatus> RebuildViewAsync(string viewName, CancellationToken cancellationToken = default)` |
| `ReconcileViewAsync` | `Task<TreeViewReconcileResult> ReconcileViewAsync(string viewName, CancellationToken cancellationToken = default)` |
| `DropViewAsync` | `Task DropViewAsync(string viewName, CancellationToken cancellationToken = default)` |
| `ListTagIndexesAsync` | `Task<TreeTagIndexCatalog> ListTagIndexesAsync(CancellationToken cancellationToken = default)` |
| `GetTagIndexStatusAsync` | `Task<TreeTagIndexStatus> GetTagIndexStatusAsync(string indexName, CancellationToken cancellationToken = default)` |
| `ReconcileTagIndexAsync` | `Task<TreeTagReconcileReport> ReconcileTagIndexAsync(string indexName, CancellationToken cancellationToken = default)` |
| `TriggerShardCompactionAsync` | `Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)` |
| `GetHistoryRetentionAsync` | `Task<TreeHistoryRetention> GetHistoryRetentionAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SetHistoryRetentionAsync` | `Task<TreeHistoryRetention> SetHistoryRetentionAsync(string treeId, TreeHistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)` |

`DropViewAsync` and `RevertTreeRestoreAsync` return a bare `Task`; `GetAuthSchemeAsync` is the one unauthenticated call and returns the endpoint's advertised auth schemes. Every other method returns the facade result record.

### Wire message records

The request records this package defines, each an Orleans-serialized `[GenerateSerializer]` record whose stable alias carries the `oitg.` prefix (the alias constants live in the public `GrpcTreeAdminTypeAliases` class). Properties marked `required` must be set by the caller. Responses are the facade result records from `Orleans.Lattice.Api.Abstractions`, except where noted above.

| Record | Members | Used by |
|---|---|---|
| `TreeAdminTreeRequest` | `required string TreeId` | `ProbeCapabilities`, `GetShardHotness`, `InspectShardMap`, `GetTreeStats`, `CheckTreeExists`, `ResolveTreeAlias`, `GetTreeConfig`, `GetShardMap`, `DeleteTree`, `RecoverTree`, `GetTreeDeletionStatus`, `GetReshardStatus`, `UndoTreeResize`, `GetResizeStatus`, `GetSnapshotStatus`, `GetWalPlacement`, `AuditWalPlacement`, `GetHistoryRetention` |
| `TreeAdminDiagnosticsRequest` | `required string TreeId`, `bool Deep` | `GetDiagnostics` |
| `TreeAdminShardRequest` | `required string TreeId`, `int ShardIndex` | `GetProjectionDigest`, `TriggerShardCompaction` |
| `TreeAdminStorageUsageRequest` | `bool Deep` | `GetStorageUsage` |
| `TreeAdminCreateRequest` | `required string TreeId`, `int? ShardCount`, `int? MaxLeafKeys`, `int? MaxInternalChildren` | `CreateTree` |
| `TreeAdminSetAliasRequest` | `required string TreeId`, `required string PhysicalTreeId` | `SetTreeAlias` |
| `TreeAdminSetConfigRequest` | `required string TreeId`, `required TreeConfigurationUpdate Update` | `SetTreeConfig` |
| `TreeAdminPurgeRequest` | `required string TreeId`, `bool Confirm` | `PurgeTree` |
| `TreeAdminBulkLoadSessionRequest` | `required string TreeId`, `required string OperationId` | `BeginBulkLoad`, `CommitBulkLoad` |
| `TreeAdminBulkLoadAppendRequest` | `required string TreeId`, `required string OperationId`, `long ChunkIndex`, `IReadOnlyList<DataEntry> Entries` | `AppendBulkLoad` |
| `TreeAdminRestoreRequest` | `required string TreeId`, `required string BackupId`, `string? OperationId` | `RestoreTree` |
| `TreeAdminRestoreSetRequest` | `required string SetId` | `RestoreTreeSet` |
| `TreeAdminReshardRequest` | `required string TreeId`, `int TargetShardCount` | `ReshardTree` |
| `TreeAdminResizeRequest` | `required string TreeId`, `int NewMaxLeafKeys`, `int NewMaxInternalChildren` | `ResizeTree` |
| `TreeAdminSnapshotRequest` | `required string TreeId`, `required string DestinationTreeId`, `TreeSnapshotMode Mode`, `int? MaxLeafKeys`, `int? MaxInternalChildren` | `SnapshotTree` |
| `TreeAdminOrphanedLeafRequest` | `required string TreeId`, `string? ResumeFrom`, `bool Survey` | `AuditOrphanedLeaves` (with `Survey` set for the survey), `RepairOrphanedLeaves` |
| `TreeAdminWalMovePlanRequest` | `required string TreeId`, `int Partition`, `required string TargetProviderKey` | `PlanWalMove` |
| `TreeAdminWalMoveExecuteRequest` | `required string TreeId`, `int Partition`, `required string TargetProviderKey`, `TreeWalMoveOptions? Options` | `ExecuteWalMove` |
| `TreeAdminWalReclaimRequest` | `required string TreeId`, `int Partition`, `required string SourceProviderKey` | `ReclaimMovedWalSource` |
| `TreeAdminViewListRequest` | (empty) | `ListViews` |
| `TreeAdminCreateViewRequest` | `required string ViewName`, `required string SourceTreeId`, `required string ProviderKey`, `byte[] Payload` | `CreateView` |
| `TreeAdminViewRequest` | `required string ViewName` | `GetViewStatus`, `RebuildView`, `ReconcileView`, `DropView` (also its response) |
| `TreeAdminTagIndexListRequest` | (empty) | `ListTagIndexes` |
| `TreeAdminTagIndexRequest` | `required string IndexName` | `GetTagIndexStatus`, `ReconcileTagIndex` |
| `TreeAdminSetRetentionRequest` | `required string TreeId`, `TreeHistoryRetentionMode? Mode`, `TimeSpan? Window` | `SetHistoryRetention` |
| `AuthSchemeAdvertisementRequest` | (empty) | `GetAuthScheme` |

## Quick Start

Register the binding on a silo that already exposes the tree-administration facade, then map its routes. The host must expose the facade (`Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin`) in the same service provider - typically by co-hosting Orleans with `AddLattice(...).AddLatticeSchemaEnforcement(...).AddLatticeSchemaApi().AddLatticeTreeAdminApi()` on the same host. The binding fails closed, so register an `ILatticeTreeAdminApiAuthorizer` before serving traffic (or set `RequireAuthorization = false` behind an outer authentication boundary):

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeTreeAdminApiGrpc(o => o.RequireAuthorization = true);

var app = builder.Build();
app.MapLatticeTreeAdminApiGrpc();
```

## Client

`LatticeTreeAdminApiGrpcClient` is created over a caller-supplied `CallInvoker` and an `IServiceProvider` with Orleans serialization registered, via `LatticeTreeAdminApiGrpcClient.Create(callInvoker, serializerProvider)`. The typed client carries no address, TLS, retry, deadline, or credential policy of its own. A call the caller is not permitted to make surfaces as a `PermissionDenied` `RpcException` rather than an unhandled error; the binding never issues `Unauthenticated`.

## Status mapping

The service maps every facade outcome onto an explicit gRPC status rather than letting it fall through to a generic fault:

| Exception | gRPC status | Why |
|---|---|---|
| `LatticeAuthorizationDeniedException` | `PermissionDenied` | The caller lacks the tier the verb requires. A refusal by the transport authorizer is also `PermissionDenied`. |
| `LatticeTenantAccessDeniedException` | `PermissionDenied` | Fail-closed tenant resolution refused the call: the asserted tenant failed validation against the caller's membership, or, under an asserted tenant, the call named a `sys-` tree or a malformed `t/` id. A call that asserts no tenant is not refused here; it resolves the default tenant. |
| `KeyNotFoundException` | `NotFound` | The named materialised view or tag index is not registered. |
| `TreeNotEmptyException` | `FailedPrecondition` | A bulk-load session was opened against a tree that already holds data. |
| `BulkLoadOrderException` | `InvalidArgument` | A bulk-load chunk's keys were not strictly ascending. |
| `InvalidOperationException` | `FailedPrecondition` | A precondition refused on a well-formed request - for example no backup engine or view subsystem registered, a reshard or resize already in flight, a tree that still sources a materialised view, a tenant quota breach, or a saturation refusal (`LatticeQuotaExceededException` and `LatticeSaturatedException` both derive from it, so neither surfaces as `ResourceExhausted` here). |
| `ArgumentException` | `InvalidArgument` | A malformed or out-of-range argument, a reserved tree id, or an unconfirmed purge. |
| `OperationCanceledException` | `Cancelled` | The caller's deadline or cancellation token fired. |
| anything else | `Internal` | Logged server-side and returned with a generic message, without echoing the exception text. |

## Configuration

`LatticeTreeAdminApiGrpcOptions` controls the server-side binding:

| Property | Type | Default | Purpose |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor enforces `ILatticeTreeAdminApiAuthorizer` on every inbound call. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound request-header name that carries the caller's credential token, bridged into the ambient Lattice credential. The default bridge reads it on every call; without the `Orleans.Lattice.Auth` add-on the core no-op access gate ignores the bridged credential. |
| `CredentialScheme` | `string` | `Bearer` | The authentication scheme stamped on the bridged credential. A case-insensitive scheme prefix on the header value (for example `"Bearer "`) is stripped before the remaining token is used. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` | The inbound request-header name carrying the tenant the caller is acting as, lifted onto the ambient active-tenant scope for the duration of the call. Set to an empty string to disable header-based tenant selection. The header is read on every call; without the tenancy add-on the core no-op resolver ignores the stamped tenant. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | The auth schemes the endpoint advertises from its unauthenticated `GetAuthScheme` RPC, in preference order. Each descriptor must carry only public configuration, never a secret. |

### Per-tenant selection

On a cluster running the optional tenancy add-on, the call's *active tenant* scopes the tree namespace every verb addresses, so a tenant-scoped caller administers only trees in its own `t/{tenant}/{name}` namespace. The header carries only an *assertion*: the tenancy add-on re-validates it against the caller's subject membership downstream, exactly as it validates the caller credential. An absent, blank, or syntactically invalid header asserts no tenant, so the call resolves the reserved `default` tenant and addresses bare tree names unchanged. An asserted tenant the caller may not act as - one that is not registered, is not `Active`, or does not list the caller as an admin subject, and any assertion from an anonymous caller - is refused, as is a `sys-` tree name or a malformed `t/` id under an asserted tenant; each surfaces as a `PermissionDenied` `RpcException`. With no tenancy add-on registered the core no-op resolver ignores the stamped tenant, so the binding behaves exactly as it did before tenancy existed.

## Authorization surface

The binding's public authorization seams (the service, marshallers, method definitions, and interceptor stay internal):

- `ILatticeTreeAdminApiAuthorizer` - the per-call transport gate. `Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)` decides whether an inbound call may run at all. Two shipped implementations: `DenyTreeAdminApiAuthorizer` (the fail-closed default, registered via `TryAdd`, rejects every call with `PermissionDenied`) and `AllowAllTreeAdminApiAuthorizer` (opt-in, permits every call - for a trusted network behind a separate authentication boundary).
- `LatticeTreeAdminApiAuthorizationContext` - the decoded inbound call handed to the authorizer. A `readonly struct` carrying `ServerCallContext Call` (headers, deadline, peer), `LatticeTreeAdminApiOperation Operation`, and `string? TargetId`.
- `LatticeTreeAdminApiOperation` - the per-operation discriminator. It names only the capability probe, the diagnostics and inspection reads, the lifecycle and configuration verbs, and view creation: `ProbeCapabilities`, `GetShardHotness`, `GetDiagnostics`, `InspectShardMap`, `GetProjectionDigest`, `GetTreeStats`, `GetStorageUsage`, `CreateTree`, `CheckTreeExists`, `SetTreeAlias`, `ResolveTreeAlias`, `GetTreeConfig`, `SetTreeConfig`, `GetShardMap`, and `CreateView`. Every other RPC - deletion and recovery, bulk load, restore, reshard, resize, snapshot, WAL placement and moves, the orphaned-leaf verbs, the remaining view verbs, tag indexes, compaction, and history retention - is presented as `Unknown`, as is an unrecognised method, so a deny-by-default per-operation policy refuses them and an authorizer cannot tell those verbs apart by operation.
- `TargetId` is the tree id the request names - the source tree for `CreateView` and the result's target tree for `RevertTreeRestore` - or `null` for the storage-usage, restore-set, view (other than create), tag-index, and list requests, which carry no tree id, and for the orphaned-leaf requests, whose `TreeId` the interceptor does not decode. For `SnapshotTree` and `SetTreeAlias` the interceptor consults the authorizer a second time, under the same operation, with the destination tree or the alias's physical tree as the target, whenever that id differs from the primary one.
- `ILatticeTreeAdminApiCredentialBridge` - the identity seam. `LatticeCredential? Resolve(ServerCallContext context)` lifts the caller's credential from the request; returning `null` leaves the caller anonymous (and an anonymous caller is denied when auth-backed control is active). The built-in default reads the configurable `CredentialHeaderName` / `CredentialScheme` header. This runs after, and independently of, the transport authorizer: the authorizer decides whether the call may run, and the resolved credential then feeds the composed facade's own fail-closed access gate.
- `ILatticeTreeAdminApiAuthSchemeSource` - supplies the advertisement the unauthenticated `GetAuthScheme` RPC returns. `AuthSchemeAdvertisement GetAdvertisement()` must return only public configuration (never a secret). The built-in options-backed source returns `AdvertisedAuthSchemes`.

### `AuthSchemeDescriptor`

One advertised authentication scheme returned by the unauthenticated discovery RPC. It carries only public configuration, never a secret.

| Property | Type | Default | Purpose |
|---|---|---|---|
| `SchemeId` | `string` (required) | - | The stable scheme id a client matches to a login provider (for example `basic` or `entra`). |
| `DisplayName` | `string` | `""` | A friendly, human-readable name for the scheme. |
| `Parameters` | `IReadOnlyDictionary<string, string>` | empty | The public parameters a client needs to run the challenge (for example an authority, tenant, client id, audience). |

## Reference

- [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/README.md) - the transport-agnostic whole-tree administration facade this binding projects, including the operation set and the fail-closed access gate.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared, versioned API contract the facade and this binding consume.
- [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/README.md) - the sibling control-facade binding this one mirrors; the tree-administration facade composes the schema control facade by delegation.
