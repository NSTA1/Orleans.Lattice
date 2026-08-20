# Orleans.Lattice.Api.TreeAdmin.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.TreeAdmin](../lattice.api.treeadmin/README.md) - projects the whole-tree administration control facade onto a gRPC service and a public typed client, marshalled with the Orleans binary serializer over code-first request and response records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.TreeAdmin.Grpc` is the remote transport for the cluster's tree-administration control plane. A host references it when a dashboard, a CLI, or an operations tool needs to create, inspect, reconfigure, reshard, resize, snapshot, restore, bulk-load, or retire whole trees, manage their materialised views and tag indexes, and audit or move their WAL placement over the network rather than in-process.

It provides:

- **A code-first gRPC service.** One unary RPC per facade operation - the capability probe, diagnostics and inspection reads, tree lifecycle, alias and configuration, deletion and recovery, bulk load, restore, reshard, resize, snapshot, WAL placement and movement, view and tag-index management, shard compaction, history retention, and the unauthenticated auth-scheme discovery call - bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeTreeAdminApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Every wire message is a `[GenerateSerializer]` record serialized with the Orleans binary serializer. Each RPC wraps the facade operation's arguments in this package's own request record (for example `TreeAdminCreateRequest`), and its response reuses the facade's result record from `Orleans.Lattice.Api.Abstractions`. The gRPC contract therefore adapts over the facade DTOs rather than being wire-identical to them, and client and server stay in lock-step by construction.
- **Fail-closed authorization.** A per-call `ILatticeTreeAdminApiAuthorizer` seam gates every protected RPC at the edge, and the composed facade re-authorizes the resolved caller through the core access gate. Both default to deny.

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
| Views | `ListViewsAsync`, `GetViewStatusAsync`, `RebuildViewAsync`, `ReconcileViewAsync`, `DropViewAsync` |
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
| `ListViewsAsync` | `Task<TreeViewCatalog> ListViewsAsync(CancellationToken cancellationToken = default)` |
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

`LatticeTreeAdminApiGrpcClient` is created over a caller-supplied `CallInvoker` and an `IServiceProvider` with Orleans serialization registered, via `LatticeTreeAdminApiGrpcClient.Create(callInvoker, serializerProvider)`. The typed client carries no address, TLS, retry, deadline, or credential policy of its own. A call the caller is not permitted to make surfaces as a `PermissionDenied` or `Unauthenticated` `RpcException` rather than an unhandled error.

## Configuration

`LatticeTreeAdminApiGrpcOptions` controls the server-side binding:

| Property | Type | Default | Purpose |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Whether the interceptor enforces `ILatticeTreeAdminApiAuthorizer` on every inbound call. Set to `false` only when an outer authentication boundary already guards the endpoint. |
| `CredentialHeaderName` | `string` | `authorization` | The inbound request-header name that carries the caller's credential token, bridged into the ambient Lattice credential. Only consulted when the `Orleans.Lattice.Auth` add-on is registered. |
| `CredentialScheme` | `string` | `Bearer` | The authentication scheme stamped on the bridged credential. A case-insensitive scheme prefix on the header value (for example `"Bearer "`) is stripped before the remaining token is used. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | empty | The auth schemes the endpoint advertises from its unauthenticated `GetAuthScheme` RPC, in preference order. Each descriptor must carry only public configuration, never a secret. |

## Authorization surface

The binding's public authorization seams (the service, marshallers, method definitions, and interceptor stay internal):

- `ILatticeTreeAdminApiAuthorizer` - the per-call transport gate. `Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)` decides whether an inbound call may run at all. Two shipped implementations: `DenyTreeAdminApiAuthorizer` (the fail-closed default, registered via `TryAdd`, rejects every call with `PermissionDenied`) and `AllowAllTreeAdminApiAuthorizer` (opt-in, permits every call - for a trusted network behind a separate authentication boundary).
- `LatticeTreeAdminApiAuthorizationContext` - the decoded inbound call handed to the authorizer. A `readonly struct` carrying `ServerCallContext Call` (headers, deadline, peer), `LatticeTreeAdminApiOperation Operation` (the specific operation being invoked), and `string? TargetId` (the tree id the call targets, or `null` for calls not scoped to a single tree, such as the auth-scheme discovery call).
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
