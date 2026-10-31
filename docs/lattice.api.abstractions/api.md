---
agent_spec: "docs/agents/api/state.json"
---

# Public API

Every public type of `Orleans.Lattice.Api.Abstractions`, grouped by namespace. The package is the transport-agnostic contract of the API facades: service interfaces, the request / response models they exchange, their typed exceptions, and one stable serialization-alias class per namespace. It has no registration extension, no options, and no implementation of its own service interfaces. The [package overview](README.md) explains why the contract is published separately, and [Architecture](architecture.md) describes how the facades, their gRPC bindings, the MCP server and the Explorer meet at it.

Behaviour - authorization, scoping, validation, the exceptions a call actually throws - belongs to the facade that implements an interface, so each namespace below links to its facade and binding docs for the narrative.

## Namespace map

| Namespace | Service interfaces | Implemented by | Bound to gRPC by | Alias class (prefix) |
|---|---|---|---|---|
| [`Orleans.Lattice.Api.Operations`](#orleanslatticeapioperations) | `ILatticeOperations` | the backup, schema and tree-administration facades | each of those facades' bindings | `ApiOperationTypeAliases` (`oio.`) |
| [`Orleans.Lattice.Api.Region`](#orleanslatticeapiregion) | `ILatticeRegionCatalog` | [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/api.md) | - | `ApiRegionTypeAliases` (`olrg.`) |
| [`Orleans.Lattice.Api.State`](#orleanslatticeapistate) | `ILatticeStateMetricsObserver`, `ILatticeStateObserver`, `ILatticeStateQuery` | [`Orleans.Lattice.Api.State`](../lattice.api.state/api.md) | [`Orleans.Lattice.Api.State.Grpc`](../lattice.api.state.grpc/api.md) | `ApiStateTypeAliases` (`ola.`) |
| [`Orleans.Lattice.Api.Data`](#orleanslatticeapidata) | `ILatticeDataApi` | [`Orleans.Lattice.Api.Data`](../lattice.api.data/api.md) | [`Orleans.Lattice.Api.Data.Grpc`](../lattice.api.data.grpc/api.md) | `DataApiTypeAliases` (`olad.`) |
| [`Orleans.Lattice.Api.Auth`](#orleanslatticeapiauth) | `ILatticeAuthAdmin` | [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/api.md) | [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/api.md) | `ApiAuthTypeAliases` (`oli.`) |
| [`Orleans.Lattice.Api.Backup`](#orleanslatticeapibackup) | `ILatticeBackupControl`, `ILatticeBackupOperations` | [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/api.md) | [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/api.md) | `ApiBackupTypeAliases` (`oib.`) |
| [`Orleans.Lattice.Api.Schema`](#orleanslatticeapischema) | `ILatticeSchemaComplianceOperations`, `ILatticeSchemaControl`, `ILatticeSchemaOperations` | [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/api.md) | [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/api.md) | `ApiSchemaTypeAliases` (`ois.`) |
| [`Orleans.Lattice.Api.Replication`](#orleanslatticeapireplication) | `ILatticeReplicationControl`, `ILatticeReplicationPeerAdmin`, `ILatticeReplicationStatus` | [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/api.md) | [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/api.md) | `ApiReplicationTypeAliases` (`oir.`) |
| [`Orleans.Lattice.Api.Telemetry`](#orleanslatticeapitelemetry) | `ILatticeTelemetry` | [`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/api.md) | [`Orleans.Lattice.Api.Telemetry.Grpc`](../lattice.api.telemetry.grpc/api.md) | `ApiTelemetryTypeAliases` (`oitl.`) |
| [`Orleans.Lattice.Api.TreeAdmin`](#orleanslatticeapitreeadmin) | `ILatticeStorageUsageOperations`, `ILatticeTreeAdmin`, `ILatticeTreeAdminOperations`, `ILatticeWalReclamation` | [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/api.md) | [`Orleans.Lattice.Api.TreeAdmin.Grpc`](../lattice.api.treeadmin.grpc/api.md) | `ApiTreeAdminTypeAliases` (`oit.`) |
| [`Orleans.Lattice.Api.TenantAdmin`](#orleanslatticeapitenantadmin) | `ILatticeTenantAccessAdmin`, `ILatticeTenantAdmin`, `ILatticeTenantDirectoryAdmin`, `ILatticeTenantGrantAdmin`, `ILatticeTenantPolicyAdmin`, `ILatticeTenantQuotaUsage`, `ILatticeTenantRegionAdmin`, `ILatticeTenantSelfService` | [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/api.md) | [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/api.md) | `ApiTenantAdminTypeAliases` (`oitn.`) |
| [`Orleans.Lattice.Api.Apps`](#orleanslatticeapiapps) | `ILatticeAppBridge`, `ILatticeAppCatalog`, `ILatticeAppRoleBindings`, `ILatticeAppsControl`, `ILatticeAppWorkspace` | [`Orleans.Lattice.Api.Apps`](../lattice.api.apps/api.md) | [`Orleans.Lattice.Api.Apps.Grpc`](../lattice.api.apps.grpc/api.md) | `ApiAppsTypeAliases` (`oia.`) |

The in-process facades and the remote adapters implement the same interfaces, so a consumer written against an interface works unchanged in-process and over the wire. The remote adapters are the Orleans.Lattice.Api.Mcp server in remote mode (which implements the interfaces over the gRPC bindings; co-hosted, it resolves the facades from dependency injection instead), the Explorer's shell transports, and the public gRPC clients some bindings ship - for example LatticeReplicationStatusGrpcClient (ILatticeReplicationStatus) and the apps clients (ILatticeAppsControl, ILatticeAppRoleBindings, ILatticeAppCatalog, ILatticeAppWorkspace, ILatticeAppBridge).

## Conventions

- **Serialization.** Most models are Orleans-serializable: they carry [GenerateSerializer] and an [Alias] drawn from their namespace's alias class in the table above. The alias string is the wire and persisted identity of the type and never changes, so the alias classes are part of the public surface. The exceptions are the state facade's in-process result wrappers (TreeSummaryResult, ShardSummariesResult, TreeStructureResult, EntryScanResult, EntryDetailResult and EntryHistoryResult), a few enums that carry no attribute, and most typed exceptions (next bullet).
- **Exceptions.** The typed exceptions are raised in-process by the facades, so a binding can surface each as a distinct outcome. Only AppBridgeException and LatticeReplicationEngineNotHostedException are Orleans-serializable. Every exception derives directly from System.Exception except two: LatticeReplicationEngineNotHostedException (InvalidOperationException) and TenantAccessConfinementException (ArgumentException). Each of those two ships a no-op same-silo copier beside it and implements the core library's ILatticeDomainFault marker, which tells a broad catch written for the BCL base type that the exception is a deliberate domain refusal to let propagate, not a framework failure to recover from.
- **Long-running work.** Operation-tracking interfaces (`ILatticeBackupOperations`, `ILatticeSchemaOperations`, `ILatticeSchemaComplianceOperations`, `ILatticeTreeAdminOperations`, `ILatticeStorageUsageOperations`) extend `ILatticeOperations` and return a `LatticeOperationHandle` from each start verb. See [Long-running operations](operations.md).
- **Shared core types.** Several models expose types from the packages this one references - the core library (for example `HybridLogicalClock` and `LatticeMergeMode`), `Orleans.Lattice.Membership`, `Orleans.Lattice.Auth`, `Orleans.Lattice.Backup` and `Orleans.Lattice.Schema` - so a contract consumer takes those packages as transitive dependencies.

## Reading the declaration reference

Each namespace section lists every public type with its kind and summary, followed by the declaration reference: the declaration line and every directly declared public member and overload, in source order, with enum values. Inherited framework members and compiler-generated record members are not listed. For `Orleans.Lattice.Api.TreeAdmin`, `Orleans.Lattice.Api.TenantAdmin` and `Orleans.Lattice.Api.Apps` the member-level reference is maintained once, in the facade's own API page, and is linked rather than repeated here.

Totals: 327 public types across 12 namespaces.

## `Orleans.Lattice.Api.Operations`

The shared accept-then-poll contract for long-running facade work. Implemented by the backup, schema and tree-administration facades through their operation-tracking interfaces. Narrative: [Long-running operations](operations.md).

| Type | Kind | Summary |
|---|---|---|
| `ILatticeOperations` | interface | The shared read-and-cancel surface over long-running operations. |
| `ApiOperationTypeAliases` | static class | Centralized Orleans serialization alias constants for the shared long-running-operation contract in `Orleans.Lattice.Api.Abstractions`. |
| `LatticeOperationHandle` | sealed record | The handle a start verb returns as soon as a long-running operation has been accepted: the id to poll with, its kind and its scope. |
| `LatticeOperationListRequest` | sealed record | A request for one page of the caller's operations, newest-first. |
| `LatticeOperationPage` | sealed record | One page of operations, newest-first. |
| `LatticeOperationScope` | sealed record | What a long-running operation acts on: the tenant that owns it and the effective (tenant-scoped) trees it targets. |
| `LatticeOperationState` | enum | The lifecycle state of a long-running operation. |
| `LatticeOperationStatus` | sealed record | A point-in-time snapshot of one long-running operation. |

### `ILatticeOperations`

[Source](../../src/lattice.api.abstractions/Operations/ILatticeOperations.cs) (line 16).

`public interface ILatticeOperations`

- `Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)`

### `ApiOperationTypeAliases`

[Source](../../src/lattice.api.abstractions/Operations/Model/ApiOperationTypeAliases.cs) (line 9).

`public static class ApiOperationTypeAliases`

- `public const string AliasPrefix`
- `public const string LatticeOperationScope`
- `public const string LatticeOperationHandle`
- `public const string LatticeOperationStatus`
- `public const string LatticeOperationListRequest`
- `public const string LatticeOperationPage`
- `public const string LatticeOperationState`

### `LatticeOperationHandle`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationHandle.cs) (line 11).

`public sealed record LatticeOperationHandle`

- `public required string OperationId { get; init; }`
- `public required string Kind { get; init; }`
- `public required LatticeOperationScope Scope { get; init; }`
- `public bool Created { get; init; }`

### `LatticeOperationListRequest`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationListRequest.cs) (line 7).

`public sealed record LatticeOperationListRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public int EffectivePageSize`

### `LatticeOperationPage`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationPage.cs) (line 11).

`public sealed record LatticeOperationPage`

- `public IReadOnlyList<LatticeOperationStatus> Operations { get; init; }`
- `public string? NextPageToken { get; init; }`

### `LatticeOperationScope`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationScope.cs) (line 11).

`public sealed record LatticeOperationScope`

- `public required string TenantId { get; init; }`
- `public IReadOnlyList<string> TreeIds { get; init; }`

### `LatticeOperationState`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationState.cs) (line 9).

`public enum LatticeOperationState`

- `Queued`
- `Running`
- `Succeeded`
- `Failed`
- `Cancelled`

### `LatticeOperationStatus`

[Source](../../src/lattice.api.abstractions/Operations/Model/LatticeOperationStatus.cs) (line 19).

`public sealed record LatticeOperationStatus`

- `public required string OperationId { get; init; }`
- `public required string Kind { get; init; }`
- `public required LatticeOperationScope Scope { get; init; }`
- `public required LatticeOperationState State { get; init; }`
- `public required string Phase { get; init; }`
- `public int? PhaseIndex { get; init; }`
- `public int? PhaseCount { get; init; }`
- `public long CompletedUnits { get; init; }`
- `public long? TotalUnits { get; init; }`
- `public string? UnitName { get; init; }`
- `public DateTimeOffset StartedAtUtc { get; init; }`
- `public DateTimeOffset? FinishedAtUtc { get; init; }`
- `public string? FailureReason { get; init; }`
- `public string? ResultReference { get; init; }`
- `public IReadOnlyDictionary<string, string> Result { get; init; }`
- `public bool CancelRequested { get; init; }`
- `public bool IsTerminal`

## `Orleans.Lattice.Api.Region`

Region discovery: the regions a server can route a call to and the facade groups each one serves. Implemented today by the [MCP server](../lattice.api.mcp/README.md) (`lattice_list_regions`).

| Type | Kind | Summary |
|---|---|---|
| `ILatticeRegionCatalog` | interface | The transport-agnostic region discovery contract: enumerates the regions the server can route API calls to (the current cluster plus any reachable, credentialed peer), each with its per-group reachability. |
| `ApiRegionTypeAliases` | static class | Centralised Orleans serialization alias constants for every `Orleans.Lattice.Api.Region` type that participates in the wire format. |
| `LatticeRegionDescriptor` | sealed record | A region the server can route API calls to: the current cluster the binding is co-hosted with (or primarily wired to), plus any reachable, credentialed peer. |
| `LatticeRegionGroupReachability` | sealed record | The reachability of a single Lattice API facade group within one region, as reported by `ILatticeRegionCatalog`. |
| `LatticeRegionTenantScope` | sealed record | One tenant's standing in one region, attached to a `LatticeRegionDescriptor` when the catalog answered a tenant-scoped discovery call. |

### `ILatticeRegionCatalog`

[Source](../../src/lattice.api.abstractions/Region/ILatticeRegionCatalog.cs) (line 18).

`public interface ILatticeRegionCatalog`

- `Task<IReadOnlyList<LatticeRegionDescriptor>> ListRegionsAsync(CancellationToken cancellationToken = default)`

### `ApiRegionTypeAliases`

[Source](../../src/lattice.api.abstractions/Region/Model/ApiRegionTypeAliases.cs) (line 16).

`public static class ApiRegionTypeAliases`

- `public const string LatticeRegionGroupReachability`
- `public const string LatticeRegionDescriptor`
- `public const string LatticeRegionTenantScope`

### `LatticeRegionDescriptor`

[Source](../../src/lattice.api.abstractions/Region/Model/LatticeRegionDescriptor.cs) (line 21).

`public sealed record LatticeRegionDescriptor`

- `public required string RegionId { get; init; }`
- `public string ClusterId { get; init; }`
- `public required bool IsCurrent { get; init; }`
- `public IReadOnlyList<LatticeRegionGroupReachability> Groups { get; init; }`
- `public LatticeRegionTenantScope? TenantScope { get; init; }`

### `LatticeRegionGroupReachability`

[Source](../../src/lattice.api.abstractions/Region/Model/LatticeRegionGroupReachability.cs) (line 21).

`public sealed record LatticeRegionGroupReachability`

- `public required string Group { get; init; }`
- `public required bool Available { get; init; }`
- `public string? Endpoint { get; init; }`

### `LatticeRegionTenantScope`

[Source](../../src/lattice.api.abstractions/Region/Model/LatticeRegionTenantScope.cs) (line 31).

`public sealed record LatticeRegionTenantScope`

- `public required string TenantId { get; init; }`
- `public required bool IsAllowed { get; init; }`
- `public required TenantRegionLifecycleStatus Status { get; init; }`
- `public required bool IsResident { get; init; }`

## `Orleans.Lattice.Api.State`

Implemented by [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and exposed remotely by [`Orleans.Lattice.Api.State.Grpc`](../lattice.api.state.grpc/README.md). 52 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeStateMetricsObserver` | interface | Transport-agnostic live metadata / metrics observation facade. |
| `ILatticeStateObserver` | interface | Transport-agnostic live change-observation facade over a cluster's lattice state. |
| `ILatticeStateQuery` | interface | Transport-agnostic read facade over a cluster's lattice state. |
| `ApiStateTypeAliases` | static class | Centralised Orleans serialization alias constants for every `Orleans.Lattice.Api.State` type that participates in the wire format. |
| `CatalogRequest` | sealed record | Paging and filtering request for the discovery / catalog endpoints (`ListTreesAsync` and `ListViewsAsync`). |
| `ClusterInfo` | sealed record | Identity and high-level metadata for the cluster a state-API client is connected to. |
| `ClusterInfoRequest` | sealed record | Request for `GetClusterInfoAsync`. |
| `CoveredTreeCatalogPage` | sealed record | One page of the subject trees a tag index covers, as surfaced by `ListCoveredTreesAsync`. |
| `DeadLetterEntryRecord` | sealed record | Read-only record of a single strict-mode dead-letter entry surfaced by `ListDeadLettersAsync`. |
| `DeadLetterQueuePage` | sealed record | One page of a tree's strict-mode dead-letter queue, in append (time) order. |
| `DeadLetterQueueRequest` | sealed record | Paging request for the per-tree dead-letter-queue listing (`ListDeadLettersAsync`). |
| `DeadLetterSourceKind` | enum | Identifies the ingest path that produced a dead-letter entry when strict-mode schema enforcement diverted a non-compliant item instead of applying it. |
| `EntryDetailResult` | sealed record | Result of `GetEntryAsync`: the full record for a single existing key, or a typed not-found that distinguishes an unknown tree from a missing key. |
| `EntryHistoryBound` | enum | Tells a consumer how a key's returned change history is bounded below, so it can distinguish a clean, durable timeline from a best-effort fallback that may have lost its oldest revisions. |
| `EntryHistoryRequest` | sealed record | Request for the per-key change-history endpoint (`GetEntryHistoryAsync`). |
| `EntryHistoryResult` | sealed record | Result of `GetEntryHistoryAsync`: a continuation-paged page of a key's revision timeline plus the history metadata (how the timeline is bounded and, when truncated, the oldest still-readable revision), or a typed not-found when the tree does not exist. |
| `EntryRecord` | sealed record | Read-only record of a single entry returned by the entry / key-range inspection endpoint. |
| `EntryRevisionRecord` | sealed record | Read-only record of a single revision in a key's change-history timeline, returned by `GetEntryHistoryAsync`. |
| `EntryScanCancelRequest` | sealed record | Request for the entry-scan cancel endpoint (`CancelScanAsync`). |
| `EntryScanMode` | enum | Selects the cursor isolation an entry scan (`ScanEntriesAsync`) opens. |
| `EntryScanRequest` | sealed record | Request for the entry / key-range inspection endpoint (`ScanEntriesAsync`). |
| `EntryScanResult` | sealed record | Result of `ScanEntriesAsync`: a snapshot-isolated, key-ordered page of entries plus an opaque continuation token, or a typed not-found when the tree does not exist. |
| `LatticeStateCursorExpiredException` | sealed class (exception) | Thrown when a change-observation subscription is resumed from a `ContinuationToken` that has fallen outside the WAL retention window, so the missed changes can no longer be replayed. |
| `NodeKind` | enum | Kind of a B+ tree node surfaced by the structure-query endpoint. |
| `NodeStateSummary` | sealed record | Read-only summary of a single B+ tree node, used by the tree-structure query endpoint to render the node graph. |
| `RevisionRetention` | readonly record struct | The retention shape that was actually applied to a single history revision when it was written, carried per row so a consumer can detect a retention-configuration transition by diffing adjacent revisions of the same key (the "retention changed here" boundary). |
| `ShardHotness` | sealed record | Per-shard hotness sample carried in a `TreeMetrics` when the request opts in via `IncludeShardHotness`. |
| `ShardStateSummary` | sealed record | Read-only, point-in-time summary of a single physical shard's state. |
| `ShardSummariesResult` | sealed record | Result of `GetShardSummariesAsync`: either the per-shard summaries or a typed not-found. |
| `StateChangeKind` | enum | The kind of state change carried by a `StateChangeNotification`. |
| `StateChangeNotification` | sealed record | A single live state-change notification yielded by `ObserveAsync` (and the gRPC change subscription built over it). |
| `StateObserveRequest` | sealed record | Request for the change-observation endpoint (`ObserveAsync`). |
| `StateQueryStatus` | enum | Outcome of a state-query lookup that can fail to resolve its target. |
| `StructureRequest` | sealed record | Request for the tree-structure query endpoint (`GetTreeStructureAsync`). |
| `TagIndexCatalogPage` | sealed record | One page of the tag-index catalog. |
| `TagIndexStateSummary` | sealed record | Read-only summary of a tag-index membership tree as surfaced by the cluster state API's tag-index discovery endpoint (`ListTagIndexesAsync`). |
| `TagMember` | sealed record | A single `(tree, key)` member of a tag, as surfaced by `ScanTagMembersAsync`: the key `Key` in subject tree `TreeId` currently carries the requested tag and still exists. |
| `TagMemberScanPage` | sealed record | One page of the members of a single tag across every tree a tag index covers, as surfaced by `ScanTagMembersAsync`. |
| `TagMemberScanRequest` | sealed record | Paging request for `ScanTagMembersAsync`: the members of a single tag across every subject tree a tag index covers. |
| `TagValueCatalogPage` | sealed record | One page of the distinct tag values carried by a single tag index over its subject tree. |
| `TreeCatalogEntry` | sealed record | A single entry in the tree catalog returned by `ListTreesAsync`: the lightweight "what trees exist?" projection the explorer opens with, sourced from the internal tree registry without the caller talking to registry grains. |
| `TreeCatalogPage` | sealed record | One page of the tree catalog. |
| `TreeConfigSummary` | sealed record | Effective per-tree configuration as observed by the read-only state API. |
| `TreeLifecycleState` | enum | Lifecycle state of a tree as seen by the read-only cluster state API. |
| `TreeMetrics` | sealed record | Low-cardinality live aggregates for a single tree, as sampled by the metrics-observation feed. |
| `TreeMetricsRequest` | sealed record | Request for a live metadata / metrics observation - either a one-shot poll (`SampleAsync`) or a sampled stream (`ObserveAsync`). |
| `TreeMetricsSnapshot` | sealed record | One sample tick of the metrics-observation feed. |
| `TreeStateSummary` | sealed record | Read-only, point-in-time summary of a single tree's state, aggregated by the cluster state API from the core diagnostics surface and tree registry. |
| `TreeStructureResult` | sealed record | Result of `GetTreeStructureAsync`: either a bounded, depth-limited node graph (one entry per shard root for a whole-tree read, or a single subtree for a sub-path descent) or a typed not-found. |
| `TreeSummaryResult` | sealed record | Result of `GetTreeSummaryAsync`: either a populated `TreeStateSummary` or a typed not-found, so an unknown tree never surfaces an Orleans-internal exception to callers. |
| `ViewCatalogPage` | sealed record | One page of the materialised-view catalog. |
| `ViewStateSummary` | sealed record | Read-only summary of a materialised view as surfaced by the cluster state API's discovery endpoint. |

### `ILatticeStateMetricsObserver`

[Source](../../src/lattice.api.abstractions/State/ILatticeStateMetricsObserver.cs) (line 12).

`public interface ILatticeStateMetricsObserver`

- `Task<TreeMetricsSnapshot> SampleAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<TreeMetricsSnapshot> ObserveAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`

### `ILatticeStateObserver`

[Source](../../src/lattice.api.abstractions/State/ILatticeStateObserver.cs) (line 30).

`public interface ILatticeStateObserver`

- `IAsyncEnumerable<StateChangeNotification> ObserveAsync(StateObserveRequest request, CancellationToken cancellationToken = default)`

### `ILatticeStateQuery`

[Source](../../src/lattice.api.abstractions/State/ILatticeStateQuery.cs) (line 16).

`public interface ILatticeStateQuery`

- `Task<TreeSummaryResult> GetTreeSummaryAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)`
- `Task<ShardSummariesResult> GetShardSummariesAsync(string treeId, bool deep = true, CancellationToken cancellationToken = default)`
- `Task<int?> GetPhysicalShardCountAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)`
- `Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)`
- `Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)`
- `Task<TreeStructureResult> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)`
- `Task<EntryScanResult> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)`
- `Task<EntryDetailResult> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<EntryHistoryResult> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)`
- `Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)`
- `Task<int> GetDeadLetterCountAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)`

### `ApiStateTypeAliases`

[Source](../../src/lattice.api.abstractions/State/Model/ApiStateTypeAliases.cs) (line 15).

`public static class ApiStateTypeAliases`

- `public const string TreeConfigSummary`
- `public const string TreeStateSummary`
- `public const string ShardStateSummary`
- `public const string NodeStateSummary`
- `public const string ViewStateSummary`
- `public const string EntryRecord`
- `public const string CatalogRequest`
- `public const string TreeCatalogEntry`
- `public const string TreeCatalogPage`
- `public const string ViewCatalogPage`
- `public const string StructureRequest`
- `public const string EntryScanRequest`
- `public const string EntryScanMode`
- `public const string EntryScanCancelRequest`
- `public const string StateChangeNotification`
- `public const string StateObserveRequest`
- `public const string TreeMetricsRequest`
- `public const string ShardHotness`
- `public const string TreeMetrics`
- `public const string TreeMetricsSnapshot`
- `public const string ClusterInfoRequest`
- `public const string ClusterInfo`
- `public const string TagIndexStateSummary`
- `public const string TagIndexCatalogPage`
- `public const string TagValueCatalogPage`
- `public const string EntryHistoryRequest`
- `public const string EntryRevisionRecord`
- `public const string RevisionRetention`
- `public const string EntryHistoryBound`
- `public const string CoveredTreeCatalogPage`
- `public const string TagMemberScanRequest`
- `public const string TagMember`
- `public const string TagMemberScanPage`
- `public const string DeadLetterSourceKind`
- `public const string DeadLetterEntryRecord`
- `public const string DeadLetterQueueRequest`
- `public const string DeadLetterQueuePage`

### `CatalogRequest`

[Source](../../src/lattice.api.abstractions/State/Model/CatalogRequest.cs) (line 17).

`public sealed record CatalogRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public bool IncludeSystemTrees { get; init; }`
- `public bool IncludeViewStats { get; init; }`
- `public string? SourceTreeId { get; init; }`
- `public string? IndexName { get; init; }`
- `public int EffectivePageSize`

### `ClusterInfo`

[Source](../../src/lattice.api.abstractions/State/Model/ClusterInfo.cs) (line 18).

`public sealed record ClusterInfo`

- `public string ClusterId { get; init; }`
- `public string ServiceId { get; init; }`

### `ClusterInfoRequest`

[Source](../../src/lattice.api.abstractions/State/Model/ClusterInfoRequest.cs) (line 12).

`public sealed record ClusterInfoRequest`

### `CoveredTreeCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 94).

`public sealed record CoveredTreeCatalogPage`

- `public IReadOnlyList<string> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `DeadLetterEntryRecord`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterEntryRecord.cs) (line 19).

`public sealed record DeadLetterEntryRecord`

- `public required string Key { get; init; }`
- `public byte[] ValuePreview { get; init; }`
- `public int ValueByteLength { get; init; }`
- `public required string Reason { get; init; }`
- `public DeadLetterSourceKind Source { get; init; }`
- `public DateTimeOffset TimestampUtc { get; init; }`
- `public bool PreviewTruncated { get; init; }`
- `public bool Equals(DeadLetterEntryRecord? other)`
- `public override int GetHashCode()`

### `DeadLetterQueuePage`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterQueuePage.cs) (line 12).

`public sealed record DeadLetterQueuePage`

- `public IReadOnlyList<DeadLetterEntryRecord> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `DeadLetterQueueRequest`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterQueueRequest.cs) (line 13).

`public sealed record DeadLetterQueueRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public required string TreeId { get; init; }`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public int EffectivePageSize`

### `DeadLetterSourceKind`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterSourceKind.cs) (line 11).

`public enum DeadLetterSourceKind`

- `Replication`
- `Restore`
- `LocalRejected`
- `Unknown`

### `EntryDetailResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 207).

`public sealed record EntryDetailResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public EntryRecord? Entry { get; init; }`
- `public static EntryDetailResult Found(string treeId, EntryRecord entry)`
- `public static EntryDetailResult TreeNotFound(string treeId, string key)`
- `public static EntryDetailResult KeyNotFound(string treeId, string key)`

### `EntryHistoryBound`

[Source](../../src/lattice.api.abstractions/State/Model/EntryHistoryBound.cs) (line 11).

`public enum EntryHistoryBound`

- `BoundedByAge`
- `Truncated`
- `WalWindowFallback`

### `EntryHistoryRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryHistoryRequest.cs) (line 15).

`public sealed record EntryHistoryRequest`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public HybridLogicalClock? FromHlc { get; init; }`
- `public HybridLogicalClock? ToHlc { get; init; }`
- `public int Limit { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public int ValuePreviewBudget { get; init; }`
- `public bool Reverse { get; init; }`

### `EntryHistoryResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 258).

`public sealed record EntryHistoryResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public IReadOnlyList<EntryRevisionRecord> Revisions { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public EntryHistoryBound Bound { get; init; }`
- `public HybridLogicalClock EarliestAvailable { get; init; }`
- `public static EntryHistoryResult Found(string treeId, string key, IReadOnlyList<EntryRevisionRecord> revisions, string? continuationToken, EntryHistoryBound bound, HybridLogicalClock earliestAvailable)`
- `public static EntryHistoryResult TreeNotFound(string treeId, string key)`
- `public static EntryHistoryResult KeyNotFound(string treeId, string key)`

### `EntryRecord`

[Source](../../src/lattice.api.abstractions/State/Model/EntryRecord.cs) (line 13).

`public sealed record EntryRecord`

- `public required string Key { get; init; }`
- `public byte[] ValuePreview { get; init; }`
- `public int ValueLength { get; init; }`
- `public bool Truncated { get; init; }`
- `public HybridLogicalClock Hlc { get; init; }`
- `public bool IsTombstone { get; init; }`
- `public long ExpiresAtTicks { get; init; }`
- `public string? CrdtShape { get; init; }`
- `public IReadOnlyList<CrdtMemberValue> CurrentMembers { get; init; }`
- `public LatticeMergeMode? MergeMode { get; init; }`
- `public bool Raw { get; init; }`
- `public bool Equals(EntryRecord? other)`
- `public override int GetHashCode()`

### `EntryRevisionRecord`

[Source](../../src/lattice.api.abstractions/State/Model/EntryRevisionRecord.cs) (line 15).

`public sealed record EntryRevisionRecord`

- `public HybridLogicalClock Hlc { get; init; }`
- `public HistoryRowKind Kind { get; init; }`
- `public MutationCategory Category { get; init; }`
- `public required string SourceKey { get; init; }`
- `public string? OriginClusterId { get; init; }`
- `public byte[]? ValuePreview { get; init; }`
- `public int ValueLength { get; init; }`
- `public bool Truncated { get; init; }`
- `public long ValueHash { get; init; }`
- `public byte[]? Delta { get; init; }`
- `public LatticeMergeMode Mode { get; init; }`
- `public IReadOnlyList<CrdtMemberChange> MemberChanges { get; init; }`
- `public RevisionRetention Retention { get; init; }`
- `public string? EndKey { get; init; }`
- `public bool Equals(EntryRevisionRecord? other)`
- `public override int GetHashCode()`

### `EntryScanCancelRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanCancelRequest.cs) (line 15).

`public sealed record EntryScanCancelRequest`

- `public required string TreeId { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `EntryScanMode`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanMode.cs) (line 12).

`public enum EntryScanMode`

- `Snapshot`
- `Live`
- `LivePointInTime`

### `EntryScanRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanRequest.cs) (line 19).

`public sealed record EntryScanRequest`

- `public required string TreeId { get; init; }`
- `public string? StartInclusive { get; init; }`
- `public string? EndExclusive { get; init; }`
- `public bool Reverse { get; init; }`
- `public int PageSize { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public int ValuePreviewBudget { get; init; }`
- `public LatticePredicateNode? Predicate { get; init; }`
- `public string? IndexName { get; init; }`
- `public string? Tag { get; init; }`
- `public EntryScanMode Mode { get; init; }`

### `EntryScanResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 148).

`public sealed record EntryScanResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public IReadOnlyList<EntryRecord> Entries { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public static EntryScanResult Found(string treeId, IReadOnlyList<EntryRecord> entries, string? continuationToken)`
- `public static EntryScanResult NotFound(string treeId)`
- `public static EntryScanResult IndexNotFound(string treeId)`

### `LatticeStateCursorExpiredException`

[Source](../../src/lattice.api.abstractions/State/LatticeStateCursorExpiredException.cs) (line 10).

`public sealed class LatticeStateCursorExpiredException : Exception`

- `public LatticeStateCursorExpiredException()`
- `public LatticeStateCursorExpiredException(string message)`
- `public LatticeStateCursorExpiredException(string message, Exception innerException)`

### `NodeKind`

[Source](../../src/lattice.api.abstractions/State/Model/StateEnums.cs) (line 30).

`public enum NodeKind`

- `ShardRoot`
- `Internal`
- `Leaf`

### `NodeStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/NodeStateSummary.cs) (line 13).

`public sealed record NodeStateSummary`

- `public NodeKind Kind { get; init; }`
- `public required string NodeId { get; init; }`
- `public int ShardIndex { get; init; }`
- `public int Depth { get; init; }`
- `public string? KeyRangeLow { get; init; }`
- `public string? KeyRangeHigh { get; init; }`
- `public int ChildCount { get; init; }`
- `public long SubtreeKeyCount { get; init; }`
- `public long SubtreeTombstoneCount { get; init; }`
- `public bool SplitInProgress { get; init; }`
- `public bool HasMoreChildren { get; init; }`
- `public IReadOnlyList<NodeStateSummary> Children { get; init; }`

### `RevisionRetention`

[Source](../../src/lattice.api.abstractions/State/Model/RevisionRetention.cs) (line 14).

`public readonly record struct RevisionRetention`

- `public HistoryRetentionMode Mode { get; init; }`
- `public bool ValueRetained { get; init; }`

### `ShardHotness`

[Source](../../src/lattice.api.abstractions/State/Model/ShardHotness.cs) (line 11).

`public sealed record ShardHotness`

- `public int ShardIndex { get; init; }`
- `public double OpsPerSecond { get; init; }`
- `public long LiveKeys { get; init; }`
- `public bool SplitInProgress { get; init; }`

### `ShardStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/ShardStateSummary.cs) (line 9).

`public sealed record ShardStateSummary`

- `public int ShardIndex { get; init; }`
- `public int Depth { get; init; }`
- `public bool RootIsLeaf { get; init; }`
- `public long LiveKeys { get; init; }`
- `public long Tombstones { get; init; }`
- `public double OpsPerSecond { get; init; }`
- `public bool SplitInProgress { get; init; }`

### `ShardSummariesResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 61).

`public sealed record ShardSummariesResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public IReadOnlyList<ShardStateSummary> Shards { get; init; }`
- `public static ShardSummariesResult Found(string treeId, IReadOnlyList<ShardStateSummary> shards)`
- `public static ShardSummariesResult NotFound(string treeId)`

### `StateChangeKind`

[Source](../../src/lattice.api.abstractions/State/Model/StateChangeKind.cs) (line 10).

`public enum StateChangeKind`

- `Set`
- `Delete`
- `DeleteRange`

### `StateChangeNotification`

[Source](../../src/lattice.api.abstractions/State/Model/StateChangeNotification.cs) (line 23).

`public sealed record StateChangeNotification`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public string? EndExclusiveKey { get; init; }`
- `public StateChangeKind Kind { get; init; }`
- `public HybridLogicalClock Hlc { get; init; }`
- `public MutationCategory Category { get; init; }`
- `public required string Position { get; init; }`

### `StateObserveRequest`

[Source](../../src/lattice.api.abstractions/State/Model/StateObserveRequest.cs) (line 21).

`public sealed record StateObserveRequest`

- `public required string TreeId { get; init; }`
- `public string? StartInclusive { get; init; }`
- `public string? EndExclusive { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public bool IncludeMaintenance { get; init; }`

### `StateQueryStatus`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 6).

`public enum StateQueryStatus`

- `Found`
- `TreeNotFound`
- `KeyNotFound`
- `IndexNotFound`

### `StructureRequest`

[Source](../../src/lattice.api.abstractions/State/Model/StructureRequest.cs) (line 18).

`public sealed record StructureRequest`

- `public const int DefaultDepthLimit`
- `public const int MaxDepthLimit`
- `public const int DefaultMaxNodes`
- `public const int MaxNodeBudget`
- `public required string TreeId { get; init; }`
- `public int? ShardIndex { get; init; }`
- `public string? SubPathNodeId { get; init; }`
- `public int DepthLimit { get; init; }`
- `public int MaxNodes { get; init; }`
- `public int EffectiveDepthLimit`
- `public int EffectiveMaxNodes`

### `TagIndexCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 51).

`public sealed record TagIndexCatalogPage`

- `public IReadOnlyList<TagIndexStateSummary> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `TagIndexStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TagIndexStateSummary.cs) (line 11).

`public sealed record TagIndexStateSummary`

- `public required string IndexName { get; init; }`
- `public required string TreeId { get; init; }`
- `public int ShardCount { get; init; }`

### `TagMember`

[Source](../../src/lattice.api.abstractions/State/Model/TagMember.cs) (line 12).

`public sealed record TagMember`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`

### `TagMemberScanPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 116).

`public sealed record TagMemberScanPage`

- `public IReadOnlyList<TagMember> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `TagMemberScanRequest`

[Source](../../src/lattice.api.abstractions/State/Model/TagMemberScanRequest.cs) (line 16).

`public sealed record TagMemberScanRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public required string IndexName { get; init; }`
- `public required string Tag { get; init; }`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public int EffectivePageSize`

### `TagValueCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 72).

`public sealed record TagValueCatalogPage`

- `public IReadOnlyList<string> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `TreeCatalogEntry`

[Source](../../src/lattice.api.abstractions/State/Model/TreeCatalogEntry.cs) (line 12).

`public sealed record TreeCatalogEntry`

- `public required string TreeId { get; init; }`
- `public bool IsAlias { get; init; }`
- `public string? PhysicalTreeId { get; init; }`
- `public TreeLifecycleState Lifecycle { get; init; }`
- `public int ShardCount { get; init; }`
- `public required TreeConfigSummary Config { get; init; }`
- `public string? RestoreShadowOfTreeId { get; init; }`

### `TreeCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 11).

`public sealed record TreeCatalogPage`

- `public IReadOnlyList<TreeCatalogEntry> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `TreeConfigSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TreeConfigSummary.cs) (line 17).

`public sealed record TreeConfigSummary`

- `public int ShardCount { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public int? MaxLeafKeys { get; init; }`
- `public int? MaxInternalChildren { get; init; }`
- `public int WalPartitions { get; init; }`
- `public TimeSpan SoftDeleteDuration { get; init; }`

### `TreeLifecycleState`

[Source](../../src/lattice.api.abstractions/State/Model/StateEnums.cs) (line 6).

`public enum TreeLifecycleState`

- `Active`
- `SoftDeleted`
- `Purging`

### `TreeMetrics`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetrics.cs) (line 12).

`public sealed record TreeMetrics`

- `public required string TreeId { get; init; }`
- `public TreeLifecycleState Lifecycle { get; init; }`
- `public int ShardCount { get; init; }`
- `public long LiveKeys { get; init; }`
- `public long Tombstones { get; init; }`
- `public int MinDepth { get; init; }`
- `public int MaxDepth { get; init; }`
- `public int ShardsSplitting { get; init; }`
- `public int? ViewCount { get; init; }`
- `public long? ViewLagTotal { get; init; }`
- `public IReadOnlyList<ShardHotness> ShardHotness { get; init; }`
- `public bool DetailPaused { get; init; }`

### `TreeMetricsRequest`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetricsRequest.cs) (line 14).

`public sealed record TreeMetricsRequest`

- `public IReadOnlyList<string>? TreeIds { get; init; }`
- `public bool IncludeShardHotness { get; init; }`
- `public bool IncludeViewLag { get; init; }`
- `public bool IncludeSystemTrees { get; init; }`
- `public TimeSpan? SampleInterval { get; init; }`

### `TreeMetricsSnapshot`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetricsSnapshot.cs) (line 14).

`public sealed record TreeMetricsSnapshot`

- `public DateTimeOffset SampledAt { get; init; }`
- `public bool IsInitial { get; init; }`
- `public IReadOnlyList<TreeMetrics> Trees { get; init; }`
- `public IReadOnlyList<string> RemovedTreeIds { get; init; }`

### `TreeStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TreeStateSummary.cs) (line 10).

`public sealed record TreeStateSummary`

- `public required string TreeId { get; init; }`
- `public TreeLifecycleState Lifecycle { get; init; }`
- `public int ShardCount { get; init; }`
- `public long TotalLiveKeys { get; init; }`
- `public long TombstoneCount { get; init; }`
- `public int MinDepth { get; init; }`
- `public int MaxDepth { get; init; }`
- `public int ShardsSplitting { get; init; }`
- `public TreeConfigSummary? Config { get; init; }`
- `public DateTimeOffset SampledAt { get; init; }`

### `TreeStructureResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 94).

`public sealed record TreeStructureResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public IReadOnlyList<NodeStateSummary> Roots { get; init; }`
- `public bool Truncated { get; init; }`
- `public static TreeStructureResult Found(string treeId, IReadOnlyList<NodeStateSummary> roots, bool truncated)`
- `public static TreeStructureResult NotFound(string treeId)`

### `TreeSummaryResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 31).

`public sealed record TreeSummaryResult`

- `public StateQueryStatus Status { get; init; }`
- `public required string TreeId { get; init; }`
- `public TreeStateSummary? Summary { get; init; }`
- `public static TreeSummaryResult Found(TreeStateSummary summary)`
- `public static TreeSummaryResult NotFound(string treeId)`

### `ViewCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 31).

`public sealed record ViewCatalogPage`

- `public IReadOnlyList<ViewStateSummary> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `ViewStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/ViewStateSummary.cs) (line 10).

`public sealed record ViewStateSummary`

- `public required string ViewName { get; init; }`
- `public required string SourceTreeId { get; init; }`
- `public long? Lag { get; init; }`
- `public long? EntryCount { get; init; }`
- `public string? LastDigest { get; init; }`
- `public bool IsAggregation { get; init; }`
- `public bool IsHistory { get; init; }`
- `public string? ProjectionProviderKey { get; init; }`
- `public string? ProjectionVersion { get; init; }`

## `Orleans.Lattice.Api.Data`

Implemented by [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) and exposed remotely by [`Orleans.Lattice.Api.Data.Grpc`](../lattice.api.data.grpc/README.md). 10 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeDataApi` | interface | Transport-agnostic read-write facade over a cluster's lattice data plane. |
| `DataApiTypeAliases` | static class | Centralised Orleans serialization alias constants for every `Orleans.Lattice.Api.Data` type that participates in the wire format. |
| `DataAtomicBatch` | sealed record | A single-tree atomic write batch: the key / value `Upserts` and the `DeleteKeys` to commit all-or-nothing on one tree, so no reader ever observes a partial application. |
| `DataEntry` | sealed record | A single key / value entry. |
| `DataRangeDeleteRequest` | sealed record | Request to delete every authorized key in a bounded half-open key range `[`StartInclusive`, `EndExclusive`)` on a single tree. |
| `DataRangeDeleteResult` | sealed record | Result of a `DataRangeDeleteRequest`: the tree the delete ran against and the total number of keys tombstoned across the whole range. |
| `DataRangePage` | sealed record | One page of a bounded range read: the key-ordered `Entries` and, when more entries remain beyond this page, a `ContinuationToken` to pass back on the next `DataRangeRequest` to resume. |
| `DataRangeRequest` | sealed record | Request for a single-page, bounded range read over a tree's entries in ascending key order. |
| `DataReadResult` | sealed record | Result of a point read. |
| `DataTreeBatch` | sealed record | One participating tree's slice of a cross-tree atomic write: the logical `TreeId` to write into, the key / value `Upserts`, and the `DeleteKeys` to commit atomically on that tree. |

### `ILatticeDataApi`

[Source](../../src/lattice.api.abstractions/Data/ILatticeDataApi.cs) (line 32).

`public interface ILatticeDataApi`

- `Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<DataRangeDeleteResult> DeleteRangeAsync(DataRangeDeleteRequest request, CancellationToken cancellationToken = default)`
- `Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default)`
- `Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(IReadOnlyList<DataTreeBatch> batches, string operationId, CancellationToken cancellationToken = default)`
- `Task SetManyAsync(string treeId, IReadOnlyList<DataEntry> upserts, CancellationToken cancellationToken = default)`
- `Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)`
- `Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)`
- `Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)`
- `Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`
- `Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)`
- `Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)`

### `DataApiTypeAliases`

[Source](../../src/lattice.api.abstractions/Data/Model/DataApiTypeAliases.cs) (line 16).

`public static class DataApiTypeAliases`

- `public const string DataEntry`
- `public const string DataAtomicBatch`
- `public const string DataTreeBatch`
- `public const string DataRangeRequest`
- `public const string DataRangePage`
- `public const string DataReadResult`
- `public const string DataRangeDeleteRequest`
- `public const string DataRangeDeleteResult`

### `DataAtomicBatch`

[Source](../../src/lattice.api.abstractions/Data/Model/DataAtomicBatch.cs) (line 19).

`public sealed record DataAtomicBatch`

- `public List<DataEntry> Upserts { get; init; }`
- `public List<string> DeleteKeys { get; init; }`

### `DataEntry`

[Source](../../src/lattice.api.abstractions/Data/Model/DataEntry.cs) (line 18).

`public sealed record DataEntry`

- `public required string Key { get; init; }`
- `public byte[] Value { get; init; }`
- `public LatticeMergeMode? MergeMode { get; init; }`
- `public bool Raw { get; init; }`
- `public bool Equals(DataEntry? other)`
- `public override int GetHashCode()`

### `DataRangeDeleteRequest`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeDeleteRequest.cs) (line 20).

`public sealed record DataRangeDeleteRequest`

- `public required string TreeId { get; init; }`
- `public required string StartInclusive { get; init; }`
- `public required string EndExclusive { get; init; }`

### `DataRangeDeleteResult`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeDeleteResult.cs) (line 15).

`public sealed record DataRangeDeleteResult`

- `public required string TreeId { get; init; }`
- `public required int DeletedCount { get; init; }`

### `DataRangePage`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangePage.cs) (line 12).

`public sealed record DataRangePage`

- `public required string TreeId { get; init; }`
- `public IReadOnlyList<DataEntry> Entries { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `DataRangeRequest`

[Source](../../src/lattice.api.abstractions/Data/Model/DataRangeRequest.cs) (line 19).

`public sealed record DataRangeRequest`

- `public required string TreeId { get; init; }`
- `public string? StartInclusive { get; init; }`
- `public string? EndExclusive { get; init; }`
- `public int PageSize { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `DataReadResult`

[Source](../../src/lattice.api.abstractions/Data/Model/DataReadResult.cs) (line 12).

`public sealed record DataReadResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public bool Found { get; init; }`
- `public byte[] Value { get; init; }`
- `public LatticeMergeMode? MergeMode { get; init; }`
- `public bool Raw { get; init; }`
- `public bool Equals(DataReadResult? other)`
- `public override int GetHashCode()`

### `DataTreeBatch`

[Source](../../src/lattice.api.abstractions/Data/Model/DataTreeBatch.cs) (line 19).

`public sealed record DataTreeBatch`

- `public required string TreeId { get; init; }`
- `public List<DataEntry> Upserts { get; init; }`
- `public List<string> DeleteKeys { get; init; }`

## `Orleans.Lattice.Api.Auth`

Implemented by [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) and exposed remotely by [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md). 14 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeAuthAdmin` | interface | Transport-agnostic configuration and control facade over a cluster's authorization system: membership administration, policy administration, and policy introspection behind a single surface. |
| `AccessAuthenticationMode` | enum | The authentication posture a silo can authoritatively determine for itself from its own registrations, surfaced by `GetAccessModelAsync` so a UI can render the right create-form guidance and warn when access is effectively open. |
| `AccessModelDescriptor` | sealed record | A best-effort description of a cluster's access model, returned by `GetAccessModelAsync`, reporting what the silo can authoritatively determine from its own registrations: the active authentication mode, whether authorization rules are actually enforced, and whether an identity directory is available for validating candidate ids (together with the provider id and operator-facing explanation a create form should render). |
| `ApiAuthTypeAliases` | static class | Centralised Orleans serialization alias constants for every `Orleans.Lattice.Api.Auth` type that participates in the wire format. |
| `AuthEffectivePermissions` | sealed record | The result of `EffectivePermissionsAsync`: the authored authorization rules that name a subject directly or through its groups, resolved for dashboards and UX. |
| `AuthExplanation` | sealed record | The result of `ExplainAsync`: the authorization verdict for a subject / operation / scope, plus the supporting detail that explains it. |
| `AuthGroup` | sealed record | A group record as surfaced by the admin facade: a stable `GroupId` and an optional human-readable `DisplayName`. |
| `AuthGroupPage` | sealed record | One page of the group catalog. |
| `AuthPageRequest` | sealed record | Paging request for the admin facade's paged list endpoints (the group catalog, and the rule catalog whole or per tree; group-member lists are returned whole and take no paging request). |
| `AuthPolicyPosture` | sealed record | The opt-in authorization posture of a cluster: the two tier flags that are off by default and, while off, change nothing an operator can see without inspecting configuration. |
| `AuthRulePage` | sealed record | One page of the authorization rule catalog. |
| `DirectoryPrincipalDescriptor` | sealed record | A principal (user or group) as it exists in the configured external identity source, surfaced by `SearchDirectoryAsync` and `ResolveDirectoryPrincipalAsync`. |
| `DirectorySearchRequest` | sealed record | A typeahead / browse request against the configured identity directory, forwarded by `SearchDirectoryAsync` to validate candidate principal ids before an operator grants them access. |
| `DirectorySearchResult` | sealed record | One page of identity-directory search results returned by `SearchDirectoryAsync`: the matched principals, an optional continuation token for a 'load more' follow-up, and an `Available` flag distinguishing 'no principals matched' from 'no identity directory is configured'. |

### `ILatticeAuthAdmin`

[Source](../../src/lattice.api.abstractions/Auth/ILatticeAuthAdmin.cs) (line 34).

`public interface ILatticeAuthAdmin`

- `Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)`
- `Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)`
- `Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)`
- `Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)`
- `Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task<AuthExplanation> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)`
- `Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)`
- `Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)`
- `Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)`
- `Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)`

### `AccessAuthenticationMode`

[Source](../../src/lattice.api.abstractions/Auth/Model/AccessAuthenticationMode.cs) (line 18).

`public enum AccessAuthenticationMode`

- `Unknown`
- `Anonymous`
- `Claims`
- `Basic`

### `AccessModelDescriptor`

[Source](../../src/lattice.api.abstractions/Auth/Model/AccessModelDescriptor.cs) (line 21).

`public sealed record AccessModelDescriptor`

- `public AccessAuthenticationMode AuthenticationMode { get; init; }`
- `public bool RulesEnforced { get; init; }`
- `public bool DirectoryAvailable { get; init; }`
- `public required string DirectoryProviderId { get; init; }`
- `public required string DirectoryExplanation { get; init; }`
- `public bool LocalMembershipEffective { get; init; }`
- `public bool AllTreesGrantsEnabled { get; init; }`
- `public bool AccessAdministrationDelegationEnabled { get; init; }`
- `public bool DelegatedTenantAccessAdministrationEnabled { get; init; }`
- `public bool DirectoryValidationRequired { get; init; }`

### `ApiAuthTypeAliases`

[Source](../../src/lattice.api.abstractions/Auth/Model/ApiAuthTypeAliases.cs) (line 16).

`public static class ApiAuthTypeAliases`

- `public const string AuthGroup`
- `public const string AuthPageRequest`
- `public const string AuthGroupPage`
- `public const string AuthRulePage`
- `public const string AuthExplanation`
- `public const string AuthEffectivePermissions`
- `public const string AuthPolicyPosture`
- `public const string DirectorySearchRequest`
- `public const string DirectorySearchResult`
- `public const string DirectoryPrincipalDescriptor`
- `public const string AccessModelDescriptor`
- `public const string AuthGroupRef`
- `public const string AuthMemberRef`
- `public const string AuthRuleRef`
- `public const string AuthMemberEdge`
- `public const string AuthPutRule`
- `public const string AuthTreeRulesPage`
- `public const string AuthExplainQuery`
- `public const string AuthSubjectRef`
- `public const string AuthAck`
- `public const string AuthGroupResult`
- `public const string AuthRuleResult`
- `public const string AuthStringList`
- `public const string AuthRuleRemoved`
- `public const string AuthPrincipalRef`
- `public const string AuthDirectoryPrincipalResult`
- `public const string AuthAccessModelQuery`

### `AuthEffectivePermissions`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthEffectivePermissions.cs) (line 25).

`public sealed record AuthEffectivePermissions`

- `public required string SubjectId { get; init; }`
- `public IReadOnlyList<string> GroupIds { get; init; }`
- `public IReadOnlyList<LatticeAuthorizationRule> Rules { get; init; }`
- `public AuthPolicyPosture Posture { get; init; }`
- `public IReadOnlyList<TenantRuleLayer> RuleLayers { get; init; }`

### `AuthExplanation`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthExplanation.cs) (line 17).

`public sealed record AuthExplanation`

- `public required string SubjectId { get; init; }`
- `public IReadOnlyList<string> GroupIds { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public required LatticeScope Scope { get; init; }`
- `public bool Allowed { get; init; }`
- `public bool Filtered { get; init; }`
- `public string? Reason { get; init; }`
- `public LatticeEffect DefaultEffect { get; init; }`
- `public IReadOnlyList<LatticeAuthorizationRule> MatchedRules { get; init; }`
- `public AuthPolicyPosture Posture { get; init; }`
- `public TenantRuleLayer? DecidingLayer { get; init; }`
- `public string? DecidingRuleId { get; init; }`

### `AuthGroup`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthGroup.cs) (line 13).

`public sealed record AuthGroup`

- `public required string GroupId { get; init; }`
- `public string? DisplayName { get; init; }`

### `AuthGroupPage`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthGroupPage.cs) (line 11).

`public sealed record AuthGroupPage`

- `public IReadOnlyList<AuthGroup> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `AuthPageRequest`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthPageRequest.cs) (line 15).

`public sealed record AuthPageRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public bool ActiveTenantOnly { get; init; }`
- `public bool IncludeTenantGroups { get; init; }`
- `public int EffectivePageSize`

### `AuthPolicyPosture`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthPolicyPosture.cs) (line 21).

`public sealed record AuthPolicyPosture`

- `public bool AllTreesGrantsEnabled { get; init; }`
- `public bool AccessAdministrationDelegationEnabled { get; init; }`

### `AuthRulePage`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthRulePage.cs) (line 16).

`public sealed record AuthRulePage`

- `public IReadOnlyList<LatticeAuthorizationRule> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`
- `public string? Tenant { get; init; }`
- `public IReadOnlyList<string?> TenantRuleTenants { get; init; }`

### `DirectoryPrincipalDescriptor`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectoryPrincipalDescriptor.cs) (line 17).

`public sealed record DirectoryPrincipalDescriptor`

- `public required string Id { get; init; }`
- `public required string DisplayName { get; init; }`
- `public DirectoryPrincipalKind Kind { get; init; }`
- `public IReadOnlyDictionary<string, string>? Claims { get; init; }`

### `DirectorySearchRequest`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectorySearchRequest.cs) (line 15).

`public sealed record DirectorySearchRequest`

- `public string Term { get; init; }`
- `public DirectoryPrincipalKind? Kind { get; init; }`
- `public int PageSize { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `DirectorySearchResult`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectorySearchResult.cs) (line 13).

`public sealed record DirectorySearchResult`

- `public IReadOnlyList<DirectoryPrincipalDescriptor> Principals { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public bool Available { get; init; }`
- `public static DirectorySearchResult Unavailable { get; }`

## `Orleans.Lattice.Api.Backup`

Implemented by [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/api.md) and exposed remotely by [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/api.md). 9 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeBackupControl` | interface | Transport-agnostic backup / restore control facade. |
| `ILatticeBackupOperations` | interface | Accept-then-poll backup and restore. |
| `ApiBackupTypeAliases` | static class | Centralized Orleans serialization alias constants for the `Orleans.Lattice.Api.Backup` control-API package. |
| `BackupCatalogPage` | sealed record | One page of the backup catalog. |
| `BackupCatalogRequest` | sealed record | Paging request for the backup-catalog listing (`ListBackupsAsync`). |
| `BackupChainDescription` | sealed record | Describes a single backup and its restore chain: the target backup's `Manifest` plus the base-first ordered list of backup ids that must be replayed to restore it (`ChainBackupIds`). |
| `BackupInventoryReport` | sealed record | An operator-facing inventory summary of the whole backup catalog: absolute counts and byte totals read from the durable catalog, plus the process-lifetime aggregate capture-failure, restore-failure, and bytes-reclaimed tallies read from the in-memory metric registry. |
| `BackupScopeCapabilities` | sealed record | The read-only result of a capability probe: which backup / restore operations the current caller may perform over a single `BackupScopeSelector`, evaluated through the same fail-closed backup access gate the real operations use but with no side effects - no data is read, captured, restored, or deleted. |
| `BackupScopeStatus` | sealed record | An operator-facing status report for a single backup scope: whether each schedule kind is registered, the start and success timestamps and terminal outcome of the scope's most recent full and incremental capture cycles, the current chain depth of the scope's latest backup, and the runtime schedule intervals. |

### `ILatticeBackupControl`

[Source](../../src/lattice.api.abstractions/Backup/ILatticeBackupControl.cs) (line 21).

`public interface ILatticeBackupControl`

- `Task ScheduleBackupAsync(LatticeBackupScheduleRequest request, CancellationToken cancellationToken = default)`
- `Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default)`
- `Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default)`
- `Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(string backupId, string artifactId, CancellationToken cancellationToken = default)`
- `Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default)`
- `Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)`
- `Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)`
- `Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)`
- `Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)`
- `Task<BackupHealthConfig?> GetBackupHealthConfigAsync(string backupId, CancellationToken cancellationToken = default)`

### `ILatticeBackupOperations`

[Source](../../src/lattice.api.abstractions/Backup/ILatticeBackupOperations.cs) (line 31).

`public interface ILatticeBackupOperations : ILatticeOperations`

- `Task<LatticeOperationHandle> StartBackupAsync(LatticeBackupCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartBackupSetAsync(LatticeBackupSetCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartColdRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartBackupHealthCheckAsync(string backupId, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartCatalogRebuildAsync(string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartCatalogScrubAsync(bool pruneOrphans = false, string? operationId = null, CancellationToken cancellationToken = default)`

### `ApiBackupTypeAliases`

[Source](../../src/lattice.api.abstractions/Backup/Model/ApiBackupTypeAliases.cs) (line 16).

`public static class ApiBackupTypeAliases`

- `public const string AliasPrefix`
- `public const string BackupCatalogRequest`
- `public const string BackupCatalogPage`
- `public const string BackupChainDescription`
- `public const string BackupInventoryReport`
- `public const string BackupScopeStatus`
- `public const string BackupScopeCapabilities`

### `BackupCatalogPage`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupCatalogPage.cs) (line 13).

`public sealed record BackupCatalogPage`

- `public IReadOnlyList<BackupManifest> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`
- `public string? Tenant { get; init; }`

### `BackupCatalogRequest`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupCatalogRequest.cs) (line 31).

`public sealed record BackupCatalogRequest`

- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public bool OrderByCreatedDescending { get; init; }`
- `public BackupKind? Kind { get; init; }`
- `public string? NamePrefix { get; init; }`
- `public string? TreeId { get; init; }`
- `public string? CreatedPrefix { get; init; }`
- `public bool ActiveTenantOnly { get; init; }`

### `BackupChainDescription`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupChainDescription.cs) (line 16).

`public sealed record BackupChainDescription`

- `public BackupChainDescription(BackupManifest manifest, IReadOnlyList<string> chainBackupIds)`
- `public BackupManifest Manifest { get; init; }`
- `public IReadOnlyList<string> ChainBackupIds { get; init; }`

### `BackupInventoryReport`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupInventoryReport.cs) (line 14).

`public sealed record BackupInventoryReport`

- `public BackupInventoryReport(long totalBackupCount, long totalCatalogBytes, long fullBackupCount, long incrementalBackupCount, DateTimeOffset? oldestBackupUtc, DateTimeOffset? newestBackupUtc, long captureFailureCount, long restoreFailureCount, long bytesReclaimed)`
- `public long TotalBackupCount { get; init; }`
- `public long TotalCatalogBytes { get; init; }`
- `public long FullBackupCount { get; init; }`
- `public long IncrementalBackupCount { get; init; }`
- `public DateTimeOffset? OldestBackupUtc { get; init; }`
- `public DateTimeOffset? NewestBackupUtc { get; init; }`
- `public long CaptureFailureCount { get; init; }`
- `public long RestoreFailureCount { get; init; }`
- `public long BytesReclaimed { get; init; }`

### `BackupScopeCapabilities`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupScopeCapabilities.cs) (line 36).

`public sealed record BackupScopeCapabilities`

- `public required BackupScopeSelector Scope { get; init; }`
- `public bool CanList { get; init; }`
- `public bool CanCapture { get; init; }`
- `public bool CanCaptureIncremental { get; init; }`
- `public bool CanRestore { get; init; }`
- `public bool CanDelete { get; init; }`

### `BackupScopeStatus`

[Source](../../src/lattice.api.abstractions/Backup/Model/BackupScopeStatus.cs) (line 15).

`public sealed record BackupScopeStatus`

- `public BackupScopeStatus(BackupScopeSelector scope, bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, int chainDepth, TimeSpan? runtimeFullBackupInterval = null, TimeSpan? runtimeIncrementalBackupInterval = null)`
- `public BackupScopeSelector Scope { get; init; }`
- `public bool FullScheduleRegistered { get; init; }`
- `public bool IncrementalScheduleRegistered { get; init; }`
- `public DateTimeOffset? LastFullRunUtc { get; init; }`
- `public DateTimeOffset? LastFullSuccessUtc { get; init; }`
- `public DateTimeOffset? LastIncrementalRunUtc { get; init; }`
- `public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }`
- `public BackupScopeRunOutcome LastRunOutcome { get; init; }`
- `public int ChainDepth { get; init; }`
- `public TimeSpan? RuntimeFullBackupInterval { get; init; }`
- `public TimeSpan? RuntimeIncrementalBackupInterval { get; init; }`

## `Orleans.Lattice.Api.Schema`

Implemented by [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/api.md) and exposed remotely by [`Orleans.Lattice.Api.Schema.Grpc`](../lattice.api.schema.grpc/api.md). 5 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeSchemaComplianceOperations` | interface | Accept-then-poll schema compliance scans. |
| `ILatticeSchemaControl` | interface | Transport-agnostic schema-management control facade. |
| `ILatticeSchemaOperations` | interface | Accept-then-poll schema remediation and eager migration. |
| `ApiSchemaTypeAliases` | static class | Centralized Orleans serialization alias constants for the `Orleans.Lattice.Api.Schema` control-API package. |
| `LatticeSchemaCapabilities` | sealed record | The read-only result of a schema-management capability probe: caller permissions, independently installed host features, and advisory operation readiness for one tree. |

### `ILatticeSchemaComplianceOperations`

[Source](../../src/lattice.api.abstractions/Schema/ILatticeSchemaComplianceOperations.cs) (line 24).

`public interface ILatticeSchemaComplianceOperations : ILatticeOperations`

- `Task<LatticeOperationHandle> StartComplianceScanAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)`

### `ILatticeSchemaControl`

[Source](../../src/lattice.api.abstractions/Schema/ILatticeSchemaControl.cs) (line 28).

`public interface ILatticeSchemaControl`

- `Task<LatticeSchemaRuntimeRegistration> GetRegistryAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task RegisterSchemaAsync(string treeId, LatticeSchemaRuntimeRegistration registration, CancellationToken cancellationToken = default)`
- `Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)`
- `Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)`
- `Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)`

### `ILatticeSchemaOperations`

[Source](../../src/lattice.api.abstractions/Schema/ILatticeSchemaOperations.cs) (line 42).

`public interface ILatticeSchemaOperations : ILatticeOperations`

- `Task<LatticeOperationHandle> StartRemediationAsync(string treeId, LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartMigrationAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(string treeId, uint newTargetVersion, string? operationId = null, CancellationToken cancellationToken = default)`

### `ApiSchemaTypeAliases`

[Source](../../src/lattice.api.abstractions/Schema/Model/ApiSchemaTypeAliases.cs) (line 15).

`public static class ApiSchemaTypeAliases`

- `public const string AliasPrefix`
- `public const string LatticeSchemaCapabilities`

### `LatticeSchemaCapabilities`

[Source](../../src/lattice.api.abstractions/Schema/Model/LatticeSchemaCapabilities.cs) (line 40).

`public sealed record LatticeSchemaCapabilities`

- `public required string TreeId { get; init; }`
- `public bool CanViewPolicy { get; init; }`
- `public bool CanViewDeadLetters { get; init; }`
- `public bool CanViewVersionConfig { get; init; }`
- `public bool CanViewRemediationStatus { get; init; }`
- `public bool CanScanCompliance { get; init; }`
- `public bool CanManagePolicy { get; init; }`
- `public bool CanManageVersion { get; init; }`
- `public bool CanRemediate { get; init; }`
- `public bool SupportsEnforcement { get; init; }`
- `public bool SupportsVersioning { get; init; }`
- `public bool SupportsRemediation { get; init; }`
- `public bool SupportsCompliance { get; init; }`
- `public bool SupportsDeadLetters { get; init; }`
- `public bool SupportsRuntimeRegistration { get; init; }`
- `public bool CanStartMigration { get; init; }`
- `public bool CanStartRemediation { get; init; }`
- `public string? MigrationUnavailableReason { get; init; }`
- `public string? RemediationUnavailableReason { get; init; }`

## `Orleans.Lattice.Api.Replication`

Implemented by [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/api.md) and exposed remotely by [`Orleans.Lattice.Api.Replication.Grpc`](../lattice.api.replication.grpc/api.md). 17 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeReplicationControl` | interface | Transport-agnostic control facade for runtime per-tree cross-cluster replication configuration. |
| `ILatticeReplicationPeerAdmin` | interface | Transport-agnostic facade over the cluster-wide peer decommission verb. |
| `ILatticeReplicationStatus` | interface | Transport-agnostic, read-only facade over the per-peer replication link status a replication-enabled cluster tracks: for each replicated tree and each peer region, in each direction, the outstanding backlog, the error streak, the time since the last successful contact, the in-flight batch count, and a derived `ReplicationLinkHealth`. |
| `ApiReplicationTypeAliases` | static class | Centralized Orleans serialization alias constants for the replication control-API surface (the transport-agnostic contract in `Orleans.Lattice.Api.Abstractions` and the sibling gRPC / MCP bindings that reuse this registry). |
| `LatticeReplicationEngineNotHostedException` | sealed class (exception) | Thrown by `DecommissionPeerAsync` when the caller is authorized but no replication engine is hosted in this process, so there is no `ILatticeReplicationPeerDecommissioner` to carry the decommission out. |
| `ReplicationConfigReport` | sealed record | An operator-facing report of the effective replicated-tree set: one `ReplicationTreeConfigEntry` per enrolled tree the caller is authorized to see. |
| `ReplicationDecommissionPeerResult` | sealed record | The transport-agnostic outcome of a `DecommissionPeerAsync` call. |
| `ReplicationDisableResult` | sealed record | The transport-agnostic outcome of a `DisableReplicationAsync` call. |
| `ReplicationEnableResult` | sealed record | The transport-agnostic outcome of an `EnableReplicationAsync` call. |
| `ReplicationEnrollmentSource` | enum | Which enrollment source put a tree's reported replication configuration in force: the runtime config tree authored through `EnableReplicationAsync`, or the static replicated-tree map supplied as deployment configuration. |
| `ReplicationLinkDirection` | enum | Which way a replication link carries entries, relative to the region that produced the report. |
| `ReplicationLinkHealth` | enum | The health of one replication link, derived by the status facade from the link's backlog, error streak and time since last contact against configured thresholds. |
| `ReplicationLinkStallReason` | enum | The operator-actionable cause of a stalled replication link. |
| `ReplicationPeerStatusEntry` | sealed record | The status of one replication link - one tree, one peer region, one direction - carried in a `ReplicationPeerStatusPage`. |
| `ReplicationPeerStatusPage` | sealed record | One page of per-peer replication link status, returned by `GetPeerStatusAsync`. |
| `ReplicationPeerStatusQuery` | sealed record | The query for `GetPeerStatusAsync`: optional tree and peer-region filters, a page size, and an opaque continuation from a previous page. |
| `ReplicationTreeConfigEntry` | sealed record | A read-only projection of a single tree's effective replication configuration, carried in a `ReplicationConfigReport` returned by `GetReplicationConfigAsync`. |

### `ILatticeReplicationControl`

[Source](../../src/lattice.api.abstractions/Replication/ILatticeReplicationControl.cs) (line 31).

`public interface ILatticeReplicationControl`

- `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)`
- `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)`

### `ILatticeReplicationPeerAdmin`

[Source](../../src/lattice.api.abstractions/Replication/ILatticeReplicationPeerAdmin.cs) (line 13).

`public interface ILatticeReplicationPeerAdmin`

- `Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)`

### `ILatticeReplicationStatus`

[Source](../../src/lattice.api.abstractions/Replication/ILatticeReplicationStatus.cs) (line 47).

`public interface ILatticeReplicationStatus`

- `Task<ReplicationPeerStatusPage> GetPeerStatusAsync(ReplicationPeerStatusQuery query, CancellationToken cancellationToken = default)`

### `ApiReplicationTypeAliases`

[Source](../../src/lattice.api.abstractions/Replication/Model/ApiReplicationTypeAliases.cs) (line 17).

`public static class ApiReplicationTypeAliases`

- `public const string AliasPrefix`
- `public const string ReplicationConfigReport`
- `public const string ReplicationTreeConfigEntry`
- `public const string ReplicationEnableResult`
- `public const string ReplicationDisableResult`
- `public const string ReplicationEnrollmentSource`
- `public const string ReplicationPeerStatusQuery`
- `public const string ReplicationPeerStatusPage`
- `public const string ReplicationPeerStatusEntry`
- `public const string ReplicationLinkHealth`
- `public const string ReplicationLinkDirection`
- `public const string ReplicationLinkStallReason`
- `public const string ReplicationDecommissionPeerResult`
- `public const string LatticeReplicationEngineNotHostedException`

### `LatticeReplicationEngineNotHostedException`

[Source](../../src/lattice.api.abstractions/Replication/LatticeReplicationEngineNotHostedException.cs) (line 26).

`public sealed class LatticeReplicationEngineNotHostedException : InvalidOperationException, ILatticeDomainFault`

- `public LatticeReplicationEngineNotHostedException()`
- `public LatticeReplicationEngineNotHostedException(string message)`
- `public LatticeReplicationEngineNotHostedException(string message, Exception innerException)`

### `ReplicationConfigReport`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationConfigReport.cs) (line 27).

`public sealed record ReplicationConfigReport`

- `public ReplicationConfigReport(IReadOnlyList<ReplicationTreeConfigEntry> trees)`
- `public IReadOnlyList<ReplicationTreeConfigEntry> Trees { get; init; }`
- `public static ReplicationConfigReport Empty { get; }`

### `ReplicationDecommissionPeerResult`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationDecommissionPeerResult.cs) (line 16).

`public sealed record ReplicationDecommissionPeerResult`

- `public ReplicationDecommissionPeerResult(string peerClusterId, int treeCount, bool alreadyDecommissioned)`
- `public string PeerClusterId { get; init; }`
- `public int TreeCount { get; init; }`
- `public bool AlreadyDecommissioned { get; init; }`

### `ReplicationDisableResult`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationDisableResult.cs) (line 16).

`public sealed record ReplicationDisableResult`

- `public ReplicationDisableResult(string treeId, bool alreadyDisabled)`
- `public string TreeId { get; init; }`
- `public bool AlreadyDisabled { get; init; }`

### `ReplicationEnableResult`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationEnableResult.cs) (line 14).

`public sealed record ReplicationEnableResult`

- `public ReplicationEnableResult(string treeId, LatticeMergeMode mode, bool alreadyEnabled, bool bootstrapRequested)`
- `public string TreeId { get; init; }`
- `public LatticeMergeMode Mode { get; init; }`
- `public bool AlreadyEnabled { get; init; }`
- `public bool BootstrapRequested { get; init; }`

### `ReplicationEnrollmentSource`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationEnrollmentSource.cs) (line 21).

`public enum ReplicationEnrollmentSource`

- `Runtime`
- `Static`
- `RuntimeAndStatic`

### `ReplicationLinkDirection`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationLinkDirection.cs) (line 11).

`public enum ReplicationLinkDirection`

- `Outbound`
- `Inbound`

### `ReplicationLinkHealth`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationLinkHealth.cs) (line 10).

`public enum ReplicationLinkHealth`

- `Unknown`
- `Healthy`
- `Lagging`
- `Stalled`

### `ReplicationLinkStallReason`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationLinkStallReason.cs) (line 6).

`public enum ReplicationLinkStallReason`

- `ReseedRequired`
- `DeadLetterQueueFull`

### `ReplicationPeerStatusEntry`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationPeerStatusEntry.cs) (line 13).

`public sealed record ReplicationPeerStatusEntry`

- `public ReplicationPeerStatusEntry(string treeId, string peerRegionId, ReplicationLinkDirection direction, long entriesBehind, long bytesBehind, long consecutiveErrors, TimeSpan? timeSinceLastContact, long inFlight, ReplicationLinkHealth health)`
- `public string TreeId { get; init; }`
- `public string PeerRegionId { get; init; }`
- `public ReplicationLinkDirection Direction { get; init; }`
- `public long EntriesBehind { get; init; }`
- `public long BytesBehind { get; init; }`
- `public long ConsecutiveErrors { get; init; }`
- `public TimeSpan? TimeSinceLastContact { get; init; }`
- `public long InFlight { get; init; }`
- `public ReplicationLinkHealth Health { get; init; }`
- `public ReplicationLinkStallReason? StallReason { get; init; }`

### `ReplicationPeerStatusPage`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationPeerStatusPage.cs) (line 14).

`public sealed record ReplicationPeerStatusPage`

- `public ReplicationPeerStatusPage(string localRegionId, IReadOnlyList<ReplicationPeerStatusEntry> peers, string? continuationToken)`
- `public string LocalRegionId { get; init; }`
- `public IReadOnlyList<ReplicationPeerStatusEntry> Peers { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public static ReplicationPeerStatusPage Empty(string localRegionId)`

### `ReplicationPeerStatusQuery`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationPeerStatusQuery.cs) (line 11).

`public sealed record ReplicationPeerStatusQuery`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public static ReplicationPeerStatusQuery All { get; }`
- `public string? TreeId { get; init; }`
- `public string? PeerRegionId { get; init; }`
- `public int PageSize { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public int ResolvePageSize()`

### `ReplicationTreeConfigEntry`

[Source](../../src/lattice.api.abstractions/Replication/Model/ReplicationTreeConfigEntry.cs) (line 17).

`public sealed record ReplicationTreeConfigEntry`

- `public ReplicationTreeConfigEntry(string treeId, bool enabled, LatticeMergeMode? mode, bool ambiguous)`
- `public string TreeId { get; init; }`
- `public bool Enabled { get; init; }`
- `public LatticeMergeMode? Mode { get; init; }`
- `public bool Ambiguous { get; init; }`
- `public ReplicationEnrollmentSource Source { get; init; }`

## `Orleans.Lattice.Api.Telemetry`

Implemented by [`Orleans.Lattice.Api.Telemetry`](../lattice.api.telemetry/README.md) and exposed remotely by [`Orleans.Lattice.Api.Telemetry.Grpc`](../lattice.api.telemetry.grpc/README.md). 23 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeTelemetry` | interface | Transport-agnostic telemetry facade: the read-only time-series surface over the cluster's metrics backend. |
| `ApiTelemetryTypeAliases` | static class | Centralized Orleans serialization alias constants for the telemetry control-API surface (the transport-agnostic contract in `Orleans.Lattice.Api.Abstractions` and the sibling gRPC / MCP bindings that reuse this registry). |
| `TelemetryBackendException` | sealed class (exception) | Thrown when the metrics backend could not answer a curated query: it was unreachable, timed out, returned a non-success status, or returned a payload that could not be read. |
| `TelemetryBoundsViolation` | enum | The single reason a requested time range failed a catalogue entry's declared bounds, as reported by `Validate(TelemetryTimeRange, DateTimeOffset)`. |
| `TelemetryDataPoint` | readonly record struct | One (timestamp, value) sample of a returned time series. |
| `TelemetryInstrumentReference` | readonly record struct | A reference from a curated named query to one metric instrument it reads, carrying the instrument's own declared unit and measurement semantic. |
| `TelemetryLabel` | readonly record struct | One label on a returned time series: a name and its value, for example `tree` / `t/acme/orders` or `tenant` / `acme`. |
| `TelemetryMeasurementSemantic` | enum | What a metric instrument (or a named query built over one) actually measures, as distinct from what its name or a panel title implies. |
| `TelemetryQueryBounds` | readonly record struct | The bounds a curated named query declares for the caller-supplied parameters it accepts: the permitted resolution steps, the longest window, how far back a window may reach, and the sample-point budget. |
| `TelemetryQueryBoundsException` | sealed class (exception) | Thrown when a telemetry query request supplies a window that violates the bounds its catalogue entry declares. |
| `TelemetryQueryCatalog` | sealed record | The versioned set of curated named queries a caller may select from. |
| `TelemetryQueryDefinition` | sealed record | The server-side authoring shape of one curated named query: its client-facing `Descriptor` plus the query-language template that actually evaluates it. |
| `TelemetryQueryDescriptor` | sealed record | One entry in the curated named-query catalogue: everything a client needs to render and parameterise a panel, and nothing that lets it author a query. |
| `TelemetryQueryKind` | enum | Whether a curated named query is evaluated at a single instant or across a time range. |
| `TelemetryQueryNotFoundException` | sealed class (exception) | Thrown when a telemetry query is requested by an id that the caller's catalogue does not contain. |
| `TelemetryQueryParameters` | enum | The closed set of bounded parameters a curated named query may accept, declared per catalogue entry. |
| `TelemetryQueryRequest` | sealed record | A request to evaluate one curated named query: the query's catalogue id plus the bounded parameters that entry declares. |
| `TelemetryQueryResponse` | sealed record | The result of evaluating one curated named query: the series the backend returned, the shape they take, the window actually evaluated, and - always - the tenant scope the facade pinned. |
| `TelemetryResultKind` | enum | The shape of a query result, projected from the metrics backend's own result type into a closed, transport-neutral enum so a binding never has to interpret a free-form backend string. |
| `TelemetryTenantScope` | readonly record struct | The tenant scope a telemetry query was actually evaluated under, reported on every response. |
| `TelemetryTenantVisibility` | enum | The tenant visibility a telemetry query is evaluated under. |
| `TelemetryTimeRange` | readonly record struct | The evaluation window of a telemetry query: a start instant, an end instant, and a resolution step. |
| `TelemetryTimeSeries` | sealed record | One time series in a query result: its label set and its samples. |

### `ILatticeTelemetry`

[Source](../../src/lattice.api.abstractions/Telemetry/ILatticeTelemetry.cs) (line 43).

`public interface ILatticeTelemetry`

- `Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)`
- `Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)`

### `ApiTelemetryTypeAliases`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/ApiTelemetryTypeAliases.cs) (line 24).

`public static class ApiTelemetryTypeAliases`

- `public const string AliasPrefix`
- `public const string TelemetryQueryCatalog`
- `public const string TelemetryQueryDescriptor`
- `public const string TelemetryQueryDefinition`
- `public const string TelemetryQueryBounds`
- `public const string TelemetryQueryParameters`
- `public const string TelemetryQueryKind`
- `public const string TelemetryInstrumentReference`
- `public const string TelemetryMeasurementSemantic`
- `public const string TelemetryQueryRequest`
- `public const string TelemetryQueryResponse`
- `public const string TelemetryTimeRange`
- `public const string TelemetryTimeSeries`
- `public const string TelemetryDataPoint`
- `public const string TelemetryLabel`
- `public const string TelemetryTenantScope`
- `public const string TelemetryTenantVisibility`
- `public const string TelemetryResultKind`
- `public const string TelemetryBoundsViolation`

### `TelemetryBackendException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryBackendException.cs) (line 34).

`public sealed class TelemetryBackendException : Exception`

- `public TelemetryBackendException(string queryId, string message)`
- `public TelemetryBackendException(string queryId, string message, Exception innerException)`
- `public string QueryId { get; }`

### `TelemetryBoundsViolation`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryBoundsViolation.cs) (line 13).

`public enum TelemetryBoundsViolation`

- `None`
- `RangeNotAscending`
- `StepBelowMinimum`
- `StepAboveMaximum`
- `RangeTooLong`
- `LookbackTooOld`
- `TooManyPoints`

### `TelemetryDataPoint`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryDataPoint.cs) (line 20).

`public readonly record struct TelemetryDataPoint`

- `public TelemetryDataPoint(DateTimeOffset timestamp, double value)`
- `public DateTimeOffset Timestamp { get; init; }`
- `public double Value { get; init; }`
- `public bool IsFinite`

### `TelemetryInstrumentReference`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryInstrumentReference.cs) (line 19).

`public readonly record struct TelemetryInstrumentReference`

- `public TelemetryInstrumentReference(string name, string meter, string unit, TelemetryMeasurementSemantic semantic)`
- `public string Name { get; init; }`
- `public string Meter { get; init; }`
- `public string Unit { get; init; }`
- `public TelemetryMeasurementSemantic Semantic { get; init; }`

### `TelemetryLabel`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryLabel.cs) (line 13).

`public readonly record struct TelemetryLabel`

- `public TelemetryLabel(string name, string value)`
- `public string Name { get; init; }`
- `public string Value { get; init; }`

### `TelemetryMeasurementSemantic`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryMeasurementSemantic.cs) (line 20).

`public enum TelemetryMeasurementSemantic`

- `Unspecified`
- `PerOperation`
- `PerRecord`
- `PerBatch`
- `Duration`
- `Level`
- `Ratio`

### `TelemetryQueryBounds`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryBounds.cs) (line 27).

`public readonly record struct TelemetryQueryBounds`

- `public TimeSpan MinStep { get; init; }`
- `public TimeSpan MaxStep { get; init; }`
- `public TimeSpan DefaultStep { get; init; }`
- `public TimeSpan MaxRange { get; init; }`
- `public TimeSpan MaxLookback { get; init; }`
- `public int MaxPoints { get; init; }`
- `public static TelemetryQueryBounds Unbounded`
- `public bool IsUnbounded`
- `public TimeSpan EffectiveStep(TimeSpan requested)`
- `public TelemetryBoundsViolation Validate(TelemetryTimeRange range, DateTimeOffset now)`

### `TelemetryQueryBoundsException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryBoundsException.cs) (line 14).

`public sealed class TelemetryQueryBoundsException : Exception`

- `public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation)`
- `public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation, string message)`
- `public string QueryId { get; }`
- `public TelemetryBoundsViolation Violation { get; }`

### `TelemetryQueryCatalog`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryCatalog.cs) (line 32).

`public sealed record TelemetryQueryCatalog`

- `public required int Version { get; init; }`
- `public required IReadOnlyList<TelemetryQueryDescriptor> Queries { get; init; }`
- `public static TelemetryQueryCatalog Empty`
- `public int Count`
- `public bool TryGetQuery(string queryId, [NotNullWhen(true)] out TelemetryQueryDescriptor? descriptor)`
- `public bool Contains(string queryId)`

### `TelemetryQueryDefinition`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryDefinition.cs) (line 30).

`public sealed record TelemetryQueryDefinition`

- `public required TelemetryQueryDescriptor Descriptor { get; init; }`
- `public required string QueryTemplate { get; init; }`
- `public string QueryId`

### `TelemetryQueryDescriptor`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryDescriptor.cs) (line 36).

`public sealed record TelemetryQueryDescriptor`

- `public required string QueryId { get; init; }`
- `public required string Title { get; init; }`
- `public required string Description { get; init; }`
- `public required string Unit { get; init; }`
- `public TelemetryQueryKind Kind { get; init; }`
- `public TelemetryMeasurementSemantic Semantic { get; init; }`
- `public TelemetryQueryParameters Parameters { get; init; }`
- `public TelemetryQueryBounds Bounds { get; init; }`
- `public required IReadOnlyList<TelemetryInstrumentReference> Instruments { get; init; }`
- `public bool Accepts(TelemetryQueryParameters parameter)`
- `public bool ReadsInstrument(string instrumentName)`

### `TelemetryQueryKind`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryKind.cs) (line 11).

`public enum TelemetryQueryKind`

- `Instant`
- `Range`

### `TelemetryQueryNotFoundException`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryNotFoundException.cs) (line 16).

`public sealed class TelemetryQueryNotFoundException : Exception`

- `public TelemetryQueryNotFoundException(string queryId)`
- `public TelemetryQueryNotFoundException(string queryId, string message)`
- `public string QueryId { get; }`

### `TelemetryQueryParameters`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryParameters.cs) (line 30).

`public enum TelemetryQueryParameters`

- `None`
- `TimeRange`
- `Step`
- `TreeFilter`

### `TelemetryQueryRequest`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryRequest.cs) (line 38).

`public sealed record TelemetryQueryRequest`

- `public required string QueryId { get; init; }`
- `public TelemetryTimeRange Range { get; init; }`
- `public string? TreeId { get; init; }`
- `public TelemetryTenantVisibility RequestedVisibility { get; init; }`
- `public string? RequestedTenantId { get; init; }`

### `TelemetryQueryResponse`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryQueryResponse.cs) (line 32).

`public sealed record TelemetryQueryResponse`

- `public required string QueryId { get; init; }`
- `public required TelemetryTenantScope Scope { get; init; }`
- `public TelemetryResultKind ResultKind { get; init; }`
- `public required IReadOnlyList<TelemetryTimeSeries> Series { get; init; }`
- `public TelemetryTimeRange Range { get; init; }`
- `public int SeriesCount`
- `public bool IsEmpty`

### `TelemetryResultKind`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryResultKind.cs) (line 10).

`public enum TelemetryResultKind`

- `Empty`
- `Vector`
- `Matrix`
- `Scalar`

### `TelemetryTenantScope`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTenantScope.cs) (line 38).

`public readonly record struct TelemetryTenantScope`

- `public TelemetryTenantVisibility RequestedVisibility { get; init; }`
- `public TelemetryTenantVisibility EffectiveVisibility { get; init; }`
- `public string? TenantId { get; init; }`
- `public bool WasDowngraded`
- `public bool IsCrossTenant`
- `public static TelemetryTenantScope PinnedTo(string tenantId, TelemetryTenantVisibility requestedVisibility)`
- `public static TelemetryTenantScope AtRequestedTenant(string tenantId)`
- `public static TelemetryTenantScope AcrossAllTenants()`

### `TelemetryTenantVisibility`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTenantVisibility.cs) (line 34).

`public enum TelemetryTenantVisibility`

- `ActiveTenant`
- `AllTenants`
- `SingleTenant`

### `TelemetryTimeRange`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTimeRange.cs) (line 27).

`public readonly record struct TelemetryTimeRange`

- `public DateTimeOffset StartUtc { get; init; }`
- `public DateTimeOffset EndUtc { get; init; }`
- `public TimeSpan Step { get; init; }`
- `public static TelemetryTimeRange At(DateTimeOffset instant)`
- `public static TelemetryTimeRange Between(DateTimeOffset startUtc, DateTimeOffset endUtc, TimeSpan step)`
- `public TimeSpan Duration`
- `public bool IsInstant`
- `public bool IsAscending`
- `public long PointCount { get; }`
- `public TelemetryTimeRange WithStep(TimeSpan step)`

### `TelemetryTimeSeries`

[Source](../../src/lattice.api.abstractions/Telemetry/Model/TelemetryTimeSeries.cs) (line 12).

`public sealed record TelemetryTimeSeries`

- `public required IReadOnlyList<TelemetryLabel> Labels { get; init; }`
- `public required IReadOnlyList<TelemetryDataPoint> Points { get; init; }`
- `public static TelemetryTimeSeries Empty`
- `public bool TryGetLabel(string name, out string? value)`

## `Orleans.Lattice.Api.TreeAdmin`

Implemented by [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/api.md) and exposed remotely by [`Orleans.Lattice.Api.TreeAdmin.Grpc`](../lattice.api.treeadmin.grpc/README.md). 68 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeStorageUsageOperations` | interface | Accept-then-poll fresh storage usage. |
| `ILatticeTreeAdmin` | interface | Transport-agnostic tree administration control facade: one coherent, discoverable, authorized surface for whole-tree lifecycle and administration operations. |
| `ILatticeTreeAdminOperations` | interface | Accept-then-poll tree maintenance: a materialised-view rebuild or reconcile, a tag-index reconcile sweep, a WAL partition move, and a whole-tree orphaned-leaf audit or repair. |
| `ILatticeWalReclamation` | interface | Read-only WAL reclamation diagnostics for a tree: which durable pin holds the tree's write-ahead-log floor and whether it has wedged reclamation (issue #4195). |
| `ApiTreeAdminTypeAliases` | static class | Centralized Orleans serialization alias constants for the `Orleans.Lattice.Api.TreeAdmin` control-API package. |
| `BulkLoadOrderException` | sealed class (exception) | Thrown when a bulk-load chunk contains keys that are not in strictly ascending order. |
| `ClusterStorageUsageSummary` | sealed record | A cluster-wide storage accounting summary: total persisted bytes across every tree, split by surface, plus a per-tree breakdown. |
| `LatticeTreeAdminCapabilities` | sealed record | The read-only result of a tree-administration capability probe: which whole-tree administration operations the current caller may perform over a single tree, evaluated through the same fail-closed access gates the real operations use but with no side effects. |
| `ShardDiagnosticSnapshot` | sealed record | A single physical shard's diagnostic snapshot, carried as one row of a `TreeAdminDiagnosticReport`. |
| `ShardHotnessSnapshot` | sealed record | A single physical shard's read/write hotness sample, carried as one row of a `TreeHotnessReport`. |
| `ShardMapInspection` | sealed record | A read-only inspection of a tree's shard-map topology: how the virtual routing space maps onto physical shards. |
| `ShardProjectionDigestReport` | sealed record | A per-shard leaf-projection digest: a content hash plus lightweight counts that identify a shard's committed leaf state at a point in time. |
| `StorageUsageRefreshOperation` | static class | The tracked long-running operation that re-measures every tree's storage usage in the background (`StartStorageUsageRefreshAsync`): its kind, its phase and its unit. |
| `StorageUsageRefreshResults` | static class | Converts between the cluster totals of a storage-usage refresh and the string result map its tracked operation (`Kind`) records. |
| `TreeAdminDiagnosticReport` | sealed record | A whole-tree diagnostic report: one `ShardDiagnosticSnapshot` per physical shard plus tree-level roll-ups, sampled at a single instant. |
| `TreeAdminOperationKinds` | static class | The operation kinds the tree-administration facade runs as tracked long-running operations (`ILatticeTreeAdminOperations`). |
| `TreeAdminOperationPhases` | static class | The phase names a tree-administration operation reports in `Phase`. |
| `TreeAdminOperationResultKeys` | static class | The keys of the string result map a succeeded tree-administration operation carries in `Result`. |
| `TreeAdminOperationUnits` | static class | The unit names a tree-administration operation reports in `UnitName`. |
| `TreeAliasResolution` | sealed record | The alias state for a logical tree: which physical tree the logical id currently resolves to, and whether an explicit alias indirection is in effect. |
| `TreeBulkLoadChunkAck` | sealed record | The acknowledgement of a single appended bulk-load chunk from `AppendBulkLoadAsync`. |
| `TreeBulkLoadResult` | sealed record | The result of finalizing a resumable bulk-load session with `CommitBulkLoadAsync`. |
| `TreeBulkLoadSession` | sealed record | The result of opening a resumable bulk-load (tree creation) session with `BeginBulkLoadAsync`. |
| `TreeCompactionTriggerResult` | sealed record | The outcome of an operator-triggered tombstone-compaction pass scoped to a single physical shard. |
| `TreeConfigurationReport` | sealed record | A read-only snapshot of a tree's registry-backed configuration: its structural sizing pins, its alias target, and its per-tree runtime overrides (publish-events, projection-digest maintenance, durable-history retention, WAL retained-byte ceiling). |
| `TreeConfigurationUpdate` | sealed record | A partial update to a tree's per-tree registry configuration. |
| `TreeCreationResult` | sealed record | The result of an explicit tree-creation request. |
| `TreeDeletionStatus` | sealed record | The read-only status of a tree's soft-deletion lifecycle, returned by the tree delete / recover / purge verbs and the standalone status read. |
| `TreeExistenceResult` | sealed record | The result of a tree-existence check: whether the named logical tree is registered in the tree registry. |
| `TreeHistoryRetention` | sealed record | A tree's effective durable-history retention policy: the resolved `TreeHistoryRetentionMode` applied to LWW value bytes and the age-bound window after which a revision row expires. |
| `TreeHistoryRetentionMode` | enum | How much of an LWW (last-writer-wins) byte value a tree's durable-history revision rows retain per revision. |
| `TreeHotnessReport` | sealed record | A whole-tree read/write hotness report: one `ShardHotnessSnapshot` per physical shard plus tree-level roll-ups, sampled at a single instant. |
| `TreeNotEmptyException` | sealed class (exception) | Thrown when a bulk-load (tree creation) session is opened against a tree that is not empty. |
| `TreeOrphanedLeafDisposition` | enum | What the orphaned-leaf audit or repair decided about one descent-unreachable leaf. |
| `TreeOrphanedLeafFinding` | sealed record | One descent-unreachable leaf the orphaned-leaf audit or repair found, and what was decided about it. |
| `TreeOrphanedLeafGap` | sealed record | One region of a tree over which an orphaned-leaf audit or repair could not establish a verdict, and why (issue 3301). |
| `TreeOrphanedLeafGapReason` | enum | Why an orphaned-leaf audit or repair could not establish a verdict over some part of a tree. |
| `TreeOrphanedLeafReport` | sealed record | The verdict of an orphaned-leaf audit or repair over one tree: every leaf found spliced into a shard's sibling chain but unreachable by descent from that shard's root, and what was decided about each. |
| `TreeReshardStatus` | sealed record | The read-only status of a tree's online reshard, returned by the reshard trigger verb and the standalone status read. |
| `TreeResizePhase` | enum | The step an online resize has reached, reported by `Phase`. |
| `TreeResizeStatus` | sealed record | The read-only status of a tree's online resize, returned by the resize trigger and undo verbs and the standalone status read. |
| `TreeRestoreMode` | enum | How a tree-administration restore applied a backup to its target tree. |
| `TreeRestoreResult` | sealed record | The outcome of a tree-administration restore (`RestoreTreeAsync` or a single member of `RestoreTreeSetAsync`): the backup and target it applied, the resolved idempotency key, the base-first ordered chain of backup ids replayed, and how many entries were installed. |
| `TreeRestoreSetResult` | sealed record | The per-member outcome of a backup-set restore (`RestoreTreeSetAsync`): one `TreeRestoreResult` for every member tree this cluster restored, in the order the backup engine applied them. |
| `TreeShardMapView` | sealed record | A read-only view of a tree's registry-persisted shard map: the durable record of how the virtual routing space maps onto physical shards, as stored in the tree registry. |
| `TreeSnapshotMode` | enum | Whether a tree-administration snapshot locks the source tree during the copy. |
| `TreeSnapshotPhase` | enum | The step a tree snapshot has reached, reported by `Phase`. |
| `TreeSnapshotStatus` | sealed record | The read-only status of a tree's snapshot machinery, returned by the snapshot capture verb and the standalone status read. |
| `TreeStatsReport` | sealed record | A rolled-up statistics snapshot for one tree: topology and live-key counts from the cheap diagnostic projection, joined with the tree's storage byte breakdown. |
| `TreeStorageUsageSnapshot` | sealed record | One tree's storage byte breakdown, carried as a row of a `ClusterStorageUsageSummary`. |
| `TreeTagIndexCatalog` | sealed record | The result of listing the cluster's tag indexes: one `TreeTagIndexInfo` per index discovered in the tree registry (its backing membership tree carries the reserved `tag-` prefix). |
| `TreeTagIndexInfo` | sealed record | A single entry in the tag-index catalog listing, describing one tag index's identity, backing membership tree, and the subject trees it currently covers. |
| `TreeTagIndexStatus` | sealed record | Read-only status of a single tag index: its identity, backing membership tree, the subject trees it currently covers, and whether its background reconciliation coordinator is idle. |
| `TreeTagReconcileReport` | sealed record | The idempotent outcome of an operator-driven tag-index reconciliation sweep, pairing the index identity with the counts the core reconcile pass reports: how much of the covered keyspace was inspected and how many orphaned membership rows were repaired. |
| `TreeViewCatalog` | sealed record | The result of listing the cluster's runtime-registered materialised views: a snapshot of every durable runtime-view registration. |
| `TreeViewInfo` | sealed record | A single entry in the materialised-view catalog listing, describing one runtime-registered view's identity and shape. |
| `TreeViewReconcileResult` | sealed record | The outcome of a materialised-view reconcile: view anti-entropy that builds the expected view from current source state into a shadow generation, compares it against the live view via a content digest, and swaps the shadow in (repairing the view) only when they diverge. |
| `TreeViewStatus` | sealed record | The read-only status of a single materialised view, returned by the view status read and the view rebuild verb. |
| `TreeWalFloorHolder` | sealed record | The durable write-ahead-log materialiser pin holding a tree's WAL floor, and the leaf behind it. |
| `TreeWalFloorHolderState` | enum | The durable state of the leaf behind the pin holding a tree's WAL floor, read from the leaf's persisted checkpoint without activating it. |
| `TreeWalMoveOptions` | readonly record struct | Optional tunables for the tree-admin WAL move execute verb. |
| `TreeWalMoveOutcome` | enum | The classification of a WAL move or reclaim operation. |
| `TreeWalMovePlan` | sealed record | A read-only preview of a WAL move operation, returned by the tree-admin WAL move plan verb. |
| `TreeWalMoveReceipt` | sealed record | The result of a WAL move or reclaim operation, returned by the tree-admin WAL move execute and reclaim verbs. |
| `TreeWalPartitionPlacement` | readonly record struct | The provider placement of a single WAL partition: which storage provider key backs it and whether the reporting silo can currently resolve that key. |
| `TreeWalPlacement` | sealed record | A point-in-time view of a tree's durable WAL placement, returned by the tree-admin WAL placement inspection verb. |
| `TreeWalPlacementAudit` | sealed record | A health audit of a tree's WAL placement against the resolving silo's WAL storage provider catalog, returned by the tree-admin WAL placement audit verb. |
| `TreeWalReclamationReport` | sealed record | Which durable pin holds a tree's write-ahead-log floor, and whether that pin has wedged the tree's WAL reclamation. |

Member-level reference: the [shared contract declaration reference](../lattice.api.treeadmin/api.md#shared-contract-declaration-reference) in the `Orleans.Lattice.Api.TreeAdmin` API page lists the declaration line and every public member of each type above.

## `Orleans.Lattice.Api.TenantAdmin`

Implemented by [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/api.md) and exposed remotely by [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md). 64 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeTenantAccessAdmin` | interface | Transport-agnostic tenant access administration control facade: one coherent, discoverable, fail-closed surface for managing which subjects hold tenant-admin authority over a tenant - list, add, and remove. |
| `ILatticeTenantAdmin` | interface | Transport-agnostic tenant administration control facade: one coherent, discoverable, authorized surface for the tenant lifecycle - create, suspend, resume, and delete (delete cascading the tenant's trees). |
| `ILatticeTenantDirectoryAdmin` | interface | Transport-agnostic tenant directory facade for delegated tenant access administration: a tenant's own groups, their direct members, and the tenant's member set - the subjects who may act as the tenant on the data plane. |
| `ILatticeTenantGrantAdmin` | interface | Transport-agnostic cross-tenant grant administration control facade: one coherent, discoverable, fail-closed surface over the two-step agreement by which one tenant exposes a scope of its data to another - offer, approve, reject, revoke, and list. |
| `ILatticeTenantPolicyAdmin` | interface | Transport-agnostic tenant policy facade for delegated tenant access administration: the tenant-tier rules a tenant's administrators author on the tenant's own trees, a layer-aware explanation of any decision on those trees, a subject's effective permissions there, and the tenant's access posture. |
| `ILatticeTenantQuotaUsage` | interface | Transport-agnostic tenant usage-against-quota read facade: the one surface that reports what a tenant is actually consuming next to what it is allowed to consume. |
| `ILatticeTenantRegionAdmin` | interface | Transport-agnostic per-tenant region-residency control facade: one coherent, discoverable, fail-closed surface for the three region-residency operations - an operator authorizing a tenant's allowed region set, a tenant admin setting the tenant's residency within that allowed set, and reading a tenant's per-region status. |
| `ILatticeTenantSelfService` | interface | Transport-agnostic tenant self-awareness facade: the read-only counterpart to `ILatticeTenantAdmin`. |
| `ApiTenantAdminTypeAliases` | static class | Centralized Orleans serialization alias constants for the tenant-administration control-API surface (the transport-agnostic contract in `Orleans.Lattice.Api.Abstractions` and the sibling gRPC / MCP bindings that reuse this registry). |
| `ReservedTenantOperationException` | sealed class (exception) | Thrown when a tenant-administration operation targets the reserved legacy-adoption default tenant in a way the lifecycle forbids (suspend or delete). |
| `TenantAccessAdministrationDisabledException` | sealed class (exception) | Thrown by every operation of `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` - except the posture probe `GetPostureAsync` - while delegated tenant access administration is disabled on the cluster (`LatticeTenancyOptions.DelegatedAccessAdministrationEnabled` is `false`, the default). |
| `TenantAccessConfinementException` | sealed class (exception) | Thrown when a delegated tenant-access write would reach outside the tenant: a tenant group nested into a group the tenant does not own, an entry naming another tenant's group, or a tenant-tier rule on a tree or operation the tenant may not govern. |
| `TenantAccessConfinementRule` | enum | The confinement rule a delegated tenant-access write violated, carried by `Rule` so a transport binding or a user interface can explain the refusal without parsing its message. |
| `TenantAccessPageRequest` | sealed record | Paging request for the paged listings of the delegated tenant-access surface (a tenant's groups, its member set, and its rules). |
| `TenantAccessPosture` | sealed record | The result of `GetPostureAsync`: whether delegated tenant access administration is enabled, the caller's standing towards the tenant, and the tenant's per-tenant access caps with current usage. |
| `TenantAdminSubjectChangeResult` | sealed record | The result of adding or removing a single tenant-admin subject: the tenant and subject the call targeted, whether the call actually wrote to the membership set, and the resulting live subject set. |
| `TenantAdminSubjectReport` | sealed record | The read-only projection of a tenant's live tenant-admin subject set: the tenant id and the subject ids that currently hold tenant-admin authority over it, in ordinal order. |
| `TenantAlreadyExistsException` | sealed class (exception) | Thrown when a tenant-creation request targets a tenant id that is already registered. |
| `TenantCreationResult` | sealed record | The result of an explicit tenant-creation request. |
| `TenantDeletionResult` | sealed record | The result of a tenant deletion. |
| `TenantDescriptor` | sealed record | A compact, read-only description of a single tenant as surfaced by the tenant self-awareness facade (`ILatticeTenantSelfService`): the tenant id, its current lifecycle status, and whether it is the reserved legacy-adoption default tenant. |
| `TenantEffectivePermissions` | sealed record | The result of `EffectivePermissionsAsync`: the rules of both layers that name a subject directly or through one of its groups and govern the tenant's trees, grants and denies alike. |
| `TenantExplanation` | sealed record | The result of `ExplainAsync`: whether a subject may perform an operation on one of the tenant's trees, and which layer and rule decided it. |
| `TenantGrantAccess` | enum | The transport-agnostic set of operations a cross-tenant grant authorizes on the granting tenant's scope, as reported and requested through the tenant-administration control facade. |
| `TenantGrantChangeResult` | sealed record | The result of a cross-tenant grant lifecycle operation (offer, approve, reject, or revoke): the grant as it stands after the call, and whether the call actually wrote to the registry. |
| `TenantGrantDescriptor` | sealed record | One cross-tenant grant as reported by the tenant-administration control facade: the two tenants party to it, the scope of the granting tenant's data it covers, the operations it authorizes there, and its current lifecycle state. |
| `TenantGrantLifecycleState` | enum | The transport-agnostic lifecycle state of a cross-tenant grant as reported by the tenant-administration control facade. |
| `TenantGrantNotFoundException` | sealed class (exception) | Thrown when a cross-tenant grant lifecycle operation names a grant that does not exist: no grant has been offered by that granting tenant to that grantee tenant on that scope, or the granting tenant is not registered at all. |
| `TenantGrantReport` | sealed record | A tenant's cross-tenant grants, split into the two directions a tenant admin needs to see: the grants the tenant has `Issued` (offers it made, exposing its own data) and the grants `Received` (offers other tenants made to it - the inbox it approves or rejects from). |
| `TenantGrantTransitionException` | sealed class (exception) | Thrown when a cross-tenant grant lifecycle operation is not legal from the grant's current state: for example approving a grant that has already been revoked, revoking one that was never approved, or re-offering new terms over a live agreement the grantee approved on its existing terms. |
| `TenantGroupDescriptor` | sealed record | A tenant group as surfaced by `ILatticeTenantDirectoryAdmin`: its tenant-local `Name` and an optional human-readable `DisplayName`. |
| `TenantGroupMember` | sealed record | One direct member of a tenant group: a user, another group of the same tenant, or a cluster group. |
| `TenantGroupPage` | sealed record | One page of a tenant's groups, in ascending ordinal order of their local names. |
| `TenantGroupRemovalResult` | sealed record | The result of `RemoveGroupAsync`: whether the group existed and was removed, and everything the removal cascaded to. |
| `TenantLastAdminSubjectException` | sealed class (exception) | Thrown when removing a tenant-admin subject would empty the tenant's admin-subject set, orphaning the tenant: no subject would be left able to administer it, and it would disappear from every self-service surface, leaving only a platform operator able to reach it. |
| `TenantLastRegionException` | sealed class (exception) | Thrown when a tenant admin's residency change would remove the tenant's last resident region, leaving it with nowhere to hold its data. |
| `TenantLifecycleStatus` | enum | The transport-agnostic lifecycle status of a tenant as reported by the tenant-administration control facade. |
| `TenantMemberEntry` | sealed record | One entry of a tenant's member set or admin set: a user, one of the tenant's own groups, or a cluster group. |
| `TenantMemberPage` | sealed record | One page of a tenant's member set, in ascending ordinal order of entry id. |
| `TenantMembershipChangeResult` | sealed record | The result of adding or removing a tenant group member or a tenant member-set entry: the tenant, the group (when the call targeted one), the subject the call added or removed, and whether it actually wrote. |
| `TenantNotFoundException` | sealed class (exception) | Thrown when a tenant-administration operation (suspend, resume, or delete) targets a tenant that is not registered. |
| `TenantQuotaDimensionUsage` | readonly record struct | One quota dimension's usage-against-ceiling reading on a `TenantQuotaUsageReport`: the tenant's current consumption, the steady-state ceiling, the burst-adjusted admission ceiling, and both the live and accrued overage above the steady-state cap. |
| `TenantQuotaEnforcementScope` | enum | The aggregate a tenant's quota reading was taken against, reported on `EnforcementScope`. |
| `TenantQuotasDescriptor` | readonly record struct | The transport-agnostic projection of a tenant's resource quotas and burst allowance, authored through `SetTenantQuotasAsync` and reported on `Quotas`. |
| `TenantQuotasUpdateResult` | sealed record | The result of authoring a tenant's resource quotas through `SetTenantQuotasAsync`. |
| `TenantQuotaUsageReport` | sealed record | The read-only usage-against-quota report for one tenant returned by `GetQuotaUsageAsync`: the current consumption alongside the ceiling on every dimension the quota model defines, the burst headroom, and the accrued metered overage, qualified by the enforcement scope the reading was taken under. |
| `TenantRegionAuthorizationResult` | sealed record | The result of an operator authorizing a tenant's allowed region set: the tenant id and the resulting live allowed region ids (ordered). |
| `TenantRegionBackfillProgress` | sealed record | Observable receiver-side progress for one tenant region's backfill. |
| `TenantRegionBackfillTreeProgress` | sealed record | Observable receiver-side bootstrap progress for one tenant tree. |
| `TenantRegionLifecycleStatus` | enum | The transport-agnostic per-region lifecycle status of a tenant as reported by the tenant-administration control facade. |
| `TenantRegionNotAllowedException` | sealed class (exception) | Thrown when a tenant admin tries to set residency on a region that is not in the tenant's operator-authorized allowed set, or when an operator tries to revoke a region from the allowed set while the tenant is still resident there. |
| `TenantRegionStatusDescriptor` | sealed record | One row of a tenant's per-region residency report: a region id, its current `TenantRegionLifecycleStatus`, and whether it is in the tenant's operator-authorized allowed set. |
| `TenantRegionStatusReport` | sealed record | The result of reading a tenant's per-region residency status: the tenant id and one `TenantRegionStatusDescriptor` per region that is either in the tenant's allowed set or carries a non-`None` status, ordered by region id. |
| `TenantResidencyChangeResult` | sealed record | The result of a tenant admin setting a tenant's residency set: the tenant id, the regions this call began adding (transitioned to `Provisioning`) and removing (transitioned to `Draining`), and the resulting per-region status rows (ordered by region id). |
| `TenantRuleDraft` | sealed record | A tenant-tier authorization rule as authored through `PutRuleAsync`: a local rule id, a subject, a scope over the tenant's own keyspace, the operations it covers, and whether it grants or denies them. |
| `TenantRuleLayer` | enum | The policy layer an authorization rule belongs to, as seen by a tenant administrator. |
| `TenantRuleOrigin` | enum | Where a rule shown to a tenant administrator comes from, which decides how much of it the tenant may see. |
| `TenantRulePage` | sealed record | One page of the rules governing a tenant, ordered by tenant-local tree name (tenant-wide rules first) and then rule id. |
| `TenantRuleScopeKind` | enum | The extent of the keyspace a tenant-tier rule governs. |
| `TenantRuleView` | sealed record | A rule as a tenant administrator sees it: a tenant-tier rule (editable) or a platform rule that governs the tenant (read-only). |
| `TenantStatusChangeResult` | sealed record | The result of a tenant status transition (suspend or resume). |
| `TenantStatusReport` | sealed record | The read-only inspection report for one tenant returned by `GetTenantAsync`: the tenant's lifecycle status together with its per-region residency rows. |
| `TenantSubjectKind` | enum | The kind of principal a delegated tenant-access entry names: a group member, a tenant member-set or admin-set entry, or the subject of a tenant-tier rule. |
| `TenantSubjectResolution` | sealed record | The result of `ResolveSubjectAsync`: whether a subject is an admin or a member of a tenant, and through which admin-set and member-set entries. |

Member-level reference: the [shared contract declaration reference](../lattice.api.tenantadmin/api.md#shared-contract-declaration-reference) in the `Orleans.Lattice.Api.TenantAdmin` API page lists the declaration line and every public member of each type above.

## `Orleans.Lattice.Api.Apps`

Implemented by [`Orleans.Lattice.Api.Apps`](../lattice.api.apps/api.md) and exposed remotely by [`Orleans.Lattice.Api.Apps.Grpc`](../lattice.api.apps.grpc/api.md). 52 public types.

| Type | Kind | Summary |
|---|---|---|
| `ILatticeAppBridge` | interface | Transport-independent data access for an untrusted app UI, addressed only by app slug, install revision and app-local tree name. |
| `ILatticeAppCatalog` | interface | Transport-independent, administrative view of the app sources a cluster offers and the apps each one makes available, before anything is installed. |
| `ILatticeAppRoleBindings` | interface | Transport-independent re-binding of an installed app's roles to membership groups in the caller's active tenant, or the cluster when tenancy is disabled. |
| `ILatticeAppsControl` | interface | Transport-independent app lifecycle and consent management in the caller's active tenant, or the cluster when tenancy is disabled. |
| `ILatticeAppWorkspace` | interface | Transport-independent, per-user view of the enabled apps in which the caller holds a role in the active tenant, the sanitised description any role holder may see, and the installed version's presentation and UI assets. |
| `ApiAppsTypeAliases` | static class | Stable app control DTO aliases; each uses the reserved oia. prefix and at most six characters. |
| `AppBridgeException` | sealed class (exception) | Thrown by `ILatticeAppBridge` when a request is refused or cannot be served, carrying a closed `AppBridgeFailure` code a transport maps without parsing the message. |
| `AppBridgeFailure` | enum | The closed set of reasons an app bridge request fails. |
| `AppBridgePage` | sealed record | One page of entries scanned through the app bridge. |
| `AppBridgeTarget` | sealed record | The only address an app UI may use for data: its app slug, the install revision it was launched against, and an app-local tree name. |
| `AppBridgeValue` | sealed record | A key and its stored value, read through the app bridge. |
| `AppCapabilityCeilingDescriptor` | sealed record | Operator-approved operations and exception scopes. |
| `AppCatalog` | sealed record | Installed app summaries visible in the active isolation context; contains no tree ids. |
| `AppConsentReport` | sealed record | The effective version-pinned consent; scope references never expose composed physical ids. |
| `AppConsentUpdate` | sealed record | A full replacement consent for an exact installed version, using no composed tree ids. |
| `AppDescriptor` | sealed record | A pre-code-load manifest review and matching installation state. |
| `AppExceptionScope` | sealed record | An explicitly approved scope outside the structural app namespace. |
| `AppIconAsset` | sealed record | An app's presentation icon, returned only after its bytes matched the manifest digest. |
| `AppIconDescriptor` | sealed record | A reference to a declared icon asset: its bundle-relative path and pinned digest. |
| `AppInstallRequest` | sealed record | An exact source version, group bindings, and explicit consent to install without enabling. |
| `AppLifecycleResult` | sealed record | The outcome of an app lifecycle operation, with no physical tree identifiers. |
| `AppLifecycleState` | enum | Wire-visible installation lifecycle, independent of the registry implementation. |
| `AppMcpToolDescriptor` | sealed record | An inspectable manifest MCP tool declaration, not executable app code. |
| `AppPresentationDescriptor` | sealed record | How an app is shown, mirrored from the manifest's optional presentation section. |
| `AppProvenanceDescriptor` | sealed record | Inspectable source metadata, never proof of trust or authorization. |
| `AppReplicationDescriptor` | sealed record | Manifest replication intent for an app-local tree, never a composed physical id. |
| `AppRoleBindingDescriptor` | sealed record | An install-time binding from an app role to a membership group, never a user. |
| `AppRoleBindingsReport` | sealed record | An installed app's role-to-group bindings as read back after a re-binding. |
| `AppRoleBindingsUpdate` | sealed record | A full replacement of an installed app's role-to-group bindings for an exact installed version. |
| `AppRoleDescriptor` | sealed record | A manifest role's requested operations and app-local scopes, never composed physical ids. |
| `AppRoleScope` | sealed record | A requested role scope using an app-local tree name, never a composed physical id. |
| `AppSchemaDescriptor` | sealed record | Manifest schema intent for an app-local tree, never a composed physical id. |
| `AppSourceSummary` | sealed record | A configured app source, as the administrative catalogue reports it. |
| `AppSourceSummaryCapabilities` | enum | An API-layer mirror of what an app source supports, independent of the app engine's source types. |
| `AppSourceSummaryKind` | enum | An API-layer mirror of an app source's kind, independent of the app engine's source types. |
| `AppSubscriptionDescriptor` | sealed record | A requested change-feed subscription using an app-local tree name, never a composed physical id. |
| `AppSummary` | sealed record | A tree-free summary of an installed app in the active isolation context. |
| `AppTreeDescriptor` | sealed record | A manifest tree declaration. |
| `AppUiAsset` | sealed record | One file of an installed app's UI bundle, returned only after its bytes matched the manifest digest. |
| `AppUiAssetDescriptor` | sealed record | A file in an app UI bundle, pinned by its manifest digest. |
| `AppUiBridgeGrantDescriptor` | sealed record | One requested or consented app UI bridge grant: an operation, optionally restricted to a single app-local tree. |
| `AppUiDescriptor` | sealed record | The untrusted UI bundle an app may ship, mirrored from the manifest's optional ui section. |
| `AppUiScriptDescriptor` | sealed record | A script in an app UI bundle, loaded in declaration order. |
| `AvailableAppFilter` | enum | Which available apps a catalogue listing returns, relative to the active tenant's installations. |
| `AvailableAppPage` | sealed record | One page of available apps. |
| `AvailableAppQuery` | sealed record | A request for one page of the apps the configured sources make available. |
| `AvailableAppSummary` | sealed record | An app one source offers, joined with its installation in the active tenant. |
| `LatticeAppCatalogCapabilities` | sealed record | Advisory caller permissions for the app catalogue surface; every flag defaults to denied. |
| `LatticeAppsCapabilities` | sealed record | Advisory caller permissions for the app control surface; every flag defaults to denied. |
| `WorkspaceAppDescriptor` | sealed record | The sanitised, non-administrative description of an installed app that any of its role holders may read. |
| `WorkspaceAppSummary` | sealed record | An enabled app in which the caller holds at least one role in the active tenant. |
| `WorkspaceTreeDescriptor` | sealed record | An app's tree as any role holder may see it: its app-local name, shape and retention, and whether it was adopted, but never a physical or adopted tree id. |

Member-level reference: the [shared contract declaration reference](../lattice.api.apps/api.md#shared-contract-declaration-reference) in the `Orleans.Lattice.Api.Apps` API page lists the declaration line and every public member of each type above.

## Related

- [Package overview](README.md)
- [Architecture](architecture.md)
- [Long-running operations](operations.md)
