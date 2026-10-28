---
agent_spec: "docs/agents/api/state.json"
---

# Public API

`Orleans.Lattice.Api.State` declares three public types of its own: the `LatticeApiStateOptions` options class, the `LatticeStateApiReadVisibility` posture enum, and the `AddLatticeStateApi` registration extension. Registration installs the package's implementations of three shared contracts - `ILatticeStateQuery`, `ILatticeStateObserver` and `ILatticeStateMetricsObserver` - whose interfaces, request/response records, result envelopes and `LatticeStateCursorExpiredException` live in `Orleans.Lattice.Api.Abstractions` under the `Orleans.Lattice.Api.State` namespace. Both inventories are listed below.

This page is the declaration reference. What each request and response means is explained request by request in [Surfaces](surfaces.md); the wire RPCs that carry them are in the [gRPC contract](grpc-contract.md), and the remote binding's own public types (client, wire envelopes, authorization seams) are in the [gRPC binding's public API](../lattice.api.state.grpc/api.md).

## Registration

`AddLatticeStateApi(this ISiloBuilder builder, Action<LatticeApiStateOptions>? configure = null)` binds `LatticeApiStateOptions` and registers the three facades as silo singletons. It must be called after `AddLattice(...)`: called first, it throws `InvalidOperationException`. A repeat call registers nothing new but still applies any supplied configure delegate. See [Setup](setup.md) for the full wiring and [Configuration](configuration.md) for the option values.

## Contract summary

| Contract | Methods | Surfaces |
|---|---|---|
| `ILatticeStateQuery` | `ListTreesAsync`, `ListViewsAsync`, `ListTagIndexesAsync`, `ListTagValuesAsync`, `ListCoveredTreesAsync`, `ListIndexTagsAsync`, `ScanTagMembersAsync`, `GetClusterInfoAsync` | [Discovery](surfaces.md#discovery), [Cluster info](surfaces.md#cluster-info) |
| | `GetTreeStructureAsync`, `GetTreeSummaryAsync`, `GetShardSummariesAsync`, `GetPhysicalShardCountAsync` | [Structure](surfaces.md#structure) |
| | `ScanEntriesAsync`, `GetEntryAsync`, `CancelScanAsync` | [Entries](surfaces.md#entries) |
| | `GetEntryHistoryAsync` | [Change history](surfaces.md#change-history) |
| | `GetDeadLetterCountAsync`, `ListDeadLettersAsync` | [Dead letters](surfaces.md#dead-letters) |
| `ILatticeStateObserver` | `ObserveAsync(StateObserveRequest, CancellationToken)` | [Change observation](surfaces.md#change-observation) |
| `ILatticeStateMetricsObserver` | `SampleAsync`, `ObserveAsync(TreeMetricsRequest, CancellationToken)` | [Metrics](surfaces.md#metrics) |

Tree-scoped reads return a result envelope (`TreeSummaryResult`, `EntryScanResult`, `EntryHistoryResult` and so on) whose `StateQueryStatus` distinguishes a found result from an unknown tree, key or tag index. A change-observation subscription that resumes from a cursor which has fallen out of the write-ahead-log retention window fails with `LatticeStateCursorExpiredException` rather than skipping changes silently.

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Surfaces](surfaces.md)
- [gRPC binding public API](../lattice.api.state.grpc/api.md)
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.State.LatticeStateApiReadVisibility`

[Source](../../src/lattice.api.state/LatticeApiStateOptions.cs) (line 18).

`public enum LatticeStateApiReadVisibility`

- `Auto = 0`
- `Enforced = 1`
- `Disabled = 2`

### `Orleans.Lattice.Api.State.LatticeApiStateOptions`

[Source](../../src/lattice.api.state/LatticeApiStateOptions.cs) (line 59).

`public sealed class LatticeApiStateOptions`

- `public int DefaultScanPageSize`
- `public int MaxScanPageSize`
- `public int DefaultScanValuePreviewBytes`
- `public int MaxScanValuePreviewBytes`
- `public int SingleEntryValuePreviewBytes`
- `public int DefaultHistoryPageSize`
- `public int MaxHistoryPageSize`
- `public int DefaultHistoryValuePreviewBytes`
- `public int MaxHistoryValuePreviewBytes`
- `public TimeSpan ChangeObservationPollInterval`
- `public int ChangeObservationPageSize`
- `public TimeSpan MetricsSampleInterval`
- `public LatticeStateApiReadVisibility ReadVisibility`

### `Orleans.Lattice.Api.State.LatticeApiStateServiceCollectionExtensions`

[Source](../../src/lattice.api.state/LatticeApiStateServiceCollectionExtensions.cs) (line 11).

`public static class LatticeApiStateServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeStateApi(this ISiloBuilder builder, Action<LatticeApiStateOptions>? configure = null)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record members are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.State.TreeCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 11).

`public sealed record TreeCatalogPage`

- `public IReadOnlyList<TreeCatalogEntry> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.ViewCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 31).

`public sealed record ViewCatalogPage`

- `public IReadOnlyList<ViewStateSummary> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.TagIndexCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 51).

`public sealed record TagIndexCatalogPage`

- `public IReadOnlyList<TagIndexStateSummary> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.TagValueCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 72).

`public sealed record TagValueCatalogPage`

- `public IReadOnlyList<string> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.CoveredTreeCatalogPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 94).

`public sealed record CoveredTreeCatalogPage`

- `public IReadOnlyList<string> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.TagMemberScanPage`

[Source](../../src/lattice.api.abstractions/State/CatalogResults.cs) (line 116).

`public sealed record TagMemberScanPage`

- `public IReadOnlyList<TagMember> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.ILatticeStateMetricsObserver`

[Source](../../src/lattice.api.abstractions/State/ILatticeStateMetricsObserver.cs) (line 12).

`public interface ILatticeStateMetricsObserver`

- `Task<TreeMetricsSnapshot> SampleAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<TreeMetricsSnapshot> ObserveAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.State.ILatticeStateObserver`

[Source](../../src/lattice.api.abstractions/State/ILatticeStateObserver.cs) (line 30).

`public interface ILatticeStateObserver`

- `IAsyncEnumerable<StateChangeNotification> ObserveAsync(StateObserveRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.State.ILatticeStateQuery`

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

### `Orleans.Lattice.Api.State.LatticeStateCursorExpiredException`

[Source](../../src/lattice.api.abstractions/State/LatticeStateCursorExpiredException.cs) (line 10).

`public sealed class LatticeStateCursorExpiredException : Exception`

- `public LatticeStateCursorExpiredException()`
- `public LatticeStateCursorExpiredException(string message)`
- `public LatticeStateCursorExpiredException(string message, Exception innerException)`

### `Orleans.Lattice.Api.State.ApiStateTypeAliases`

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

### `Orleans.Lattice.Api.State.CatalogRequest`

[Source](../../src/lattice.api.abstractions/State/Model/CatalogRequest.cs) (line 17).

`public sealed record CatalogRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize`
- `public string? PageToken`
- `public bool IncludeSystemTrees`
- `public bool IncludeViewStats`
- `public string? SourceTreeId`
- `public string? IndexName`
- `public int EffectivePageSize`

### `Orleans.Lattice.Api.State.ClusterInfo`

[Source](../../src/lattice.api.abstractions/State/Model/ClusterInfo.cs) (line 18).

`public sealed record ClusterInfo`

- `public string ClusterId`
- `public string ServiceId`

### `Orleans.Lattice.Api.State.ClusterInfoRequest`

[Source](../../src/lattice.api.abstractions/State/Model/ClusterInfoRequest.cs) (line 12).

`public sealed record ClusterInfoRequest`

### `Orleans.Lattice.Api.State.DeadLetterEntryRecord`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterEntryRecord.cs) (line 19).

`public sealed record DeadLetterEntryRecord`

- `public required string Key`
- `public byte[] ValuePreview`
- `public int ValueByteLength`
- `public required string Reason`
- `public DeadLetterSourceKind Source`
- `public DateTimeOffset TimestampUtc`
- `public bool PreviewTruncated`
- `public bool Equals(DeadLetterEntryRecord? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.State.DeadLetterQueuePage`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterQueuePage.cs) (line 12).

`public sealed record DeadLetterQueuePage`

- `public IReadOnlyList<DeadLetterEntryRecord> Entries`
- `public string? NextPageToken`

### `Orleans.Lattice.Api.State.DeadLetterQueueRequest`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterQueueRequest.cs) (line 13).

`public sealed record DeadLetterQueueRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public required string TreeId`
- `public int PageSize`
- `public string? PageToken`
- `public int EffectivePageSize`

### `Orleans.Lattice.Api.State.DeadLetterSourceKind`

[Source](../../src/lattice.api.abstractions/State/Model/DeadLetterSourceKind.cs) (line 11).

`public enum DeadLetterSourceKind`

- `Replication = 0`
- `Restore = 1`
- `LocalRejected = 2`
- `Unknown = 3`

### `Orleans.Lattice.Api.State.EntryHistoryBound`

[Source](../../src/lattice.api.abstractions/State/Model/EntryHistoryBound.cs) (line 11).

`public enum EntryHistoryBound`

- `BoundedByAge = 0`
- `Truncated = 1`
- `WalWindowFallback = 2`

### `Orleans.Lattice.Api.State.EntryHistoryRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryHistoryRequest.cs) (line 15).

`public sealed record EntryHistoryRequest`

- `public required string TreeId`
- `public required string Key`
- `public HybridLogicalClock? FromHlc`
- `public HybridLogicalClock? ToHlc`
- `public int Limit`
- `public string? ContinuationToken`
- `public int ValuePreviewBudget`
- `public bool Reverse`

### `Orleans.Lattice.Api.State.EntryRecord`

[Source](../../src/lattice.api.abstractions/State/Model/EntryRecord.cs) (line 13).

`public sealed record EntryRecord`

- `public required string Key`
- `public byte[] ValuePreview`
- `public int ValueLength`
- `public bool Truncated`
- `public HybridLogicalClock Hlc`
- `public bool IsTombstone`
- `public long ExpiresAtTicks`
- `public string? CrdtShape`
- `public IReadOnlyList<CrdtMemberValue> CurrentMembers`
- `public LatticeMergeMode? MergeMode`
- `public bool Raw`
- `public bool Equals(EntryRecord? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.State.EntryRevisionRecord`

[Source](../../src/lattice.api.abstractions/State/Model/EntryRevisionRecord.cs) (line 15).

`public sealed record EntryRevisionRecord`

- `public HybridLogicalClock Hlc`
- `public HistoryRowKind Kind`
- `public MutationCategory Category`
- `public required string SourceKey`
- `public string? OriginClusterId`
- `public byte[]? ValuePreview`
- `public int ValueLength`
- `public bool Truncated`
- `public long ValueHash`
- `public byte[]? Delta`
- `public LatticeMergeMode Mode`
- `public IReadOnlyList<CrdtMemberChange> MemberChanges`
- `public RevisionRetention Retention`
- `public string? EndKey`
- `public bool Equals(EntryRevisionRecord? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.State.EntryScanCancelRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanCancelRequest.cs) (line 15).

`public sealed record EntryScanCancelRequest`

- `public required string TreeId`
- `public string? ContinuationToken`

### `Orleans.Lattice.Api.State.EntryScanMode`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanMode.cs) (line 12).

`public enum EntryScanMode`

- `Snapshot = 0`
- `Live = 1`
- `LivePointInTime = 2`

### `Orleans.Lattice.Api.State.EntryScanRequest`

[Source](../../src/lattice.api.abstractions/State/Model/EntryScanRequest.cs) (line 19).

`public sealed record EntryScanRequest`

- `public required string TreeId`
- `public string? StartInclusive`
- `public string? EndExclusive`
- `public bool Reverse`
- `public int PageSize`
- `public string? ContinuationToken`
- `public int ValuePreviewBudget`
- `public LatticePredicateNode? Predicate`
- `public string? IndexName`
- `public string? Tag`
- `public EntryScanMode Mode`

### `Orleans.Lattice.Api.State.NodeStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/NodeStateSummary.cs) (line 13).

`public sealed record NodeStateSummary`

- `public NodeKind Kind`
- `public required string NodeId`
- `public int ShardIndex`
- `public int Depth`
- `public string? KeyRangeLow`
- `public string? KeyRangeHigh`
- `public int ChildCount`
- `public long SubtreeKeyCount`
- `public long SubtreeTombstoneCount`
- `public bool SplitInProgress`
- `public bool HasMoreChildren`
- `public IReadOnlyList<NodeStateSummary> Children`

### `Orleans.Lattice.Api.State.RevisionRetention`

[Source](../../src/lattice.api.abstractions/State/Model/RevisionRetention.cs) (line 14).

`public readonly record struct RevisionRetention`

- `public HistoryRetentionMode Mode`
- `public bool ValueRetained`

### `Orleans.Lattice.Api.State.ShardHotness`

[Source](../../src/lattice.api.abstractions/State/Model/ShardHotness.cs) (line 11).

`public sealed record ShardHotness`

- `public int ShardIndex`
- `public double OpsPerSecond`
- `public long LiveKeys`
- `public bool SplitInProgress`

### `Orleans.Lattice.Api.State.ShardStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/ShardStateSummary.cs) (line 9).

`public sealed record ShardStateSummary`

- `public int ShardIndex`
- `public int Depth`
- `public bool RootIsLeaf`
- `public long LiveKeys`
- `public long Tombstones`
- `public double OpsPerSecond`
- `public bool SplitInProgress`

### `Orleans.Lattice.Api.State.StateChangeKind`

[Source](../../src/lattice.api.abstractions/State/Model/StateChangeKind.cs) (line 10).

`public enum StateChangeKind`

- `Set = 0`
- `Delete = 1`
- `DeleteRange = 2`

### `Orleans.Lattice.Api.State.StateChangeNotification`

[Source](../../src/lattice.api.abstractions/State/Model/StateChangeNotification.cs) (line 23).

`public sealed record StateChangeNotification`

- `public required string TreeId`
- `public required string Key`
- `public string? EndExclusiveKey`
- `public StateChangeKind Kind`
- `public HybridLogicalClock Hlc`
- `public MutationCategory Category`
- `public required string Position`

### `Orleans.Lattice.Api.State.TreeLifecycleState`

[Source](../../src/lattice.api.abstractions/State/Model/StateEnums.cs) (line 6).

`public enum TreeLifecycleState`

- `Active = 0`
- `SoftDeleted = 1`
- `Purging = 2`

### `Orleans.Lattice.Api.State.NodeKind`

[Source](../../src/lattice.api.abstractions/State/Model/StateEnums.cs) (line 30).

`public enum NodeKind`

- `ShardRoot = 0`
- `Internal = 1`
- `Leaf = 2`

### `Orleans.Lattice.Api.State.StateObserveRequest`

[Source](../../src/lattice.api.abstractions/State/Model/StateObserveRequest.cs) (line 21).

`public sealed record StateObserveRequest`

- `public required string TreeId`
- `public string? StartInclusive`
- `public string? EndExclusive`
- `public string? ContinuationToken`
- `public bool IncludeMaintenance`

### `Orleans.Lattice.Api.State.StructureRequest`

[Source](../../src/lattice.api.abstractions/State/Model/StructureRequest.cs) (line 18).

`public sealed record StructureRequest`

- `public const int DefaultDepthLimit`
- `public const int MaxDepthLimit`
- `public const int DefaultMaxNodes`
- `public const int MaxNodeBudget`
- `public required string TreeId`
- `public int? ShardIndex`
- `public string? SubPathNodeId`
- `public int DepthLimit`
- `public int MaxNodes`
- `public int EffectiveDepthLimit`
- `public int EffectiveMaxNodes`

### `Orleans.Lattice.Api.State.TagIndexStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TagIndexStateSummary.cs) (line 11).

`public sealed record TagIndexStateSummary`

- `public required string IndexName`
- `public required string TreeId`
- `public int ShardCount`

### `Orleans.Lattice.Api.State.TagMember`

[Source](../../src/lattice.api.abstractions/State/Model/TagMember.cs) (line 12).

`public sealed record TagMember`

- `public required string TreeId`
- `public required string Key`

### `Orleans.Lattice.Api.State.TagMemberScanRequest`

[Source](../../src/lattice.api.abstractions/State/Model/TagMemberScanRequest.cs) (line 16).

`public sealed record TagMemberScanRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public required string IndexName`
- `public required string Tag`
- `public int PageSize`
- `public string? PageToken`
- `public int EffectivePageSize`

### `Orleans.Lattice.Api.State.TreeCatalogEntry`

[Source](../../src/lattice.api.abstractions/State/Model/TreeCatalogEntry.cs) (line 12).

`public sealed record TreeCatalogEntry`

- `public required string TreeId`
- `public bool IsAlias`
- `public string? PhysicalTreeId`
- `public TreeLifecycleState Lifecycle`
- `public int ShardCount`
- `public required TreeConfigSummary Config`
- `public string? RestoreShadowOfTreeId`

### `Orleans.Lattice.Api.State.TreeConfigSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TreeConfigSummary.cs) (line 17).

`public sealed record TreeConfigSummary`

- `public int ShardCount`
- `public int VirtualShardCount`
- `public int? MaxLeafKeys`
- `public int? MaxInternalChildren`
- `public int WalPartitions`
- `public TimeSpan SoftDeleteDuration`

### `Orleans.Lattice.Api.State.TreeMetrics`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetrics.cs) (line 12).

`public sealed record TreeMetrics`

- `public required string TreeId`
- `public TreeLifecycleState Lifecycle`
- `public int ShardCount`
- `public long LiveKeys`
- `public long Tombstones`
- `public int MinDepth`
- `public int MaxDepth`
- `public int ShardsSplitting`
- `public int? ViewCount`
- `public long? ViewLagTotal`
- `public IReadOnlyList<ShardHotness> ShardHotness`
- `public bool DetailPaused`

### `Orleans.Lattice.Api.State.TreeMetricsRequest`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetricsRequest.cs) (line 14).

`public sealed record TreeMetricsRequest`

- `public IReadOnlyList<string>? TreeIds`
- `public bool IncludeShardHotness`
- `public bool IncludeViewLag`
- `public bool IncludeSystemTrees`
- `public TimeSpan? SampleInterval`

### `Orleans.Lattice.Api.State.TreeMetricsSnapshot`

[Source](../../src/lattice.api.abstractions/State/Model/TreeMetricsSnapshot.cs) (line 14).

`public sealed record TreeMetricsSnapshot`

- `public DateTimeOffset SampledAt`
- `public bool IsInitial`
- `public IReadOnlyList<TreeMetrics> Trees`
- `public IReadOnlyList<string> RemovedTreeIds`

### `Orleans.Lattice.Api.State.TreeStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/TreeStateSummary.cs) (line 10).

`public sealed record TreeStateSummary`

- `public required string TreeId`
- `public TreeLifecycleState Lifecycle`
- `public int ShardCount`
- `public long TotalLiveKeys`
- `public long TombstoneCount`
- `public int MinDepth`
- `public int MaxDepth`
- `public int ShardsSplitting`
- `public TreeConfigSummary? Config`
- `public DateTimeOffset SampledAt`

### `Orleans.Lattice.Api.State.ViewStateSummary`

[Source](../../src/lattice.api.abstractions/State/Model/ViewStateSummary.cs) (line 10).

`public sealed record ViewStateSummary`

- `public required string ViewName`
- `public required string SourceTreeId`
- `public long? Lag`
- `public long? EntryCount`
- `public string? LastDigest`
- `public bool IsAggregation`
- `public bool IsHistory`
- `public string? ProjectionProviderKey`
- `public string? ProjectionVersion`

### `Orleans.Lattice.Api.State.StateQueryStatus`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 6).

`public enum StateQueryStatus`

- `Found = 0`
- `TreeNotFound = 1`
- `KeyNotFound = 2`
- `IndexNotFound = 3`

### `Orleans.Lattice.Api.State.TreeSummaryResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 31).

`public sealed record TreeSummaryResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public TreeStateSummary? Summary`
- `public static TreeSummaryResult Found(TreeStateSummary summary)`
- `public static TreeSummaryResult NotFound(string treeId)`

### `Orleans.Lattice.Api.State.ShardSummariesResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 61).

`public sealed record ShardSummariesResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public IReadOnlyList<ShardStateSummary> Shards`
- `public static ShardSummariesResult Found(string treeId, IReadOnlyList<ShardStateSummary> shards)`
- `public static ShardSummariesResult NotFound(string treeId)`

### `Orleans.Lattice.Api.State.TreeStructureResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 94).

`public sealed record TreeStructureResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public IReadOnlyList<NodeStateSummary> Roots`
- `public bool Truncated`
- `public static TreeStructureResult Found(string treeId, IReadOnlyList<NodeStateSummary> roots, bool truncated)`
- `public static TreeStructureResult NotFound(string treeId)`

### `Orleans.Lattice.Api.State.EntryScanResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 148).

`public sealed record EntryScanResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public IReadOnlyList<EntryRecord> Entries`
- `public string? ContinuationToken`
- `public static EntryScanResult Found(string treeId, IReadOnlyList<EntryRecord> entries, string? continuationToken)`
- `public static EntryScanResult NotFound(string treeId)`
- `public static EntryScanResult IndexNotFound(string treeId)`

### `Orleans.Lattice.Api.State.EntryDetailResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 207).

`public sealed record EntryDetailResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public required string Key`
- `public EntryRecord? Entry`
- `public static EntryDetailResult Found(string treeId, EntryRecord entry)`
- `public static EntryDetailResult TreeNotFound(string treeId, string key)`
- `public static EntryDetailResult KeyNotFound(string treeId, string key)`

### `Orleans.Lattice.Api.State.EntryHistoryResult`

[Source](../../src/lattice.api.abstractions/State/StateQueryResults.cs) (line 258).

`public sealed record EntryHistoryResult`

- `public StateQueryStatus Status`
- `public required string TreeId`
- `public required string Key`
- `public IReadOnlyList<EntryRevisionRecord> Revisions`
- `public string? ContinuationToken`
- `public EntryHistoryBound Bound`
- `public HybridLogicalClock EarliestAvailable`
- `public static EntryHistoryResult Found(string treeId, string key, IReadOnlyList<EntryRevisionRecord> revisions, string? continuationToken, EntryHistoryBound bound, HybridLogicalClock earliestAvailable)`
- `public static EntryHistoryResult TreeNotFound(string treeId, string key)`
- `public static EntryHistoryResult KeyNotFound(string treeId, string key)`
