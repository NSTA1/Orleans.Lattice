using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The read-only state-API surface the explorer consumes, mirroring
/// <see cref="LatticeStateApiGrpcClient"/> one-to-one. Abstracting it behind an
/// interface keeps the connection layer testable without a live gRPC server and
/// preserves the hard rule that the explorer only ever talks to the state API,
/// never to grains.
/// </summary>
public interface ILatticeStateClient
{
    /// <summary>Lists the registered trees as a deterministic, paged catalog.</summary>
    Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists the materialised views as a deterministic, paged catalog.</summary>
    Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists the tag-index membership trees as a deterministic, paged catalog.</summary>
    Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists the distinct tag values of one tag index as a deterministic, paged catalog.</summary>
    Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists the subject trees a tag index covers as a deterministic, paged catalog.</summary>
    Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists a tag index's distinct tags across every covered tree as a deterministic, paged catalog.</summary>
    Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Scans the live members of a tag across a tag index as a deterministic, paged result.</summary>
    Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default);

    /// <summary>Returns the structural node graph of a tree.</summary>
    Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default);

    /// <summary>Scans a key-ordered page of entries under a snapshot-isolated cursor.</summary>
    Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default);

    /// <summary>Returns the full record for a single key.</summary>
    Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default);

    /// <summary>Returns a page of a single key's change-history timeline.</summary>
    Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default);

    /// <summary>Releases a snapshot scan cursor named by a continuation token.</summary>
    Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default);

    /// <summary>Returns a single live metrics snapshot.</summary>
    Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default);

    /// <summary>Returns identity and metadata for the connected cluster.</summary>
    Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default);

    /// <summary>Counts a tree's strict-mode dead-letter entries.</summary>
    Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default);

    /// <summary>Lists a tree's strict-mode dead-letter queue as a deterministic, paged read.</summary>
    Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to live change notifications for a tree.</summary>
    IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to live metric snapshots for a tree.</summary>
    IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default);
}
