using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

/// <summary>
/// An <see cref="ILatticeStateClient"/> double whose discovery handlers receive
/// the full <see cref="CatalogRequest"/> so tests can assert on paging inputs and
/// route selection.
/// </summary>
internal sealed class FakeStateClientCapture : ILatticeStateClient
{
    public Func<CatalogRequest, Task<TreeCatalogPage>> OnListTrees { get; set; } =
        _ => Task.FromResult(new TreeCatalogPage());

    public Func<CatalogRequest, Task<ViewCatalogPage>> OnListViews { get; set; } =
        _ => Task.FromResult(new ViewCatalogPage());

    public Func<CatalogRequest, Task<TagIndexCatalogPage>> OnListTagIndexes { get; set; } =
        _ => Task.FromResult(new TagIndexCatalogPage());

    public Func<CatalogRequest, Task<TagValueCatalogPage>> OnListTagValues { get; set; } =
        _ => Task.FromResult(new TagValueCatalogPage());

    public Func<CatalogRequest, Task<CoveredTreeCatalogPage>> OnListCoveredTrees { get; set; } =
        _ => Task.FromResult(new CoveredTreeCatalogPage());

    public Func<CatalogRequest, Task<TagValueCatalogPage>> OnListIndexTags { get; set; } =
        _ => Task.FromResult(new TagValueCatalogPage());

    public Func<TagMemberScanRequest, Task<TagMemberScanPage>> OnScanTagMembers { get; set; } =
        _ => Task.FromResult(new TagMemberScanPage());

    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListTrees(request);

    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListViews(request);

    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListTagIndexes(request);

    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListTagValues(request);

    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListCoveredTrees(request);

    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListIndexTags(request);

    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
        => OnScanTagMembers(request);

    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new StructureResponse { TreeId = "t" });

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanResponse { TreeId = "t" });

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryGetResponse { TreeId = "t", Key = "k" });

    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryHistoryResponse { TreeId = "t", Key = "k" });

    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanCancelResponse());

    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeMetricsSnapshot());

    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ClusterInfo());

    public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new DeadLetterCountResponse { TreeId = request.TreeId, Count = 0 });

    public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new DeadLetterQueuePage());

    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync<StateChangeNotification>();

    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync<TreeMetricsSnapshot>();

#pragma warning disable CS1998 // async iterator with no await is intentional for an empty sequence
    private static async IAsyncEnumerable<T> EmptyAsync<T>()
    {
        yield break;
    }
#pragma warning restore CS1998
}
