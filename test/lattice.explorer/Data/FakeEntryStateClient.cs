using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Data;

/// <summary>
/// An <see cref="ILatticeStateClient"/> double whose entry handlers receive the
/// full request so tests can assert on paging inputs and preview budgets.
/// </summary>
internal sealed class FakeEntryStateClient : ILatticeStateClient
{
    public EntryScanRequest? LastScan { get; private set; }
    public EntryGetRequest? LastGet { get; private set; }
    public CatalogRequest? LastTagIndexes { get; private set; }
    public CatalogRequest? LastTagValues { get; private set; }
    public EntryScanCancelRequest? LastCancel { get; private set; }
    public EntryHistoryRequest? LastHistory { get; private set; }

    public Func<EntryScanRequest, EntryScanResponse> OnScan { get; set; } =
        _ => new EntryScanResponse { TreeId = "t" };

    public Func<EntryGetRequest, EntryGetResponse> OnGet { get; set; } =
        r => new EntryGetResponse { TreeId = r.TreeId, Key = r.Key, Status = StateQueryStatus.KeyNotFound };

    public Func<EntryHistoryRequest, EntryHistoryResponse> OnHistory { get; set; } =
        r => new EntryHistoryResponse { TreeId = r.TreeId, Key = r.Key };

    public Func<CatalogRequest, TagIndexCatalogPage> OnListTagIndexes { get; set; } =
        _ => new TagIndexCatalogPage();

    public Func<CatalogRequest, TagValueCatalogPage> OnListTagValues { get; set; } =
        _ => new TagValueCatalogPage();

    public CatalogRequest? LastCoveredTrees { get; private set; }
    public CatalogRequest? LastIndexTags { get; private set; }
    public TagMemberScanRequest? LastTagMembers { get; private set; }

    public Func<CatalogRequest, CoveredTreeCatalogPage> OnListCoveredTrees { get; set; } =
        _ => new CoveredTreeCatalogPage();

    public Func<CatalogRequest, TagValueCatalogPage> OnListIndexTags { get; set; } =
        _ => new TagValueCatalogPage();

    public Func<TagMemberScanRequest, TagMemberScanPage> OnScanTagMembers { get; set; } =
        _ => new TagMemberScanPage();

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
    {
        LastScan = request;
        return Task.FromResult(OnScan(request));
    }

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
    {
        LastGet = request;
        return Task.FromResult(OnGet(request));
    }

    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
    {
        LastHistory = request;
        return Task.FromResult(OnHistory(request));
    }

    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
    {
        LastCancel = request;
        return Task.FromResult(new EntryScanCancelResponse());
    }

    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeCatalogPage());
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ViewCatalogPage());

    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        LastTagIndexes = request;
        return Task.FromResult(OnListTagIndexes(request));
    }

    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        LastTagValues = request;
        return Task.FromResult(OnListTagValues(request));
    }

    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        LastCoveredTrees = request;
        return Task.FromResult(OnListCoveredTrees(request));
    }

    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        LastIndexTags = request;
        return Task.FromResult(OnListIndexTags(request));
    }

    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
    {
        LastTagMembers = request;
        return Task.FromResult(OnScanTagMembers(request));
    }
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new StructureResponse { TreeId = "t" });
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeMetricsSnapshot());

    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ClusterInfo());
    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync<StateChangeNotification>();
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync<TreeMetricsSnapshot>();

#pragma warning disable CS1998
    private static async IAsyncEnumerable<T> EmptyAsync<T>() { yield break; }
#pragma warning restore CS1998
}
