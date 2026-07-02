using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Metrics;

namespace Orleans.Lattice.Explorer.Tests.Metrics;

[TestFixture]
public class MetricsReaderTests
{
    private sealed class MetricsStateClient : ILatticeStateClient
    {
        public TreeMetricsRequest? LastRequest { get; private set; }
        public Func<TreeMetricsRequest, TreeMetricsSnapshot> OnGet { get; set; } = _ => new TreeMetricsSnapshot();

        public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            return Task.FromResult(OnGet(request));
        }

        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TreeCatalogPage());
        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ViewCatalogPage());
        public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TagIndexCatalogPage());
        public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TagValueCatalogPage());
        public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new CoveredTreeCatalogPage());
        public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TagValueCatalogPage());
        public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new TagMemberScanPage());
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

    private static TreeMetrics Metrics(string id) => new() { TreeId = id, ShardCount = 2, LiveKeys = 10 };

    [Test]
    public async Task GetAsync_ReturnsMatchingTree()
    {
        var client = new MetricsStateClient
        {
            OnGet = _ => new TreeMetricsSnapshot
            {
                Trees = new[] { Metrics("alpha"), Metrics("beta") },
            },
        };
        var reader = new MetricsReader(client);

        var result = await reader.GetAsync("beta");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.TreeId, Is.EqualTo("beta"));
    }

    [Test]
    public async Task GetAsync_RequestsHotnessAndViewLagForSelectedId()
    {
        var client = new MetricsStateClient();
        var reader = new MetricsReader(client);

        await reader.GetAsync("alpha");

        Assert.That(client.LastRequest, Is.Not.Null);
        Assert.That(client.LastRequest!.TreeIds, Is.EqualTo(new[] { "alpha" }));
        Assert.That(client.LastRequest.IncludeShardHotness, Is.True);
        Assert.That(client.LastRequest.IncludeViewLag, Is.True);
    }

    [Test]
    public async Task GetAsync_NoMatch_ReturnsNull()
    {
        var client = new MetricsStateClient
        {
            OnGet = _ => new TreeMetricsSnapshot { Trees = new[] { Metrics("other") } },
        };
        var reader = new MetricsReader(client);

        var result = await reader.GetAsync("alpha");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetAsync_EmptySnapshot_ReturnsNull()
    {
        var reader = new MetricsReader(new MetricsStateClient());

        var result = await reader.GetAsync("alpha");

        Assert.That(result, Is.Null);
    }

    [Test]
    public void GetAsync_EmptyId_Throws()
    {
        var reader = new MetricsReader(new MetricsStateClient());

        Assert.That(async () => await reader.GetAsync(""), Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void Constructor_NullClient_Throws()
    {
        Assert.That(() => new MetricsReader(null!), Throws.ArgumentNullException);
    }
}
