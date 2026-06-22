using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class TopologyReaderTests
{
    private sealed class StructureStateClient : ILatticeStateClient
    {
        public StructureRequest? LastRequest { get; private set; }
        public Func<StructureRequest, StructureResponse> OnGet { get; set; } =
            _ => new StructureResponse { TreeId = "t" };

        public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
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
        public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new EntryScanResponse { TreeId = "t" });
        public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new EntryGetResponse { TreeId = "t", Key = "k" });
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

    [Test]
    public async Task GetAsync_RequestsTreeRoots()
    {
        var client = new StructureStateClient();
        var reader = new TopologyReader(client);

        await reader.GetAsync("tree-1");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastRequest!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastRequest!.ShardIndex, Is.Null);
            Assert.That(client.LastRequest!.SubPathNodeId, Is.Null);
        });
    }

    [Test]
    public async Task ExpandAsync_RequestsScopedSubtree()
    {
        var client = new StructureStateClient();
        var reader = new TopologyReader(client);

        await reader.ExpandAsync("tree-1", shardIndex: 3, subPathNodeId: "node-7");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastRequest!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastRequest!.ShardIndex, Is.EqualTo(3));
            Assert.That(client.LastRequest!.SubPathNodeId, Is.EqualTo("node-7"));
        });
    }

    [Test]
    public async Task GetAsync_MapsRootsAndTruncation()
    {
        var client = new StructureStateClient
        {
            OnGet = _ => new StructureResponse
            {
                TreeId = "t",
                Truncated = true,
                Roots = new[]
                {
                    new NodeStateSummary
                    {
                        NodeId = "r",
                        Kind = NodeKind.ShardRoot,
                        SubtreeKeyCount = 42,
                        Children = new[] { new NodeStateSummary { NodeId = "c", Kind = NodeKind.Leaf } },
                    },
                },
            },
        };
        var reader = new TopologyReader(client);

        var fetch = await reader.GetAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(fetch.Truncated, Is.True);
            Assert.That(fetch.Roots.Single().NodeId, Is.EqualTo("r"));
            Assert.That(fetch.Roots.Single().SubtreeKeyCount, Is.EqualTo(42));
            Assert.That(fetch.Roots.Single().Children.Single().NodeId, Is.EqualTo("c"));
        });
    }

    [Test]
    public async Task GetAsync_EmptyResponse_ReturnsNoRoots()
    {
        var client = new StructureStateClient { OnGet = _ => new StructureResponse { TreeId = "t" } };
        var reader = new TopologyReader(client);

        var fetch = await reader.GetAsync("t");

        Assert.That(fetch.Roots, Is.Empty);
    }
}
