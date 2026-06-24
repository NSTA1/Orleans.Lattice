using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// A configurable <see cref="ILatticeStateClient"/> test double. Per-method
/// handlers let a test drive success, transient, and permanent outcomes, and the
/// dispose counter verifies channel teardown on rebuild.
/// </summary>
internal sealed class FakeStateClient : ILatticeStateClient, IDisposable
{
    public int DisposeCount { get; private set; }

    public Func<CancellationToken, Task<TreeCatalogPage>> ListTreesHandler { get; set; } =
        _ => Task.FromResult(new TreeCatalogPage());

    public Func<CancellationToken, Task<EntryScanResponse>> ScanEntriesHandler { get; set; } =
        _ => Task.FromResult(new EntryScanResponse { TreeId = "t" });

    public Func<CancellationToken, Task<TreeMetricsSnapshot>> MetricsHandler { get; set; } =
        _ => Task.FromResult(new TreeMetricsSnapshot());

    public Func<CancellationToken, Task<ClusterInfo>> ClusterInfoHandler { get; set; } =
        _ => Task.FromResult(new ClusterInfo());

    public Func<CancellationToken, IAsyncEnumerable<TreeMetricsSnapshot>> ObserveMetricsHandler { get; set; } =
        _ => EmptyAsync<TreeMetricsSnapshot>();

    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => ListTreesHandler(cancellationToken);

    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new ViewCatalogPage());

    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TagIndexCatalogPage());

    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new StructureResponse { TreeId = "t" });

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => ScanEntriesHandler(cancellationToken);

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryGetResponse { TreeId = "t", Key = "k" });

    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanCancelResponse());

    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => MetricsHandler(cancellationToken);

    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => ClusterInfoHandler(cancellationToken);

    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => EmptyAsync<StateChangeNotification>();

    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => ObserveMetricsHandler(cancellationToken);

    public void Dispose() => DisposeCount++;

#pragma warning disable CS1998 // async iterator with no await is intentional for an empty sequence
    private static async IAsyncEnumerable<T> EmptyAsync<T>()
    {
        yield break;
    }
#pragma warning restore CS1998
}
