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

    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListTrees(request);

    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => OnListViews(request);

    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new StructureResponse { TreeId = "t" });

    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryScanResponse { TreeId = "t" });

    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new EntryGetResponse { TreeId = "t", Key = "k" });

    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new TreeMetricsSnapshot());

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
