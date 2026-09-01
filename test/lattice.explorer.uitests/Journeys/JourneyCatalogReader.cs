using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A catalog reader over the journey head's demo cluster, standing in for the state
/// API that head has no connection to.
/// <para>
/// It exists so the journeys that turn on <i>having something to open</i> - opening a
/// tree and landing on its first detail surface, restoring a selection across a
/// reload, watching a listing re-scope when the active tenant changes - measure the
/// real navigation, selection and detail-panel code rather than an empty-catalog
/// placeholder. The projection it returns is the ordinary <see cref="CatalogItem"/>
/// shape, so nothing downstream can tell it from a live read.
/// </para>
/// <para>
/// <b>It is tenant-scoped on purpose.</b> Each demo tenant owns differently-named
/// trees, so "the catalog re-scoped after the tenant changed" is an observable change
/// in what is listed rather than an inference from the picker's own value. A reader
/// that ignored the tenant would let the tenant-scope journey pass while the scope
/// reached nothing.
/// </para>
/// </summary>
/// <param name="tenantView">The active tenant view, absent when tenancy is off.</param>
internal sealed class JourneyCatalogReader(IExplorerTenantView? tenantView = null) : ICatalogReader
{
    /// <summary>The tree the journeys open, listed while <see cref="JourneyWorld.AcmeTenant"/> is active.</summary>
    internal const string OrdersTree = "acme-orders";

    /// <summary>A second acme tree, so a selection is a genuine choice rather than the only row.</summary>
    internal const string CustomersTree = "acme-customers";

    /// <summary>The tree that appears only once <see cref="JourneyWorld.GlobexTenant"/> is the active tenant.</summary>
    internal const string GlobexTree = "globex-shipments";

    private static readonly CatalogPage AcmeTrees = new()
    {
        Items =
        [
            new CatalogItem { Id = OrdersTree, Kind = CatalogKind.Trees, Lifecycle = "active", ShardCount = 64 },
            new CatalogItem { Id = CustomersTree, Kind = CatalogKind.Trees, Lifecycle = "active", ShardCount = 8 },
        ],
    };

    private static readonly CatalogPage GlobexTrees = new()
    {
        Items =
        [
            new CatalogItem { Id = GlobexTree, Kind = CatalogKind.Trees, Lifecycle = "active", ShardCount = 16 },
        ],
    };

    private static readonly CatalogPage ViewPage = new()
    {
        Items =
        [
            new CatalogItem
            {
                Id = "view-orders-by-region",
                DisplayName = "orders-by-region",
                Kind = CatalogKind.Views,
                SourceTreeId = OrdersTree,
                IsAggregation = true,
            },
        ],
    };

    private static readonly CatalogPage TagIndexPage = new()
    {
        Items =
        [
            new CatalogItem
            {
                Id = "tag-orders-status",
                DisplayName = "orders-status",
                Kind = CatalogKind.TagIndexes,
                IndexName = "orders-status",
            },
        ],
    };

    private static readonly CatalogPage EmptyPage = new();

    private readonly IExplorerTenantView? _tenantView = tenantView;

    /// <inheritdoc />
    public Task<CatalogPage> LoadAsync(
        CatalogKind kind,
        string? pageToken,
        int pageSize,
        CancellationToken cancellationToken = default) =>
        Task.FromResult(kind switch
        {
            CatalogKind.Trees => TreesForActiveTenant(),
            CatalogKind.Views => ViewPage,
            CatalogKind.TagIndexes => TagIndexPage,
            _ => EmptyPage,
        });

    private CatalogPage TreesForActiveTenant() =>
        _tenantView?.ActiveTenant?.Value == JourneyWorld.GlobexTenant ? GlobexTrees : AcmeTrees;
}
