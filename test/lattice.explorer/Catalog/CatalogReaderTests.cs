using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Tests.Connection;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

[TestFixture]
public class CatalogReaderTests
{
    private static TreeCatalogEntry Tree(string id, TreeLifecycleState lifecycle, int shards) => new()
    {
        TreeId = id,
        Lifecycle = lifecycle,
        ShardCount = shards,
        Config = new TreeConfigSummary { ShardCount = shards },
    };

    private static ViewStateSummary View(string name, string source, bool aggregation) => new()
    {
        ViewName = name,
        SourceTreeId = source,
        IsAggregation = aggregation,
    };

    [Test]
    public async Task LoadAsync_Trees_MapsEntriesAndToken()
    {
        var client = new FakeStateClient
        {
            ListTreesHandler = _ => Task.FromResult(new TreeCatalogPage
            {
                Entries = new[] { Tree("alpha", TreeLifecycleState.Active, 4), Tree("beta", TreeLifecycleState.SoftDeleted, 1) },
                NextPageToken = "beta",
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        Assert.That(page.Items, Has.Count.EqualTo(2));
        Assert.That(page.Items[0].Id, Is.EqualTo("alpha"));
        Assert.That(page.Items[0].Kind, Is.EqualTo(CatalogKind.Trees));
        Assert.That(page.Items[0].ShardCount, Is.EqualTo(4));
        Assert.That(page.Items[0].Lifecycle, Is.EqualTo(nameof(TreeLifecycleState.Active)));
        Assert.That(page.Items[1].Lifecycle, Is.EqualTo(nameof(TreeLifecycleState.SoftDeleted)));
        Assert.That(page.NextPageToken, Is.EqualTo("beta"));
        Assert.That(page.HasMore, Is.True);
    }

    [Test]
    public async Task LoadAsync_Trees_PassesPageTokenAndSize()
    {
        CatalogRequest? captured = null;
        var client = new FakeStateClientCapture
        {
            OnListTrees = req =>
            {
                captured = req;
                return Task.FromResult(new TreeCatalogPage());
            },
        };
        var reader = new CatalogReader(client);

        await reader.LoadAsync(CatalogKind.Trees, pageToken: "cursor", pageSize: 25);

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.PageToken, Is.EqualTo("cursor"));
        Assert.That(captured.PageSize, Is.EqualTo(25));
    }

    [Test]
    public async Task LoadAsync_Views_MapsSummariesAndLastPage()
    {
        var client = new FakeStateClientCapture
        {
            OnListViews = _ => Task.FromResult(new ViewCatalogPage
            {
                Entries = new[] { View("v1", "alpha", aggregation: true), View("v2", "beta", aggregation: false) },
                NextPageToken = null,
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Views, pageToken: null, pageSize: 50);

        Assert.That(page.Items, Has.Count.EqualTo(2));
        Assert.That(page.Items[0].Id, Is.EqualTo("v1"));
        Assert.That(page.Items[0].Kind, Is.EqualTo(CatalogKind.Views));
        Assert.That(page.Items[0].SourceTreeId, Is.EqualTo("alpha"));
        Assert.That(page.Items[0].IsAggregation, Is.True);
        Assert.That(page.Items[1].IsAggregation, Is.False);
        Assert.That(page.Items[0].ShardCount, Is.Null);
        Assert.That(page.Items[0].Lifecycle, Is.Null);
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task LoadAsync_Views_RoutesToListViews()
    {
        var calledTrees = false;
        var calledViews = false;
        var client = new FakeStateClientCapture
        {
            OnListTrees = _ => { calledTrees = true; return Task.FromResult(new TreeCatalogPage()); },
            OnListViews = _ => { calledViews = true; return Task.FromResult(new ViewCatalogPage()); },
        };
        var reader = new CatalogReader(client);

        await reader.LoadAsync(CatalogKind.Views, pageToken: null, pageSize: 10);

        Assert.That(calledViews, Is.True);
        Assert.That(calledTrees, Is.False);
    }

    [Test]
    public void Constructor_NullClient_Throws()
    {
        Assert.That(() => new CatalogReader(null!), Throws.ArgumentNullException);
    }
}
