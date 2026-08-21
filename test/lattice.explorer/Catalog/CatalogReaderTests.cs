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

    private static ViewStateSummary View(
        string name,
        string source,
        bool aggregation,
        bool history = false,
        string? providerKey = null,
        string? projectionVersion = null) => new()
    {
        ViewName = name,
        SourceTreeId = source,
        IsAggregation = aggregation,
        IsHistory = history,
        ProjectionProviderKey = providerKey,
        ProjectionVersion = projectionVersion,
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
        Assert.That(page.Items[0].DisplayName, Is.Null);
        Assert.That(page.Items[0].Label, Is.EqualTo("alpha"));
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
                Entries = new[]
                {
                    View(
                        "v1",
                        "alpha",
                        aggregation: true,
                        providerKey: "app.orders.v1",
                        projectionVersion: "v3"),
                    View("v2", "beta", aggregation: false),
                },
                NextPageToken = null,
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Views, pageToken: null, pageSize: 50);

        Assert.That(page.Items, Has.Count.EqualTo(2));
        Assert.That(page.Items[0].Id, Is.EqualTo("view-v1"));
        Assert.That(page.Items[0].DisplayName, Is.EqualTo("v1"));
        Assert.That(page.Items[0].Label, Is.EqualTo("v1"));
        Assert.That(page.Items[1].Id, Is.EqualTo("view-v2"));
        Assert.That(page.Items[1].DisplayName, Is.EqualTo("v2"));
        Assert.That(page.Items[0].Kind, Is.EqualTo(CatalogKind.Views));
        Assert.That(page.Items[0].SourceTreeId, Is.EqualTo("alpha"));
        Assert.That(page.Items[0].IsAggregation, Is.True);
        Assert.That(page.Items[1].IsAggregation, Is.False);
        Assert.That(page.Items[0].IsHistory, Is.False, "a plain projection / aggregation view is not a history view");
        Assert.That(page.Items[1].IsHistory, Is.False);
        Assert.That(page.Items[0].ProjectionProviderKey, Is.EqualTo("app.orders.v1"));
        Assert.That(page.Items[0].ProjectionVersion, Is.EqualTo("v3"));
        Assert.That(page.Items[1].ProjectionProviderKey, Is.Null);
        Assert.That(page.Items[1].ProjectionVersion, Is.Null);
        Assert.That(page.Items[0].ShardCount, Is.Null);
        Assert.That(page.Items[0].Lifecycle, Is.Null);
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task LoadAsync_Views_MapsHistoryFlag()
    {
        var client = new FakeStateClientCapture
        {
            OnListViews = _ => Task.FromResult(new ViewCatalogPage
            {
                Entries = new[]
                {
                    View("hist", "mfg-parts-label", aggregation: false, history: true),
                    View("plain", "orders", aggregation: false, history: false),
                },
                NextPageToken = null,
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Views, pageToken: null, pageSize: 50);

        var history = page.Items.Single(i => i.DisplayName == "hist");
        Assert.That(history.IsHistory, Is.True, "a change-history view must surface IsHistory so the Data tab can guide to History");
        Assert.That(history.SourceTreeId, Is.EqualTo("mfg-parts-label"),
            "the source tree the Data-tab guidance routes the operator to");
        var plain = page.Items.Single(i => i.DisplayName == "plain");
        Assert.That(plain.IsHistory, Is.False);
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
    public async Task LoadAsync_Trees_MapsRestoreShadowMarker()
    {
        var shadow = Tree("mfg-facts-bkprestore-abc123", TreeLifecycleState.Active, 4) with
        {
            RestoreShadowOfTreeId = "mfg-facts",
        };
        var client = new FakeStateClient
        {
            ListTreesHandler = _ => Task.FromResult(new TreeCatalogPage
            {
                Entries = new[] { Tree("mfg-facts", TreeLifecycleState.Active, 4), shadow },
                NextPageToken = null,
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 50);

        var live = page.Items.Single(i => i.Id == "mfg-facts");
        Assert.That(live.IsRestoreShadow, Is.False, "an ordinary tree is not a restore shadow");
        Assert.That(live.RestoreShadowOfTreeId, Is.Null);
        var restored = page.Items.Single(i => i.Id == "mfg-facts-bkprestore-abc123");
        Assert.That(restored.IsRestoreShadow, Is.True, "the marker is carried from the state API, not inferred from the name");
        Assert.That(restored.RestoreShadowOfTreeId, Is.EqualTo("mfg-facts"));
    }

    [Test]
    public void Constructor_NullClient_Throws()
    {
        Assert.That(() => new CatalogReader(null!), Throws.ArgumentNullException);
    }

    private static TagIndexStateSummary TagIndex(string indexName, string treeId, int shards) => new()
    {
        IndexName = indexName,
        TreeId = treeId,
        ShardCount = shards,
    };

    [Test]
    public async Task LoadAsync_TagIndexes_MapsSummariesAndToken()
    {
        var client = new FakeStateClientCapture
        {
            OnListTagIndexes = _ => Task.FromResult(new TagIndexCatalogPage
            {
                Entries = new[] { TagIndex("by-status", "tag-by-status", 3), TagIndex("by-owner", "tag-by-owner", 1) },
                NextPageToken = "tag-by-status",
            }),
        };
        var reader = new CatalogReader(client);

        var page = await reader.LoadAsync(CatalogKind.TagIndexes, pageToken: null, pageSize: 50);

        Assert.That(page.Items, Has.Count.EqualTo(2));
        Assert.That(page.Items[0].Id, Is.EqualTo("tag-by-status"));
        Assert.That(page.Items[0].Kind, Is.EqualTo(CatalogKind.TagIndexes));
        Assert.That(page.Items[0].IndexName, Is.EqualTo("by-status"));
        Assert.That(page.Items[0].ShardCount, Is.EqualTo(3));
        Assert.That(page.NextPageToken, Is.EqualTo("tag-by-status"));
        Assert.That(page.HasMore, Is.True);
    }

    [Test]
    public async Task LoadAsync_TagIndexes_RoutesToListTagIndexes()
    {
        var calledTrees = false;
        var calledTagIndexes = false;
        var client = new FakeStateClientCapture
        {
            OnListTrees = _ => { calledTrees = true; return Task.FromResult(new TreeCatalogPage()); },
            OnListTagIndexes = _ => { calledTagIndexes = true; return Task.FromResult(new TagIndexCatalogPage()); },
        };
        var reader = new CatalogReader(client);

        await reader.LoadAsync(CatalogKind.TagIndexes, pageToken: null, pageSize: 10);

        Assert.That(calledTagIndexes, Is.True);
        Assert.That(calledTrees, Is.False);
    }
}
