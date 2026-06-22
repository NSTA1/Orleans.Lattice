using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for the discovery / catalog endpoint
/// (<see cref="ILatticeStateQuery.ListTreesAsync"/> and
/// <see cref="ILatticeStateQuery.ListViewsAsync"/>): enumeration completeness
/// and ordering, lifecycle surfacing, alias transparency, effective-config
/// reporting, system-tree hiding, paging, and view discovery.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeCatalogIntegrationTests
{
    private CatalogClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new CatalogClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ListTreesAsync_returns_registered_user_trees_sorted()
    {
        await _fixture.RegisterTreeAsync("tree-charlie");
        await _fixture.RegisterTreeAsync("tree-alpha");
        await _fixture.RegisterTreeAsync("tree-bravo");

        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Select(e => e.TreeId),
            Is.EqualTo(new[] { "tree-alpha", "tree-bravo", "tree-charlie" }));
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListTreesAsync_reports_shard_count_and_effective_config()
    {
        await _fixture.RegisterTreeAsync("tree-cfg", shardCount: 3, maxLeafKeys: 7);

        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest());
        var entry = page.Entries.Single(e => e.TreeId == "tree-cfg");

        Assert.That(entry.ShardCount, Is.EqualTo(3));
        Assert.That(entry.Config.ShardCount, Is.EqualTo(3));
        Assert.That(entry.Config.MaxLeafKeys, Is.EqualTo(7));
        Assert.That(entry.Config.MaxInternalChildren, Is.EqualTo(LatticeConstants.DefaultMaxInternalChildren));
        Assert.That(entry.Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
        Assert.That(entry.IsAlias, Is.False);
    }

    [Test]
    public async Task ListTreesAsync_surfaces_soft_deleted_lifecycle()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-live", keyCount: 4);
        await _fixture.CreatePopulatedTreeAsync("tree-gone", keyCount: 4);
        await _fixture.SoftDeleteTreeAsync("tree-gone");

        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest());

        var live = page.Entries.Single(e => e.TreeId == "tree-live");
        var gone = page.Entries.Single(e => e.TreeId == "tree-gone");
        Assert.That(live.Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
        Assert.That(gone.Lifecycle, Is.EqualTo(TreeLifecycleState.SoftDeleted));
    }

    [Test]
    public async Task ListTreesAsync_reports_alias_transparency()
    {
        await _fixture.RegisterTreeAsync("tree-physical");
        await _fixture.RegisterTreeAsync("tree-logical");
        await _fixture.SetAliasAsync("tree-logical", "tree-physical");

        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest());
        var aliased = page.Entries.Single(e => e.TreeId == "tree-logical");

        Assert.That(aliased.IsAlias, Is.True);
        Assert.That(aliased.PhysicalTreeId, Is.EqualTo("tree-physical"));
    }

    [Test]
    public async Task ListTreesAsync_hides_system_trees_by_default_and_shows_when_requested()
    {
        await _fixture.RegisterTreeAsync("tree-user");
        await _fixture.RegisterViewBackingTreeAsync("view-probe");

        var hidden = await _fixture.Query.ListTreesAsync(new CatalogRequest());
        Assert.That(hidden.Entries.Select(e => e.TreeId), Does.Not.Contain("view-probe"));
        Assert.That(hidden.Entries.Select(e => e.TreeId), Does.Contain("tree-user"));

        var shown = await _fixture.Query.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });
        Assert.That(shown.Entries.Select(e => e.TreeId), Does.Contain("view-probe"));
    }

    [Test]
    public async Task ListTreesAsync_pages_completely_without_overlap()
    {
        for (var i = 0; i < 5; i++)
        {
            await _fixture.RegisterTreeAsync($"tree-{i:D2}");
        }

        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 2, PageToken = token });
            seen.AddRange(page.Entries.Select(e => e.TreeId));
            token = page.NextPageToken;
            pages++;
            Assert.That(pages, Is.LessThanOrEqualTo(10), "paging did not terminate");
        }
        while (token is not null);

        Assert.That(seen, Is.EqualTo(new[] { "tree-00", "tree-01", "tree-02", "tree-03", "tree-04" }));
        Assert.That(seen, Is.Unique);
        Assert.That(pages, Is.EqualTo(3));
    }

    [Test]
    public async Task ListTreesAsync_returns_empty_page_for_empty_cluster()
    {
        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest());

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListViewsAsync_returns_created_views_with_source_tree()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-src", keyCount: 2);
        _fixture.CreateView("tree-src", "orders-open");

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        var view = page.Entries.Single(e => e.ViewName == "orders-open");
        Assert.That(view.SourceTreeId, Is.EqualTo("tree-src"));
        Assert.That(view.Lag, Is.Null, "lag is not sampled unless requested");
        Assert.That(view.EntryCount, Is.Null, "entry count is not sampled unless requested");
    }

    [Test]
    public async Task ListViewsAsync_samples_stats_when_requested()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-src2", keyCount: 2);
        _fixture.CreateView("tree-src2", "orders-stats");

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest { IncludeViewStats = true });

        var view = page.Entries.Single(e => e.ViewName == "orders-stats");
        Assert.That(view.Lag, Is.Not.Null);
        Assert.That(view.EntryCount, Is.Not.Null);
    }

    [Test]
    public async Task ListViewsAsync_returns_empty_when_no_views_registered()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-noview", keyCount: 1);

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListTagIndexesAsync_returns_registered_tag_index_trees()
    {
        await _fixture.RegisterTagIndexTreeAsync("orders-by-status", shardCount: 3);

        var page = await _fixture.Query.ListTagIndexesAsync(new CatalogRequest());

        var entry = page.Entries.Single();
        Assert.That(entry.IndexName, Is.EqualTo("orders-by-status"));
        Assert.That(entry.TreeId, Is.EqualTo(LatticeConstants.TagIndexTreePrefix + "orders-by-status"));
        Assert.That(entry.ShardCount, Is.EqualTo(3));
    }

    [Test]
    public async Task ListTreesAsync_excludes_tag_index_trees()
    {
        await _fixture.RegisterTreeAsync("tree-user");
        await _fixture.RegisterTagIndexTreeAsync("hidden-index");

        var trees = await _fixture.Query.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });

        Assert.That(trees.Entries.Select(e => e.TreeId), Does.Contain("tree-user"));
        Assert.That(trees.Entries.Select(e => e.TreeId),
            Does.Not.Contain(LatticeConstants.TagIndexTreePrefix + "hidden-index"));
    }

    [Test]
    public async Task ListTagIndexesAsync_filters_to_indexes_covering_the_source_tree()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 4);
        await _fixture.CreatePopulatedTreeAsync("widgets", keyCount: 2);

        var ordersIndex = _fixture.CreateTagIndex("orders", "orders-by-status");
        await ordersIndex.Key("key-00000").AddAsync(["open"]);

        var widgetsIndex = _fixture.CreateTagIndex("widgets", "widgets-by-kind");
        await widgetsIndex.Key("key-00000").AddAsync(["bolt"]);

        var forOrders = await _fixture.Query.ListTagIndexesAsync(new CatalogRequest { SourceTreeId = "orders" });
        Assert.That(forOrders.Entries.Select(e => e.IndexName), Is.EqualTo(new[] { "orders-by-status" }));

        var forNone = await _fixture.Query.ListTagIndexesAsync(new CatalogRequest { SourceTreeId = "no-such-tree" });
        Assert.That(forNone.Entries, Is.Empty);
    }

    [Test]
    public async Task ScanEntriesAsync_with_index_and_tag_returns_only_tagged_rows()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 6);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        await index.Key("key-00000").AddAsync(["open"]);
        await index.Key("key-00002").AddAsync(["open"]);
        await index.Key("key-00001").AddAsync(["closed"]);

        var open = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "orders",
            IndexName = "orders-by-status",
            Tag = "open",
            PageSize = 100,
        });

        Assert.That(open.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(open.Entries.Select(e => e.Key), Is.EqualTo(new[] { "key-00000", "key-00002" }));

        var closed = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "orders",
            IndexName = "orders-by-status",
            Tag = "closed",
            PageSize = 100,
        });

        Assert.That(closed.Entries.Select(e => e.Key), Is.EqualTo(new[] { "key-00001" }));
    }

    [Test]
    public async Task ScanEntriesAsync_with_tag_pages_without_overlap()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 5);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        for (var i = 0; i < 5; i++)
        {
            await index.Key($"key-{i:D5}").AddAsync(["open"]);
        }

        var seen = new List<string>();
        string? token = null;
        do
        {
            var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = "orders",
                IndexName = "orders-by-status",
                Tag = "open",
                PageSize = 2,
                ContinuationToken = token,
            });
            seen.AddRange(page.Entries.Select(e => e.Key));
            token = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(token));

        Assert.That(seen, Is.Unique);
        Assert.That(seen, Is.EqualTo(new[] { "key-00000", "key-00001", "key-00002", "key-00003", "key-00004" }));
    }
}
