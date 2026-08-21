using Orleans.Lattice.BPlusTree;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

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
    public async Task ListTreesAsync_hides_sys_system_data_trees_by_default_and_shows_when_requested()
    {
        // The identity / authorization add-ons dogfood registered "sys-" trees
        // (sys-membership-*, sys-auth-*). They must not leak into the default
        // tree catalog an operator browsing user data sees, but stay reachable
        // when a caller explicitly opts in to system trees.
        await _fixture.RegisterTreeAsync("tree-user");
        await _fixture.RegisterSystemDataTreeAsync("sys-membership-users");
        await _fixture.RegisterSystemDataTreeAsync("sys-auth-policy");

        var hidden = await _fixture.Query.ListTreesAsync(new CatalogRequest());
        var hiddenIds = hidden.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(hiddenIds, Does.Contain("tree-user"));
        Assert.That(hiddenIds, Does.Not.Contain("sys-membership-users"));
        Assert.That(hiddenIds, Does.Not.Contain("sys-auth-policy"));

        var shown = await _fixture.Query.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });
        var shownIds = shown.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(shownIds, Does.Contain("sys-membership-users"));
        Assert.That(shownIds, Does.Contain("sys-auth-policy"));
    }

    [Test]
    public async Task ListTreesAsync_hides_sys_backup_catalog_trees_by_default_and_shows_when_requested()
    {
        // The backup add-on dogfoods reserved "sys-backup-" trees (the manifest
        // store and the catalog). They carry the core "sys-" system-data prefix,
        // so they must stay out of the default operator tree catalog yet remain
        // reachable when a caller explicitly opts in to system trees. The backup
        // API is then the sole enumeration surface for backups.
        await _fixture.RegisterTreeAsync("tree-user");
        await _fixture.RegisterSystemDataTreeAsync("sys-backup-store");
        await _fixture.RegisterSystemDataTreeAsync("sys-backup-catalog");

        var hidden = await _fixture.Query.ListTreesAsync(new CatalogRequest());
        var hiddenIds = hidden.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(hiddenIds, Does.Contain("tree-user"));
        Assert.That(hiddenIds, Does.Not.Contain("sys-backup-store"));
        Assert.That(hiddenIds, Does.Not.Contain("sys-backup-catalog"));

        var shown = await _fixture.Query.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });
        var shownIds = shown.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(shownIds, Does.Contain("sys-backup-store"));
        Assert.That(shownIds, Does.Contain("sys-backup-catalog"));
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
    public async Task ListViewsAsync_marks_plain_projection_view_as_not_history()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-plainhist", keyCount: 2);
        _fixture.CreateView("tree-plainhist", "orders-plain");

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        var view = page.Entries.Single(e => e.ViewName == "orders-plain");
        Assert.That(view.IsHistory, Is.False, "a predicate / re-project view is not a history view");
        Assert.That(view.IsAggregation, Is.False);
    }

    [Test]
    public async Task ListViewsAsync_marks_aggregation_view_as_not_history()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-agghist", keyCount: 2);
        _fixture.CreateAggregationView("tree-agghist", "orders-agg-hist");

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        var view = page.Entries.Single(e => e.ViewName == "orders-agg-hist");
        Assert.That(view.IsAggregation, Is.True);
        Assert.That(view.IsHistory, Is.False, "an aggregation view is not a history view");
        Assert.That(view.ProjectionProviderKey, Is.EqualTo("tests.state.count.v1"));
        Assert.That(view.ProjectionVersion, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task ListViewsAsync_startupViewDoesNotInheritShadowedRuntimeMetadata()
    {
        const string startupName = "startup-shadow";
        const string legacyName = "legacy-runtime";
        var services = _fixture.SiloServices;
        var startupRegistrations = (IList<StartupViewRegistration>)services
            .GetRequiredService<IReadOnlyList<StartupViewRegistration>>();
        var startup = new StartupViewRegistration(
            startupName,
            "startup-source",
            _ => new PredicateLatticeViewProjection());
        startupRegistrations.Add(startup);
        var catalog = services.GetRequiredService<IViewCatalog>();
        catalog.Register(startup.Resolve(services));
        catalog.Register(new ViewRegistration(
            legacyName,
            "legacy-source",
            new PredicateLatticeViewProjection()));
        var registry = services.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        await registry.RegisterAsync(new RuntimeViewRegistration
        {
            ViewName = startupName,
            SourceTreeId = "stale-runtime-source",
            ProjectionTypeName = typeof(PredicateLatticeViewProjection).FullName!,
            ProjectionVersion = "stale-version",
            ProjectionProviderKey = "stale-provider",
            ProjectionProviderPayload = [],
        });
        await registry.RegisterAsync(new RuntimeViewRegistration
        {
            ViewName = legacyName,
            SourceTreeId = "legacy-source",
            ProjectionTypeName = typeof(PredicateLatticeViewProjection).FullName!,
            ProjectionVersion = "legacy-version",
        });

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        var startupView = page.Entries.Single(entry => entry.ViewName == startupName);
        var legacyView = page.Entries.Single(entry => entry.ViewName == legacyName);
        Assert.Multiple(() =>
        {
            Assert.That(startupView.SourceTreeId, Is.EqualTo("startup-source"));
            Assert.That(startupView.ProjectionProviderKey, Is.Null);
            Assert.That(startupView.ProjectionVersion, Is.Null);
            Assert.That(legacyView.ProjectionProviderKey, Is.Null);
            Assert.That(legacyView.ProjectionVersion, Is.EqualTo("legacy-version"));
        });
    }

    [Test]
    public async Task ListViewsAsync_marks_history_view_as_history()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-histsrc", keyCount: 2);
        _fixture.CreateHistoryView("tree-histsrc", "orders-history");

        var page = await _fixture.Query.ListViewsAsync(new CatalogRequest());

        var view = page.Entries.Single(e => e.ViewName == "orders-history");
        Assert.That(view.IsHistory, Is.True, "an accumulative change-history view must be flagged as history");
        Assert.That(view.IsAggregation, Is.False);
        Assert.That(view.SourceTreeId, Is.EqualTo("tree-histsrc"),
            "the history view's source tree backs the History tab the Data tab routes to");
    }

    [Test]
    public async Task ListViewsAsync_hides_system_views_unless_requested()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-sysview", keyCount: 2);
        _fixture.CreateView("tree-sysview", "orders-visible");
        _fixture.CreateView("tree-sysview", "sys-backup-catalog-index");

        var hidden = await _fixture.Query.ListViewsAsync(new CatalogRequest());
        Assert.That(hidden.Entries.Select(e => e.ViewName), Does.Contain("orders-visible"));
        Assert.That(hidden.Entries.Select(e => e.ViewName), Does.Not.Contain("sys-backup-catalog-index"),
            "a system-prefixed view must be hidden from the default listing");

        var shown = await _fixture.Query.ListViewsAsync(new CatalogRequest { IncludeSystemTrees = true });
        Assert.That(shown.Entries.Select(e => e.ViewName), Does.Contain("sys-backup-catalog-index"),
            "opting in to system trees also reveals system views");
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
    public async Task ScanEntries_inspects_a_materialised_view_through_its_view_tree_id()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-view-src", keyCount: 3);
        var view = _fixture.CreateView("tree-view-src", "orders-inspect");
        await view.RebuildAsync();

        // The detail tabs query the physical "view-<name>" tree id; the read path
        // must admit it under an authorised view-read scope and return the
        // materialised entries rather than "tree not found".
        var result = await _fixture.Query.ScanEntriesAsync(
            new EntryScanRequest { TreeId = "view-orders-inspect", PageSize = 100 });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found),
            "a materialised view must be inspectable read-only via its view-tree id");
        Assert.That(result.Entries, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task GetTreeStructure_inspects_a_materialised_view_through_its_view_tree_id()
    {
        await _fixture.CreatePopulatedTreeAsync("tree-view-struct", keyCount: 3);
        var view = _fixture.CreateView("tree-view-struct", "orders-struct");
        await view.RebuildAsync();

        var result = await _fixture.Query.GetTreeStructureAsync(
            new StructureRequest { TreeId = "view-orders-struct" });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found),
            "a materialised view must expose its topology read-only via its view-tree id");
        Assert.That(result.Roots, Is.Not.Empty);
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
    public async Task ListTagValuesAsync_returns_distinct_values_in_ordinal_order()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 6);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        await index.Key("key-00000").AddAsync(["open"]);
        await index.Key("key-00001").AddAsync(["closed"]);
        await index.Key("key-00002").AddAsync(["open"]);
        await index.Key("key-00003").AddAsync(["pending"]);

        var page = await _fixture.Query.ListTagValuesAsync(
            new CatalogRequest { SourceTreeId = "orders", IndexName = "orders-by-status" });

        Assert.That(page.Entries, Is.EqualTo(new[] { "closed", "open", "pending" }));
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListTagValuesAsync_pages_with_continuation_token()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 6);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        await index.Key("key-00000").AddAsync(["alpha"]);
        await index.Key("key-00001").AddAsync(["bravo"]);
        await index.Key("key-00002").AddAsync(["charlie"]);

        var first = await _fixture.Query.ListTagValuesAsync(
            new CatalogRequest { SourceTreeId = "orders", IndexName = "orders-by-status", PageSize = 2 });
        Assert.That(first.Entries, Is.EqualTo(new[] { "alpha", "bravo" }));
        Assert.That(first.NextPageToken, Is.EqualTo("bravo"));

        var second = await _fixture.Query.ListTagValuesAsync(
            new CatalogRequest
            {
                SourceTreeId = "orders",
                IndexName = "orders-by-status",
                PageSize = 2,
                PageToken = first.NextPageToken,
            });
        Assert.That(second.Entries, Is.EqualTo(new[] { "charlie" }));
        Assert.That(second.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListTagValuesAsync_returns_empty_for_unknown_tree()
    {
        var page = await _fixture.Query.ListTagValuesAsync(
            new CatalogRequest { SourceTreeId = "no-such-tree", IndexName = "orders-by-status" });

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListCoveredTreesAsync_returns_covered_trees_in_ordinal_order()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 2);
        await _fixture.CreatePopulatedTreeAsync("archive", keyCount: 2);

        var ordersIndex = _fixture.CreateTagIndex("orders", "by-status");
        await ordersIndex.Key("key-00000").AddAsync(["open"]);
        var archiveIndex = _fixture.CreateTagIndex("archive", "by-status");
        await archiveIndex.Key("key-00000").AddAsync(["open"]);

        var page = await _fixture.Query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = "by-status" });

        Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "orders" }));
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListCoveredTreesAsync_pages_with_continuation_token()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 1);
        await _fixture.CreatePopulatedTreeAsync("archive", keyCount: 1);
        await _fixture.CreatePopulatedTreeAsync("widgets", keyCount: 1);

        foreach (var tree in new[] { "orders", "archive", "widgets" })
        {
            var index = _fixture.CreateTagIndex(tree, "by-status");
            await index.Key("key-00000").AddAsync(["open"]);
        }

        var first = await _fixture.Query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = "by-status", PageSize = 2 });
        Assert.That(first.Entries, Is.EqualTo(new[] { "archive", "orders" }));
        Assert.That(first.NextPageToken, Is.EqualTo("orders"));

        var second = await _fixture.Query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = "by-status", PageSize = 2, PageToken = first.NextPageToken });
        Assert.That(second.Entries, Is.EqualTo(new[] { "widgets" }));
        Assert.That(second.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListCoveredTreesAsync_returns_empty_for_unknown_index()
    {
        var page = await _fixture.Query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = "no-such-index" });

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListIndexTagsAsync_returns_distinct_tags_across_covered_trees()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);
        await _fixture.CreatePopulatedTreeAsync("archive", keyCount: 3);

        var ordersIndex = _fixture.CreateTagIndex("orders", "by-status");
        await ordersIndex.Key("key-00000").AddAsync(["open"]);
        await ordersIndex.Key("key-00001").AddAsync(["pending"]);

        var archiveIndex = _fixture.CreateTagIndex("archive", "by-status");
        await archiveIndex.Key("key-00000").AddAsync(["closed"]);
        await archiveIndex.Key("key-00001").AddAsync(["open"]);

        var page = await _fixture.Query.ListIndexTagsAsync(
            new CatalogRequest { IndexName = "by-status" });

        Assert.That(page.Entries, Is.EqualTo(new[] { "closed", "open", "pending" }));
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ListIndexTagsAsync_pages_with_continuation_token()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);

        var index = _fixture.CreateTagIndex("orders", "by-status");
        await index.Key("key-00000").AddAsync(["alpha"]);
        await index.Key("key-00001").AddAsync(["bravo"]);
        await index.Key("key-00002").AddAsync(["charlie"]);

        var first = await _fixture.Query.ListIndexTagsAsync(
            new CatalogRequest { IndexName = "by-status", PageSize = 2 });
        Assert.That(first.Entries, Is.EqualTo(new[] { "alpha", "bravo" }));
        Assert.That(first.NextPageToken, Is.EqualTo("bravo"));

        var second = await _fixture.Query.ListIndexTagsAsync(
            new CatalogRequest { IndexName = "by-status", PageSize = 2, PageToken = first.NextPageToken });
        Assert.That(second.Entries, Is.EqualTo(new[] { "charlie" }));
        Assert.That(second.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ScanTagMembersAsync_returns_live_members_across_trees_in_order()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);
        await _fixture.CreatePopulatedTreeAsync("archive", keyCount: 3);

        var ordersIndex = _fixture.CreateTagIndex("orders", "by-status");
        await ordersIndex.Key("key-00000").AddAsync(["open"]);
        await ordersIndex.Key("key-00002").AddAsync(["open"]);

        var archiveIndex = _fixture.CreateTagIndex("archive", "by-status");
        await archiveIndex.Key("key-00001").AddAsync(["open"]);

        var page = await _fixture.Query.ScanTagMembersAsync(
            new TagMemberScanRequest { IndexName = "by-status", Tag = "open" });

        Assert.That(
            page.Entries.Select(m => (m.TreeId, m.Key)),
            Is.EqualTo(new[]
            {
                ("archive", "key-00001"),
                ("orders", "key-00000"),
                ("orders", "key-00002"),
            }));
        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ScanTagMembersAsync_pages_with_continuation_token()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);

        var index = _fixture.CreateTagIndex("orders", "by-status");
        await index.Key("key-00000").AddAsync(["open"]);
        await index.Key("key-00001").AddAsync(["open"]);
        await index.Key("key-00002").AddAsync(["open"]);

        var first = await _fixture.Query.ScanTagMembersAsync(
            new TagMemberScanRequest { IndexName = "by-status", Tag = "open", PageSize = 2 });
        Assert.That(
            first.Entries.Select(m => m.Key),
            Is.EqualTo(new[] { "key-00000", "key-00001" }));
        Assert.That(first.NextPageToken, Is.Not.Null);

        var second = await _fixture.Query.ScanTagMembersAsync(
            new TagMemberScanRequest
            {
                IndexName = "by-status",
                Tag = "open",
                PageSize = 2,
                PageToken = first.NextPageToken,
            });
        Assert.That(second.Entries.Select(m => m.Key), Is.EqualTo(new[] { "key-00002" }));
        Assert.That(second.NextPageToken, Is.Null);
    }

    [Test]
    public async Task ScanTagMembersAsync_excludes_stale_members_whose_key_is_absent()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 2);

        var index = _fixture.CreateTagIndex("orders", "by-status");
        await index.Key("key-00000").AddAsync(["open"]);
        // A membership row for a key that does not exist in the source tree: the
        // liveness filter must drop it so the browse never shows a phantom key.
        await index.Key("key-99999").AddAsync(["open"]);

        var page = await _fixture.Query.ScanTagMembersAsync(
            new TagMemberScanRequest { IndexName = "by-status", Tag = "open" });

        Assert.That(page.Entries.Select(m => m.Key), Is.EqualTo(new[] { "key-00000" }));
    }

    [Test]
    public async Task ScanTagMembersAsync_returns_empty_for_unknown_tag()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 1);
        var index = _fixture.CreateTagIndex("orders", "by-status");
        await index.Key("key-00000").AddAsync(["open"]);

        var page = await _fixture.Query.ScanTagMembersAsync(
            new TagMemberScanRequest { IndexName = "by-status", Tag = "no-such-tag" });

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextPageToken, Is.Null);
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
    public async Task ScanEntriesAsync_with_typoed_index_name_returns_index_not_found()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        await index.Key("key-00000").AddAsync(["open"]);

        // A mistyped index name names no materialised tag index. It reports
        // IndexNotFound (issue #1396 N4) so the caller can tell a typo from a
        // real-but-empty index, which both otherwise return an empty Found page.
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "orders",
            IndexName = "orders-by-statuss",
            Tag = "open",
            PageSize = 100,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.IndexNotFound));
            Assert.That(result.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task ScanEntriesAsync_with_real_index_but_absent_tag_returns_found_empty()
    {
        await _fixture.CreatePopulatedTreeAsync("orders", keyCount: 3);

        var index = _fixture.CreateTagIndex("orders", "orders-by-status");
        await index.Key("key-00000").AddAsync(["open"]);

        // The index exists but carries no members for this tag: that is a
        // real-but-empty result (Found with zero entries), distinct from the
        // IndexNotFound reported for a mistyped index name.
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "orders",
            IndexName = "orders-by-status",
            Tag = "no-such-tag",
            PageSize = 100,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Entries, Is.Empty);
        });
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
