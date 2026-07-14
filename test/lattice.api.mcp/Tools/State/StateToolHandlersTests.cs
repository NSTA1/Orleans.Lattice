using NSubstitute;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="StateToolHandlers"/> - the read-only adapter methods
/// behind the state tool module. Each test drives a handler with a fake
/// <see cref="ILatticeStateQuery"/> and proves it maps the tool arguments onto
/// the facade request faithfully and returns the facade result unchanged. All
/// deterministic; no cluster, no MCP transport, no timing or ordering
/// dependencies.
/// </summary>
[TestFixture]
public sealed class StateToolHandlersTests
{
    private static ILatticeStateQuery Query() => Substitute.For<ILatticeStateQuery>();

    [Test]
    public async Task GetClusterInfoAsync_passes_through_the_facade_result()
    {
        var query = Query();
        var info = new ClusterInfo { ClusterId = "c1", ServiceId = "s1" };
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).Returns(info);

        var result = await StateToolHandlers.GetClusterInfoAsync(query, CancellationToken.None);

        Assert.That(result, Is.SameAs(info));
    }

    [Test]
    public async Task ListTreesAsync_maps_paging_and_system_tree_flag()
    {
        var query = Query();
        var page = new TreeCatalogPage();
        CatalogRequest? captured = null;
        query.ListTreesAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListTreesAsync(query, CancellationToken.None, 7, "tok", includeSystemTrees: true);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.PageSize, Is.EqualTo(7));
            Assert.That(captured!.PageToken, Is.EqualTo("tok"));
            Assert.That(captured!.IncludeSystemTrees, Is.True);
        });
    }

    [Test]
    public async Task ListTreesAsync_defaults_hide_system_trees_and_page_at_100()
    {
        var query = Query();
        CatalogRequest? captured = null;
        query.ListTreesAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(new TreeCatalogPage());

        await StateToolHandlers.ListTreesAsync(query, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(captured!.PageSize, Is.EqualTo(CatalogRequest.DefaultPageSize));
            Assert.That(captured!.PageToken, Is.Null);
            Assert.That(captured!.IncludeSystemTrees, Is.False);
        });
    }

    [Test]
    public async Task ListViewsAsync_maps_paging_and_view_stats_flag()
    {
        var query = Query();
        var page = new ViewCatalogPage();
        CatalogRequest? captured = null;
        query.ListViewsAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListViewsAsync(query, CancellationToken.None, 3, "v", includeViewStats: true);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.PageSize, Is.EqualTo(3));
            Assert.That(captured!.PageToken, Is.EqualTo("v"));
            Assert.That(captured!.IncludeViewStats, Is.True);
        });
    }

    [Test]
    public async Task ListTagIndexesAsync_maps_source_tree_filter()
    {
        var query = Query();
        var page = new TagIndexCatalogPage();
        CatalogRequest? captured = null;
        query.ListTagIndexesAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListTagIndexesAsync(query, CancellationToken.None, 9, "t", sourceTreeId: "tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.PageSize, Is.EqualTo(9));
            Assert.That(captured!.PageToken, Is.EqualTo("t"));
            Assert.That(captured!.SourceTreeId, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public async Task ListTagValuesAsync_maps_index_name_and_subject_tree()
    {
        var query = Query();
        var page = new TagValueCatalogPage();
        CatalogRequest? captured = null;
        query.ListTagValuesAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListTagValuesAsync(query, CancellationToken.None, "idx", "tree-b", 4, "p");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.IndexName, Is.EqualTo("idx"));
            Assert.That(captured!.SourceTreeId, Is.EqualTo("tree-b"));
            Assert.That(captured!.PageSize, Is.EqualTo(4));
            Assert.That(captured!.PageToken, Is.EqualTo("p"));
        });
    }

    [Test]
    public async Task ListCoveredTreesAsync_maps_index_name()
    {
        var query = Query();
        var page = new CoveredTreeCatalogPage();
        CatalogRequest? captured = null;
        query.ListCoveredTreesAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListCoveredTreesAsync(query, CancellationToken.None, "idx-c", 2, "c");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.IndexName, Is.EqualTo("idx-c"));
            Assert.That(captured!.PageSize, Is.EqualTo(2));
            Assert.That(captured!.PageToken, Is.EqualTo("c"));
        });
    }

    [Test]
    public async Task ListIndexTagsAsync_maps_index_name()
    {
        var query = Query();
        var page = new TagValueCatalogPage();
        CatalogRequest? captured = null;
        query.ListIndexTagsAsync(Arg.Do<CatalogRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ListIndexTagsAsync(query, CancellationToken.None, "idx-d", 6, "d");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.IndexName, Is.EqualTo("idx-d"));
            Assert.That(captured!.PageSize, Is.EqualTo(6));
            Assert.That(captured!.PageToken, Is.EqualTo("d"));
        });
    }

    [Test]
    public async Task ScanTagMembersAsync_maps_index_name_and_tag()
    {
        var query = Query();
        var page = new TagMemberScanPage();
        TagMemberScanRequest? captured = null;
        query.ScanTagMembersAsync(Arg.Do<TagMemberScanRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(page);

        var result = await StateToolHandlers.ScanTagMembersAsync(query, CancellationToken.None, "idx-e", "red", 8, "m");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(page));
            Assert.That(captured!.IndexName, Is.EqualTo("idx-e"));
            Assert.That(captured!.Tag, Is.EqualTo("red"));
            Assert.That(captured!.PageSize, Is.EqualTo(8));
            Assert.That(captured!.PageToken, Is.EqualTo("m"));
        });
    }

    [Test]
    public async Task GetTreeSummaryAsync_forwards_tree_id_and_deep_flag()
    {
        var query = Query();
        var summary = TreeSummaryResult.NotFound("tree-x");
        query.GetTreeSummaryAsync("tree-x", false, Arg.Any<CancellationToken>()).Returns(summary);

        var result = await StateToolHandlers.GetTreeSummaryAsync(query, CancellationToken.None, "tree-x", deep: false);

        Assert.That(result, Is.SameAs(summary));
        await query.Received(1).GetTreeSummaryAsync("tree-x", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeSummaryAsync_defaults_to_a_deep_read()
    {
        var query = Query();
        query.GetTreeSummaryAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(TreeSummaryResult.NotFound("tree-y"));

        await StateToolHandlers.GetTreeSummaryAsync(query, CancellationToken.None, "tree-y");

        await query.Received(1).GetTreeSummaryAsync("tree-y", true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetShardSummariesAsync_forwards_tree_id_and_deep_flag()
    {
        var query = Query();
        var shards = ShardSummariesResult.NotFound("tree-z");
        query.GetShardSummariesAsync("tree-z", false, Arg.Any<CancellationToken>()).Returns(shards);

        var result = await StateToolHandlers.GetShardSummariesAsync(query, CancellationToken.None, "tree-z", deep: false);

        Assert.That(result, Is.SameAs(shards));
        await query.Received(1).GetShardSummariesAsync("tree-z", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_wraps_the_count_for_an_existing_tree()
    {
        var query = Query();
        query.GetPhysicalShardCountAsync("tree-p", Arg.Any<CancellationToken>()).Returns(4);

        var result = await StateToolHandlers.GetPhysicalShardCountAsync(query, CancellationToken.None, "tree-p");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("tree-p"));
            Assert.That(result.PhysicalShardCount, Is.EqualTo(4));
            Assert.That(result.TreeExists, Is.True);
        });
    }

    [Test]
    public async Task GetPhysicalShardCountAsync_reports_not_found_as_a_null_count()
    {
        var query = Query();
        query.GetPhysicalShardCountAsync("gone", Arg.Any<CancellationToken>()).Returns((int?)null);

        var result = await StateToolHandlers.GetPhysicalShardCountAsync(query, CancellationToken.None, "gone");

        Assert.Multiple(() =>
        {
            Assert.That(result.PhysicalShardCount, Is.Null);
            Assert.That(result.TreeExists, Is.False);
        });
    }

    [Test]
    public async Task GetTreeStructureAsync_maps_scope_and_budgets()
    {
        var query = Query();
        var structure = TreeStructureResult.NotFound("tree-s");
        StructureRequest? captured = null;
        query.GetTreeStructureAsync(Arg.Do<StructureRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(structure);

        var result = await StateToolHandlers.GetTreeStructureAsync(
            query, CancellationToken.None, "tree-s", shardIndex: 2, subPathNodeId: "n7", depthLimit: 3, maxNodes: 50);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(structure));
            Assert.That(captured!.TreeId, Is.EqualTo("tree-s"));
            Assert.That(captured!.ShardIndex, Is.EqualTo(2));
            Assert.That(captured!.SubPathNodeId, Is.EqualTo("n7"));
            Assert.That(captured!.DepthLimit, Is.EqualTo(3));
            Assert.That(captured!.MaxNodes, Is.EqualTo(50));
        });
    }

    [Test]
    public async Task GetTreeStructureAsync_defaults_to_the_whole_tree_and_default_budgets()
    {
        var query = Query();
        StructureRequest? captured = null;
        query.GetTreeStructureAsync(Arg.Do<StructureRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(TreeStructureResult.NotFound("tree-w"));

        await StateToolHandlers.GetTreeStructureAsync(query, CancellationToken.None, "tree-w");

        Assert.Multiple(() =>
        {
            Assert.That(captured!.ShardIndex, Is.Null);
            Assert.That(captured!.SubPathNodeId, Is.Null);
            Assert.That(captured!.DepthLimit, Is.EqualTo(StructureRequest.DefaultDepthLimit));
            Assert.That(captured!.MaxNodes, Is.EqualTo(StructureRequest.DefaultMaxNodes));
        });
    }

    [Test]
    public async Task ScanEntriesAsync_maps_range_paging_preview_and_mode()
    {
        var query = Query();
        var scan = EntryScanResult.Found("tree-e", Array.Empty<EntryRecord>(), "next");
        EntryScanRequest? captured = null;
        query.ScanEntriesAsync(Arg.Do<EntryScanRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(scan);

        var result = await StateToolHandlers.ScanEntriesAsync(
            query, CancellationToken.None, "tree-e",
            startInclusive: "a", endExclusive: "z", reverse: true,
            pageSize: 25, continuationToken: "ct", valuePreviewBudget: 64,
            indexName: "idx", tag: "blue", mode: EntryScanMode.Live);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(scan));
            Assert.That(captured!.TreeId, Is.EqualTo("tree-e"));
            Assert.That(captured!.StartInclusive, Is.EqualTo("a"));
            Assert.That(captured!.EndExclusive, Is.EqualTo("z"));
            Assert.That(captured!.Reverse, Is.True);
            Assert.That(captured!.PageSize, Is.EqualTo(25));
            Assert.That(captured!.ContinuationToken, Is.EqualTo("ct"));
            Assert.That(captured!.ValuePreviewBudget, Is.EqualTo(64));
            Assert.That(captured!.IndexName, Is.EqualTo("idx"));
            Assert.That(captured!.Tag, Is.EqualTo("blue"));
            Assert.That(captured!.Mode, Is.EqualTo(EntryScanMode.Live));
        });
    }

    [Test]
    public async Task ScanEntriesAsync_defaults_are_a_forward_snapshot_scan_of_the_whole_tree()
    {
        var query = Query();
        EntryScanRequest? captured = null;
        query.ScanEntriesAsync(Arg.Do<EntryScanRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(EntryScanResult.NotFound("tree-f"));

        await StateToolHandlers.ScanEntriesAsync(query, CancellationToken.None, "tree-f");

        Assert.Multiple(() =>
        {
            Assert.That(captured!.StartInclusive, Is.Null);
            Assert.That(captured!.EndExclusive, Is.Null);
            Assert.That(captured!.Reverse, Is.False);
            Assert.That(captured!.ContinuationToken, Is.Null);
            Assert.That(captured!.IndexName, Is.Null);
            Assert.That(captured!.Tag, Is.Null);
            Assert.That(captured!.Mode, Is.EqualTo(EntryScanMode.Snapshot));
        });
    }

    [Test]
    public async Task GetEntryAsync_forwards_tree_id_and_key()
    {
        var query = Query();
        var detail = EntryDetailResult.KeyNotFound("tree-g", "k1");
        query.GetEntryAsync("tree-g", "k1", Arg.Any<CancellationToken>()).Returns(detail);

        var result = await StateToolHandlers.GetEntryAsync(query, CancellationToken.None, "tree-g", "k1");

        Assert.That(result, Is.SameAs(detail));
        await query.Received(1).GetEntryAsync("tree-g", "k1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetEntryHistoryAsync_maps_key_paging_preview_and_order()
    {
        var query = Query();
        var history = EntryHistoryResult.TreeNotFound("tree-h", "k2");
        EntryHistoryRequest? captured = null;
        query.GetEntryHistoryAsync(Arg.Do<EntryHistoryRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(history);

        var result = await StateToolHandlers.GetEntryHistoryAsync(
            query, CancellationToken.None, "tree-h", "k2",
            limit: 15, continuationToken: "hct", valuePreviewBudget: 32, reverse: true);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(history));
            Assert.That(captured!.TreeId, Is.EqualTo("tree-h"));
            Assert.That(captured!.Key, Is.EqualTo("k2"));
            Assert.That(captured!.Limit, Is.EqualTo(15));
            Assert.That(captured!.ContinuationToken, Is.EqualTo("hct"));
            Assert.That(captured!.ValuePreviewBudget, Is.EqualTo(32));
            Assert.That(captured!.Reverse, Is.True);
        });
    }

    [Test]
    public async Task CancelScanAsync_forwards_token_and_acknowledges()
    {
        var query = Query();

        var result = await StateToolHandlers.CancelScanAsync(query, CancellationToken.None, "tree-c", "tok-9");

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("tree-c"));
            Assert.That(result.Acknowledged, Is.True);
        });
        await query.Received(1).CancelScanAsync("tree-c", "tok-9", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CancelScanAsync_tolerates_a_null_token()
    {
        var query = Query();

        var result = await StateToolHandlers.CancelScanAsync(query, CancellationToken.None, "tree-c");

        Assert.That(result.Acknowledged, Is.True);
        await query.Received(1).CancelScanAsync("tree-c", null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_query()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => StateToolHandlers.GetClusterInfoAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                () => StateToolHandlers.ListTreesAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                () => StateToolHandlers.GetPhysicalShardCountAsync(null!, CancellationToken.None, "t"),
                Throws.ArgumentNullException);
            Assert.That(
                () => StateToolHandlers.CancelScanAsync(null!, CancellationToken.None, "t"),
                Throws.ArgumentNullException);
        });
    }
}
