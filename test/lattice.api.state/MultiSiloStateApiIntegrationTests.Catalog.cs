using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Discovery / catalog multi-silo coverage. A runtime view created on one
/// silo must be visible to a facade served by another silo (it is recorded in the
/// cluster-wide view registry, not just the originating silo's local catalog), and
/// the tree catalog must enumerate trees regardless of which silo hosts their
/// shards. Reserved (<c>view-</c> / system) trees must stay invisible to the
/// per-tree summary surfaces, not just to <c>ListTreesAsync</c>.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    /// <summary>
    /// The bound on cross-silo view visibility. Generous because the guarantee
    /// under test is eventual and the write it waits on competes for the thread
    /// pool with the rest of a loaded CI runner; a failure here means the
    /// registration never landed, not that it was slow.
    /// </summary>
    private static readonly TimeSpan ViewVisibilityTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task ListViews_created_on_one_silo_is_visible_from_another_silo()
    {
        const string treeId = "multisilo-view-src";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 8, shardCount: MultiSiloStateApiClusterFixture.ShardCount);
        _fixture.CreateViewOnSilo(treeId, "multisilo-view", siloIndex: 0);

        // Cross-silo visibility is an *eventual* guarantee on this path, so poll
        // to a bound rather than asserting on the first read. The synchronous
        // ILatticeViewFactory.Create overload the fixture uses is documented as
        // the "compatibility path" that "persists and activates in the
        // background": it publishes to the originating silo's local IViewCatalog
        // and starts the cluster-wide registry write fire-and-forget, which is
        // precisely what distinguishes it from CreateAsync ("waits until any
        // runtime registration has been persisted before returning"). The serving
        // silo's local catalog has never seen this view, so it can only learn of
        // it from the registry grain once that unawaited write lands. Asserting
        // immediately races it - and a fixed sleep would only be a slower race.
        var view = await WaitForViewAsync(_fixture.QueryFromOtherSilo(), "multisilo-view");

        Assert.That(view, Is.Not.Null,
            "a runtime view created on one silo must be discoverable from a facade served by another silo");
        Assert.That(view!.SourceTreeId, Is.EqualTo(treeId));
    }

    /// <summary>
    /// Polls <paramref name="query"/> until <paramref name="viewName"/> appears in
    /// the view catalog, returning the entry, or <see langword="null"/> once
    /// <see cref="ViewVisibilityTimeout"/> elapses without it appearing.
    /// </summary>
    private static async Task<ViewStateSummary?> WaitForViewAsync(ILatticeStateQuery query, string viewName)
    {
        using var cts = new CancellationTokenSource(ViewVisibilityTimeout);
        while (true)
        {
            var page = await query.ListViewsAsync(new CatalogRequest(), cts.Token);
            var view = page.Entries.SingleOrDefault(e => e.ViewName == viewName);
            if (view is not null)
            {
                return view;
            }

            try
            {
                await Task.Delay(25, cts.Token);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }
    }

    [Test]
    public async Task ListTrees_enumerates_trees_across_silos()
    {
        await _fixture.CreatePopulatedTreeAsync("multisilo-cat-a", keyCount: 4);
        await _fixture.CreatePopulatedTreeAsync("multisilo-cat-b", keyCount: 4);

        var page = await _fixture.QueryFromOtherSilo().ListTreesAsync(new CatalogRequest());

        var ids = page.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(ids, Does.Contain("multisilo-cat-a"));
        Assert.That(ids, Does.Contain("multisilo-cat-b"));
    }

    [Test]
    public async Task GetTreeSummary_treats_reserved_tree_as_not_found()
    {
        var registry = _fixture.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync("view-reserved-probe", new TreeRegistryEntry { ShardCount = 1 });

        var summary = await _fixture.Query.GetTreeSummaryAsync("view-reserved-probe");

        Assert.That(summary.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "a materialised view belongs to the Views catalog, so it stays invisible to the tree-summary surface, not just to ListTrees");
    }
}
