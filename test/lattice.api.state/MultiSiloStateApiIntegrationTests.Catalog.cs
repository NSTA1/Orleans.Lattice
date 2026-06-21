using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// F-113 discovery / catalog multi-silo coverage. A runtime view created on one
/// silo must be visible to a facade served by another silo (it is recorded in the
/// cluster-wide view registry, not just the originating silo's local catalog), and
/// the tree catalog must enumerate trees regardless of which silo hosts their
/// shards. Reserved (<c>view-</c> / system) trees must stay invisible to the
/// per-tree summary surfaces, not just to <c>ListTreesAsync</c>.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    [Test]
    public async Task ListViews_created_on_one_silo_is_visible_from_another_silo()
    {
        const string treeId = "multisilo-view-src";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 8, shardCount: MultiSiloStateApiClusterFixture.ShardCount);
        _fixture.CreateViewOnSilo(treeId, "multisilo-view", siloIndex: 0);

        var page = await _fixture.QueryFromOtherSilo().ListViewsAsync(new CatalogRequest());

        var view = page.Entries.SingleOrDefault(e => e.ViewName == "multisilo-view");
        Assert.That(view, Is.Not.Null,
            "a runtime view created on one silo must be discoverable from a facade served by another silo");
        Assert.That(view!.SourceTreeId, Is.EqualTo(treeId));
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
        var shards = await _fixture.Query.GetShardSummariesAsync("view-reserved-probe");

        Assert.That(summary.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
            "reserved trees must be invisible to the per-tree summary surface, not just to ListTrees");
        Assert.That(shards.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }
}
