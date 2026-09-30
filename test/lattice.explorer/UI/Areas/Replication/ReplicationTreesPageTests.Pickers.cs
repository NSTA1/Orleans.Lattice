using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// Issue #3949: enabling replication names the tree with a picker over the
/// caller's trees, refusing one that does not exist, and suggests the bootstrap
/// cluster from the regions this cluster knows.
/// </summary>
public sealed partial class ReplicationTreesPageTests
{
    [Test]
    public void The_enable_dialog_offers_trees_and_refuses_one_that_does_not_exist()
    {
        UseTrees();
        Services.UseTreeCatalogue("inventory", "orders");
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();

        Assert.That(SuggestionFields.Offers(cut, "Tree", "inv"), Is.EqualTo(new[] { "inventory" }));

        SuggestionFields.Box(cut, "Tree").Input("inventroy");
        cut.Find("form.lt-replication-form select").Change("PnCounter");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named inventroy. Choose one from the list.")));
        Assert.That(Control.Enables, Is.Empty);
    }

    [Test]
    public void The_bootstrap_cluster_is_suggested_from_the_known_regions()
    {
        UseTrees();
        UseEstate();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.GreaterThan(0)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();

        var offered = SuggestionFields.Offers(cut, "Bootstrap from cluster (optional)", string.Empty);

        Assert.That(offered[0], Is.EqualTo(Status.LocalRegionId), "this region first, then its peers");
    }
}
