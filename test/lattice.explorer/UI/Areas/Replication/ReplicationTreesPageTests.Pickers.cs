using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;
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

    [Test]
    public void Once_a_bootstrap_source_is_named_the_picker_accepts_a_tree_this_cluster_does_not_hold_yet()
    {
        UseTrees();
        Services.UseTreeCatalogue("inventory", "orders");

        // The enable is held so the dialog, and its Tree field, are still drawn when the picker's
        // verdict is read: a successful enable closes the dialog and removes the field.
        Control.ChangeGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();

        SuggestionFields.Box(cut, "Tree").Input("factory-orders");
        SuggestionFields.Box(cut, "Bootstrap from cluster (optional)").Input("west");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() => Assert.That(Control.Enables, Is.EqualTo(new[] { ("factory-orders", LatticeMergeMode.LwwRegister, (string?)"west") })));
        Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.Null, "the bootstrap brings the tree, so it need not exist here yet");

        cut.InvokeAsync(() => Control.ChangeGate.SetResult());
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication enabled for factory-orders (LWW register). A snapshot bootstrap was requested."));
        });
    }

    [Test]
    public void Clearing_the_bootstrap_source_makes_the_picker_refuse_a_missing_tree_again()
    {
        UseTrees();
        Services.UseTreeCatalogue("inventory", "orders");
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();

        SuggestionFields.Box(cut, "Bootstrap from cluster (optional)").Input("west");
        Assert.That(cut.Instance.EnableTreeMode, Is.EqualTo(LtComboBoxMode.Suggest));
        SuggestionFields.Box(cut, "Bootstrap from cluster (optional)").Input(string.Empty);
        SuggestionFields.Box(cut, "Tree").Input("inventroy");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named inventroy. Choose one from the list.")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.EnableTreeMode, Is.EqualTo(LtComboBoxMode.PickExisting));
            Assert.That(Control.Enables, Is.Empty);
        });
    }
}
