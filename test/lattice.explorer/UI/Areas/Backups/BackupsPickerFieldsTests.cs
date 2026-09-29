using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Issue #3949: every Backups field that names a tree is a type-ahead picker over
/// the caller's tree catalogue. Capture and schedules refuse a tree that does not
/// exist; the restore target and the catalogue filter only suggest, because a
/// restore may create a tree and a backup may outlive its tree.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsPickerFieldsTests : BackupsTestContext
{
    [Test]
    public void Capture_offers_trees_and_refuses_one_that_does_not_exist()
    {
        Services.UseTreeCatalogue("orders", "orders-archive", "billing");
        var cut = RenderAt<BackupCapturePage>("backups/new");

        Assert.That(SuggestionFields.Offers(cut, "Tree", "ord"), Is.EqualTo(new[] { "orders", "orders-archive" }));

        Labelled(cut, "Name").Input("nightly");
        SuggestionFields.Box(cut, "Tree").Input("ordrs");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Is.EqualTo("No tree is named ordrs. Choose one from the list.")));
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CreateBackupAsync)), Is.Zero);
    }

    [Test]
    public void A_set_adds_only_trees_that_exist()
    {
        Services.UseTreeCatalogue("orders", "billing");
        var cut = RenderAt<BackupCapturePage>("backups/new");
        Labelled(cut, "Kind").Change("set");

        Assert.That(SuggestionFields.Offers(cut, "Tree to add", "bil"), Is.EqualTo(new[] { "billing" }));
        cut.FindAll("[role=option]")[0].Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-backups-trees__item"), Has.Count.EqualTo(1)));

        SuggestionFields.Box(cut, "Tree to add").Input("nowhere");
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Add tree").Click();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree to add"), Is.EqualTo("No tree is named nowhere. Choose one from the list.")));
        Assert.That(cut.FindAll(".lt-backups-trees__item"), Has.Count.EqualTo(1));
    }

    [Test]
    public void Schedules_offer_trees_and_refuse_one_that_does_not_exist()
    {
        Services.UseTreeCatalogue("orders");
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules");

        Assert.That(SuggestionFields.Offers(cut, "Tree", "o"), Is.EqualTo(new[] { "orders" }));

        SuggestionFields.Box(cut, "Tree").Input("orderz");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Does.StartWith("No tree is named orderz.")));
        Assert.That(CurrentPath, Is.EqualTo("/backups/schedules"));
    }

    [Test]
    public void The_catalogue_filter_suggests_trees_and_accepts_one_that_no_longer_exists()
    {
        Services.UseTreeCatalogue("orders");
        var cut = RenderAt<BackupsCataloguePage>("backups");

        Assert.That(SuggestionFields.Offers(cut, "Filter by tree", "or"), Is.EqualTo(new[] { "orders" }));

        SuggestionFields.Box(cut, "Filter by tree").Input("deleted-tree");
        SuggestionFields.Box(cut, "Filter by tree").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "Enter" });

        cut.WaitUntil(() => Assert.That(CurrentPath, Does.Contain("tree=deleted-tree")));
    }

    [Test]
    public void The_restore_target_suggests_trees_flags_an_existing_one_and_accepts_a_new_name()
    {
        Services.UseTreeCatalogue("orders", "orders-copy-1");
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(SuggestionFields.Box(cut, "Restore into tree"), Is.Not.Null));

        Assert.That(SuggestionFields.Offers(cut, "Restore into tree", "orders"), Is.EqualTo(new[] { "orders", "orders-copy-1" }));
        cut.WaitUntil(() => Assert.That(SuggestionFields.FlagOf(cut, "Restore into tree"), Is.EqualTo("This tree exists: restoring replaces what it holds.")));

        SuggestionFields.Box(cut, "Restore into tree").Input("orders-copy-2");
        cut.WaitUntil(() => Assert.That(SuggestionFields.FlagOf(cut, "Restore into tree"), Is.Null));
        cut.Find("form.lt-backups-form").Submit();

        Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("orders-copy-2"), "a new tree name is accepted");
    }

    private static AngleSharp.Dom.IElement Labelled<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.Find("#" + cut.FindAll("label").Single(candidate => candidate.TextContent.Trim() == label).GetAttribute("for"));
}
