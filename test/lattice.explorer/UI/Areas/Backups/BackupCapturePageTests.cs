using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The capture form at <c>/backups/new</c>: full (whole tree, prefix, key),
/// incremental on a chosen base, and a set of trees, each validated before it
/// starts a staged operation and moves to its status page.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupCapturePageTests : BackupsTestContext
{
    [Test]
    public void The_capture_page_shows_the_way_back_to_the_catalogue_not_a_row_with_no_tab_selected()
    {
        // #3987: /backups/new is none of Catalogue, Schedules, Health or Maintenance.
        var cut = RenderAt<BackupCapturePage>("backups/new");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-backups-back__link"), Has.Count.EqualTo(1)));
        var back = cut.Find(".lt-backups-back__link");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-backups-nav__link"), Is.Empty);
            Assert.That(back.TextContent, Is.EqualTo(BackupsNav.BackText));
            Assert.That(back.GetAttribute("href"), Is.EqualTo("backups"));
            Assert.That(back.Closest("nav")!.GetAttribute("aria-label"), Is.EqualTo("Backups pages"));
        });
    }

    [Test]
    public void A_full_capture_of_a_whole_tree_starts_an_operation_and_opens_its_status_page()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=a/crm/orders");

        Input(cut, "Name", "nightly");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/1")));
        var request = Backups.LastOf<LatticeBackupCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Name, Is.EqualTo("nightly"));
            Assert.That(request.Scope, Is.EqualTo(BackupScopeSelector.WholeTree("a/crm/orders")));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Capture a backup"));
        });
    }

    [Test]
    [TestCase("prefix", "order/2026-09/", 1)]
    [TestCase("key", "order/1", 2)]
    public void A_full_capture_can_be_limited_to_a_prefix_or_a_key(string scope, string value, int kind)
    {
        var cut = RenderAt<BackupCapturePage>("backups/new");

        Input(cut, "Name", "part");
        Input(cut, "Tree", "orders");
        Select(cut, "Scope", scope);
        Input(cut, scope == "prefix" ? "Key prefix" : "Key", value);
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeBackupCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupAsync));
        Assert.That(request.Scope, Is.EqualTo(new BackupScopeSelector((BackupScopeKind)kind, "orders", value)));
    }

    [Test]
    public void Missing_fields_are_named_and_nothing_starts()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new");

        Select(cut, "Scope", "prefix");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            var errors = cut.FindAll(".lt-field__error").Select(error => error.TextContent.Trim()).ToArray();
            Assert.That(errors, Has.Some.Contains("Give the backup a name."));
            Assert.That(errors, Has.Some.Contains("Name the tree to back up."));
            Assert.That(Operations.Recent, Is.Empty);
        });

        Input(cut, "Name", "x");
        Input(cut, "Tree", "orders");
        cut.Find("form").Submit();
        Assert.That(cut.FindAll(".lt-field__error").Select(error => error.TextContent.Trim()), Has.Some.Contains("Give the key prefix."));
    }

    [Test]
    public void An_incremental_capture_builds_on_a_chosen_full_backup_of_the_tree()
    {
        Seed(FakeBackupControl.Manifest("full1", "sunday", "orders"), FakeBackupControl.Manifest("full0", "earlier", "orders", DateTimeOffset.UnixEpoch));
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=orders");

        Select(cut, "Kind", "incremental");
        Input(cut, "Name", "monday");
        cut.Find("form").Submit();
        Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo("Choose the full backup this one builds on."));

        cut.FindAll("button").Single(button => button.TextContent == "Find full backups of this tree").Click();
        cut.WaitUntil(() => Assert.That(SelectNamed(cut, "Base backup").QuerySelectorAll("option"), Has.Length.EqualTo(2)));
        var query = Backups.LastOf<BackupCatalogRequest>(nameof(ILatticeBackupControl.ListBackupsAsync));
        Assert.Multiple(() =>
        {
            Assert.That(query.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(query.TreeId, Is.EqualTo("orders"));
        });

        Select(cut, "Base backup", "full0");
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartIncrementalBackupAsync)), Is.EqualTo(1)));
        Assert.That(Backups.LastOf<LatticeBackupIncrementalCaptureRequest>(nameof(ILatticeBackupOperations.StartIncrementalBackupAsync)).BaseBackupId, Is.EqualTo("full0"));
    }

    [Test]
    public void A_tree_with_no_full_backup_says_to_capture_one_first()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=orders");

        Select(cut, "Kind", "incremental");
        cut.FindAll("button").Single(button => button.TextContent == "Find full backups of this tree").Click();

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("no full backup to build on yet")));
    }

    [Test]
    public void Finding_bases_needs_a_tree_and_reports_a_refusal()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new");
        Select(cut, "Kind", "incremental");
        cut.FindAll("button").Single(button => button.TextContent == "Find full backups of this tree").Click();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the tree first."));

        Backups.List = _ => Task.FromException<BackupCatalogPage>(new LatticeAuthorizationDeniedException());
        Input(cut, "Tree", "orders");
        cut.FindAll("button").Single(button => button.TextContent == "Find full backups of this tree").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void A_set_capture_collects_trees_one_line_each_and_captures_them_together()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=orders");

        Select(cut, "Kind", "set");
        Input(cut, "Name", "quarter");
        Input(cut, "Tree to add", "a/crm/customers");
        cut.FindAll("button").Single(button => button.TextContent == "Add tree").Click();
        Input(cut, "Tree to add", "stock");
        cut.FindAll("button").Single(button => button.TextContent == "Add tree").Click();
        Input(cut, "Tree to add", "stock");
        cut.FindAll("button").Single(button => button.TextContent == "Add tree").Click();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("already in the set"));

        cut.Find("button[aria-label='Remove stock']").Click();
        Assert.That(cut.FindAll(".lt-backups-trees__item"), Has.Count.EqualTo(2));
        cut.Find("form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupSetAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeBackupSetCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupSetAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Scopes.Select(scope => scope.TreeId), Is.EqualTo(new[] { "orders", "a/crm/customers" }));
            Assert.That(request.CrossTreeConsistent, Is.True);
        });
    }

    [Test]
    public void A_set_needs_at_least_one_tree_and_an_empty_tree_cannot_be_added()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new");

        Select(cut, "Kind", "set");
        Input(cut, "Name", "quarter");
        cut.FindAll("button").Single(button => button.TextContent == "Add tree").Click();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name a tree to add."));
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Add at least one tree."));
            Assert.That(Operations.Recent, Is.Empty);
        });
    }

    [Test]
    public void Cancel_returns_to_the_catalogue()
    {
        var cut = RenderAt<BackupCapturePage>("backups/new");

        Assert.That(cut.FindAll("a").Single(a => a.TextContent == "Cancel").GetAttribute("href"), Is.EqualTo("backups"));
    }

    private static void Input(IRenderedComponent<BackupCapturePage> cut, string label, string value)
    {
        var id = cut.FindAll("label").Single(candidate => candidate.TextContent == label).GetAttribute("for");
        cut.Find("#" + id).Input(value);
    }

    private static AngleSharp.Dom.IElement SelectNamed(IRenderedComponent<BackupCapturePage> cut, string label)
    {
        var id = cut.FindAll("label").Single(candidate => candidate.TextContent == label).GetAttribute("for");
        return cut.Find("#" + id);
    }

    private static void Select(IRenderedComponent<BackupCapturePage> cut, string label, string value) =>
        SelectNamed(cut, label).Change(value);
}
