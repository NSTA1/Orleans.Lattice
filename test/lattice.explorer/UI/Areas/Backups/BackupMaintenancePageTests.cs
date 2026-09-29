using Microsoft.Extensions.DependencyInjection;
using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Catalogue maintenance at <c>/backups/maintenance</c>, and the area's small
/// components: the page links, the health pill and the tree label.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupMaintenancePageTests : BackupsTestContext
{
    [Test]
    public void A_connection_that_does_not_serve_maintenance_says_so_and_disables_it()
    {
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent is "Rebuild catalogue..." or "Check catalogue"), Has.All.Matches<AngleSharp.Dom.IElement>(button => button.HasAttribute("disabled")));
            Assert.That(cut.Find(".lt-backups-nav__link[aria-current]").TextContent, Is.EqualTo("Maintenance"));
        });
    }

    [Test]
    public void Rebuild_asks_first_then_runs_as_a_staged_operation()
    {
        ServeExtensions();
        Backups.Rebuild = () => Task.FromResult(new BackupCatalogRebuildReport(4, 1, 3));
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");
        cut.WaitUntil(() => Assert.That(Button(cut, "Rebuild catalogue...").HasAttribute("disabled"), Is.False));

        Button(cut, "Rebuild catalogue...").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel").Click();
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RebuildCatalogFromSinkAsync)), Is.Zero);

        Button(cut, "Rebuild catalogue...").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Rebuild").Click();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/1")));
        Assert.That(Operations.Find("1")!.Kind, Is.EqualTo(BackupOperationKind.RebuildCatalogue));

        var again = RenderAt<BackupMaintenancePage>("backups/maintenance");
        again.WaitUntil(() => Assert.That(again.FindAll("a").Single(a => a.GetAttribute("href") == "backups/operations/1").TextContent, Is.EqualTo("Rebuilt the catalogue from the backup store.")));
    }

    [Test]
    public void A_check_that_finds_orphans_offers_their_removal_behind_a_typed_confirmation()
    {
        ServeExtensions();
        Backups.Scrub = prune => Task.FromResult(new BackupCatalogScrubReport(5, 2, prune ? 2 : 0, prune, ["o1", "o2"]));
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");
        cut.WaitUntil(() => Assert.That(Button(cut, "Check catalogue").HasAttribute("disabled"), Is.False));

        Button(cut, "Check catalogue").Click();
        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/1")));
        Assert.That(Backups.LastOf<bool>(nameof(ILatticeBackupControl.ScrubCatalogAgainstSinkAsync)), Is.False, "the check changes nothing");

        var page = RenderAt<BackupMaintenancePage>("backups/maintenance");
        page.WaitUntil(() => Assert.That(page.FindAll("button").Where(button => button.TextContent == "Remove orphan rows..."), Has.Exactly(1).Items));
        Button(page, "Remove orphan rows...").Click();
        Assert.That(page.Find("[role=alertdialog]").TextContent, Does.Contain("The store itself is not touched"));
        page.Find("[role=alertdialog] input").Input("backup catalogue");
        page.Find("[role=alertdialog] form").Submit();

        page.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScrubCatalogAgainstSinkAsync)), Is.EqualTo(2)));
        Assert.That(Backups.LastOf<bool>(nameof(ILatticeBackupControl.ScrubCatalogAgainstSinkAsync)), Is.True);
    }

    [Test]
    public void A_clean_check_offers_no_removal()
    {
        ServeExtensions();
        Backups.Scrub = _ => Task.FromResult(new BackupCatalogScrubReport(5, 0, 0, false, []));
        var operation = Services.GetService<BackupActions>()!.ScrubCatalogue(false);
        Assert.That(operation.Status, Is.EqualTo(BackupOperationStatus.Succeeded));

        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Every catalogue row has its backup in the store.")));
        Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Remove orphan rows..."), Is.Empty);
    }

    [Test]
    public void The_page_links_mark_the_current_page_and_follow_health_availability()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);

        var cut = Render<BackupsNav>(parameters => parameters.Add(nav => nav.Current, BackupsNav.HealthPage));

        cut.WaitUntil(() =>
        {
            var links = cut.FindAll(".lt-backups-nav__link");
            Assert.That(links.Select(link => link.TextContent), Is.EqualTo(new[] { "Catalogue", "Schedules", "Health", "Maintenance" }));
            Assert.That(links.Single(link => link.GetAttribute("aria-current") == "page").TextContent, Is.EqualTo("Health"));
            Assert.That(links.Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "backups", "backups/schedules", "backups/health", "backups/maintenance" }));
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Backups pages"));
        });
        cut.Instance.Dispose();
    }

    [Test]
    [TestCase(null, false, "Not checked", "unknown")]
    [TestCase(null, true, "Checking", "unknown")]
    [TestCase(1, false, "Healthy", "healthy")]
    [TestCase(2, false, "Warning", "drift")]
    [TestCase(3, false, "Missing", "failed")]
    [TestCase(0, false, "Unknown", "unknown")]
    public void The_health_pill_names_the_status_with_its_state_role(int? status, bool pending, string text, string role)
    {
        var report = status is { } value
            ? new BackupHealthReport("b1", (BackupHealthStatus)value, true, [], [], DateTimeOffset.UnixEpoch, "x")
            : null;

        var cut = Render<BackupHealthPill>(parameters => parameters.Add(pill => pill.Report, report).Add(pill => pill.Pending, pending));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-pill").TextContent.Trim(), Is.EqualTo(text));
            Assert.That(cut.Find(".lt-pill").GetAttribute("data-lt-state"), Is.EqualTo(role));
        });
    }

    [Test]
    public void The_tree_label_names_an_app_tree_by_its_local_name_and_app()
    {
        var app = Render<BackupTreeLabel>(parameters => parameters.Add(label => label.TreeId, "t/acme/a/crm/orders").Add(label => label.AppLabel, "<i>CRM</i>"));
        var plain = Render<BackupTreeLabel>(parameters => parameters.Add(label => label.TreeId, "inventory").Add(label => label.Mono, false));

        Assert.Multiple(() =>
        {
            Assert.That(app.Find("span").TextContent, Is.EqualTo("orders"));
            Assert.That(app.Find("span").GetAttribute("title"), Is.EqualTo("t/acme/a/crm/orders"));
            Assert.That(app.Find(".lt-backups-tree__app").TextContent, Is.EqualTo("app <i>CRM</i>"));
            Assert.That(app.FindAll("i"), Is.Empty, "an app's presentation text renders as text");
            Assert.That(plain.Find("span").TextContent, Is.EqualTo("inventory"));
            Assert.That(plain.Find("span").ClassList, Is.Empty);
            Assert.That(plain.FindAll(".lt-backups-tree__app"), Is.Empty);
            Assert.That(() => Render<BackupTreeLabel>(), Throws.InvalidOperationException);
        });
    }

    private void ServeExtensions() =>
        Backups.Inventory = () => Task.FromResult(new BackupInventoryReport(0, 0, 0, 0, null, null, 0, 0, 0));

    private static AngleSharp.Dom.IElement Button<T>(IRenderedComponent<T> cut, string text)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("button").Single(button => button.TextContent == text);
}
