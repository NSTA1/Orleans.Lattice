using Microsoft.Extensions.DependencyInjection;
using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Catalogue maintenance at <c>/backups/maintenance</c>, and the area's small
/// components: the page links, the health pill and the tree label. A rebuild or a
/// scrub runs on the cluster as a tracked operation (#4125): the page shows each
/// one's latest run as the cluster reports it, followed with real progress, so a
/// run started before a reload or in another tab is still shown.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupMaintenancePageTests : BackupsTestContext
{
    [Test]
    public void A_connection_that_does_not_serve_backup_operations_says_so_and_disables_maintenance()
    {
        Backups.ListFault = new NotSupportedException("not served");

        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo(BackupsFaults.NotServed));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent is "Rebuild catalogue..." or "Check catalogue"), Has.All.Matches<AngleSharp.Dom.IElement>(button => button.HasAttribute("disabled")));
            Assert.That(cut.Find(".lt-backups-nav__link[aria-current]").TextContent, Is.EqualTo("Maintenance"));
        });
    }

    [Test]
    public void Maintenance_is_offered_over_a_connection_that_serves_operations_but_not_the_inventory()
    {
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() =>
        {
            Assert.That(Button(cut, "Rebuild catalogue...").HasAttribute("disabled"), Is.False);
            Assert.That(Button(cut, "Check catalogue").HasAttribute("disabled"), Is.False);
            Assert.That(cut.FindAll("[role=status]").Where(status => status.TextContent == BackupsFaults.NotServed), Is.Empty);
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetInventoryAsync)), Is.Zero, "maintenance no longer rides on the inventory");
    }

    [Test]
    public void Rebuild_asks_first_then_starts_on_the_cluster()
    {
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");
        cut.WaitUntil(() => Assert.That(Button(cut, "Rebuild catalogue...").HasAttribute("disabled"), Is.False));

        Button(cut, "Rebuild catalogue...").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel").Click();
        Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartCatalogRebuildAsync)), Is.Zero);

        Button(cut, "Rebuild catalogue...").Click();
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Rebuild").Click();

        cut.WaitUntil(() => Assert.That(Operations.Find("1")!.ClusterOperationId, Is.EqualTo("op-1")));
        Assert.Multiple(() =>
        {
            Assert.That(Operations.Find("1")!.Kind, Is.EqualTo(BackupOperationKind.RebuildCatalogue));
            Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartCatalogRebuildAsync)), Is.EqualTo(1));
            Assert.That(Backups.CountOf("RebuildCatalogFromSinkAsync"), Is.Zero, "the blocking verb is not called");
        });
    }

    [Test]
    public void The_latest_rebuild_is_followed_with_real_progress_and_survives_a_fresh_page()
    {
        Backups.Statuses["op-3"] = FakeBackupControl.Running("op-3", BackupOperationKinds.CatalogRebuild, "sys-backup-catalog") with
        {
            State = LatticeOperationState.Running,
            Phase = BackupOperationPhases.RebuildingCatalog,
            PhaseIndex = 0,
            PhaseCount = 1,
            CompletedUnits = 4,
            UnitName = BackupOperationUnits.Manifests,
        };

        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() =>
        {
            var rebuild = cut.Find("[aria-labelledby=lt-backups-rebuild-title]");
            Assert.That(rebuild.QuerySelector(".lt-operation-progress .lt-pill")!.TextContent.Trim(), Is.EqualTo("Running"));
            Assert.That(rebuild.QuerySelector(".lt-progress__phase")!.TextContent, Is.EqualTo("Rebuilding the catalogue"));
            Assert.That(rebuild.QuerySelector(".lt-progress__detail")!.TextContent, Is.EqualTo("4 manifests so far"));
            Assert.That(rebuild.QuerySelectorAll("a").Single(a => a.GetAttribute("href") == "backups/operations/op-3"), Is.Not.Null);
        });

        Backups.Succeed("op-3", null, FakeBackupControl.RebuildResult(9, 2, 7));
        Tick();

        cut.WaitUntil(() =>
        {
            var rebuild = cut.Find("[aria-labelledby=lt-backups-rebuild-title]");
            Assert.That(rebuild.QuerySelector(".lt-operation-progress .lt-pill")!.TextContent.Trim(), Is.EqualTo("Succeeded"));
            Assert.That(rebuild.TextContent, Does.Contain("Rebuilt the catalogue from the backup store."));
        });
    }

    [Test]
    public void A_check_that_finds_orphans_offers_their_removal_behind_a_typed_confirmation()
    {
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");
        cut.WaitUntil(() => Assert.That(Button(cut, "Check catalogue").HasAttribute("disabled"), Is.False));

        Button(cut, "Check catalogue").Click();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartCatalogScrubAsync)), Is.EqualTo(1)));
        Assert.That(Backups.LastOf<bool>(nameof(ILatticeBackupOperations.StartCatalogScrubAsync)), Is.False, "the check changes nothing");
        Backups.Succeed("op-1", null, FakeBackupControl.ScrubResult(5, false, "o1", "o2"));

        var page = RenderAt<BackupMaintenancePage>("backups/maintenance");
        page.WaitUntil(() => Assert.That(page.FindAll("button").Where(button => button.TextContent == "Remove orphan rows..."), Has.Exactly(1).Items));
        Assert.That(page.Find("[aria-labelledby=lt-backups-scrub-title]").TextContent, Does.Contain("Found 2 orphan rows."));
        Button(page, "Remove orphan rows...").Click();
        Assert.That(page.Find("[role=alertdialog]").TextContent, Does.Contain("The store itself is not touched"));
        page.Find("[role=alertdialog] input").Input("backup catalogue");
        page.Find("[role=alertdialog] form").Submit();

        page.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartCatalogScrubAsync)), Is.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(Backups.LastOf<bool>(nameof(ILatticeBackupOperations.StartCatalogScrubAsync)), Is.True);
            Assert.That(Backups.CountOf("ScrubCatalogAgainstSinkAsync"), Is.Zero, "the blocking verb is not called");
        });
    }

    [Test]
    public void A_clean_check_offers_no_removal()
    {
        Backups.Statuses["op-5"] = FakeBackupControl.Running("op-5", BackupOperationKinds.CatalogScrub, "sys-backup-catalog");
        Backups.Succeed("op-5", null, FakeBackupControl.ScrubResult(5, false));

        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Every catalogue row has its backup in the store.")));
        Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Remove orphan rows..."), Is.Empty);
    }

    [Test]
    public void Leaving_the_page_stops_following_a_running_run()
    {
        Backups.Statuses["op-3"] = FakeBackupControl.Running("op-3", BackupOperationKinds.CatalogScrub, "sys-backup-catalog") with { State = LatticeOperationState.Running };
        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-operation-progress"), Has.Count.EqualTo(1)));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True);

        cut.Instance.Dispose();
        var reads = Backups.StatusReads;
        Time.Advance(ClusterStatusPoller.Interval * 3);

        Assert.That(Backups.StatusReads, Is.EqualTo(reads));
    }

    [Test]
    public void Unreadable_last_runs_are_stated_and_maintenance_stays_offered()
    {
        Backups.ListFault = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<BackupMaintenancePage>("backups/maintenance");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("p[role=alert]").TextContent, Does.Contain(BackupsFaults.NotPermitted));
            Assert.That(Button(cut, "Check catalogue").HasAttribute("disabled"), Is.False);
        });
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

    private static AngleSharp.Dom.IElement Button<T>(IRenderedComponent<T> cut, string text)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("button").Single(button => button.TextContent == text);

    /// <summary>Moves the circuit's clock one polling interval, once a follower has armed its wait.</summary>
    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }
}
