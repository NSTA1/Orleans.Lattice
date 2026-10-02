using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The catalogue at <c>/backups</c>: rows, server-side filters carried in the
/// address, paging, inventory, scope status, health, the loading, empty and
/// error states, the compact form, and tenancy.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsCataloguePageTests : BackupsTestContext
{
    [Test]
    public void The_catalogue_lists_backups_newest_first_with_app_trees_by_their_local_name()
    {
        Seed(
            FakeBackupControl.Manifest("b1", "older", "a/crm/orders", new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero)),
            FakeBackupControl.Manifest("b2", "newer", "inventory", new DateTimeOffset(2026, 9, 2, 0, 0, 0, TimeSpan.Zero)) with { SetName = "quarter" });

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].QuerySelector("a")!.TextContent, Is.EqualTo("newer"));
            Assert.That(rows[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/b2"));
            Assert.That(rows[0].TextContent, Does.Contain("Full, set quarter"));
            Assert.That(rows[1].TextContent, Does.Contain("orders").And.Contain("app crm"));
            Assert.That(rows[1].TextContent, Does.Not.Contain("a/crm/orders"), "an app tree is named by its local name");
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Backups"));
            Assert.That(cut.Find(".lt-backups-nav__link[aria-current]").TextContent, Is.EqualTo("Catalogue"));
        });
        var request = Backups.LastOf<BackupCatalogRequest>(nameof(ILatticeBackupControl.ListBackupsAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.OrderByCreatedDescending, Is.True);
            Assert.That(request.PageSize, Is.EqualTo(BackupsCataloguePage.PageSize));
        });
    }

    [Test]
    public void The_filters_live_in_the_address_and_run_on_the_server()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"), FakeBackupControl.Manifest("b2", "weekly", "orders", baseId: "b1"));

        var cut = RenderAt<BackupsCataloguePage>("backups?kind=incremental&name=week&tree=orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        var request = Backups.LastOf<BackupCatalogRequest>(nameof(ILatticeBackupControl.ListBackupsAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(request.NamePrefix, Is.EqualTo("week"));
            Assert.That(request.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void Changing_a_filter_navigates_to_the_filtered_address()
    {
        var cut = RenderAt<BackupsCataloguePage>("backups");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-empty"), Has.Count.EqualTo(1)));

        cut.Find("select").Change("full");
        Assert.That(CurrentPath, Is.EqualTo("/backups?kind=full"));

        var searches = cut.FindAll("input[type=search]");
        searches[0].Input("nig");
        searches[0].KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "Enter" });
        Assert.That(CurrentPath, Does.Contain("name=nig"));

        var tree = cut.Find("input[role=combobox]");
        tree.Input("orders");
        cut.Find("input[role=combobox]").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "Enter" });
        Assert.That(CurrentPath, Does.Contain("tree=orders"));
    }

    [Test]
    public void An_empty_catalogue_and_an_empty_filter_say_different_things()
    {
        var cut = RenderAt<BackupsCataloguePage>("backups");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No backups yet")));

        var filtered = RenderAt<BackupsCataloguePage>("backups?kind=full");
        filtered.WaitUntil(() => Assert.That(filtered.Find(".lt-empty__title").TextContent, Is.EqualTo("No backup matches these filters")));
    }

    [Test]
    public void The_catalogue_shows_a_skeleton_while_it_loads()
    {
        var list = new TaskCompletionSource<BackupCatalogPage>();
        Backups.List = _ => list.Task;

        var cut = RenderAt<BackupsCataloguePage>("backups");
        Assert.That(cut.FindAll(".lt-skeleton"), Is.Not.Empty);

        list.SetResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("b1")] });
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_failed_load_says_why_and_can_be_retried()
    {
        var calls = 0;
        Backups.List = _ => ++calls == 1
            ? Task.FromException<BackupCatalogPage>(new LatticeAuthorizationDeniedException())
            : Task.FromResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("b1")] });

        var cut = RenderAt<BackupsCataloguePage>("backups");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));

        cut.FindAll("button").Single(button => button.TextContent == "Try again").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void The_catalogue_pages_forward_and_back()
    {
        for (var i = 0; i < BackupsCataloguePage.PageSize + 3; i++)
        {
            Seed(FakeBackupControl.Manifest("b" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture), createdAt: DateTimeOffset.UnixEpoch.AddMinutes(i)));
        }

        var cut = RenderAt<BackupsCataloguePage>("backups");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(BackupsCataloguePage.PageSize)));
        Assert.That(Pager(cut, "Previous page").HasAttribute("disabled"), Is.True);

        Pager(cut, "Next page").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.Find(".lt-backups-pager__position").TextContent, Is.EqualTo("Page 2"));
            Assert.That(Pager(cut, "Next page").HasAttribute("disabled"), Is.True);
        });

        Pager(cut, "Previous page").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(BackupsCataloguePage.PageSize)));
    }

    [Test]
    public void The_lede_carries_the_inventory_when_it_is_served()
    {
        Backups.Inventory = () => Task.FromResult(new BackupInventoryReport(3, 3072, 2, 1, null, new DateTimeOffset(2026, 9, 28, 0, 0, 0, TimeSpan.Zero), 0, 0, 0));

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-page-lede").TextContent,
            Is.EqualTo("3 backups (2 full, 1 incremental), 3 KiB, newest 2026-09-28 00:00:00 UTC.")));
    }

    [Test]
    public void A_tree_filter_shows_that_trees_schedule_status()
    {
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(
            BackupScopeSelector.WholeTree("orders"), true, false, null, new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), null, null,
            BackupScopeRunOutcome.Success, 3, TimeSpan.FromHours(6));

        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");

        cut.WaitUntil(() =>
        {
            var section = cut.Find("section.lt-backups-section");
            Assert.That(section.TextContent, Does.Contain("Registered, every 6 h").And.Contain("Succeeded").And.Contain("2026-09-01 00:00:00 UTC"));
            Assert.That(section.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/schedules?tree=orders"));
        });
    }

    [Test]
    public void A_tree_with_no_status_says_so_and_a_denied_status_says_why()
    {
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.Find("section.lt-backups-section").TextContent, Does.Contain("no schedule and no backups yet")));

        Backups.ScopeStatus = _ => Task.FromException<BackupScopeStatus?>(new LatticeAuthorizationDeniedException());
        var denied = RenderAt<BackupsCataloguePage>("backups?tree=inventory");
        denied.WaitUntil(() => Assert.That(denied.Find("section [role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void The_health_column_is_hidden_where_monitoring_does_not_apply()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        var off = RenderAt<BackupsCataloguePage>("backups");
        off.WaitUntil(() =>
        {
            Assert.That(off.FindAll("tbody tr"), Has.Count.EqualTo(1));
            Assert.That(off.FindAll("th").Select(th => th.TextContent.Trim()), Does.Not.Contain("Health"));
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthAsync)), Is.Zero);
    }

    [Test]
    public void The_health_column_appears_where_monitoring_applies()
    {
        Seed(FakeBackupControl.Manifest("b1"), FakeBackupControl.Manifest("b2", createdAt: DateTimeOffset.UnixEpoch));
        Backups.HealthAvailable = () => Task.FromResult(true);
        Backups.HealthReports["b1"] = new BackupHealthReport("b1", BackupHealthStatus.Missing, false, ["a1"], [], DateTimeOffset.UnixEpoch, "gone");
        var on = RenderAt<BackupsCataloguePage>("backups");

        on.WaitUntil(() =>
        {
            Assert.That(on.FindAll("th").Select(th => th.TextContent.Trim()), Does.Contain("Health"));
            var pills = on.FindAll("tbody .lt-pill");
            Assert.That(pills.Select(pill => pill.TextContent.Trim()), Is.EquivalentTo(new[] { "Missing", "Not checked" }));
        });
    }

    [Test]
    public void The_compact_form_is_a_list_of_two_line_rows_opening_a_detail_sheet()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "a/crm/orders"));

        var cut = RenderAt<BackupsCataloguePage>("backups", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent, Is.EqualTo("nightly"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("orders - Full - 2026-09-28 14:02:11 UTC"));
        });

        cut.Find(".lt-table-list__row button").Click();
        cut.WaitUntil(() =>
        {
            var sheet = cut.Find("[role=dialog]");
            Assert.That(sheet.QuerySelector(".lt-dialog__title")!.TextContent, Is.EqualTo("nightly"));
            Assert.That(sheet.QuerySelectorAll("a").Select(a => a.TextContent), Does.Contain("Open backup"));
        });
    }

    [Test]
    public void With_tenancy_on_every_link_is_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        Seed(FakeBackupControl.Manifest("b1"));

        var cut = RenderAt<BackupsCataloguePage>("t/acme/backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("tbody a").GetAttribute("href"), Is.EqualTo("t/acme/backups/b1"));
            Assert.That(cut.Find($"[data-lt-command=\"{BackupsArea.CaptureCommandId}\"]").GetAttribute("href"), Is.EqualTo("t/acme/backups/new"));
            Assert.That(cut.FindAll(".lt-backups-nav__link").Select(link => link.GetAttribute("href")), Has.All.StartWith("t/acme/backups"));
        });
    }

    [Test]
    public void Operations_started_this_session_are_listed_so_they_can_be_resumed()
    {
        Operations.Start(BackupOperationKind.FullCapture, "Capture a full backup of orders", ["one"], (_, _) => Task.CompletedTask);

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() =>
        {
            var link = cut.FindAll("a").Single(a => a.TextContent == "Capture a full backup of orders");
            Assert.That(link.GetAttribute("href"), Is.EqualTo("backups/operations/1"));
            Assert.That(link.ParentElement!.TextContent, Does.Contain("Succeeded"));
        });
    }

    [Test]
    public void Cluster_operations_are_listed_with_their_progress_whichever_tab_started_them()
    {
        Backups.Statuses["op-run"] = FakeBackupControl.Running("op-run", BackupOperationKinds.Restore, "orders") with
        {
            State = Orleans.Lattice.Api.Operations.LatticeOperationState.Running,
            Phase = "RestoringShards",
            CompletedUnits = 2,
            TotalUnits = 5,
            UnitName = "shards",
            StartedAtUtc = new DateTimeOffset(2026, 10, 1, 10, 0, 0, TimeSpan.Zero),
        };
        Backups.Statuses["op-done"] = FakeBackupControl.Running("op-done", BackupOperationKinds.Capture, "orders");
        Backups.SucceedCapture("op-done", "b1");

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#lt-backups-cluster-operations-title").TextContent, Is.EqualTo("Backup and restore operations"));
            var items = cut.FindAll("#lt-backups-cluster-operations-title + ul li");
            Assert.That(items, Has.Count.EqualTo(2));
            Assert.That(items[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/operations/op-run"));
            Assert.That(items[0].TextContent, Does.Contain("Restore orders").And.Contain("Running, Restoring shards, 2 of 5 shards"));
            Assert.That(items[1].TextContent, Does.Contain("Capture a full backup of orders").And.Contain("Succeeded"));
            Assert.That(cut.FindAll("#lt-backups-operations-title"), Is.Empty, "no staged operation ran in this circuit");
        });
    }

    [Test]
    public void A_listing_that_fails_says_so_and_a_denied_one_is_hidden()
    {
        Backups.ListFault = new InvalidOperationException("the cluster is unreachable");
        var failed = RenderAt<BackupsCataloguePage>("backups");
        failed.WaitUntil(() => Assert.That(
            failed.FindAll(".lt-backups-note").Select(note => note.TextContent),
            Has.Some.StartsWith("The recent backup and restore operations could not be listed")));

        Backups.ListFault = new LatticeAuthorizationDeniedException();
        Backups.List = _ => Task.FromResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("b1")] });
        Services.GetRequiredService<BackupOperationList>().Forget();
        var denied = RenderAt<BackupsCataloguePage>("backups");
        denied.WaitUntil(() => Assert.That(denied.FindAll("tbody tr"), Is.Not.Empty));
        Assert.Multiple(() =>
        {
            Assert.That(denied.FindAll("#lt-backups-cluster-operations-title"), Is.Empty);
            Assert.That(denied.FindAll(".lt-backups-note").Select(note => note.TextContent), Has.None.Contains("could not be listed"));
        });
    }

    [Test]
    public void A_staged_operation_handed_to_the_cluster_is_listed_once_as_the_clusters()
    {
        var staged = Operations.Start(BackupOperationKind.FullCapture, "Capture a full backup of orders", ["one"], (_, _) => Task.CompletedTask);
        staged.HandOff("op-1");
        Backups.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#lt-backups-cluster-operations-title + ul li"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("#lt-backups-operations-title"), Is.Empty);
        });
    }
    private static AngleSharp.Dom.IElement Pager(IRenderedComponent<BackupsCataloguePage> cut, string text) =>
        cut.FindAll(".lt-backups-pager button").Single(button => button.TextContent == text);
}
