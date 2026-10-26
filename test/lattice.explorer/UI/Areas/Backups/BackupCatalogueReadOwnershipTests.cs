using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupCatalogueReadOwnershipTests : BackupsTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Superseded_catalogue_answers_and_faults_cannot_replace_the_current_filter(bool fails)
    {
        var pending = new TaskCompletionSource<BackupCatalogPage>();
        Backups.List = request => request.TreeId == "orders"
            ? pending.Task
            : Task.FromResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("current", "stock backup", "stock")] });
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups?tree=stock"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("tbody").TextContent, Does.Contain("stock backup")));
        var renders = cut.RenderCount;
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                pending.SetException(new InvalidOperationException("old catalogue failed"));
            }
            else
            {
                pending.SetResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("old", "orders backup")] });
            }
        });
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("tbody").TextContent, Does.Contain("stock backup").And.Not.Contain("orders backup"));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Superseded_schedule_answers_and_faults_cannot_replace_the_current_trees_status(bool fails)
    {
        var pending = new TaskCompletionSource<BackupScopeStatus?>();
        Backups.ScopeStatus = scope => scope.TreeId == "orders" ? pending.Task : Task.FromResult<BackupScopeStatus?>(Status(scope, 7));
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups?tree=stock"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("section.lt-backups-section").TextContent, Does.Contain("7")));
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                pending.SetException(new InvalidOperationException("old schedule failed"));
            }
            else
            {
                pending.SetResult(Status(BackupScopeSelector.WholeTree("orders"), 99));
            }
        });
        // Drain the fire-and-forget extras continuation through its renderer notification.
        await cut.InvokeAsync(() => Task.CompletedTask);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("section.lt-backups-section").TextContent, Does.Contain("7").And.Not.Contain("99"));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [Test]
    public async Task A_superseded_health_read_cannot_overwrite_a_refreshed_report_for_the_same_backup()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        Backups.HealthAvailable = () => Task.FromResult(true);
        var pending = new TaskCompletionSource<BackupHealthReport?>();
        var calls = 0;
        Backups.GetHealth = _ => ++calls == 1 ? pending.Task : Task.FromResult<BackupHealthReport?>(
            new BackupHealthReport("b1", BackupHealthStatus.Healthy, true, [], [], DateTimeOffset.UnixEpoch, "current"));
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        cut.WaitUntil(() => Assert.That(calls, Is.EqualTo(1)));
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups?tree=orders&name=night"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("tbody .lt-pill").TextContent.Trim(), Is.EqualTo("Healthy")));
        await cut.InvokeAsync(() => pending.SetResult(
            new BackupHealthReport("b1", BackupHealthStatus.Missing, false, ["a1"], [], DateTimeOffset.UnixEpoch, "old")));
        await cut.InvokeAsync(() => Task.CompletedTask);
        cut.WaitUntil(() => Assert.That(cut.Find("tbody .lt-pill").TextContent.Trim(), Is.EqualTo("Healthy")));
    }

    [Test]
    public async Task A_superseded_inventory_cannot_replace_the_current_summary()
    {
        var pending = new TaskCompletionSource<BackupInventoryReport>();
        var calls = 0;
        Backups.Inventory = () => ++calls == 1 ? pending.Task : Task.FromResult(Inventory(7));
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups?tree=stock"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.StartWith("7 backups")));
        await cut.InvokeAsync(() => pending.SetResult(Inventory(99)));
        await cut.InvokeAsync(() => Task.CompletedTask);
        Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.StartWith("7 backups"));
    }

    [Test]
    public async Task Superseded_health_availability_cannot_hide_the_current_health_column()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        var pending = new TaskCompletionSource<bool>();
        var calls = 0;
        Backups.HealthAvailable = () => ++calls == 1 ? pending.Task : Task.FromResult(true);
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups?tree=orders&name=night"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll("th").Select(cell => cell.TextContent.Trim()), Does.Contain("Health")));
        await cut.InvokeAsync(() => pending.SetResult(false));
        await cut.InvokeAsync(() => Task.CompletedTask);
        Assert.That(cut.FindAll("th").Select(cell => cell.TextContent.Trim()), Does.Contain("Health"));
    }

    [Test]
    public async Task Paging_does_not_cancel_the_catalogues_pending_schedule_and_inventory_reads()
    {
        var status = new TaskCompletionSource<BackupScopeStatus?>();
        var inventory = new TaskCompletionSource<BackupInventoryReport>();
        Backups.ScopeStatus = _ => status.Task;
        Backups.Inventory = () => inventory.Task;
        Backups.List = request => Task.FromResult(new BackupCatalogPage
        {
            Entries = [FakeBackupControl.Manifest(request.PageToken is null ? "first" : "second")],
            NextPageToken = request.PageToken is null ? "next" : null,
        });
        var cut = RenderAt<BackupsCataloguePage>("backups?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        await cut.FindAll(".lt-backups-pager button").Single(button => button.TextContent == "Next page").ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-backups-pager__position").TextContent, Is.EqualTo("Page 2")));
        await cut.InvokeAsync(() =>
        {
            status.SetResult(Status(BackupScopeSelector.WholeTree("orders"), 7));
            inventory.SetResult(Inventory(7));
        });
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("section.lt-backups-section").TextContent, Does.Contain("7"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.StartWith("7 backups"));
        });
    }

    private static BackupInventoryReport Inventory(int count) => new(count, 0, count, 0, null, null, 0, 0, 0);

    private static BackupScopeStatus Status(BackupScopeSelector scope, int depth) =>
        new(scope, false, false, null, null, null, null, BackupScopeRunOutcome.None, depth);
}
