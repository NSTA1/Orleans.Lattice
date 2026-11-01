using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Issue #5517: a role binding (or a group membership, or an install) made elsewhere
/// while a user's console is open reaches that console without a reload - the open
/// "Your apps" page, the spine badge and Home - once the circuit's snapshot is older
/// than the capability refresh interval and a navigation asks the area again.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsBindingRefreshTests : AppsTestContext
{
    // Comfortably past the capability refresh interval (ten seconds by default).
    private static readonly TimeSpan PastTheRefreshInterval = TimeSpan.FromMinutes(1);

    // A bound on waiting for the background re-read, never a pause.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Test]
    public void A_binding_made_after_the_first_probe_reaches_the_open_apps_page_on_the_next_navigation()
    {
        Restrict();
        var cut = RenderAt<AppsPage>("/apps");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No apps yet")));

        // Bound elsewhere - another administrator, a seeder, a directory sync - while the page is open.
        Workspace.Apps.Add(AppsTestData.Mine("task-board", hasUi: true, name: "Task board", "viewer"));
        cut.InvokeAsync(() => Time.Advance(PastTheRefreshInterval)).GetAwaiter().GetResult();

        // A navigation: the directory asks the area again.
        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
        cut.InvokeAsync(async () => await area.GetAvailabilityAsync(CancellationToken.None)).GetAwaiter().GetResult();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("tbody tr").TextContent, Does.Contain("Task board").And.Contain("viewer"));
    }

    [Test]
    public async Task A_binding_made_after_the_first_probe_reaches_the_spine_badge_and_home_without_a_reload()
    {
        Restrict();
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();
        var area = directory.Areas.OfType<AppsArea>().Single();

        var before = await directory.GetEntriesAsync();
        var homeBefore = await directory.GetHomeStatusAsync(area);

        var told = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        directory.Changed += () => told.TrySetResult();
        Workspace.Apps.Add(AppsTestData.Mine("task-board", hasUi: true, name: "Task board", "viewer"));
        Time.Advance(PastTheRefreshInterval);

        // The navigation after the interval is answered at once, from what the circuit
        // already holds; the directory is told when the re-read answers differently.
        await directory.GetEntriesAsync();
        await told.Task.WaitAsync(Bound);

        var after = await directory.GetEntriesAsync();
        var homeAfter = await directory.GetHomeStatusAsync(area);
        Assert.Multiple(() =>
        {
            Assert.That(before.Single(entry => entry.Area == area).Badge, Is.Null);
            Assert.That(homeBefore?.Text, Is.EqualTo("No app is assigned to you yet"));
            Assert.That(after.Single(entry => entry.Area == area).Badge, Is.EqualTo("1"));
            Assert.That(homeAfter?.Text, Is.EqualTo("1 app available to you"));
        });
    }
}
