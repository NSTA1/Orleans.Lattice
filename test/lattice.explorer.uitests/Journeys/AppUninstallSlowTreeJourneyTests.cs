using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5482 (epic #5401): uninstalling an app whose tree is slow to retire, as a loaded
/// cluster retires a replicated tree. The tree's delete is held past the app-control
/// facade's accept window and the cluster's 30 second grain response timeout: the page
/// shows the change in progress rather than an error, and reports it done once the tree
/// retires. Each step keeps an ordered screenshot under <c>screenshots/journey</c> for the
/// pull request.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AppUninstallSlowTreeJourneyTests : UiTestBase
{
    private const string TasksTree = "a/task-board/tasks";

    private static readonly TimeSpan HoldBound = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Uninstalling_an_app_whose_tree_is_slow_to_retire()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();
        await world.AppsControl().DisableAsync(TaskBoardApp.Slug);
        try
        {
            var page = await OpenAsync(world.Head, $"/t/default/apps/catalogue/in-image/{TaskBoardApp.Slug}", WorldIdentities.Admin);
            var lifecycle = page.Locator("section[aria-labelledby=lt-apps-lifecycle]");
            await Expect(lifecycle.GetByRole(AriaRole.Button, new() { Name = "Uninstall", Exact = true })).ToBeVisibleAsync();
            await CaptureJourneyStepAsync(page, "d5482-01-disabled-task-board");

            using (var hold = world.TreeDeletes.Arm(TasksTree))
            {
                await lifecycle.GetByRole(AriaRole.Button, new() { Name = "Uninstall", Exact = true }).ClickAsync();
                var confirm = page.GetByRole(AriaRole.Alertdialog);
                await confirm.Locator("input").FillAsync(TaskBoardApp.Slug);
                await confirm.GetByRole(AriaRole.Button, new() { Name = "Uninstall", Exact = true }).ClickAsync();
                await hold.Entered.Task.WaitAsync(HoldBound);

                // Past the accept window the change is shown in progress, never as a failure.
                var pending = lifecycle.Locator("[data-lt-app-lifecycle-pending=uninstall]");
                await Expect(pending).ToContainTextAsync("Uninstalling Task board. The cluster accepted the change", new() { Timeout = 90_000 });
                await Expect(lifecycle.Locator("[role=alert]")).ToHaveCountAsync(0);
                await CaptureJourneyStepAsync(page, "d5482-02-uninstall-while-the-tree-retires");
            }

            // Once the tree retires, the run finishes and the page reports it.
            await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Task board is uninstalled." })).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await Expect(lifecycle).ToHaveCountAsync(0);
            await CaptureJourneyStepAsync(page, "d5482-03-after-the-tree-retired");
        }
        finally
        {
            await world.RemoveTaskBoardAsync();
        }
    }
}
