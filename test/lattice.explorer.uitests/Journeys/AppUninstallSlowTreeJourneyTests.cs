using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5482 (epic #5401): uninstalling an app whose tree is slow to retire, as a loaded
/// cluster retires a replicated tree. The tree's delete is held past the cluster's 30 second
/// grain response timeout, and each step keeps an ordered screenshot under
/// <c>screenshots/journey</c> for the pull request.
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

                // Whatever the page reports while the tree is still held.
                await Expect(lifecycle.Locator("[role=alert], [data-lt-app-lifecycle-pending]"))
                    .ToBeVisibleAsync(new() { Timeout = 90_000 });
                await CaptureJourneyStepAsync(page, "d5482-02-uninstall-while-the-tree-retires");
            }

            await page.ReloadAsync();
            await Expect(Shell.Heading(page)).ToHaveTextAsync("Task board");
            await CaptureJourneyStepAsync(page, "d5482-03-after-the-tree-retired");
        }
        finally
        {
            await world.RemoveTaskBoardAsync();
        }
    }
}
