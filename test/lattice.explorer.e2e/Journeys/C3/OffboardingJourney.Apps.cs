using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>C3.3: the task board is disabled while dana has it open, then uninstalled.</summary>
public sealed partial class OffboardingJourney
{
    private const string TaskBoardName = "Task board";

    /// <summary>
    /// C3.3: olivia disables the task board while dana has it open; dana's frame is closed on
    /// her next read, and reopening it offers no app. olivia then uninstalls it, and its
    /// app-owned rules leave Access.
    /// </summary>
    private static async Task DisableAndUninstallTaskBoardAsync(ChapterContext context, PersonaSession olivia)
    {
        using var step = C3Step.Begin(3, "olivia disables the task board while dana has it open, then uninstalls it");
        await using var dana = await context.Estate.SignInAsAsync(Personas.Dana);
        await dana.Shell.GotoAsync($"/apps/{Names.Apps.TaskBoard}/window");
        var board = new TaskBoardWindow(dana.Page);
        await board.WaitForBoardAsync();
        await board.ExpectRoleAsync(editor: false);
        await step.ShotAsync(dana, "dana-board-viewer", "dana, an auditor, has the task board open as a viewer");

        await olivia.Shell.GotoAsync("/apps");
        await olivia.Shell.ExpectHeadingAsync("Apps");
        await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Disable {TaskBoardName}", Exact = true }).ClickAsync();
        var disable = olivia.Page.GetByRole(AriaRole.Dialog, new() { Name = $"Disable {TaskBoardName}?" });
        await Expect(disable).ToContainTextAsync("Its access rules are withdrawn at once");
        await step.ShotAsync(olivia, "confirm-disable", "Disable Task board?: its rules are withdrawn at once, its trees kept");
        await disable.GetByRole(AriaRole.Button, new() { Name = "Disable", Exact = true }).ClickAsync();
        // A disable may be answered pending (#5482); the page follows it and toasts when it settles.
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"{TaskBoardName} is disabled." })).ToHaveCountAsync(1, new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await step.ShotAsync(olivia, "disabled", "The task board is disabled");

        await Expect(board.Board.Locator("#tb-refresh")).ToBeVisibleAsync();
        var refreshDeadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
        while (!await board.RefreshIsWithdrawnAsync() && DateTime.UtcNow < refreshDeadline)
        {
            await Task.Delay(TimeSpan.FromSeconds(5));
        }

        await Expect(board.Closed.Or(board.Status.Filter(new() { HasTextRegex = new Regex(@"\(denied\)") }))).ToBeVisibleAsync();
        if (await board.Closed.IsVisibleAsync())
        {
            await Expect(board.Closed).ToContainTextAsync(new Regex("This app (was disabled|is no longer open to you)"));
        }

        await step.ShotAsync(dana, "dana-frame-closed", "dana's next read finds the app just changed: the board no longer answers her");

        await dana.Shell.GotoAsync($"/apps/{Names.Apps.TaskBoard}/window");
        await board.ExpectNoAppAsync(dana.Shell);
        await step.ShotAsync(dana, "dana-reopens", "Reopening it, dana finds no task board to open while it is disabled");

        await olivia.Shell.GotoAsync("/apps");
        await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Uninstall {TaskBoardName}", Exact = true }).ClickAsync();
        var uninstall = new ConfirmDialog(olivia.Page, $"Uninstall {TaskBoardName}?");
        await uninstall.TypeAsync(Names.Apps.TaskBoard);
        await Expect(uninstall.Dialog).ToContainTextAsync(context.Recall(StoryKeys.TaskBoardTree));
        await step.ShotAsync(olivia, "confirm-uninstall", "Uninstall Task board?: its rules are removed and its tasks tree soft-deleted; olivia types task-board");
        await uninstall.ConfirmAsync("Uninstall");

        // A replicated uninstall may outlast the facade's 20 s answer and come back pending (#5482);
        // the run carries on, so follow it until the app leaves the list. An error is final.
        var appsError = olivia.Shell.Content.Locator(".lt-apps-error[role=alert]");
        var installed = olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Uninstall {TaskBoardName}", Exact = true });
        var deadline = DateTime.UtcNow + OperationPatience;
        do
        {
            await Expect(appsError).ToHaveCountAsync(0);
            await olivia.Shell.GotoAsync("/apps");
            await olivia.Shell.ExpectHeadingAsync("Apps");
            await Expect(appsError).ToHaveCountAsync(0);
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Install app...", Exact = true })).ToBeVisibleAsync();
            if (await installed.CountAsync() == 0)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromSeconds(5));
        }
        while (DateTime.UtcNow < deadline);

        await Expect(installed).ToHaveCountAsync(0);
        await Expect(olivia.Shell.Content.GetByText(TaskBoardName, new() { Exact = true })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "uninstalled", "The task board is no longer installed");

        await olivia.Shell.GotoAsync("/access/rules");
        await olivia.Shell.Content.GetByLabel("Search rules").FillAsync(Names.Apps.TaskBoard);
        await Expect(olivia.Shell.Content.Locator(".lt-access-count")).ToBeVisibleAsync();
        await Expect(olivia.Shell.Content.GetByRole(AriaRole.Link, new() { NameRegex = new Regex($"app {Regex.Escape(Names.Apps.TaskBoard)}") })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "app-rules-gone", "Access: searching for task-board finds none of its app-owned rules");
        Coverage.Hit("use-case:apps/disable-and-uninstall");
        step.Complete();
    }
}
