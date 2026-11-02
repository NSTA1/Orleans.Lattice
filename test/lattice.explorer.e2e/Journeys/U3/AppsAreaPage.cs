using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U3;

/// <summary>
/// The Apps area as an end user meets it: <c>/apps</c>, whose Your apps table lists the
/// enabled apps the caller holds a role in, with the roles they hold; and an app's own page.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class AppsAreaPage(PersonaSession session)
{
    /// <summary>The task board's name, as its manifest presents it.</summary>
    public const string TaskBoard = "Task board";

    /// <summary>The Your apps table.</summary>
    public ILocator YourApps => session.Shell.Content.GetByRole(AriaRole.Table, new() { Name = "Your apps" });

    /// <summary>The task board's row in Your apps.</summary>
    public ILocator TaskBoardRow => YourApps.GetByRole(AriaRole.Row).Filter(new() { HasText = TaskBoard });

    /// <summary>The Your roles cell of the task board's row: after the App row header, the Version cell, then Your roles.</summary>
    public ILocator TaskBoardRoles => TaskBoardRow.Locator("td").Nth(1);

    /// <summary>The task board's Open control in Your apps, which opens its window in a new browser window.</summary>
    public ILocator OpenTaskBoard => TaskBoardRow.GetByRole(AriaRole.Link, new() { Name = $"Open {TaskBoard} in a new window", Exact = true });

    /// <summary>
    /// Opens <c>/apps</c> through the directory spine. A role grant reaches the workspace a moment
    /// after a binding changes, so when <paramref name="expectTaskBoard"/> is set and Your apps
    /// does not list the task board yet, the page is loaded again, a bounded number of times, as
    /// a user would reload it.
    /// </summary>
    /// <param name="expectTaskBoard">Whether the caller holds a role in the task board.</param>
    /// <param name="viaSpine">Whether to follow the directory spine's Apps stop, or else type the address.</param>
    public async Task OpenAsync(bool expectTaskBoard, bool viaSpine = true)
    {
        if (viaSpine)
        {
            await session.Shell.GoToAreaAsync("apps");
        }
        else
        {
            await session.Shell.GotoAsync("/apps");
        }

        await session.Shell.ExpectHeadingAsync("Apps");
        if (!expectTaskBoard)
        {
            return;
        }

        for (var attempt = 0; ; attempt++)
        {
            await Expect(YourApps.Or(session.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "No apps yet" }))).ToBeVisibleAsync(new() { Timeout = 60_000 });
            if (await TaskBoardRow.IsVisibleAsync() || attempt >= 10)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
            await session.Shell.GotoAsync("/apps");
        }

        await Expect(TaskBoardRow).ToBeVisibleAsync();
    }

    /// <summary>Asserts the task board is listed in Your apps with exactly <paramref name="roles"/> as the caller's roles.</summary>
    /// <param name="roles">The roles, as the column writes them.</param>
    public async Task ExpectTaskBoardRolesAsync(string roles)
    {
        await Expect(YourApps.GetByRole(AriaRole.Columnheader, new() { Name = "Your roles", Exact = true })).ToBeVisibleAsync();
        await Expect(TaskBoardRoles).ToHaveTextAsync(roles);
    }

    /// <summary>Asserts Your apps lists nothing: no enabled app grants the caller a role.</summary>
    public async Task ExpectNoAppsAsync()
    {
        await Expect(session.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "No apps yet", Exact = true })).ToBeVisibleAsync();
        await Expect(session.Shell.Content.GetByText(TaskBoard)).ToHaveCountAsync(0);
    }

    /// <summary>Asserts the page is the task board's own overview, in the full console.</summary>
    public async Task ExpectTaskBoardOverviewAsync()
    {
        await Expect(session.Page).ToHaveURLAsync(new Regex("/apps/task-board(/overview)?$"));
        await session.Shell.ExpectHeadingAsync(TaskBoard);
        await Expect(session.Shell.Banner).ToBeVisibleAsync();
        await Expect(session.Shell.Content.GetByRole(AriaRole.Tab, new() { Name = "Overview", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
    }
}
