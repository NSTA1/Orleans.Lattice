using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// An installed app's window as a journey drives it: the frame host, the app's sandboxed
/// frame, and the task board inside it (<c>samples/Explorer/Apps/TaskBoard</c>).
/// </summary>
/// <param name="page">A page showing <c>/apps/{slug}/window</c>.</param>
internal sealed class TaskBoardWindow(IPage page)
{
    /// <summary>The frame host's section.</summary>
    public ILocator Host => page.Locator("section.appframe");

    /// <summary>The app's sandboxed frame element.</summary>
    public ILocator Frame => Host.Locator("iframe");

    /// <summary>The board inside the frame.</summary>
    public IFrameLocator Board => Frame.ContentFrame;

    /// <summary>What the frame host shows in place of the app once it has closed it.</summary>
    public ILocator Closed => Host.GetByRole(AriaRole.Alert);

    /// <summary>The board's status line.</summary>
    public ILocator Status => Board.Locator("#tb-status");

    /// <summary>The board's card for <paramref name="title"/>, in any column.</summary>
    /// <param name="title">The card's title.</param>
    public ILocator Card(string title) => Board.Locator("button.tb-card").Filter(new() { HasText = title });

    /// <summary>Waits until the board has loaded its cards.</summary>
    public async Task WaitForBoardAsync()
    {
        await Expect(Frame).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(Board.Locator("#tb-context")).ToContainTextAsync("task-board", new() { Timeout = 60_000 });
        await Expect(Board.Locator("#tb-cards-todo li").First).ToBeVisibleAsync();
    }

    /// <summary>Whether the board offers its write controls (an editor) or reads as read-only (a viewer).</summary>
    /// <param name="editor">Whether the board's role is an editor's.</param>
    public async Task ExpectRoleAsync(bool editor)
    {
        if (editor)
        {
            await Expect(Board.Locator("#tb-add")).ToBeVisibleAsync();
            await Expect(Board.Locator("#tb-read-only")).ToBeHiddenAsync();
        }
        else
        {
            await Expect(Board.Locator("#tb-read-only")).ToBeVisibleAsync();
            await Expect(Board.Locator("#tb-add")).ToBeHiddenAsync();
        }
    }

    /// <summary>
    /// Waits until the board's last action has been answered: the frame host has closed the
    /// app (its authority was withdrawn), or the board reports the change was refused.
    /// </summary>
    public async Task ExpectChangeWithdrawnAsync()
    {
        var refused = Status.Filter(new() { HasTextRegex = new Regex(@"\((denied|unavailable)\)") });
        await Expect(Closed.Or(refused)).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    /// <summary>Asserts the window offers no app any more: no frame, whatever the page says instead.</summary>
    /// <param name="shell">The page's shell.</param>
    public async Task ExpectNoAppAsync(ShellPage shell)
    {
        await Expect(shell.Heading).ToBeVisibleAsync();
        await Expect(Frame).ToHaveCountAsync(0);
    }
}
