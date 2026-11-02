using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// The Data directory (<c>/data</c>) as olivia watches it: her circuit reads the tree
/// catalogue once, so she refreshes it until a tree an outside writer created appears.
/// </summary>
/// <param name="session">The session driving the page.</param>
internal sealed class DataDirectoryPage(PersonaSession session)
{
    private ILocator Content => session.Shell.Content;

    /// <summary>The directory's row for <paramref name="tree"/>, at either width.</summary>
    /// <param name="tree">The tree.</param>
    public ILocator Tree(string tree) => Content.Locator(".lt-table-frame, .lt-table-list").GetByText(tree, new() { Exact = true });

    /// <summary>Opens the Data directory from the spine.</summary>
    public async Task OpenAsync()
    {
        await session.Shell.GoToAreaAsync("data");
        await session.Shell.ExpectHeadingAsync("Data");
        await Expect(Content.Locator(".lt-data-toolbar__status[role=status]")).ToBeVisibleAsync();
    }

    /// <summary>
    /// Presses <b>Refresh</b> until <paramref name="tree"/> is listed, at most for
    /// <paramref name="patience"/>.
    /// </summary>
    /// <param name="tree">The tree.</param>
    /// <param name="patience">How long the writer may take.</param>
    public async Task RefreshUntilListedAsync(string tree, TimeSpan patience)
    {
        var refresh = Content.Locator("button[data-lt-command=\"data.refresh\"]");
        var deadline = DateTime.UtcNow + patience;
        while (true)
        {
            await refresh.ClickAsync();
            try
            {
                await Expect(Tree(tree)).ToBeVisibleAsync(new() { Timeout = 5_000 });
                return;
            }
            catch (PlaywrightException) when (DateTime.UtcNow < deadline)
            {
            }
        }
    }
}
