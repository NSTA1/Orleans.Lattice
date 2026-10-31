using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>The Data area's directory of trees and views, filtered by name.</summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class DataDirectoryPage(ShellPage shell) : A8PageObject(shell)
{
    /// <summary>Opens Data from its spine stop.</summary>
    public async Task OpenAsync()
    {
        await Shell.GoToAreaAsync("data");
        await Expect(Content.GetByLabel("Filter trees and views")).ToBeVisibleAsync();
    }

    /// <summary>
    /// Filters the directory to <paramref name="tree"/> and sees it listed, pressing Refresh
    /// once if the directory was read before the tree existed.
    /// </summary>
    /// <param name="tree">The tree.</param>
    public async Task ExpectTreeAsync(string tree)
    {
        await Content.GetByLabel("Filter trees and views").FillAsync(tree);
        var entry = Content.GetByText(tree, new() { Exact = true });
        try
        {
            await entry.First.WaitForAsync(new() { Timeout = 15_000 });
        }
        catch (PlaywrightException)
        {
            await Content.Locator("button[data-lt-command=\"data.refresh\"]").ClickAsync();
        }

        await Expect(entry.First).ToBeVisibleAsync();
    }
}
