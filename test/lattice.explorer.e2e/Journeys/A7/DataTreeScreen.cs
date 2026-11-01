using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// The Data area as A7 reads it: the directory of trees, and a tree's workspace heading with
/// its live-key count (<c>N live keys</c> in the summary line under the heading).
/// </summary>
internal sealed class DataTreeScreen(ShellPage shell)
{
    private static readonly Regex LiveKeysPattern = new("^([\\d,]+) live keys?$");

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The summary line's live-key count.</summary>
    public ILocator LiveKeys => Shell.Content.GetByText(LiveKeysPattern).First;

    /// <summary>Goes to the Data directory from the spine.</summary>
    public async Task OpenDirectoryAsync()
    {
        await Shell.GoToAreaAsync("data");
        await Shell.ExpectHeadingAsync("Data");
    }

    /// <summary>The directory's link to <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree id.</param>
    public ILocator TreeLink(string treeId) => Shell.Content.GetByRole(AriaRole.Link, new() { Name = treeId, Exact = true }).First;

    /// <summary>Opens <paramref name="treeId"/> from the directory and waits for its live-key count.</summary>
    /// <param name="treeId">The tree id.</param>
    public async Task OpenTreeAsync(string treeId)
    {
        await Expect(TreeLink(treeId)).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await TreeLink(treeId).ClickAsync();
        await Shell.ExpectHeadingAsync(treeId);
        await Expect(LiveKeys).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>The live-key count the workspace shows.</summary>
    public async Task<long> LiveKeyCountAsync()
    {
        await Expect(LiveKeys).ToBeVisibleAsync(new() { Timeout = 60_000 });
        var match = LiveKeysPattern.Match((await LiveKeys.InnerTextAsync()).Trim());
        return match.Success
            ? long.Parse(match.Groups[1].Value, NumberStyles.AllowThousands, CultureInfo.InvariantCulture)
            : throw new InvalidOperationException($"The summary line's live-key count is unreadable: '{await LiveKeys.InnerTextAsync()}'.");
    }

    /// <summary>Reads the workspace afresh by reloading its document.</summary>
    public async Task ReloadAsync()
    {
        await Page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await Page.ReloadAsync();
        await JourneyBrowser.WaitForInteractiveAsync(Page);
    }
}
