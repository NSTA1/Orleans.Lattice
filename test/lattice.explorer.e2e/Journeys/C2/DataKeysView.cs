using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// The Data area's Keys tab as C2 drives it: open a tree from the directory, narrow the keys
/// by prefix, follow them live, and wait for keys written later to appear.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class DataKeysView(PersonaSession session)
{
    /// <summary>The note the tab shows when it stopped following the change feed, and its way back.</summary>
    public const string RestartLiveButton = "Restart live updates";

    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    /// <summary>The live-updates switch.</summary>
    public ILocator LiveSwitch => Content.GetByRole(AriaRole.Switch, new() { Name = "Live updates" });

    /// <summary>The tab's notes: live updates stopped, keys changed under the page.</summary>
    public ILocator Notes => Content.Locator(".lt-data-note");

    /// <summary>Opens <paramref name="tree"/> from the Data directory, on its Keys tab.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenTreeAsync(string tree)
    {
        await session.Shell.GoToAreaAsync("data");
        await Content.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true }).First.ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/data/{Regex.Escape(tree)}(\\?|$)"));
        await Expect(Content.GetByLabel("Key prefix")).ToBeVisibleAsync();
    }

    /// <summary>Narrows the keys to those starting with <paramref name="prefix"/>.</summary>
    /// <param name="prefix">The prefix.</param>
    public async Task FilterAsync(string prefix)
    {
        var box = Content.GetByLabel("Key prefix");
        await box.FillAsync(prefix);
        await box.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]prefix="));
    }

    /// <summary>Asserts the tab is following the change feed.</summary>
    public Task ExpectLiveAsync() => Expect(LiveSwitch).ToBeCheckedAsync();

    /// <summary>The cell or row text naming <paramref name="key"/>.</summary>
    /// <param name="key">The key.</param>
    public ILocator Key(string key) => Content.GetByText(key, new() { Exact = true });

    /// <summary>
    /// Waits until every key of <paramref name="keys"/> is listed. When the tab says it stopped
    /// following the change feed it offers <see cref="RestartLiveButton"/>, and the reader uses
    /// it, as the tab asks; each such action is returned, so the step can say what it took.
    /// </summary>
    /// <param name="keys">The keys that must appear.</param>
    /// <param name="timeout">How long to wait.</param>
    /// <returns>The actions the reader took to get there, in order; empty when the tab recovered alone.</returns>
    public async Task<IReadOnlyList<string>> WaitForKeysAsync(IReadOnlyList<string> keys, TimeSpan timeout)
    {
        var actions = new List<string>();
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var missing = new List<string>();
            foreach (var key in keys)
            {
                if (await Key(key).CountAsync() == 0)
                {
                    missing.Add(key);
                }
            }

            if (missing.Count == 0)
            {
                return actions;
            }

            var restart = Content.GetByRole(AriaRole.Button, new() { Name = RestartLiveButton, Exact = true });
            if (await restart.IsVisibleAsync())
            {
                actions.Add($"pressed '{RestartLiveButton}' after the tab said: {await Notes.First.InnerTextAsync()}");
                await restart.ClickAsync();
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new System.TimeoutException(
                    $"After {timeout.TotalSeconds:0} s the Keys tab still lists none of {string.Join(", ", missing)}. "
                    + $"Its notes read: '{string.Join(" | ", await Notes.AllInnerTextsAsync())}'; actions taken: {(actions.Count == 0 ? "none" : string.Join("; ", actions))}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }
}
