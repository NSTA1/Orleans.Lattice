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
    /// Waits until every key of <paramref name="keys"/> is listed, without touching the tab:
    /// live updates must resume on their own. Every note the tab shows on the way (live
    /// updates paused, keys changed under the page) is returned, so the step can say what the
    /// reader saw.
    /// </summary>
    /// <param name="keys">The keys that must appear.</param>
    /// <param name="timeout">How long to wait.</param>
    /// <returns>The distinct notes the tab showed while waiting, in order; empty when it showed none.</returns>
    public async Task<IReadOnlyList<string>> WaitForKeysAsync(IReadOnlyList<string> keys, TimeSpan timeout)
    {
        var seen = new List<string>();
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            foreach (var note in await Notes.AllInnerTextsAsync())
            {
                var text = note.Trim();
                if (text.Length > 0 && !seen.Contains(text))
                {
                    seen.Add(text);
                }
            }

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
                return seen;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new System.TimeoutException(
                    $"After {timeout.TotalSeconds:0} s the Keys tab still lists none of {string.Join(", ", missing)}: live updates did not resume. "
                    + $"Notes it showed: {(seen.Count == 0 ? "none" : string.Join(" | ", seen))}.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
    }
}
