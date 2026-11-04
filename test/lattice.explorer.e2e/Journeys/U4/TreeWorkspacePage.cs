using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U4;

/// <summary>
/// One tree's workspace in Data (<c>/data/{tree}</c>) as U4 reads it: the tabs, the Keys
/// table and entry panel, the History timeline, and the controls a reader must never be
/// offered. Locators are roles, accessible names and <c>data-lt-*</c> hooks only.
/// </summary>
/// <param name="shell">The Explorer chrome of the session's page.</param>
public sealed class TreeWorkspacePage(ShellPage shell)
{
    /// <summary>How the History timeline writes a revision's time: <c>2026-10-31 14:00:05 UTC</c>.</summary>
    private const string TimelineTimeFormat = "yyyy-MM-dd HH:mm:ss 'UTC'";

    /// <summary>
    /// How long a state-API read behind a panel (the keys, an entry, a key's history) may take to
    /// end, in its result or the panel's failure state: three 30-second server timeouts (the first
    /// attempt and the state client's two transient retries), plus margin.
    /// </summary>
    public static readonly float ServerReadBound = (float)TimeSpan.FromSeconds(120).TotalMilliseconds;

    private static readonly LocatorAssertionsToBeVisibleOptions ServerReadEnds = new() { Timeout = ServerReadBound };

    /// <summary>How the As of field takes an instant.</summary>
    private const string InstantFormat = "yyyy-MM-ddTHH:mm:ssZ";

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    private IPage Page => Shell.Page;

    private ILocator Content => Shell.Content;

    /// <summary>The tab row's tab named <paramref name="title"/>.</summary>
    /// <param name="title">The tab's title, such as <c>History</c>.</param>
    public ILocator Tab(string title) => Content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });

    /// <summary>The tree page's <b>Import...</b> control.</summary>
    public ILocator Import => Content.Locator("[data-lt-command=\"data.import\"]");

    /// <summary>The tree page's <b>Export...</b> control.</summary>
    public ILocator Export => Content.Locator("[data-lt-command=\"data.export\"]");

    /// <summary>The Keys toolbar's <b>New entry...</b> control.</summary>
    public ILocator NewEntry => Content.Locator("[data-lt-command=\"data.new-entry\"]");

    /// <summary>The entry panel's <b>Edit</b> (or <b>Write a value</b>) control.</summary>
    public ILocator EditEntry => Content.Locator("[data-lt-command=\"data.edit-entry\"]");

    /// <summary>The entry panel's <b>Delete...</b> control.</summary>
    public ILocator DeleteEntry => Content.Locator("[data-lt-command=\"data.delete-entry\"]");

    /// <summary>The History tab's revisions, in the order drawn.</summary>
    public ILocator Revisions => Content.Locator("li.lt-data-timeline__rev");

    /// <summary>The revision marked as in effect at the <b>As of</b> instant.</summary>
    public ILocator RevisionInEffect => Content.Locator("li.lt-data-timeline__rev[aria-current=true]");

    /// <summary>The History toolbar's revision count: <c>3 revisions</c>, or <c>50 revisions so far</c> while older ones remain.</summary>
    public ILocator RevisionCount => Content.Locator("form .lt-data-toolbar__status");

    /// <summary>The History tab's <b>Load older revisions</b> button.</summary>
    public ILocator LoadOlder => Content.GetByRole(AriaRole.Button, new() { Name = "Load older revisions", Exact = true });

    /// <summary>The History tab's <b>As of</b> field.</summary>
    public ILocator AsOf => Content.GetByRole(AriaRole.Textbox, new() { Name = "As of", Exact = true });

    /// <summary>The History panel's failure state, which offers <b>Try again</b>.</summary>
    public ILocator HistoryDidNotLoad => Content.GetByRole(AriaRole.Heading, new() { Name = "The history did not load", Exact = true });

    /// <summary>The Keys tab's key links. Only a key link carries a title: the tree header's owner link shares the class (#5509).</summary>
    public ILocator KeyLinks => Content.Locator("a.lt-data-key[title]");

    /// <summary>The Keys panel's failure state, which offers <b>Try again</b>.</summary>
    public ILocator KeysDidNotLoad => Content.GetByRole(AriaRole.Heading, new() { Name = "The keys did not load", Exact = true });

    /// <summary>
    /// Waits for the Keys tab's scan to end: <paramref name="drawn"/> (by default the first key)
    /// visible, or the panel's failure state. Returns whether the scan drew.
    /// </summary>
    /// <param name="drawn">What the Keys tab shows once the scan succeeds, or <see langword="null"/> for any key.</param>
    public async Task<bool> KeysSettledAsync(ILocator? drawn = null)
    {
        await Expect((drawn ?? KeyLinks.First).Or(KeysDidNotLoad).First).ToBeVisibleAsync(ServerReadEnds);
        return !await KeysDidNotLoad.IsVisibleAsync();
    }

    /// <summary>
    /// Waits for a History read to end: <paramref name="drawn"/> visible, or the panel's failure
    /// state. Returns whether the read drew.
    /// </summary>
    /// <param name="drawn">What the History tab shows once the read succeeds.</param>
    public async Task<bool> HistorySettledAsync(ILocator drawn)
    {
        ArgumentNullException.ThrowIfNull(drawn);
        await Expect(drawn.Or(HistoryDidNotLoad).First).ToBeVisibleAsync(ServerReadEnds);
        return !await HistoryDidNotLoad.IsVisibleAsync();
    }

    /// <summary>As <see cref="HistorySettledAsync"/>, failing with the panel's own words when the read did not draw.</summary>
    /// <param name="drawn">What the History tab shows once the read succeeds.</param>
    public async Task ExpectHistoryDrawnAsync(ILocator drawn)
    {
        if (!await HistorySettledAsync(drawn))
        {
            Assert.Fail("The History panel said the history did not load: " + await HistoryDidNotLoad.Locator("xpath=..").InnerTextAsync());
        }
    }

    /// <summary>Opens the tree from the Data directory, filtering it by name first.</summary>
    /// <param name="tree">The tree's logical id.</param>
    public async Task OpenFromDirectoryAsync(string tree)
    {
        ArgumentException.ThrowIfNullOrEmpty(tree);
        await Shell.GoToAreaAsync("data");
        await Expect(Shell.Heading).ToHaveTextAsync("Data");
        await Content.GetByLabel("Filter trees and views").FillAsync(tree);
        var link = Content.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true });
        await Expect(link).ToBeVisibleAsync();
        await link.ClickAsync();
        await ExpectOpenAsync(tree);
    }

    /// <summary>Asserts the tree workspace of <paramref name="tree"/> is shown.</summary>
    /// <param name="tree">The tree's logical id.</param>
    public async Task ExpectOpenAsync(string tree)
    {
        await Expect(Shell.Heading).ToHaveTextAsync(tree);
        await Expect(Tab("Keys")).ToBeVisibleAsync();
    }

    /// <summary>Chooses the tab titled <paramref name="title"/> and waits for its address.</summary>
    /// <param name="title">The tab's title.</param>
    /// <param name="id">The tab's <c>?tab=</c> value.</param>
    public async Task ChooseTabAsync(string title, string id)
    {
        await Tab(title).ClickAsync();
        await Expect(Tab(title)).ToHaveAttributeAsync("aria-selected", "true");
        if (id != "keys")
        {
            await Expect(Page).ToHaveURLAsync(new Regex($"[?&]tab={Regex.Escape(id)}(&|$)"));
        }
    }

    /// <summary>Narrows the Keys tab to <paramref name="prefix"/> and opens the entry <paramref name="key"/>.</summary>
    /// <param name="prefix">The key prefix to scan.</param>
    /// <param name="key">The key to open.</param>
    public async Task OpenEntryAsync(string prefix, string key)
    {
        var search = Content.GetByLabel("Key prefix");
        await search.FillAsync(prefix);
        await search.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]prefix="));
        var link = Content.Locator($"a.lt-data-key[title=\"{key}\"]");
        if (!await KeysSettledAsync(link))
        {
            Assert.Fail($"The Keys panel said the keys under '{prefix}' did not load: " + await KeysDidNotLoad.Locator("xpath=..").InnerTextAsync());
        }

        if (await Shell.IsCompactAsync())
        {
            await Content.GetByText(key, new() { Exact = true }).First.ClickAsync();
            link = Page.GetByRole(AriaRole.Link, new() { Name = "Open entry", Exact = true });
        }

        await link.ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]key=" + Regex.Escape(Uri.EscapeDataString(key)).Replace("%2F", "(%2F|/)", StringComparison.Ordinal) + "(&|$)"));
        await Expect(Content.Locator("section.lt-data-entry .lt-data-entry__key")).ToHaveTextAsync(key, new() { Timeout = ServerReadBound });
        await Expect(Content.GetByRole(AriaRole.Link, new() { Name = "History of this key", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>The entry panel's value, as its renderer shows it.</summary>
    public ILocator EntryValue => Content.Locator("section.lt-data-entry pre.lt-data-value");

    /// <summary>Follows the entry panel's <b>History of this key</b> link and waits for the timeline.</summary>
    /// <param name="key">The open key.</param>
    /// <param name="requireRevisions">Whether at least one revision must be drawn; otherwise an empty timeline is accepted once loaded.</param>
    public async Task OpenHistoryOfThisKeyAsync(string key, bool requireRevisions = true)
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = "History of this key", Exact = true }).ClickAsync();
        await ExpectHistoryOfAsync(key, requireRevisions);
    }

    /// <summary>On the History tab with no key, chooses <paramref name="key"/> in the <b>Choose a key</b> picker and shows its history.</summary>
    /// <param name="key">The key.</param>
    /// <param name="requireRevisions">Whether at least one revision must be drawn.</param>
    public async Task ChooseKeyAsync(string key, bool requireRevisions = true)
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Choose a key", Exact = true })).ToBeVisibleAsync();
        var field = Content.GetByRole(AriaRole.Combobox, new() { Name = "Key", Exact = true });
        await field.FocusAsync();
        await field.PressSequentiallyAsync(key);
        var option = Page.Locator(".lt-combobox__list [role=option]").Filter(new() { HasText = key });
        // The picker's options come from a server-side prefix scan, so the wait is a server read's bound (#5737).
        await Expect(option.First).ToBeVisibleAsync(new() { Timeout = PanelSettled.BoundMs });
        await option.First.ClickAsync();
        await Expect(field).ToHaveValueAsync(key);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Show history", Exact = true }).ClickAsync();
        await ExpectHistoryOfAsync(key, requireRevisions);
    }

    /// <summary>Asserts the History tab shows the timeline of <paramref name="key"/>, loaded, with at least one revision unless <paramref name="requireRevisions"/> is false.</summary>
    /// <param name="key">The key.</param>
    /// <param name="requireRevisions">Whether at least one revision must be drawn; otherwise the "No revisions" state is accepted.</param>
    public async Task ExpectHistoryOfAsync(string key, bool requireRevisions = true)
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { NameRegex = new Regex("^History of ") })).ToContainTextAsync(key);
        if (!requireRevisions)
        {
            await ExpectHistoryDrawnAsync(Revisions.Or(Content.GetByRole(AriaRole.Heading, new() { Name = "No revisions", Exact = true })).First);
            return;
        }

        await ExpectHistoryDrawnAsync(Revisions.First);
        await Expect(RevisionCount).ToHaveTextAsync(new Regex(@"^\d[\d,]* revisions?( so far)?$"));
    }

    /// <summary>The revisions' times, as the timeline draws them, in the order drawn (newest first by default).</summary>
    public async Task<IReadOnlyList<DateTimeOffset>> RevisionTimesAsync()
    {
        var texts = await Revisions.Locator(".lt-data-timeline__time").AllInnerTextsAsync();
        return [.. texts.Select(text => DateTimeOffset.ParseExact(text.Trim(), TimelineTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal))];
    }

    /// <summary>
    /// Types <paramref name="instant"/> into <b>As of</b>, chooses <b>Show as of</b>, and waits for
    /// the point-in-time read to end. Returns whether it drew, with one revision marked in effect;
    /// otherwise the panel shows its failure state.
    /// </summary>
    /// <param name="instant">The instant, in whole seconds.</param>
    public async Task<bool> ShowAsOfAsync(DateTimeOffset instant)
    {
        var text = instant.ToUniversalTime().ToString(InstantFormat, CultureInfo.InvariantCulture);
        await AsOf.FillAsync(text);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Show as of", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]at=" + Regex.Escape(Uri.EscapeDataString(text)) + "(&|$)"));
        var readable = instant.ToUniversalTime().ToString(TimelineTimeFormat, CultureInfo.InvariantCulture);
        await Expect(Content.GetByText("Showing the key as it stood at " + readable)).ToBeVisibleAsync();
        if (!await HistorySettledAsync(RevisionInEffect))
        {
            return false;
        }

        await Expect(RevisionInEffect).ToHaveCountAsync(1);
        return true;
    }

    /// <summary>
    /// Chooses <b>Latest</b>, leaving the point-in-time view, and waits for the re-read to end.
    /// Returns whether the timeline was drawn again; otherwise the panel shows its failure state.
    /// </summary>
    public async Task<bool> ShowLatestAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Latest", Exact = true }).ClickAsync();
        await Expect(Page).Not.ToHaveURLAsync(new Regex("[?&]at="));
        await Expect(Content.GetByText("Showing the key as it stood at")).ToHaveCountAsync(0);
        await Expect(RevisionInEffect).ToHaveCountAsync(0);
        await Expect(AsOf).ToHaveValueAsync(string.Empty);
        return await HistorySettledAsync(Revisions.First);
    }

    /// <summary>
    /// Chooses <b>Load older revisions</b> and waits for the next page to end. Returns whether the
    /// count grew past <paramref name="before"/>; otherwise the panel shows its failure state.
    /// </summary>
    /// <param name="before">The revision count before the click.</param>
    public async Task<bool> LoadOlderAsync(int before)
    {
        await LoadOlder.ClickAsync();
        var grown = RevisionCount.Filter(new() { HasNotTextRegex = new Regex("^" + before.ToString(CultureInfo.InvariantCulture) + " ") });
        return await HistorySettledAsync(grown);
    }

    /// <summary>Turns the History tab's <b>Follow new revisions</b> switch off, so the timeline holds still.</summary>
    public async Task StopFollowingAsync()
    {
        var follow = Content.GetByRole(AriaRole.Switch, new() { Name = "Follow new revisions", Exact = true });
        if (await follow.IsCheckedAsync())
        {
            await follow.ClickAsync();
        }

        await Expect(follow).Not.ToBeCheckedAsync();
    }

    /// <summary>The number the History toolbar's count names.</summary>
    public async Task<int> RevisionCountAsync()
    {
        var text = await RevisionCount.InnerTextAsync();
        var match = Regex.Match(text, @"^(\d[\d,]*)");
        Assert.That(match.Success, Is.True, $"the revision count reads '{text}'");
        return int.Parse(match.Groups[1].Value, NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
    }
}
