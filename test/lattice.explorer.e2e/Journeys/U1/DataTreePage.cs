using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>
/// A tree's Data workspace as a data steward drives it (U1): the heading and its actions,
/// the tabs, and the Keys tab's toolbar, table, pager and entry panel.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class DataTreePage(PersonaSession session)
{
    private ILocator Content => session.Shell.Content;

    private IPage Page => session.Page;

    /// <summary>The tree's heading.</summary>
    public ILocator Heading => session.Shell.Heading;

    // ----- Tabs -----

    /// <summary>The tab titled <paramref name="title"/>.</summary>
    /// <param name="title">Keys, History, Metrics, Dead letters, Tag indexes or Views.</param>
    public ILocator Tab(string title) => Content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });

    /// <summary>Opens a tab, waits for it to be selected and for <c>?tab=</c> to name it.</summary>
    /// <param name="title">The tab's title.</param>
    /// <param name="id">The tab's id, as <c>?tab=</c> carries it.</param>
    public async Task OpenTabAsync(string title, string id)
    {
        await Tab(title).ClickAsync();
        await Expect(Tab(title)).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]tab=" + Regex.Escape(id) + "(&|$)"));
        Coverage.Hit("query:data.tab");
        Coverage.Hit("tab:data/" + id);
    }

    // ----- Keys toolbar -----

    /// <summary>The key prefix input.</summary>
    public ILocator PrefixInput => Content.GetByLabel("Key prefix", new() { Exact = true });

    /// <summary>The scan mode select (Live or Snapshot).</summary>
    public ILocator ScanMode => Content.GetByLabel("Scan", new() { Exact = true });

    /// <summary>The page size select.</summary>
    public ILocator PageSize => Content.GetByLabel("Page size", new() { Exact = true });

    /// <summary>The Live updates switch.</summary>
    public ILocator LiveUpdates => Content.GetByRole(AriaRole.Switch, new() { Name = "Live updates" });

    /// <summary>The keys table's status: the range of keys shown.</summary>
    public ILocator RangeStatus => Content.Locator(".lt-toolbar .lt-data-toolbar__status").First;

    /// <summary>The New entry... control.</summary>
    public ILocator NewEntry => Content.Locator("[data-lt-command=\"data.new-entry\"]");

    /// <summary>A note the Keys tab shows above its table, such as the live-updates note.</summary>
    public ILocator Notes => Content.Locator(".lt-data-note[role=status]");

    // ----- Keys table and pager -----

    /// <summary>The keys table.</summary>
    public ILocator KeysTable => Content.GetByRole(AriaRole.Table, new() { NameRegex = new Regex("^Keys of ") });

    /// <summary>The key links of the rows on the page.</summary>
    public ILocator KeyLinks => KeysTable.Locator("tbody tr.lt-table__row a.lt-data-key");

    /// <summary>The pager's Next page control.</summary>
    public ILocator NextPage => Content.Locator(".lt-data-pager").GetByRole(AriaRole.Button, new() { Name = "Next page", Exact = true });

    /// <summary>The pager's Previous page control.</summary>
    public ILocator PreviousPage => Content.Locator(".lt-data-pager").GetByRole(AriaRole.Button, new() { Name = "Previous page", Exact = true });

    /// <summary>Which page the pager shows.</summary>
    public ILocator PagePosition => Content.Locator(".lt-data-pager__position");

    /// <summary>The empty state an expired scan cursor shows.</summary>
    public ILocator CursorExpired => Content.GetByRole(AriaRole.Heading, new() { Name = "This scan's cursor has expired" });

    /// <summary>The empty state's Restart from the first page control.</summary>
    public ILocator RestartFromFirstPage => Content.GetByRole(AriaRole.Button, new() { Name = "Restart from the first page", Exact = true });

    /// <summary>The keys on the page.</summary>
    public async Task<IReadOnlyList<string>> KeysAsync() =>
        [.. (await KeyLinks.AllInnerTextsAsync()).Select(key => key.Trim())];

    /// <summary>Waits until the keys table has loaded with at least one row.</summary>
    public async Task WaitForKeysAsync()
    {
        await Expect(KeyLinks.First).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>Applies a key prefix through the toolbar and waits for the table to show only keys under it.</summary>
    /// <param name="prefix">The prefix.</param>
    /// <param name="expectKeys">Whether keys under the prefix exist now.</param>
    public async Task SetPrefixAsync(string prefix, bool expectKeys = true)
    {
        await PrefixInput.FillAsync(prefix);
        await PrefixInput.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]prefix=" + Regex.Escape(Uri.EscapeDataString(prefix)) + "(&|$)"));
        if (expectKeys)
        {
            await WaitForKeysAsync();
            await Expect(KeyLinks.Filter(new() { HasNotTextRegex = new Regex("^\\s*" + Regex.Escape(prefix)) })).ToHaveCountAsync(0);
        }
        else
        {
            await Expect(KeysTable).ToContainTextAsync("No key starts with this prefix.");
        }

        Coverage.Hit("query:shell.prefix");
    }

    /// <summary>
    /// Chooses a page size and waits for the first page at that size. The scope must hold at
    /// least <paramref name="size"/> keys: the page is already "Page 1" before the reload, so
    /// only the row count tells the new page from the old one.
    /// </summary>
    /// <param name="size">The page size.</param>
    public async Task SetPageSizeAsync(int size)
    {
        var text = size.ToString(System.Globalization.CultureInfo.InvariantCulture);
        await PageSize.SelectOptionAsync(text);
        await Expect(PageSize).ToHaveValueAsync(text);
        await Expect(PagePosition).ToHaveTextAsync("Page 1");
        await Expect(KeyLinks).ToHaveCountAsync(size, new() { Timeout = 30_000 });
    }

    /// <summary>Chooses the scan mode (<c>Live</c> or <c>Snapshot</c>) and waits for the first page.</summary>
    /// <param name="mode">The mode.</param>
    public async Task SetScanModeAsync(string mode)
    {
        await ScanMode.SelectOptionAsync(mode);
        await Expect(ScanMode).ToHaveValueAsync(mode);
        await Expect(PagePosition).ToHaveTextAsync("Page 1");
        await WaitForKeysAsync();
    }

    /// <summary>Turns live updates on or off and waits for the switch to say so.</summary>
    /// <param name="on">Whether live updates should be on.</param>
    public async Task SetLiveUpdatesAsync(bool on)
    {
        if (await LiveUpdates.GetAttributeAsync("aria-checked") != (on ? "true" : "false"))
        {
            await LiveUpdates.ClickAsync();
        }

        await Expect(LiveUpdates).ToHaveAttributeAsync("aria-checked", on ? "true" : "false");
    }

    // ----- Entry panel -----

    /// <summary>The open entry's panel.</summary>
    public ILocator Entry => Content.Locator("section.lt-data-entry").Filter(new() { Has = Page.GetByRole(AriaRole.Heading, new() { Name = "Entry", Exact = true }) });

    /// <summary>The open entry's value block.</summary>
    public ILocator EntryValue => Entry.Locator("pre.lt-data-value");

    /// <summary>The open entry's Edit control.</summary>
    public ILocator Edit => Entry.Locator("[data-lt-command=\"data.edit-entry\"]");

    /// <summary>The open entry's Delete... control.</summary>
    public ILocator Delete => Entry.Locator("[data-lt-command=\"data.delete-entry\"]");

    /// <summary>The open entry's History of this key link.</summary>
    public ILocator HistoryOfKey => Entry.GetByRole(AriaRole.Link, new() { Name = "History of this key", Exact = true });

    /// <summary>Opens <paramref name="key"/> in <paramref name="tree"/>'s Keys tab by its address and waits for its value.</summary>
    /// <param name="tree">The tree's logical id.</param>
    /// <param name="key">The key.</param>
    public async Task OpenEntryAsync(string tree, string key)
    {
        await session.Shell.GotoAsync($"/data/{tree}?key={Uri.EscapeDataString(key)}");
        await Expect(Heading).ToHaveTextAsync(tree);
        await Expect(Entry.Locator(".lt-data-entry__key")).ToHaveTextAsync(key, new() { Timeout = 60_000 });
        await Expect(EntryValue).ToBeVisibleAsync();
        Coverage.Hit("query:shell.key");
        Coverage.Hit("use-case:data/entry-view");
    }

    // ----- Heading actions -----

    /// <summary>The Import... control.</summary>
    public ILocator Import => Content.Locator("[data-lt-command=\"data.import\"]");

    /// <summary>The Export... control.</summary>
    public ILocator Export => Content.Locator("[data-lt-command=\"data.export\"]");

    /// <summary>The Delete tree... control (#5477).</summary>
    public ILocator DeleteTree => Content.Locator("[data-lt-data-tree-lifecycle=delete]");

    /// <summary>What the page shows once the tree is deleted (#5477).</summary>
    public ILocator Deleted => Content.Locator("[data-lt-tree-deleted]");

    // ----- Confirmations -----

    /// <summary>The destructive confirmation titled <paramref name="title"/>.</summary>
    /// <param name="title">Its title.</param>
    public ILocator Confirmation(string title) => Page.GetByRole(AriaRole.Alertdialog, new() { Name = title });

    /// <summary>Types the object's name into an open destructive confirmation.</summary>
    /// <param name="title">The confirmation's title.</param>
    /// <param name="name">The object's name.</param>
    public async Task<ILocator> TypeConfirmationAsync(string title, string name)
    {
        var confirm = Confirmation(title);
        await Expect(confirm).ToBeVisibleAsync();
        await Expect(confirm).ToContainTextAsync(name);
        await confirm.GetByRole(AriaRole.Textbox).FillAsync(name);
        return confirm;
    }

    /// <summary>The toast messages on the page.</summary>
    public ILocator Toasts => session.Shell.Toasts;
}
