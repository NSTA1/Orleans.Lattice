using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// The Data area as C1 drives it: a tree's Keys tab (its live-key count, and a key looked up
/// by prefix), a key's History, the Dead letters tab, and the streaming Import dialog (G1).
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class DataTreePage(PersonaSession session)
{
    /// <summary>What the Keys tab says when no key starts with the prefix.</summary>
    public const string NoKeyUnderPrefix = "No key starts with this prefix.";

    private static readonly TimeSpan ImportPatience = TimeSpan.FromMinutes(3);

    private IPage Page => session.Page;

    /// <summary>Opens <paramref name="tree"/>'s Keys tab, optionally filtered to <paramref name="prefix"/>.</summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="prefix">The key prefix, or <see langword="null"/>.</param>
    public async Task OpenKeysAsync(string tree, string? prefix = null)
    {
        var query = prefix is null ? string.Empty : "?prefix=" + Uri.EscapeDataString(prefix);
        await session.Shell.GotoAsync($"data/{tree}{query}");
        await session.Shell.ExpectHeadingAsync(tree);
        await Expect(Page.GetByRole(AriaRole.Tab, new() { Name = "Keys", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
    }

    /// <summary>
    /// Waits until the tree's summary line counts <paramref name="count"/> live keys, reloading
    /// the page while the count is still settling (it is read once per load).
    /// </summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="count">The live keys expected.</param>
    public async Task ExpectLiveKeysAsync(string tree, long count)
    {
        var text = string.Create(CultureInfo.InvariantCulture, $"{count:N0} live keys");
        await UntilAsync($"{tree} to count {text}", async () =>
        {
            await OpenKeysAsync(tree);
            try
            {
                await Expect(Page.GetByText(text, new() { Exact = true })).ToBeVisibleAsync(new() { Timeout = 15_000 });
                return true;
            }
            catch (PlaywrightException)
            {
                return false;
            }
        });
    }

    /// <summary>The Keys tab's row for <paramref name="key"/>.</summary>
    /// <param name="key">The key.</param>
    public ILocator KeyRow(string key) =>
        Page.GetByRole(AriaRole.Row).Filter(new() { Has = Page.GetByRole(AriaRole.Link, new() { Name = key, Exact = true }) });

    /// <summary>Asserts the Keys tab, filtered to the key's prefix, lists it with a value carrying <paramref name="total"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="total">The total the value carries, as JSON writes it.</param>
    public async Task ExpectKeyWithTotalAsync(string key, string total)
    {
        var row = KeyRow(key);
        await Expect(row).ToHaveCountAsync(1);
        await Expect(row).ToContainTextAsync(TotalPattern(total));
    }

    /// <summary>
    /// Waits until the Keys tab, filtered to <paramref name="key"/>, says no key starts with it,
    /// reloading while the change has not reached this region yet.
    /// </summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="key">The key that must be gone.</param>
    /// <param name="timeout">How long to wait, or <see langword="null"/> for a minute.</param>
    public Task ExpectKeyGoneAsync(string tree, string key, TimeSpan? timeout = null) =>
        UntilAsync($"{key} to be gone from {tree}", async () =>
        {
            await OpenKeysAsync(tree, key);
            try
            {
                await Expect(Page.GetByText(NoKeyUnderPrefix, new() { Exact = true })).ToBeVisibleAsync(new() { Timeout = 10_000 });
                return true;
            }
            catch (PlaywrightException)
            {
                return false;
            }
        }, timeout ?? TimeSpan.FromMinutes(1));

    /// <summary>Opens <paramref name="key"/>'s History tab and returns its list of revisions.</summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="key">The key.</param>
    public async Task<ILocator> OpenHistoryAsync(string tree, string key)
    {
        await session.Shell.GotoAsync($"data/{tree}?tab=history&key={Uri.EscapeDataString(key)}");
        await Expect(Page.GetByRole(AriaRole.Heading, new() { Name = $"History of {key}" })).ToBeVisibleAsync();
        var revisions = Page.GetByRole(AriaRole.List, new() { NameRegex = new Regex("^Revisions, (newest|oldest) first$") });
        await Expect(revisions).ToBeVisibleAsync();
        return revisions;
    }

    /// <summary>Opens the tree's Dead letters tab and returns its table.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task<ILocator> OpenDeadLettersAsync(string tree)
    {
        await session.Shell.GotoAsync($"data/{tree}?tab=dead-letters");
        await Expect(Page.GetByRole(AriaRole.Tab, new() { Name = "Dead letters", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        var table = Page.GetByRole(AriaRole.Table, new() { Name = "Dead letters", Exact = true });
        await Expect(table).ToBeVisibleAsync();
        return table;
    }

    /// <summary>Opens the tree's Dead letters tab and asserts nothing was diverted.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task ExpectNoDeadLettersAsync(string tree)
    {
        await session.Shell.GotoAsync($"data/{tree}?tab=dead-letters");
        await Expect(Page.GetByRole(AriaRole.Tab, new() { Name = "Dead letters", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Page.GetByText("No value written to this tree has been diverted by schema enforcement.")).ToBeVisibleAsync();
    }

    /// <summary>Opens the Import dialog from the tree page's heading.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task<ILocator> OpenImportAsync(string tree)
    {
        await Page.Locator("[data-lt-command='data.import']").ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = $"Import into {tree}", Exact = true });
        await Expect(dialog.Locator(".lt-data-import[data-lt-import-state='choose']")).ToBeVisibleAsync();
        return dialog;
    }

    /// <summary>Chooses an NDJSON file to add or replace entries with, keyed by <paramref name="keyTemplate"/>.</summary>
    /// <param name="dialog">The Import dialog.</param>
    /// <param name="file">The file's path.</param>
    /// <param name="keyTemplate">The key template, such as <c>order/{/id}</c>.</param>
    public static async Task ChooseNdjsonUpsertAsync(ILocator dialog, string file, string keyTemplate)
    {
        await dialog.Locator("#data-import-file").SetInputFilesAsync(file);
        await dialog.GetByLabel("Format", new() { Exact = true }).SelectOptionAsync("Ndjson");
        await dialog.GetByLabel("Mode", new() { Exact = true }).SelectOptionAsync("Upsert");
        var key = dialog.GetByLabel("Key", new() { Exact = true });
        await key.FillAsync(keyTemplate);
        await Expect(key).ToHaveValueAsync(keyTemplate);
    }

    /// <summary>Starts the import and waits for it to finish, then returns the entries written and lines refused.</summary>
    /// <param name="dialog">The Import dialog, ready to import.</param>
    public static async Task<(int Written, int Refused)> ImportAsync(ILocator dialog)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Import", Exact = true }).ClickAsync();
        var state = dialog.Locator(".lt-data-import");
        await Expect(state).ToHaveAttributeAsync("data-lt-import-state", new Regex("^(succeeded|failed|cancelled)$"), new() { Timeout = (float)ImportPatience.TotalMilliseconds });
        await Expect(state).ToHaveAttributeAsync("data-lt-import-state", "succeeded");
        var written = int.Parse(await dialog.Locator("[data-lt-import-committed]").GetAttributeAsync("data-lt-import-committed") ?? "-1", CultureInfo.InvariantCulture);
        var refused = int.Parse(await dialog.Locator("[data-lt-import-refused]").GetAttributeAsync("data-lt-import-refused") ?? "-1", CultureInfo.InvariantCulture);
        return (written, refused);
    }

    /// <summary>Acknowledges a finished import and closes the dialog.</summary>
    /// <param name="dialog">The Import dialog.</param>
    public static async Task DoneAsync(ILocator dialog)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
    }

    /// <summary>A pattern matching a JSON <c>total</c> member holding <paramref name="total"/>.</summary>
    /// <param name="total">The total, as JSON writes it.</param>
    public static Regex TotalPattern(string total) => new($"\"total\"\\s*:\\s*{Regex.Escape(total)}(?![0-9.])");

    private static async Task UntilAsync(string what, Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromMinutes(2));
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(3));
        }
    }
}
