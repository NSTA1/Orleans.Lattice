using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// A tree's administration page at <c>/cluster/trees/{tree}</c>: its operations (Reshard,
/// Resize, Snapshot) and its tabs, of which A8 reads Shards: the shard map, and one row per
/// physical shard with its diagnostics and hotness.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
/// <param name="tree">The tree.</param>
internal sealed class ClusterTreePage(ShellPage shell, string tree) : A8PageObject(shell)
{
    /// <summary>The tree.</summary>
    public string Tree { get; } = tree;

    /// <summary>The Shards tab's "Count tombstones..." button.</summary>
    public ILocator CountTombstones => Content.Locator("button[data-lt-cluster=\"deep-diagnostics\"]");

    /// <summary>The Shards tab's note saying how and when the diagnostics were read.</summary>
    public ILocator DiagnosticsNote => Content.Locator(".lt-toolbar .lt-cluster-note");

    /// <summary>The shard rows (desktop table).</summary>
    public ILocator ShardRows => Content.Locator("table tbody tr");

    /// <summary>Opens the page at its address.</summary>
    public async Task OpenAsync()
    {
        await Shell.GotoAsync("/cluster/trees/" + Tree);
        await Shell.ExpectHeadingAsync(Tree);
        Coverage.Hit("route:/cluster/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}/{p7?}/{p8?}");
    }

    /// <summary>Follows the tree operation link named <paramref name="operation"/> (Reshard, Resize or Snapshot) to its page.</summary>
    /// <param name="operation">The link's text.</param>
    public async Task FollowOperationAsync(string operation)
    {
        await Content.GetByRole(AriaRole.Navigation, new() { Name = "Tree operations" })
            .GetByRole(AriaRole.Link, new() { Name = operation, Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/cluster/trees/" + Regex.Escape(Tree) + "/" + operation.ToLowerInvariant() + "(\\?|$)"));
        await Shell.ExpectHeadingAsync(operation + " " + Tree);
    }

    /// <summary>Opens the Shards tab and waits for its shard rows.</summary>
    public async Task OpenShardsAsync()
    {
        await Content.GetByRole(AriaRole.Tab, new() { Name = "Shards", Exact = true }).ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Tab, new() { Name = "Shards", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(Term("Physical shards")).Not.ToBeEmptyAsync();
        await Expect(DiagnosticsNote).ToContainTextAsync("Live keys only at");
        Coverage.Hit("tab:cluster/shards");
    }

    /// <summary>
    /// Reads the hot shard: on the desktop table, sorts the rows by writes, most first, and
    /// sees the first row has taken writes; on a compact list, sees a row report its rate.
    /// </summary>
    /// <returns>The hot shard's index and writes, or <see langword="null"/> on a compact list.</returns>
    public async Task<(string Shard, long Writes)?> ReadHotShardAsync()
    {
        if (await Shell.IsCompactAsync())
        {
            await Expect(Content.GetByText(new Regex(@"keys, .+ ops/s$")).First).ToBeVisibleAsync();
            return null;
        }

        var header = Content.Locator("th").Filter(new() { Has = Page.Locator("button.lt-table__sort", new() { HasTextRegex = new Regex("^Writes$") }) });
        var sort = header.Locator("button.lt-table__sort");
        await sort.ClickAsync();
        await Expect(header).ToHaveAttributeAsync("aria-sort", new Regex("^(ascending|descending)$"));
        if (await header.GetAttributeAsync("aria-sort") == "ascending")
        {
            await sort.ClickAsync();
        }

        await Expect(header).ToHaveAttributeAsync("aria-sort", "descending");
        var first = ShardRows.First;
        var writes = ParseCount(await first.Locator("td").Nth(5).TextContentAsync());
        Assert.That(writes, Is.GreaterThan(0), $"The busiest shard of {Tree} shows no writes: the line feed's traffic is not reaching the hotness sample.");
        var shard = (await first.Locator("th").TextContentAsync())!.Trim();
        return (shard, writes!.Value);
    }

    /// <summary>Asks for the tombstone count, which first asks for the tree's name.</summary>
    public async Task<ConfirmDialog> AskToCountTombstonesAsync()
    {
        await CountTombstones.ClickAsync();
        var dialog = new ConfirmDialog(Page, "Count tombstones");
        await dialog.TypeAsync(Tree, "Count tombstones");
        return dialog;
    }

    /// <summary>Confirms the count and sees the tab say it read every leaf, with a tombstone count on every shard.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task CountTombstonesAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Count tombstones");
        await Expect(DiagnosticsNote).ToContainTextAsync("Deep read at", new() { Timeout = 120_000 });
        if (!await Shell.IsCompactAsync())
        {
            var tombstones = await ShardRows.Locator("td:nth-of-type(4)").AllTextContentsAsync();
            Assert.That(tombstones, Is.Not.Empty.And.All.Matches<string>(text => ParseCount(text) is not null),
                "After a deep read every shard row shows a tombstone count, not a dash.");
        }

        Coverage.Hit("use-case:cluster/count-tombstones");
    }
}
