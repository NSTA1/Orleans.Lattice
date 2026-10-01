using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// An operator reshards a tree they have just been reading (issue #4146). Once the
/// reshard reports done, the tree's heading, its summary and its Shards tab all
/// show the new shard count, the live map agrees with the persisted one, and each
/// shard row shows the virtual slots it owns rather than one.
/// </summary>
/// <remarks>
/// Every step waits on what the page shows, never on time. The tree is the test's
/// own, so the shared world's other trees are untouched.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class TreeReshardJourneyTests : UiTestBase
{
    private static readonly LocatorAssertionsToHaveTextOptions Reshard = new() { Timeout = 120_000 };

    [Test]
    public async Task After_a_reshard_every_shard_figure_on_the_tree_page_shows_the_new_count()
    {
        var world = await UiHosts.WorldAsync();
        var treeId = "journey-reshard-" + Guid.NewGuid().ToString("N")[..8];
        await world.SeedTreeAsync(treeId, entries: 40, shardCount: 2);

        // Read the tree first, as an open page would, so every cached read is warm.
        var page = await OpenAsync(world.Head, "/cluster/trees/" + treeId + "?tab=shards", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(treeId);
        await Expect(Term(page, "Physical shards")).ToHaveTextAsync("2");

        await Shell.Content(page).GetByRole(AriaRole.Link, new() { Name = "Reshard" }).ClickAsync();
        await Shell.Content(page).GetByLabel("Target physical shards").FillAsync("4");
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Reshard..." }).ClickAsync();
        var dialog = page.Locator(".lt-confirm");
        await dialog.Locator("input").FillAsync(treeId);
        await dialog.Locator("button[type=submit]").ClickAsync();

        // The stage reads idle before the reshard starts too, so wait for the new
        // count first: idle after that is the reshard having finished.
        await Expect(Term(page, "Physical shards")).ToHaveTextAsync("4", Reshard);
        await Expect(Shell.Content(page).Locator(".lt-cluster-stage")).ToHaveTextAsync(new Regex("No reshard is running"), Reshard);

        await page.GoBackAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync(treeId);
        await Expect(page.Locator(".lt-shell-page-lede")).ToContainTextAsync("4 shards");
        await Expect(Term(page, "Physical shards")).ToHaveTextAsync("4");
        var persisted = Term(page, "Persisted map");
        await Expect(persisted).ToHaveTextAsync(new Regex(@"^Custom, version \d+$"));
        await Expect(Term(page, "Map version")).ToHaveTextAsync(((await persisted.TextContentAsync())!).Replace("Custom, version ", string.Empty));

        var rows = Shell.Content(page).Locator("tbody tr");
        await Expect(rows).ToHaveCountAsync(4);
        await Expect(rows.Locator("td:nth-of-type(1)")).ToHaveTextAsync(["1,024", "1,024", "1,024", "1,024"]);

        await Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Summary" }).ClickAsync();
        await Expect(Term(page, "Physical shards")).ToHaveTextAsync("4");
    }

    private static ILocator Term(IPage page, string term) =>
        Shell.Content(page).Locator(".lt-dl__row").Filter(new() { Has = page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }) }).Locator("dd");
}
