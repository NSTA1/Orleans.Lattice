using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// An operator deletes and then purges a tree of their own from its Lifecycle tab
/// (issue 3958). Purge is accept-then-poll: the page says "purged" only once the
/// cluster reports the purge complete, and its progress bar - a real ARIA
/// progressbar - reaches 100 percent with every shard it walked accounted for.
/// </summary>
/// <remarks>
/// Every step waits on what the page shows, never on time. The tree is the test's
/// own, so the shared world's other trees are untouched.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class TreePurgeJourneyTests : UiTestBase
{
    [Test]
    public async Task An_operator_purges_a_tree_and_watches_its_progress_reach_100_percent()
    {
        var world = await UiHosts.WorldAsync();
        var treeId = "journey-purge-" + Guid.NewGuid().ToString("N")[..8];
        await world.SeedTreeAsync(treeId, entries: 6);

        var page = await OpenAsync(world.Head, "/cluster/trees/" + treeId, WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(treeId);
        await Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Lifecycle" }).ClickAsync();

        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Delete tree..." }).ClickAsync();
        await ConfirmAsync(page, treeId);
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Tree deleted." })).ToHaveCountAsync(1);

        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Purge now..." }).ClickAsync();
        await ConfirmAsync(page, treeId);

        var bar = Shell.Content(page).GetByRole(AriaRole.Progressbar, new() { Name = "Purge progress" });
        await Expect(bar).ToHaveAttributeAsync("aria-valuenow", "100");
        await Expect(bar).ToHaveAttributeAsync("aria-valuemin", "0");
        await Expect(bar).ToHaveAttributeAsync("aria-valuemax", "100");
        await Expect(Shell.Content(page).Locator(".lt-progress__phase")).ToHaveTextAsync("Purged");
        await Expect(Shell.Content(page).Locator(".lt-progress__detail")).ToHaveTextAsync(new System.Text.RegularExpressions.Regex(@"^(\d+) of \1 shards? purged\.$"));
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Tree purged." })).ToHaveCountAsync(1);
        await Expect(Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Purge now..." })).ToHaveCountAsync(0);
    }

    private static async Task ConfirmAsync(IPage page, string treeId)
    {
        var dialog = page.Locator(".lt-confirm");
        await dialog.Locator("input").FillAsync(treeId);
        await dialog.Locator("button[type=submit]").ClickAsync();
    }
}
