using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4149: a key written once has one revision in its History, and a revision
/// whose value the tree's retention did not keep says so, rather than reading as a
/// change to the key's metadata.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class HistoryJourneyTests : UiTestBase
{
    [Test]
    public async Task A_key_written_once_shows_one_revision_whose_value_was_not_kept()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?tab=history&key=machine-007", WorldIdentities.Admin);
        var content = Shell.Content(page);

        await Expect(content.Locator(".lt-data-timeline__rev")).ToHaveCountAsync(1);
        await Expect(content.Locator(".lt-data-timeline__kind")).ToHaveTextAsync("Set - value not kept");
        await Expect(content).ToContainTextAsync("not a metadata change");
        await Expect(content).Not.ToContainTextAsync("metadata only");
    }
}
