using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5419, in a real browser against the real state gRPC binding: a user whose grant
/// on a tree is narrowed to part of it (<see cref="WorldIdentities.Dave"/>, limited to
/// <c>public/</c> on <see cref="ExplorerWorld.ExportTree"/>) opens that tree. Its whole-tree
/// measures are refused to him, and the refusal is his read's outcome alone: the banner's
/// connection pill stays Connected, the summary line omits the key count, and the Metrics
/// tab says why the measures are not available.
/// </summary>
/// <remarks>
/// Each step saves an ordered screenshot under <c>screenshots/narrowed-grant-connection/</c>,
/// which the UI lane uploads, for the pull request's journey section.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class NarrowedGrantConnectionJourneyTests : UiTestBase
{
    private static readonly string Tree = ExplorerWorld.ExportTree;

    [Test]
    public async Task A_narrowed_grant_on_a_tree_keeps_the_connection_pill_connected()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data/" + Tree, WorldIdentities.Dave);
        await Expect(Shell.Heading(page)).ToHaveTextAsync(Tree);

        var pill = ConnectionPill(page);
        await Expect(pill).ToHaveTextAsync("Connected");
        await CaptureAsync(page, "01-tree-page-pill-reads-connected");

        await Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Metrics" }).ClickAsync();
        var withheld = Shell.Content(page).Locator("[data-lt-metrics-withheld]");
        await Expect(withheld).ToContainTextAsync("read access to the whole tree");
        await Expect(Shell.Content(page).GetByRole(AriaRole.Heading, new() { Name = "Metrics are not available to you" })).ToBeVisibleAsync();

        // The refused sample has been answered by now (its outcome is on the page). Before
        // the fix it left the binding as Internal and the connection read that as the
        // endpoint failing, so watch the pill across the window in which that showed.
        await AssertPillStaysConnectedAsync(pill, TimeSpan.FromSeconds(3));
        await Expect(Shell.Content(page).Locator("p.lt-data-meta")).Not.ToContainTextAsync("live key");
        await CaptureAsync(page, "02-metrics-tab-says-why-and-pill-stays-connected");
    }

    private static ILocator ConnectionPill(IPage page) =>
        Shell.Banner(page).Locator("[role=status] > .lt-pill .lt-pill__text").First;

    private static async Task AssertPillStaysConnectedAsync(ILocator pill, TimeSpan window)
    {
        var deadline = DateTimeOffset.UtcNow + window;
        var readings = new List<string>();
        while (DateTimeOffset.UtcNow < deadline)
        {
            readings.Add((await pill.InnerTextAsync()).Trim());
            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        Assert.That(readings, Has.All.EqualTo("Connected"), "a per-tree denial must never move the connection pill");
    }

    private static async Task CaptureAsync(IPage page, string step)
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "narrowed-grant-connection");
        Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, step + ".png"), FullPage = false });
    }
}
