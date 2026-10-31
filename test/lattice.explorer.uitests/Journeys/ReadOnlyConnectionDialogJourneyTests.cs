using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5479: on a configured head that refuses browser-driven endpoint
/// configuration (the default), the connection indicator is the way into the
/// read-only Cluster connection dialog. This capture-only revision records the
/// header and the compact Menu sheet as they are before the fix.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ReadOnlyConnectionDialogJourneyTests : UiTestBase
{
    [Test]
    public async Task The_connection_indicator_on_a_read_only_head_as_it_stands()
    {
        var world = await UiHosts.WorldAsync();

        var desktop = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Banner(desktop).Locator("[role=status] > .lt-pill .lt-pill__text").First).ToHaveTextAsync("Connected");
        await CaptureAsync(desktop, "01-desktop-header");

        var compact = await OpenAsync(world.Head, "/", WorldIdentities.Admin, Shell.SmallWidth);
        await Shell.Banner(compact).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true }).ClickAsync();
        await Expect(compact.GetByRole(AriaRole.Dialog, new() { Name = "Menu" })).ToBeVisibleAsync();
        await CaptureAsync(compact, "02-compact-menu-sheet");
    }

    private static async Task CaptureAsync(IPage page, string step)
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "read-only-connection-dialog");
        Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, step + ".png"), FullPage = false });
    }
}
