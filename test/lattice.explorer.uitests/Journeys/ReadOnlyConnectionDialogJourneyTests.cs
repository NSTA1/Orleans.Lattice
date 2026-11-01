using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5479, in a real browser: on a configured head that refuses browser-driven
/// endpoint configuration (the default), the connection indicator offers
/// <b>Connection details</b> - in the header the endpoint itself, named
/// "Connection details: {endpoint}", and in the compact Menu sheet a button of that
/// name - and it opens the read-only <b>Cluster connection</b> dialog showing the
/// configured endpoint, with nothing to edit, test or save, which Close and Escape
/// dismiss.
/// </summary>
/// <remarks>
/// Each step saves an ordered screenshot under
/// <c>screenshots/read-only-connection-dialog/</c>, which the UI lane uploads, for the
/// pull request's journey section.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class ReadOnlyConnectionDialogJourneyTests : UiTestBase
{
    [Test]
    public async Task Connection_details_opens_the_read_only_cluster_connection_dialog_at_both_widths()
    {
        var world = await UiHosts.WorldAsync();

        var desktop = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        var banner = Shell.Banner(desktop);
        await Expect(banner.Locator("[role=status] > .lt-pill .lt-pill__text").First).ToHaveTextAsync("Connected");
        var details = banner.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^Connection details: ") });
        await Expect(details).ToHaveAttributeAsync("aria-haspopup", "dialog");
        await Expect(details.Locator("code")).ToHaveTextAsync(world.GrpcEndpoint);
        await Expect(banner.GetByRole(AriaRole.Button, new() { Name = "Connection settings" })).ToHaveCountAsync(0);
        await CaptureAsync(desktop, "01-desktop-header-connection-details");

        await details.ClickAsync();
        var dialog = await ExpectReadOnlyDialogAsync(desktop, world.GrpcEndpoint);
        await CaptureAsync(desktop, "02-desktop-cluster-connection-dialog");

        await dialog.GetByRole(AriaRole.Button, new() { Name = "Close", Exact = true }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();

        await details.ClickAsync();
        dialog = await ExpectReadOnlyDialogAsync(desktop, world.GrpcEndpoint);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Close", Exact = true }).FocusAsync();
        await desktop.Keyboard.PressAsync("Escape");
        await Expect(dialog).ToBeHiddenAsync();

        var compact = await OpenAsync(world.Head, "/", WorldIdentities.Admin, Shell.SmallWidth);
        await Shell.Banner(compact).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true }).ClickAsync();
        var menu = compact.GetByRole(AriaRole.Dialog, new() { Name = "Menu" });
        var folded = menu.GetByRole(AriaRole.Button, new() { Name = "Connection details", Exact = true });
        await Expect(folded).ToHaveAttributeAsync("aria-haspopup", "dialog");
        await Expect(menu.GetByRole(AriaRole.Button, new() { Name = "Connection settings" })).ToHaveCountAsync(0);
        await CaptureAsync(compact, "03-compact-menu-connection-details");

        await folded.ClickAsync();
        await Expect(menu).ToBeHiddenAsync();
        var sheet = await ExpectReadOnlyDialogAsync(compact, world.GrpcEndpoint);
        await CaptureAsync(compact, "04-compact-cluster-connection-sheet");

        await sheet.GetByRole(AriaRole.Button, new() { Name = "Close", Exact = true }).FocusAsync();
        await compact.Keyboard.PressAsync("Escape");
        await Expect(sheet).ToBeHiddenAsync();
    }

    private static async Task<ILocator> ExpectReadOnlyDialogAsync(IPage page, string endpoint)
    {
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Cluster connection" });
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(dialog).ToContainTextAsync(endpoint);
        await Expect(dialog).ToContainTextAsync("cannot be changed from the browser");
        await Expect(dialog.Locator("form, input, textarea, select")).ToHaveCountAsync(0);
        await Expect(dialog.GetByRole(AriaRole.Button)).ToHaveTextAsync(new[] { "Close" });
        return dialog;
    }

    private static async Task CaptureAsync(IPage page, string step)
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "read-only-connection-dialog");
        Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, step + ".png"), FullPage = false });
    }
}
