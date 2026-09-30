using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The very first visit: an Explorer with no configuration at all asks where its cluster
/// is, before anything else, and once told, lets the user sign in and use it.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class FirstRunJourneyTests : UiTestBase
{
    [Test]
    public async Task A_first_run_with_no_connection_asks_for_the_cluster_then_signs_in()
    {
        var world = await UiHosts.WorldAsync();
        await using var head = await ExplorerHead.StartAsync(new ExplorerHeadOptions { AllowInteractiveEndpointConfiguration = true });
        var page = await OpenAsync(head, "/");

        // Nothing is configured, so the connection dialog is the whole Explorer, and it cannot be dismissed.
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Connect to a cluster" });
        await Expect(dialog).ToBeVisibleAsync();
        await page.Keyboard.PressAsync("Escape");
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(dialog).ToHaveAttributeAsync("aria-modal", "true");

        await dialog.GetByLabel("Endpoint").FillAsync(world.GrpcEndpoint);
        await dialog.GetByLabel("Insecure loopback development mode").CheckAsync();
        await dialog.GetByLabel("Allow unencrypted HTTP/2 (h2c)").CheckAsync();
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Test connection" }).ClickAsync();
        await Expect(dialog.GetByRole(AriaRole.Status)).Not.ToBeEmptyAsync();
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Save and connect" }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();

        // Connected, and signed out: the header offers the sign-in.
        await Shell.SignInAsync(page, WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "data")).ToBeVisibleAsync();
        await Shell.Stop(page, "data").ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await Expect(Shell.Content(page)).ToContainTextAsync(ExplorerWorld.DemoTree);
    }
}
