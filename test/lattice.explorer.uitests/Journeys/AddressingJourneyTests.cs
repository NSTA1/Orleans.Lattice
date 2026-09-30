using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #3983: an address means one place. A query selects state within a page and is
/// never read as part of a tree id or drawn as a node of the address chain, a tree id is
/// one node however many slashes it holds, and neither the palette nor the tenant switcher
/// shows a state the reader did not choose just because the pointer rests on it.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AddressingJourneyTests : UiTestBase
{
    [Test]
    public async Task A_cluster_tree_address_with_a_query_opens_its_tab_once_interactive()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/cluster/trees/{ExplorerWorld.DemoTree}?tab=lifecycle", WorldIdentities.Admin);

        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerWorld.DemoTree);
        await Expect(Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Lifecycle", Selected = true })).ToHaveCountAsync(1);
        await Expect(Chain(page)).ToHaveTextAsync(["Home", "cluster", "trees", ExplorerWorld.DemoTree]);
        await Expect(Shell.Content(page)).Not.ToContainTextAsync("Nothing lives at this address");

        // Another tab is another address, and the chain never shows the query.
        await Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Storage" }).ClickAsync();
        await Expect(page).ToHaveURLAsync(world.Head.Url($"/cluster/trees/{ExplorerWorld.DemoTree}?tab=storage"));
        await Expect(Chain(page)).ToHaveTextAsync(["Home", "cluster", "trees", ExplorerWorld.DemoTree]);
    }

    [Test]
    public async Task A_data_address_with_a_query_draws_no_query_node()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?tab=history&key=machine-003", WorldIdentities.Admin);

        await Expect(Shell.Heading(page)).ToContainTextAsync(ExplorerWorld.DemoTree);
        await Expect(Chain(page)).ToHaveTextAsync(["t/default", "data", ExplorerWorld.DemoTree]);
        await Expect(Chain(page).Last).ToHaveAttributeAsync("aria-current", "page");
    }

    [Test]
    public async Task The_palette_opens_on_its_first_option_whatever_the_pointer_rests_on()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "schema")).ToBeVisibleAsync();
        var options = Shell.Suggestions(page);

        // Find where the second option draws, then park the pointer there with the list closed.
        await OpenThemeCommandsAsync(page);
        var box = await options.Nth(1).BoundingBoxAsync();
        Assert.That(box, Is.Not.Null);
        await page.Keyboard.PressAsync("Escape");
        await Expect(Shell.AddressInput(page)).ToHaveCountAsync(0);
        await page.Mouse.MoveAsync(box!.X + (box.Width / 2), box.Y + (box.Height / 2));

        await OpenThemeCommandsAsync(page);
        await Expect(options.First).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(options.Nth(1)).ToHaveAttributeAsync("aria-selected", "false");
        await NextFramesAsync(page);
        var plain = await BackgroundAsync(options.Nth(2));
        Assert.That(await BackgroundAsync(options.Nth(1)), Is.EqualTo(plain), "the option under a resting pointer is not shaded");

        // Once the pointer moves, it shades the option it is over; the active option stays put.
        await page.Mouse.MoveAsync(box.X + (box.Width / 2) + 4, box.Y + (box.Height / 2));
        await Expect(options.Nth(1)).Not.ToHaveCSSAsync("background-color", plain);
        await Expect(options.First).ToHaveAttributeAsync("aria-selected", "true");
    }

    [Test]
    public async Task The_tenant_toggle_is_marked_open_only_while_its_panel_is_open()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/cluster", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        var toggle = TenantSwitcherJourneyTests.Toggle(page);

        // Clicked open, the pointer stays resting on the toggle.
        await toggle.ClickAsync();
        await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "true");
        await NextFramesAsync(page);
        var open = await BackgroundAsync(toggle);

        await page.Keyboard.PressAsync("Escape");

        await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        await Expect(toggle).Not.ToHaveCSSAsync("background-color", open);
    }

    private static ILocator Chain(IPage page) =>
        page.GetByRole(AriaRole.Navigation, new() { Name = "Address", Exact = true }).Locator(".lt-chain__text");

    private static async Task OpenThemeCommandsAsync(IPage page)
    {
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">theme");
        await Expect(Shell.Suggestions(page)).ToHaveCountAsync(3);
    }

    private static Task<string> BackgroundAsync(ILocator element) =>
        element.EvaluateAsync<string>("element => getComputedStyle(element).backgroundColor");

    // Hover styles settle on a frame boundary; two frames later they have been applied.
    private static Task NextFramesAsync(IPage page) =>
        page.EvaluateAsync("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))");
}
