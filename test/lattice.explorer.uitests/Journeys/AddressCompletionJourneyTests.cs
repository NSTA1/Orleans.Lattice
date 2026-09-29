using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The address line completes what the cluster holds: a partial address completes to the
/// trees behind it, choosing a completion goes there, and the command palette runs a
/// command by name.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AddressCompletionJourneyTests : UiTestBase
{
    [Test]
    public async Task A_partial_address_completes_to_a_tree_and_choosing_it_goes_there()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "data")).ToBeVisibleAsync();

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("/data/");
        var tree = Shell.Suggestions(page).Filter(new() { HasText = ExplorerWorld.DemoTree });
        await Expect(tree).ToHaveCountAsync(1);

        // Arrow to the tree and go.
        var options = await Shell.Suggestions(page).AllInnerTextsAsync();
        var index = options.ToList().FindIndex(option => option.Contains(ExplorerWorld.DemoTree, StringComparison.Ordinal));
        for (var i = 0; i <= index; i++)
        {
            await page.Keyboard.PressAsync("ArrowDown");
        }

        await Expect(tree).ToHaveAttributeAsync("aria-selected", "true");
        await page.Keyboard.PressAsync("Enter");
        await Expect(page).ToHaveURLAsync(world.Head.Url($"/t/default/data/{ExplorerWorld.DemoTree}"));
        await Expect(Shell.Content(page)).ToContainTextAsync("machine-000");
    }

    [Test]
    public async Task Commands_run_by_name_from_the_palette()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "cluster")).ToBeVisibleAsync();

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">Use the Board theme");
        var command = Shell.Suggestions(page).Filter(new() { HasText = "Use the Board theme" });
        await Expect(command).ToHaveCountAsync(1);
        await command.ClickAsync();
        await Expect(page.Locator("html")).ToHaveAttributeAsync("data-bs-theme", "dark");

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">Go to Cluster");
        await Shell.Suggestions(page).Filter(new() { HasText = "Go to Cluster" }).ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
    }
}
