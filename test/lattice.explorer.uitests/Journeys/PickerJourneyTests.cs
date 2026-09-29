using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #3949: a field that names an existing tree is a type-ahead picker, operable by
/// the keyboard alone - type, arrow to a suggestion, Enter to choose it, Enter again to
/// submit - and a name that matches nothing is refused before anything is sent. (The test
/// world serves neither replication control nor tenant region administration, so the
/// region pickers' keyboard journey is proven in bUnit, in TenancyPickerFieldsTests.)
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class PickerJourneyTests : UiTestBase
{
    [Test]
    public async Task A_tree_is_picked_and_submitted_by_the_keyboard_alone()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/cluster/orphans", WorldIdentities.Admin);
        var tree = page.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await Expect(tree).ToBeVisibleAsync();

        await tree.FocusAsync();
        await tree.PressSequentiallyAsync(ExplorerWorld.DemoTree[..4]);
        var option = Options(page).Filter(new() { HasText = ExplorerWorld.DemoTree });
        await Expect(option).ToHaveCountAsync(1);

        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(tree).ToHaveAttributeAsync("aria-activedescendant", new System.Text.RegularExpressions.Regex(".+"));
        await Expect(Options(page).First).ToHaveAttributeAsync("aria-selected", "true");
        await page.Keyboard.PressAsync("Enter");
        await Expect(tree).ToHaveValueAsync(ExplorerWorld.DemoTree);
        await Expect(tree).ToHaveAttributeAsync("aria-expanded", "false");

        await page.Keyboard.PressAsync("Enter");
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex($"/cluster/orphans\\?tree={ExplorerWorld.DemoTree}$"));
    }

    [Test]
    public async Task A_tree_that_does_not_exist_is_refused_in_place()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/cluster/orphans", WorldIdentities.Admin);
        var tree = page.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await Expect(tree).ToBeVisibleAsync();

        await tree.FillAsync("no-such-tree");
        await tree.PressAsync("Enter");

        await Expect(page.Locator(".lt-field__error")).ToContainTextAsync("No tree is named no-such-tree.");
        await Expect(tree).ToHaveAttributeAsync("aria-invalid", "true");
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("/cluster/orphans$"));
    }

    private static ILocator Options(IPage page) => page.Locator(".lt-combobox__list [role=option]");
}
