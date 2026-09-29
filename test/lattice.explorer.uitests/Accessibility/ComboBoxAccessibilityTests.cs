using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Issue #3949: the type-ahead picker with its listbox open is held to the same bar as
/// every page - the axe sweep in all eight appearances, the highlighted option still
/// marked under forced colours, and focus never trapped in the field.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ComboBoxAccessibilityTests : UiTestBase
{
    private const string PickerPage = "/cluster/orphans";

    [Test]
    public async Task An_open_picker_has_no_serious_violations_in_any_appearance()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, PickerPage, WorldIdentities.Admin);
        var tree = Tree(page);
        await Expect(tree).ToBeVisibleAsync();

        foreach (var appearance in ShellAppearanceChoice.All)
        {
            await Shell.SetAppearanceAsync(page, appearance);
            await OpenListAsync(page, tree);
            await AxeConformance.SweepAsync(page, $"an open tree picker on {PickerPage} in {appearance}");
        }
    }

    [Test]
    public async Task Forced_colours_keep_the_highlighted_option_marked()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, PickerPage, WorldIdentities.Admin, configure: options => options.ForcedColors = ForcedColors.Active);
        var tree = Tree(page);
        await Expect(tree).ToBeVisibleAsync();
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(forced-colors: active)').matches"), Is.True,
            "The premise failed: the browser does not report forced colours.");

        await OpenListAsync(page, tree);

        var highlighted = page.Locator(".lt-combobox__option[aria-selected=\"true\"]");
        await Expect(highlighted).ToHaveCountAsync(1);
        var outline = await highlighted.EvaluateAsync<string>("o => { const s = getComputedStyle(o); return s.outlineStyle + ' ' + s.outlineWidth; }");
        Assert.That(outline, Does.Not.StartWith("none").And.Not.EndWith(" 0px"),
            $"Under forced colours the highlighted suggestion is not marked ({outline}).");
        await AxeConformance.SweepAsync(page, "an open tree picker under forced colours");
    }

    [Test]
    public async Task Escape_closes_the_list_and_Tab_leaves_the_field()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, PickerPage, WorldIdentities.Admin);
        var tree = Tree(page);
        await Expect(tree).ToBeVisibleAsync();

        await OpenListAsync(page, tree);
        await page.Keyboard.PressAsync("Escape");
        await Expect(tree).ToHaveAttributeAsync("aria-expanded", "false");
        await Expect(tree).ToBeFocusedAsync();

        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(tree).ToHaveAttributeAsync("aria-expanded", "true");
        await page.Keyboard.PressAsync("Tab");
        await Expect(tree).Not.ToBeFocusedAsync();
        await Expect(tree).ToHaveAttributeAsync("aria-expanded", "false");
    }

    internal static ILocator Tree(IPage page) => page.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });

    internal static async Task OpenListAsync(IPage page, ILocator combobox)
    {
        await combobox.FocusAsync();
        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(combobox).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(page.Locator(".lt-combobox__option[aria-selected=\"true\"]")).ToHaveCountAsync(1);
        await Shell.WaitForMotionToSettleAsync(page);
    }
}
