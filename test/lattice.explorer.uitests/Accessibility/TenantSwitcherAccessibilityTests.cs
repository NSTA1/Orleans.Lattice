using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Journeys;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Issue #3962: the top-bar tenant switcher, open with its list showing, is held to the
/// same bar as every page - the axe sweep in all eight appearances - and at phone width
/// it lives in the directory sheet.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenantSwitcherAccessibilityTests : UiTestBase
{
    [Test]
    public async Task An_open_tenant_switcher_has_no_serious_violations_in_any_appearance()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        var toggle = TenantSwitcherJourneyTests.Toggle(page);
        await Expect(toggle).ToBeVisibleAsync();

        foreach (var appearance in ShellAppearanceChoice.All)
        {
            await Shell.SetAppearanceAsync(page, appearance);
            await OpenSwitcherAsync(page, toggle);
            await AxeConformance.SweepAsync(page, $"an open tenant switcher in {appearance}");
            await page.Keyboard.PressAsync("Escape");
            await page.Keyboard.PressAsync("Escape");
            await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        }
    }

    [Test]
    public async Task Opening_the_switcher_lists_every_tenant_inside_its_panel_with_the_active_one_marked_under_forced_colours()
    {
        // Issue #3986: a dropdown, not an empty field - and its list stays inside the panel's border.
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin, configure: options => options.ForcedColors = ForcedColors.Active);
        var toggle = TenantSwitcherJourneyTests.Toggle(page);
        await Expect(toggle).ToBeVisibleAsync();
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(forced-colors: active)').matches"), Is.True,
            "The premise failed: the browser does not report forced colours.");

        await toggle.ClickAsync();

        await Expect(TenantSwitcherJourneyTests.Options(page)).ToHaveTextAsync(["default", "acme", "globex"], new() { UseInnerText = true });
        await Shell.WaitForMotionToSettleAsync(page);
        var panel = await page.Locator(".lt-shell-tenant__panel").BoundingBoxAsync();
        var list = await page.Locator(".lt-shell-tenant__panel [role=listbox]").BoundingBoxAsync();
        Assert.That(list!.Y + list.Height, Is.LessThanOrEqualTo(panel!.Y + panel.Height + 0.5), "The list overflows the panel's border.");
        Assert.That(list.X + list.Width, Is.LessThanOrEqualTo(panel.X + panel.Width + 0.5), "The list overflows the panel's border.");

        var active = page.Locator(".lt-shell-tenant__panel [role=option]").First.Locator(".lt-node--join");
        await Expect(active).ToHaveCountAsync(1);
        var mark = await active.EvaluateAsync<string>("n => getComputedStyle(n).backgroundColor");
        var canvas = await page.EvaluateAsync<string>("() => getComputedStyle(document.body).backgroundColor");
        Assert.That(mark, Is.Not.EqualTo(canvas).And.Not.EqualTo("rgba(0, 0, 0, 0)"), "Under forced colours the active tenant's node is not marked.");
        await AxeConformance.SweepAsync(page, "an open tenant switcher under forced colours");
    }

    [Test]
    public async Task At_phone_width_the_switcher_is_in_the_directory_sheet()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin, width: Shell.SmallWidth);
        await Expect(TenantSwitcherJourneyTests.Toggle(page)).ToHaveCountAsync(0);

        await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true }).ClickAsync();
        var sheet = page.GetByRole(AriaRole.Dialog, new() { Name = "Directory" });
        var field = sheet.GetByRole(AriaRole.Combobox, new() { Name = "Switch tenant", Exact = true });
        await Expect(field).ToBeVisibleAsync();

        await field.FocusAsync();
        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(sheet.Locator("[role=option]")).ToHaveCountAsync(3);
        await Shell.WaitForMotionToSettleAsync(page);
        await AxeConformance.SweepAsync(page, "the tenant switcher in the compact directory sheet");
        await Shell.AssertNoHorizontalPageScrollAsync(page, "the compact directory sheet with the tenant switcher");
    }

    private static async Task OpenSwitcherAsync(IPage page, ILocator toggle)
    {
        await toggle.FocusAsync();
        await page.Keyboard.PressAsync("Enter");
        var field = TenantSwitcherJourneyTests.Field(page);
        await Expect(field).ToBeFocusedAsync();
        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(field).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(page.Locator(".lt-shell-tenant__panel [role=option][aria-selected=\"true\"]")).ToHaveCountAsync(1);
        await Shell.WaitForMotionToSettleAsync(page);
    }
}
