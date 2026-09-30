using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Issue #3986: a multi-value picker is one control - its chosen values and its input
/// in one frame - held to the same bar as every page: the axe sweep in all eight
/// appearances, and the frame and its chips still drawn under forced colours.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class MultiComboBoxAccessibilityTests : UiTestBase
{
    private const string FieldName = "Allowed regions (optional)";

    [Test]
    public async Task A_multi_value_field_with_chosen_values_has_no_serious_violations_in_any_appearance()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/tenancy", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Tenancy");

        foreach (var appearance in ShellAppearanceChoice.All)
        {
            await Shell.SetAppearanceAsync(page, appearance);
            var field = await OpenWithChipsAsync(page);
            await AxeConformance.SweepAsync(page, $"a multi-value field with chosen values in {appearance}");
            await field.PressAsync("Escape");
            await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "New tenant" })).ToHaveCountAsync(0);
        }
    }

    [Test]
    public async Task The_chosen_values_and_the_input_share_one_frame_that_forced_colours_keep()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/tenancy", WorldIdentities.Admin, configure: options => options.ForcedColors = ForcedColors.Active);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Tenancy");
        Assert.That(await page.EvaluateAsync<bool>("() => matchMedia('(forced-colors: active)').matches"), Is.True,
            "The premise failed: the browser does not report forced colours.");

        var field = await OpenWithChipsAsync(page);

        var frame = page.Locator(".lt-combobox__control--tokens").Filter(new() { Has = page.Locator(".lt-combobox__chip") });
        await Expect(frame).ToHaveCountAsync(1);
        var outer = await frame.BoundingBoxAsync();
        var input = await field.BoundingBoxAsync();
        var chip = await frame.Locator(".lt-combobox__chip").First.BoundingBoxAsync();
        var border = await frame.EvaluateAsync<string>("f => { const s = getComputedStyle(f); return s.borderTopStyle + ' ' + s.borderTopWidth; }");

        Assert.Multiple(() =>
        {
            Assert.That(Inside(chip!, outer!) && Inside(input!, outer!), Is.True, "the chips and the input are drawn inside the one frame");
            Assert.That(border, Does.StartWith("solid").And.Not.EndWith(" 0px"), $"Under forced colours the frame is not drawn ({border}).");
        });
        await AxeConformance.SweepAsync(page, "a multi-value field with chosen values under forced colours");
    }

    [Test]
    public async Task Two_quick_Escapes_in_a_dialog_field_close_its_list_and_then_the_dialog()
    {
        // Issue #3986: Escape from a field inside a dialog is decided by the field on the
        // server, never by a render-time stop-propagation flag that could go stale between
        // two quick presses. This guards the outcome - list, then dialog, closed - with no
        // wait between the keys. The stale-flag race itself did not reproduce locally in a
        // dialog; it did in the tenant panel, which now closes on LtComboBox.OnDismiss.
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/tenancy", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Tenancy");
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New tenant" });

        for (var round = 0; round < 6; round++)
        {
            await page.GetByRole(AriaRole.Button, new() { Name = "New tenant", Exact = true }).ClickAsync();
            var field = page.GetByRole(AriaRole.Combobox, new() { Name = FieldName, Exact = true });
            await field.FocusAsync();
            await page.Keyboard.PressAsync("ArrowDown");
            await Expect(field).ToHaveAttributeAsync("aria-expanded", "true");

            await page.Keyboard.PressAsync("Escape");
            await page.Keyboard.PressAsync("Escape");

            await Expect(dialog).ToHaveCountAsync(0, new() { Timeout = 5000 });
        }
    }

    private static async Task<ILocator> OpenWithChipsAsync(IPage page)
    {
        await page.GetByRole(AriaRole.Button, new() { Name = "New tenant", Exact = true }).ClickAsync();
        var field = page.GetByRole(AriaRole.Combobox, new() { Name = FieldName, Exact = true });
        await Expect(field).ToBeVisibleAsync();

        // Choose the first listed region by keyboard: the world's region source lists
        // its own cluster, and a typed id it does not list would be refused.
        await field.FocusAsync();
        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(field).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(page.Locator(".lt-combobox__option[aria-selected=\"true\"]")).ToHaveCountAsync(1);
        await page.Keyboard.PressAsync("Enter");
        await Expect(page.Locator(".lt-combobox__chip")).ToHaveCountAsync(1);
        await Shell.WaitForMotionToSettleAsync(page);
        return field;
    }

    private static bool Inside(LocatorBoundingBoxResult inner, LocatorBoundingBoxResult outer) =>
        inner.X >= outer.X - 0.5
        && inner.Y >= outer.Y - 0.5
        && inner.X + inner.Width <= outer.X + outer.Width + 0.5
        && inner.Y + inner.Height <= outer.Y + outer.Height + 0.5;
}
