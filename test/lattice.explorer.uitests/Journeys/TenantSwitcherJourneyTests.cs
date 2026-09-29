using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #3962: an operator who can reach several tenants switches tenant from the top
/// bar with the keyboard alone - from the palette's Switch tenant command, through the
/// type-ahead field, to the address re-rooted at the chosen tenant - and at a
/// cluster-wide address stays where they are while the tenant changes.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenantSwitcherJourneyTests : UiTestBase
{
    [Test]
    public async Task An_operator_switches_tenant_from_the_top_bar_by_keyboard()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/data"));
        var toggle = Toggle(page);
        await Expect(toggle).ToContainTextAsync("default");

        // The palette opens the switcher and hands it the focus.
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">Switch tenant");
        await Expect(Shell.Suggestions(page).Filter(new() { HasText = "Switch tenant" })).ToHaveCountAsync(1);
        await page.Keyboard.PressAsync("Enter");
        var field = Field(page);
        await Expect(field).ToBeFocusedAsync();
        await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "true");

        // Down lists every reachable tenant, the default one included, the active one first.
        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(field).ToHaveAttributeAsync("aria-expanded", "true");
        await Expect(Options(page)).ToHaveTextAsync(["default", "acme", "globex"], new() { UseInnerText = true });

        await page.Keyboard.PressAsync("ArrowDown");
        await Expect(page.Locator(".lt-shell-tenant__panel [role=option]").Nth(1)).ToHaveAttributeAsync("aria-selected", "true");
        await page.Keyboard.PressAsync("Enter");

        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/acme/data"));
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Scoped to tenant acme." })).ToHaveCountAsync(1);
        await Expect(toggle).ToContainTextAsync("acme");
        await Expect(toggle).ToBeFocusedAsync();
    }

    [Test]
    public async Task At_a_cluster_wide_address_the_operator_stays_and_only_the_tenant_changes()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/cluster", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        var toggle = Toggle(page);

        await toggle.FocusAsync();
        await page.Keyboard.PressAsync("Enter");
        await Expect(Field(page)).ToBeFocusedAsync();
        await Field(page).FillAsync("globex");
        await Expect(Options(page)).ToHaveTextAsync(["globex"], new() { UseInnerText = true });
        await page.Keyboard.PressAsync("ArrowDown");
        await page.Keyboard.PressAsync("Enter");

        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Scoped to tenant globex." })).ToHaveCountAsync(1);
        await Expect(toggle).ToContainTextAsync("globex");
        await Expect(page).ToHaveURLAsync(world.Head.Url("/cluster"));
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
    }

    [Test]
    public async Task The_shared_world_offers_no_switcher_to_an_operator_with_one_tenant()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");

        await Expect(Toggle(page)).ToHaveCountAsync(0);
    }

    internal static ILocator Toggle(IPage page) =>
        Shell.Banner(page).Locator("button[data-lt-command=\"tenant.switch\"]");

    internal static ILocator Field(IPage page) =>
        page.GetByRole(AriaRole.Combobox, new() { Name = "Switch to tenant", Exact = true });

    internal static ILocator Options(IPage page) =>
        page.Locator(".lt-shell-tenant__panel [role=option] .lt-combobox__value");
}
