using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: tenant scope.</b> An operator with more than one reachable tenant opens
/// the picker, chooses one, is told the change took, sees the catalog re-scope to it,
/// and finds the same tenant still active after a reload and in a new session.
/// </summary>
/// <remarks>
/// <para>
/// The catalog assertion is the load-bearing one. A picker whose value changes while
/// nothing downstream re-scopes is exactly the failure a per-component test cannot
/// see: the control is correct, the switcher is correct, and the product is still
/// wrong. The journey head's catalog is genuinely tenant-scoped - each demo tenant owns
/// differently-named trees - so "the catalog re-scoped" is an observable change in what
/// is listed rather than an inference from the control's own value.
/// </para>
/// <para>
/// "A new session" means a new circuit and a new server-side scope in the same browser
/// profile. A fresh browser profile would carry no remembered anything, so it would
/// prove the opposite of what it appears to.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class TenantScopeJourneyTests : JourneyTestBase
{
    [Test]
    public async Task An_operator_with_a_choice_of_tenants_is_offered_a_picker_listing_them()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        var picker = page.Locator(JourneyShell.TenantPickerSelector);
        await Assertions.Expect(picker).ToBeVisibleAsync();

        var options = await picker.Locator("option").AllTextContentsAsync();
        Assert.That(options.Select(o => o.Trim()),
            Is.EquivalentTo(new[] { JourneyWorld.AcmeTenant, JourneyWorld.GlobexTenant }),
            "The picker must offer exactly the tenants the accessible-tenant source reports, so the "
            + "control and the tenant administration list cannot diverge.");

        // The picker names itself for a screen reader rather than relying on position.
        await Assertions.Expect(picker).ToHaveAttributeAsync("aria-describedby", new Regex(".+"));
    }

    [Test]
    public async Task Switching_tenant_is_confirmed_and_re_scopes_the_catalog()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        // Prove the starting scope genuinely rendered before changing it, so a failure
        // below cannot be a page that never reached the first tenant.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.AcmeTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.OrdersTree);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);

        // Confirmed: the change is announced in the control's own polite live region,
        // not left for the user to infer from the drop-down's value.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantScopeStatusSelector))
            .ToContainTextAsync(JourneyWorld.GlobexTenant);

        // Re-scoped: what the catalog lists actually changed.
        await Assertions
            .Expect(page.Locator(JourneyShell.CatalogRowSelector))
            .ToHaveCountAsync(1);
        await Assertions
            .Expect(page.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.GlobexTree);
    }

    [Test]
    public async Task The_chosen_tenant_survives_a_reload()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        await ReloadAsync(page);

        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.GlobexTree);
    }

    [Test]
    public async Task The_chosen_tenant_survives_into_a_new_session()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        var next = await NewSessionAsync(page);

        await Assertions
            .Expect(next.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(next.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.GlobexTree);
    }
}

