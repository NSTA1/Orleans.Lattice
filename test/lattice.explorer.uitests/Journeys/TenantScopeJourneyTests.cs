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
    public async Task Switching_tenant_is_confirmed_in_a_polite_live_region()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        // Prove the starting scope genuinely rendered before changing it, so a failure
        // below cannot be a page that never reached the first tenant.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.AcmeTenant);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);

        // Confirmed: the change is announced in the control's own polite live region,
        // not left for the user to infer from the drop-down's value.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantScopeStatusSelector))
            .ToContainTextAsync(JourneyWorld.GlobexTenant);
    }

    /// <summary>
    /// <b>Currently red, and deliberately so.</b> Switching the active tenant does not
    /// re-scope the catalog in place; the listing only follows on the next page load.
    /// <para>
    /// <b>Evidence.</b> After the switch the picker reads the new tenant and the change
    /// is announced (the sibling case above passes), but the catalog still lists the
    /// previous tenant's trees. Reloading the same page then lists the new tenant's -
    /// so the scope genuinely applied at the reader, and it is only the already-rendered
    /// listing that was never asked to reload. <c>NavigationPanel</c> subscribes to a
    /// selection change but to no tenant-scope change.
    /// </para>
    /// <para>
    /// This matters more than a stale pane usually would, because the caller has just
    /// been told the scope changed. Being told the view moved while the view has not is
    /// worse than not being told at all: the rows on screen are now attributed to the
    /// wrong tenant.
    /// </para>
    /// <para>
    /// It fails against pre-epic code too, where there was no tenant picker to switch
    /// with at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task Switching_tenant_re_scopes_the_catalog()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await Assertions
            .Expect(page.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.OrdersTree);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        // Re-scoped: what the catalog lists actually changed, without a reload.
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

