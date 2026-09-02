using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: fail-closed restore.</b> An operator chose a tenant last time; the
/// entitlement has since been withdrawn. Coming back, they must land somewhere safe
/// <i>and be told why</i>, rather than silently finding themselves reading a different
/// tenant's data with no indication anything changed.
/// </summary>
/// <remarks>
/// Failing closed and failing silently are different things, and only the pair is
/// correct. Falling back without explanation is the worse outcome of the two for a
/// person: they carry on believing they are scoped where they left off. The resolver
/// re-validates a remembered tenant against the caller's live reachable list on
/// restore, abandons it when it no longer resolves, and publishes a notice the scope
/// control announces.
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class FailClosedRestoreJourneyTests : JourneyTestBase
{
    [Test]
    public async Task A_remembered_tenant_that_is_no_longer_reachable_falls_back_and_explains_itself()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        // Establish the remembered choice through the real control and prove it took,
        // so the restore below has something genuine to fail on.
        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        await WithdrawSecondTenantAsync();

        var next = await NewSessionAsync(page);

        // Safe: the caller is scoped to a tenant they can still reach, never left on
        // the withdrawn one and never left scoped to nothing.
        await Assertions
            .Expect(next.Locator(JourneyShell.QuietTenantSelector))
            .ToHaveTextAsync(JourneyWorld.AcmeTenant);

        // Explained: and the explanation is announced politely, in the scope control's
        // own live region, so it reaches a screen-reader user too.
        var notice = next.Locator(JourneyShell.TenantScopeStatusSelector);
        await Assertions.Expect(notice).ToContainTextAsync("could not restore");
        await Assertions.Expect(notice).ToContainTextAsync(JourneyWorld.AcmeTenant);
    }

    [Test]
    public async Task The_withdrawn_tenant_is_not_left_reachable_anywhere_in_the_scope_control()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        await WithdrawSecondTenantAsync();
        var next = await NewSessionAsync(page);

        // Precondition for the absence: the control rendered for this caller.
        await Assertions.Expect(next.Locator(JourneyShell.TenantScopeSelector)).ToBeVisibleAsync();

        await Assertions
            .Expect(next.Locator(JourneyShell.TenantScopeSelector))
            .Not.ToContainTextAsync(JourneyWorld.GlobexTenant);
    }

    [Test]
    public async Task The_listing_follows_the_fallback_rather_than_the_abandoned_scope()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        await WithdrawSecondTenantAsync();
        var next = await NewSessionAsync(page);

        // The catalog is the thing the caller actually reads, so a scope that was
        // abandoned in the control but still applied to the listing would be the worst
        // outcome of all. Assert on what is listed.
        await Assertions
            .Expect(next.Locator(JourneyShell.CatalogRowSelector).First)
            .ToContainTextAsync(JourneyCatalogReader.OrdersTree);

        await Assertions
            .Expect(next.Locator("#lx-shell-catalog"))
            .Not.ToContainTextAsync(JourneyCatalogReader.GlobexTree);
    }
}
