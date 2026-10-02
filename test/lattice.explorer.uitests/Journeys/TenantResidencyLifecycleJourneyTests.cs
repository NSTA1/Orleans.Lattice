using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4114: a region removed from a tenant's residency is followed live. The
/// tenant is resident and Online in this world's serving region and a peer;
/// removing the serving region starts it Draining, and the drain completion
/// listener on this region's own silo steps it to Offline and Removed. The
/// Regions page follows those steps on its own - no Refresh - announces the
/// stage change, and once every region is steady it shows no step and stops
/// following. The tenant's residency is restored afterwards, so the shared
/// tenancy world is left as other journeys expect it.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenantResidencyLifecycleJourneyTests : UiTestBase
{
    private const string Tenant = "acme";
    private const string Peer = "peer-region";

    [Test]
    public async Task Removing_a_region_is_followed_to_removed_without_a_refresh()
    {
        var world = await UiHosts.TenantWorldAsync();
        var serving = world.ServingRegion;
        await world.MakeResidentAndOnlineAsync(Tenant, serving, Peer);
        try
        {
            var page = await OpenAsync(world.Head, $"/t/{Tenant}/tenancy/regions", WorldIdentities.Admin);
            var table = page.GetByRole(AriaRole.Table, new() { Name = $"Regions of tenant {Tenant}" });
            await Expect(Row(table, serving)).ToContainTextAsync("Online");
            await Expect(table.GetByRole(AriaRole.Progressbar)).ToHaveCountAsync(0);

            await page.GetByLabel($"Resident in {serving}", new() { Exact = true }).UncheckAsync();
            await page.GetByRole(AriaRole.Button, new() { Name = "Apply residency", Exact = true }).ClickAsync();
            var dialog = page.GetByRole(AriaRole.Alertdialog);
            await Expect(dialog).ToContainTextAsync("Remove regions from the residency?");
            await dialog.GetByRole(AriaRole.Button, new() { Name = "Drain and apply", Exact = true }).ClickAsync();
            await Expect(Shell.Toasts(page).Filter(new() { HasText = $"Tenant {Tenant} is draining {serving}." })).ToHaveCountAsync(1);

            // The region's own silo completes the drain; the page reads it again on
            // its own clock. No Refresh is pressed anywhere in this journey.
            var settled = new LocatorAssertionsToContainTextOptions { Timeout = 30_000 };
            await Expect(Row(table, serving)).ToContainTextAsync("Removed", settled);
            await Expect(page.Locator(".lt-tenancy-section > [aria-live=polite]"))
                .ToContainTextAsync($"Region {serving} of tenant {Tenant} is now Removed.", settled);
            await Expect(Row(table, serving).GetByRole(AriaRole.Progressbar)).ToHaveCountAsync(0);
            await Expect(page.Locator(".lt-tenancy-following")).ToHaveCountAsync(0);
            await Expect(Row(table, Peer)).ToContainTextAsync("Online");
            await Expect(Row(table, Peer)).ToContainTextAsync("Served");
        }
        finally
        {
            await world.MakeResidentAndOnlineAsync(Tenant, serving, Peer);
        }
    }

    private static ILocator Row(ILocator table, string region) =>
        table.GetByRole(AriaRole.Row).Filter(new() { Has = table.Page.GetByRole(AriaRole.Rowheader, new() { Name = region, Exact = true }) });
}
