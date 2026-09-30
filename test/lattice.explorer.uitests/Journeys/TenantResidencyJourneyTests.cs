using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4078: a tenant resident and Online in two regions, as the Explorer
/// sample seeds acme, has its residency narrowed to one of them from the Regions
/// section. The page shows both regions Online and serving it, previews the
/// change region by region, applies it from the primary button with only the
/// drain confirmation, and the tenant stays served where it is still Online - no
/// stop-serving dialog and no "served nowhere" warning at any step.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenantResidencyJourneyTests : UiTestBase
{
    private const string Tenant = "acme";
    private const string Peer = "peer-region";

    [Test]
    public async Task Narrowing_a_residency_to_a_region_that_is_online_keeps_the_tenant_served_with_no_stop_serving_dialog()
    {
        var world = await UiHosts.TenantWorldAsync();
        var serving = world.ServingRegion;
        await world.MakeResidentAndOnlineAsync(Tenant, serving, Peer);

        var page = await OpenAsync(world.Head, $"/t/{Tenant}/tenancy/regions", WorldIdentities.Admin);
        var table = page.GetByRole(AriaRole.Table, new() { Name = $"Regions of tenant {Tenant}" });
        await Expect(Row(table, serving)).ToContainTextAsync("Online");
        await Expect(Row(table, serving)).ToContainTextAsync("Served");
        await Expect(Row(table, Peer)).ToContainTextAsync("Online");
        await Expect(Row(table, Peer)).ToContainTextAsync("Served");
        await Expect(page.Locator(".lt-tenancy-warning")).ToHaveCountAsync(0);

        await page.GetByLabel($"Resident in {Peer}", new() { Exact = true }).UncheckAsync();

        // The table lists regions by id, and the world's serving region sorts first.
        await Expect(page.Locator(".lt-tenancy-preview__list li")).ToHaveTextAsync(
        [
            $"{serving} stays in the residency, and is still served there.",
            $"{Peer} starts draining, and stops being served there.",
        ]);
        await Expect(page.Locator(".lt-tenancy-served-nowhere")).ToHaveCountAsync(0);
        var apply = page.GetByRole(AriaRole.Button, new() { Name = "Apply residency", Exact = true });
        await Expect(apply).ToBeEnabledAsync();
        await apply.ClickAsync();

        var dialog = page.GetByRole(AriaRole.Alertdialog);
        await Expect(dialog).ToContainTextAsync("Remove regions from the residency?");
        await Expect(dialog).Not.ToContainTextAsync("Stop serving");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Drain and apply", Exact = true }).ClickAsync();

        await Expect(Shell.Toasts(page).Filter(new() { HasText = $"Tenant {Tenant} is draining {Peer}." })).ToHaveCountAsync(1);
        await Expect(Row(table, Peer)).ToContainTextAsync("Draining");
        await Expect(Row(table, serving)).ToContainTextAsync("Online");
        await Expect(Row(table, serving)).ToContainTextAsync("Served");
        await Expect(page.Locator(".lt-tenancy-warning")).ToHaveCountAsync(0);
        await Expect(page.GetByText("not served anywhere")).ToHaveCountAsync(0);
    }

    private static ILocator Row(ILocator table, string region) =>
        table.GetByRole(AriaRole.Row).Filter(new() { Has = table.Page.GetByRole(AriaRole.Rowheader, new() { Name = region, Exact = true }) });
}
