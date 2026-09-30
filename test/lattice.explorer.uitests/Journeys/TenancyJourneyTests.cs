using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Tenancy chrome on and off, and re-rooting. A platform operator sees tenancy even in
/// the reserved default tenant: every tenant-scoped address is rooted at
/// <c>/t/default</c>, an unrooted one is re-rooted, and the address line completes
/// tenants. A caller scoped to the default tenant who is not an operator sees no tenancy
/// at all: a rooted address is re-rooted to the plain one, and there is no tenant to
/// complete. Cluster-wide areas are never rooted.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenancyJourneyTests : UiTestBase
{
    [Test]
    public async Task Tenancy_is_on_for_an_operator_and_every_tenant_scoped_address_is_re_rooted()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "cluster")).ToBeVisibleAsync();

        foreach (var area in ExplorerAreas.Shown)
        {
            await Shell.GotoAsync(page, world.Head, area.PrimaryPath);
            await Expect(Shell.Heading(page)).ToHaveTextAsync(area.DisplayName);
            var expected = area.TenantScoped ? "/t/default" + area.PrimaryPath : area.PrimaryPath;
            await Expect(page).ToHaveURLAsync(world.Head.Url(expected));
        }

        // The address line shows the tenant, and completes it.
        await Shell.GotoAsync(page, world.Head, "/data");
        await Expect(page.GetByRole(AriaRole.Navigation, new() { Name = "Address" })).ToContainTextAsync("t/default");
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("t/");
        await Expect(Shell.Suggestions(page).Filter(new() { HasText = "t/default" })).ToHaveCountAsync(1);
    }

    [Test]
    public async Task Tenancy_is_off_for_a_default_tenant_user_and_a_rooted_address_is_re_rooted()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/t/default/data", WorldIdentities.Alice);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
        await Expect(page).ToHaveURLAsync(world.Head.Url("/data"));
        await Expect(page.GetByRole(AriaRole.Navigation, new() { Name = "Address" })).Not.ToContainTextAsync("t/default");

        await Shell.GotoAsync(page, world.Head, $"/t/default/data/{ExplorerWorld.DemoTree}");
        await Expect(page).ToHaveURLAsync(world.Head.Url($"/data/{ExplorerWorld.DemoTree}"));

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("t/");
        await Expect(Shell.Suggestions(page)).ToHaveCountAsync(0);
    }
}
