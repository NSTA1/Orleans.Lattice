using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Every area's addresses work as links: a deep address opened cold, in a fresh browser,
/// renders its page with the area current in the directory, at its canonical address. A
/// hidden area's deep address is the not-found page, never a partial one.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class DeepLinkJourneyTests : UiTestBase
{
    [TestCaseSource(typeof(ExplorerAreas), nameof(ExplorerAreas.Keys))]
    public async Task A_deep_link_into_every_area_opens_cold(string areaKey)
    {
        var area = ExplorerAreas.Get(areaKey);
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, area.DeepPath, WorldIdentities.Admin);

        if (!area.ShownToAdmin)
        {
            await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
            await Expect(Shell.Stop(page, area.Key)).ToHaveCountAsync(0);
            return;
        }

        await Expect(Shell.Heading(page)).ToBeVisibleAsync();
        await Expect(Shell.Heading(page)).Not.ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        await Expect(Shell.Stop(page, area.Key)).ToHaveAttributeAsync("aria-current", "page");

        var canonical = (area.TenantScoped ? "/t/default" : string.Empty) + area.DeepPath;
        await Expect(page).ToHaveURLAsync(world.Head.Url(canonical));

        // The link survives a reload: it is an address, not state held in a circuit.
        await page.ReloadAsync();
        await Shell.WaitForInteractiveAsync(page);
        await Expect(page).ToHaveURLAsync(world.Head.Url(canonical));
        await Expect(Shell.Stop(page, area.Key)).ToHaveAttributeAsync("aria-current", "page");
    }
}
