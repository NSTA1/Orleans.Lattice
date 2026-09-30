using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Moving between areas from the reserved default tenant, as an operator does: every
/// spine stop lands on its own page, every time. A page that reads its address - a
/// tree page naming its tree - used to be handed the next page's address as the
/// navigation left it, and declared that address not found, so a click on Access from
/// a tree page rendered "Nothing lives at this address" at <c>/access</c> while the
/// spine showed Access as the current, visible stop.
/// </summary>
/// <remarks>
/// Every step waits on what the page shows, never on time: a heading that is still
/// loading is waited for, and the not-found page, which would stay, fails the step.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AreaSwitchJourneyTests : UiTestBase
{
    private const int Rounds = 5;

    [Test]
    public async Task An_operator_moving_from_the_default_tenant_to_access_always_lands_on_access()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Admin);
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default"));

        for (var round = 0; round < Rounds; round++)
        {
            // From a tree page, which reads its tree from its address.
            await Shell.Stop(page, "schema").ClickAsync();
            await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/schema"));
            await Shell.Content(page).Locator("a[data-lt-command=\"schema.all-trees\"]").ClickAsync();
            await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/schema?show=all"));
            await Shell.Content(page).Locator($"a[href$=\"schema/{ExplorerWorld.DemoTree}\"]").First.ClickAsync();
            await Expect(page).ToHaveURLAsync(world.Head.Url($"/t/default/schema/{ExplorerWorld.DemoTree}"));
            await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerWorld.DemoTree);

            await LandsOnAsync("access", "/t/default/access", "Access");

            // From Home, the tenant root.
            await Shell.Stop(page, "home").ClickAsync();
            await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default"));
            await LandsOnAsync("access", "/t/default/access", "Access");

            // From a tenant-rooted area page to the Cluster area, which keeps the tenant (#4025).
            await LandsOnAsync("data", "/t/default/data", "Data");
            await LandsOnAsync("cluster", "/t/default/cluster", "Cluster");
        }

        async Task LandsOnAsync(string area, string path, string heading)
        {
            await Shell.Stop(page, area).ClickAsync();
            await Expect(page).ToHaveURLAsync(world.Head.Url(path));
            await Expect(Shell.Heading(page)).ToHaveTextAsync(heading);
            await Expect(Shell.Stop(page, area)).ToHaveAttributeAsync("aria-current", "page");
        }
    }
}
