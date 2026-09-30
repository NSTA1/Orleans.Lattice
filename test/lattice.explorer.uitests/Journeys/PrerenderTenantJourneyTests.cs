using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #3999: the server prerender cannot read the tenant an operator last held - it is
/// remembered in browser storage - so at an address that names no tenant it renders a
/// neutral state rather than the default tenant's view, and the live circuit then
/// restores the remembered tenant.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed partial class PrerenderTenantJourneyTests : UiTestBase
{
    private const string Pending = "Resolving your tenant";

    [Test]
    public async Task A_remembered_tenant_is_never_prerendered_as_the_default_and_is_restored_once_interactive()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/cluster", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        var toggle = TenantSwitcherJourneyTests.Toggle(page);

        // Switch to globex at a cluster-wide address; the switch is remembered.
        await toggle.ClickAsync();
        await TenantSwitcherJourneyTests.Field(page).FillAsync("globex");
        await Expect(TenantSwitcherJourneyTests.Options(page)).ToHaveTextAsync(["globex"], new() { UseInnerText = true });
        await page.Keyboard.PressAsync("ArrowDown");
        await page.Keyboard.PressAsync("Enter");
        await Expect(toggle).ToContainTextAsync("globex");

        // The server's prerender of a tree that exists only in the default tenant, as the
        // browser receives it before any circuit starts: signed in, but with no way to
        // read the remembered tenant.
        var response = await page.APIRequest.GetAsync(world.Head.Url($"/cluster/trees/{ExplorerWorld.DemoTree}"));
        Assert.That(response.Ok, Is.True);
        var main = MainLandmark().Match(await response.TextAsync());
        Assert.That(main.Success, Is.True, "the prerender carries the main landmark");
        Assert.Multiple(() =>
        {
            Assert.That(main.Value, Does.Contain(Pending), "the prerender shows the neutral state");
            Assert.That(main.Value, Does.Not.Contain(ExplorerWorld.DemoTree), "nothing is rendered under the default tenant");
        });

        // Once interactive, the remembered tenant is restored rather than the default.
        await Shell.GotoAsync(page, world.Head, "/cluster");
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Cluster");
        await Expect(toggle).ToContainTextAsync("globex");
        await Expect(Shell.Content(page)).Not.ToContainTextAsync(Pending);
    }

    [GeneratedRegex("<main[^>]*id=\"lt-shell-content\"[^>]*>.*?</main>", RegexOptions.Singleline)]
    private static partial Regex MainLandmark();
}
