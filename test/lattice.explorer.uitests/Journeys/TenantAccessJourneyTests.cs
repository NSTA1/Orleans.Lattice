using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4025: scoped to a tenant, the Access area lists only that tenant's rules.
/// The tenancy world seeds one rule on each tenant's own tree, a rule on a default
/// tenant tree and a cluster-wide rule; an operator switches tenant from the top bar,
/// opens Access from the directory, and sees only the chosen tenant's rule, with the
/// cluster-wide one named in a single quiet line.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class TenantAccessJourneyTests : UiTestBase
{
    [Test]
    public async Task After_switching_tenant_access_lists_only_that_tenants_rules()
    {
        var world = await UiHosts.TenantWorldAsync();
        var page = await OpenAsync(world.Head, "/data", WorldIdentities.Admin);
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/default/data"));

        // Switch to globex from the top bar.
        var toggle = TenantSwitcherJourneyTests.Toggle(page);
        await toggle.ClickAsync();
        await TenantSwitcherJourneyTests.Field(page).FillAsync("globex");
        await Expect(TenantSwitcherJourneyTests.Options(page)).ToHaveTextAsync(["globex"], new() { UseInnerText = true });
        await page.Keyboard.PressAsync("ArrowDown");
        await page.Keyboard.PressAsync("Enter");
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/globex/data"));
        await Expect(toggle).ToContainTextAsync("globex");

        // Open Access from the directory: it keeps the tenant.
        var access = Shell.Stop(page, "access");
        await Expect(access).ToHaveAttributeAsync("href", new System.Text.RegularExpressions.Regex("t/globex/access$"));
        await access.ClickAsync();
        await Expect(page).ToHaveURLAsync(world.Head.Url("/t/globex/access"));
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Access");

        var rules = Shell.Content(page).Locator("table tbody th a");
        await Expect(rules).ToHaveTextAsync([ExplorerWorld.TenantRuleId("globex")]);
        await Expect(Shell.Content(page).Locator(".lt-access-count")).ToHaveTextAsync("1 rules of tenant globex");
        await Expect(Shell.Content(page)).Not.ToContainTextAsync(ExplorerWorld.TenantRuleId("acme"));
        await Expect(Shell.Content(page)).Not.ToContainTextAsync("operators-read-factory-floor");
        await Expect(Shell.Content(page).Locator("[data-lt-cluster-wide-rules]")).ToContainTextAsync("1 cluster-wide rule also applies.");

        // The cluster-wide page is one link away, and still lists every rule.
        await Shell.Content(page).Locator("[data-lt-cluster-wide-rules] a").ClickAsync();
        await Expect(page).ToHaveURLAsync(world.Head.Url("/access/rules"));
        await Expect(Shell.Content(page)).ToContainTextAsync(ExplorerWorld.TenantRuleId("acme"));
        await Expect(Shell.Content(page)).ToContainTextAsync(ExplorerWorld.TenantRuleId("globex"));
    }
}
