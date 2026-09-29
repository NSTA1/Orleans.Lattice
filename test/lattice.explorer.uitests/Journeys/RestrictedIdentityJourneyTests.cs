using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A user with narrow rights: the directory shows only the areas they may use, the
/// command palette never offers a way into a hidden area, and a hidden area's address -
/// typed or followed - is a page that does not exist.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class RestrictedIdentityJourneyTests : UiTestBase
{
    private static readonly string[] HiddenFromAlice = ["access", "schema", "tenancy", "telemetry", "cluster"];

    [Test]
    public async Task A_restricted_user_sees_only_their_stops_and_cannot_reach_a_hidden_area()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", WorldIdentities.Alice);
        await Expect(Shell.Stop(page, "data")).ToBeVisibleAsync();

        var stops = await Shell.Directory(page).Locator("a[data-lt-command]").EvaluateAllAsync<string[]>("links => links.map(l => l.getAttribute('data-lt-command'))");
        Assert.That(stops, Is.EqualTo(new[] { "go.home", "go.data", "go.apps", "go.replication", "go.backups" }),
            "The directory shows a restricted user exactly the areas they may use, in directory order.");

        // The palette offers nothing in a hidden area.
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">");
        await Expect(Shell.Suggestions(page).Filter(new() { HasText = "Go to Data" })).ToHaveCountAsync(1);
        var commands = await Shell.Suggestions(page).AllInnerTextsAsync();
        foreach (var hidden in new[] { "Access", "Schema", "Tenancy", "Telemetry", "Cluster", "access rule", "Create a group", "Reshard", "compliance", "WAL" })
        {
            Assert.That(commands, Has.None.Contains(hidden), $"The palette offers a restricted user '{hidden}'.");
        }

        // Nor does a typed command for one.
        await Shell.AddressInput(page).FillAsync(">Go to Access");
        await Expect(Shell.Suggestions(page)).ToHaveCountAsync(0);

        // A typed address for a hidden area goes nowhere useful.
        await Shell.AddressInput(page).FillAsync("/access");
        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);

        // And every hidden area's address, followed directly, is the not-found page.
        foreach (var key in HiddenFromAlice)
        {
            var area = ExplorerAreas.Get(key);
            foreach (var path in new[] { area.PrimaryPath, area.DeepPath })
            {
                await Shell.GotoAsync(page, world.Head, path);
                await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
                await Expect(Shell.Stop(page, key)).ToHaveCountAsync(0);
            }
        }
    }
}
