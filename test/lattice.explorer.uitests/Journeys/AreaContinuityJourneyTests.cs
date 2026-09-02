using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: move between areas and come back.</b> Switch area, select things, reload,
/// and land where you left off - with the <i>area</i> restored rather than reset to
/// Explore.
/// </summary>
/// <remarks>
/// Losing the area on a reload is the defect this covers, and it is the kind that only
/// a whole-shell test sees: the router restores correctly in isolation, the preference
/// store round-trips correctly in isolation, and the shell can still land on Explore
/// because nothing joined them at session entry. <c>ExplorerShellEntryPolicy</c> is the
/// single arbitrator; this is its observable consequence.
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class AreaContinuityJourneyTests : JourneyTestBase
{
    [Test]
    public async Task Switching_area_moves_the_surface_and_the_address_together()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.AssertActiveAreaAsync(page, "Explore");

        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        // The area's own content, not merely its heading: a rail that selects a tab
        // without swapping the region is the failure this catches.
        await Assertions
            .Expect(page.GetByRole(AriaRole.Heading, new PageGetByRoleOptions
            {
                Name = JourneyWorkbenchView.Heading,
                Exact = true,
            }))
            .ToBeVisibleAsync();

        await Assertions.Expect(page).ToHaveURLAsync(new Regex(@"/area/workbench$"));
    }

    [Test]
    public async Task The_active_area_survives_a_reload()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        await ReloadAsync(page);

        await JourneyShell.AssertActiveAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);
        await Assertions
            .Expect(page.GetByRole(AriaRole.Heading, new PageGetByRoleOptions
            {
                Name = JourneyWorkbenchView.Heading,
                Exact = true,
            }))
            .ToBeVisibleAsync();
    }

    [Test]
    public async Task A_selection_made_before_leaving_is_still_selected_on_return()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.CustomersTree);
        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);
        await JourneyShell.OpenAreaAsync(page, "Explore");

        // Coming back to Explore must find the same tree open, not an empty detail
        // panel the caller has to re-choose from.
        await Assertions
            .Expect(page.Locator(JourneyShell.SelectedCatalogRowSelector))
            .ToContainTextAsync(JourneyCatalogReader.CustomersTree);
        await Assertions.Expect(page.Locator(JourneyShell.DetailTabSelector).First).ToBeVisibleAsync();
    }

    [Test]
    public async Task Returning_from_a_bare_address_restores_the_remembered_area_rather_than_resetting_to_explore()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        // A new circuit entering at the bare address is the "come back tomorrow" case:
        // the address carries no state, so the remembered view is what decides.
        var next = await NewSessionAsync(page);

        await JourneyShell.AssertActiveAreaAsync(next, JourneyWorkbenchPlugin.AreaLabel);
    }
}

