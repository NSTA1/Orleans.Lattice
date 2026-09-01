using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: deep link and share.</b> Copy the address of what you are looking at,
/// open it fresh, and arrive at that exact view; Back and Forward then behave.
/// </summary>
/// <remarks>
/// <para>
/// The area half of this journey works and is pinned below. <b>The selection half does
/// not, and the case that measures it is expected red.</b> See
/// <see cref="The_address_of_an_opened_tree_reproduces_that_tree_when_opened_fresh"/>
/// for the evidence and the seam; it is a genuine gap in the composed product, not a
/// limitation of this harness, and it is left failing deliberately rather than
/// weakened into an assertion that cannot fail.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class DeepLinkJourneyTests : JourneyTestBase
{
    [Test]
    public async Task An_area_address_opens_that_area_when_entered_fresh()
    {
        var page = await OpenAtAsync("area/workbench", ExpandedWidth);

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
    public async Task Back_and_forward_walk_the_areas_that_were_visited()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.AssertActiveAreaAsync(page, "Explore");

        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        await page.GoBackAsync();
        await JourneyShell.AssertActiveAreaAsync(page, "Explore");

        await page.GoForwardAsync();
        await JourneyShell.AssertActiveAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);
    }

    /// <summary>
    /// <b>Currently red, and deliberately so.</b> Opening a tree does not put it in the
    /// address, and an address naming a tree does not open it, so a specific view
    /// cannot be shared or bookmarked.
    /// <para>
    /// <b>Evidence.</b> Selecting a catalog row leaves the address at <c>/</c>, and
    /// entering <c>/explore/trees/{id}</c> fresh renders the detail panel's "Nothing
    /// selected" state. The mechanism is that nothing joins the two halves the epic
    /// built: the route grammar carries <c>/explore/{kind}/{id}/{surface}</c> and the
    /// preference contract declares <c>shell.selection</c> and <c>shell.surface</c>, but
    /// <c>ExplorerRoute.WithSelection</c> is called from exactly one place in the whole
    /// product - <c>ExplorerShellPreferences</c> reading its own remembered value back -
    /// and neither <c>UI/Navigation/NavigationPanel.razor</c> nor
    /// <c>UI/Detail/DetailPanel.razor</c> resolves <c>IExplorerShellRouter</c> at all.
    /// Both still hold their state in the older ad hoc <c>IUiPreferenceStore</c> keys
    /// (<c>nav-selected</c>, <c>detail-plugin</c>), which is why a selection survives a
    /// reload but never reaches the address.
    /// </para>
    /// <para>
    /// #1847's handoff flagged moving those writes onto the shell-state contract as an
    /// open item for "P1/P2", but those issues own <c>Plugins/**</c> while the two files
    /// are shell-owned <c>UI/**</c>, so no issue in the epic had the lane to do it.
    /// </para>
    /// <para>
    /// It fails against pre-epic code too, for the stronger reason that the route
    /// grammar it depends on did not exist.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_address_of_an_opened_tree_reproduces_that_tree_when_opened_fresh()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        // Step one: what a person would copy out of the address bar must name the tree
        // they are looking at.
        var shared = new Uri(page.Url).PathAndQuery;
        Assert.That(shared, Does.Contain(JourneyCatalogReader.OrdersTree),
            "Opening a tree left the address at '" + shared + "', which names no selection, so there "
            + "is nothing for a person to copy and share. ExplorerRoute.WithSelection is never called "
            + "by the shell: NavigationPanel and DetailPanel hold the selection in ad hoc "
            + "IUiPreferenceStore keys and resolve no IExplorerShellRouter.");

        // Step two: and pasting it into a fresh session must arrive at that same view.
        var fresh = await OpenAtAsync(shared.TrimStart('/'), ExpandedWidth);
        await ExplorerShell.SignInAsync(fresh, JourneyWorld.PlatformAdmin);

        await Assertions
            .Expect(fresh.Locator(JourneyShell.SelectedCatalogRowSelector))
            .ToContainTextAsync(JourneyCatalogReader.OrdersTree);
        await Assertions.Expect(fresh.Locator(JourneyShell.DetailTabSelector).First).ToBeVisibleAsync();
    }
}

