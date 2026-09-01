using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: sign in and orient.</b> Sign in as the platform operator, find a tree in
/// the catalog, open it, and land on <c>Data</c> - not on <c>Metrics</c>.
/// </summary>
/// <remarks>
/// The surface order is a deliberate epic decision, carried by two <c>Order</c>
/// literals (<c>Plugins/Data</c> 100 and <c>Plugins/Metrics</c> 300) that #1850 swapped
/// and every later issue was told not to revert. A unit test pins the literals; this
/// pins the consequence a person actually meets - that opening a tree puts its data in
/// front of them rather than a chart.
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class SignInAndOrientJourneyTests : JourneyTestBase
{
    /// <summary>The surface order a tree selection must resolve to, in strip order.</summary>
    private static readonly string[] ExpectedLeadingSurfaces = ["Data", "Topology", "Metrics", "Dead-letter"];

    [Test]
    public async Task Signing_in_then_opening_a_tree_lands_on_data_rather_than_metrics()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        var tabs = page.Locator(JourneyShell.DetailTabSelector);

        // Anti-vacuity first: an empty strip would satisfy every "Metrics is not
        // selected" phrasing below while proving the tree never opened.
        await Assertions.Expect(tabs.First).ToBeVisibleAsync();

        var labels = await tabs.AllTextContentsAsync();
        var trimmed = labels.Select(l => l.Trim()).ToArray();

        Assert.That(trimmed.Length, Is.GreaterThanOrEqualTo(ExpectedLeadingSurfaces.Length),
            $"The tree opened on only {trimmed.Length} surface(s) [{string.Join(", ", trimmed)}], so the "
            + "ordering this journey is about cannot be observed.");

        Assert.That(trimmed.Take(ExpectedLeadingSurfaces.Length), Is.EqualTo(ExpectedLeadingSurfaces),
            "Opening a tree must present Data, Topology, Metrics, Dead-letter in that order. "
            + $"The strip offered [{string.Join(", ", trimmed)}]. The order is carried by the Order "
            + "literals in Plugins/Data and Plugins/Metrics, which #1850 swapped deliberately.");

        // And the one the caller lands on is the first, not merely present.
        await Assertions
            .Expect(page.Locator(JourneyShell.DetailStripSelector + " [role=tab][aria-selected='true']"))
            .ToHaveTextAsync("Data");
    }

    [Test]
    public async Task The_surface_the_tree_opened_on_is_the_one_whose_panel_is_rendered()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        // A selected tab that controls a panel showing something else is the failure
        // mode a strip-only assertion cannot see. The shell names the panel's labelling
        // tab, so the two ends can be compared.
        var selectedTabId = await page
            .Locator(JourneyShell.DetailStripSelector + " [role=tab][aria-selected='true']")
            .GetAttributeAsync("id");

        Assert.That(selectedTabId, Is.Not.Null.And.Not.Empty,
            "The selected detail tab carries no element id, so the panel cannot name it and the "
            + "tab/panel relationship this journey checks does not exist.");

        await Assertions
            .Expect(page.Locator("#lx-shell-detail-panel"))
            .ToHaveAttributeAsync("aria-labelledby", selectedTabId!);
    }

    [Test]
    public async Task Choosing_a_different_tree_moves_the_detail_surface_with_it()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.CustomersTree);

        // The catalog must not leave two rows reading as selected, which is what a
        // panel bound to a stale selection looks like from the outside.
        await Assertions
            .Expect(page.Locator(JourneyShell.SelectedCatalogRowSelector))
            .ToHaveCountAsync(1);

        await Assertions
            .Expect(page.Locator(JourneyShell.SelectedCatalogRowSelector))
            .ToContainTextAsync(JourneyCatalogReader.CustomersTree);
    }
}
