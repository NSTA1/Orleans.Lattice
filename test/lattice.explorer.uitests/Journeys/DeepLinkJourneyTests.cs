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

        // Record every main-frame navigation. Reading the address after Back cannot
        // tell the two failure modes apart - Back never moving, versus Back moving to
        // the bare surface and the shell restoring the remembered area over it within
        // one circuit round trip - because both read back as the area address. The
        // navigation sequence distinguishes them: a restore appears as an extra entry.
        var navigations = new List<string>();
        page.FrameNavigated += (_, frame) =>
        {
            if (frame == page.MainFrame)
            {
                navigations.Add(new Uri(frame.Url).PathAndQuery);
            }
        };

        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.AssertActiveAreaAsync(page, "Explore");

        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);
        var beforeBack = navigations.Count;

        await page.GoBackAsync();
        var landed = new Uri(page.Url).PathAndQuery;

        try
        {
            await JourneyShell.AssertActiveAreaAsync(page, "Explore");
        }
        catch (Exception ex)
        {
            var after = navigations.Skip(beforeBack).ToArray();
            Assert.Fail(
                "Going back from the workbench area did not return to Explore. The address "
                + $"immediately after Back was '{landed}'. Navigations since Back: "
                + (after.Length == 0
                    ? "<none, so Back never moved the address>"
                    : "[" + string.Join(" -> ", after) + "]")
                + Environment.NewLine + "Whole navigation history: ["
                + string.Join(" -> ", navigations) + "]"
                + Environment.NewLine + ex.Message);
        }

        await page.GoForwardAsync();
        await JourneyShell.AssertActiveAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);
    }

    /// <summary>
    /// Copying the address of an opened tree and entering it fresh must arrive at that
    /// same view: the tree selected and its detail surface resolved.
    /// <para>
    /// This is the epic's share-a-link promise end to end, and it is the case that
    /// exposed the selection round trip. Producing the address and consuming it are
    /// separate mechanisms - the shell can write a correct URL and still discard the
    /// intent when the address is entered cold - so both halves are asserted, in order,
    /// with the failure naming which one held.
    /// </para>
    /// <para>
    /// <b>The identity is established before the link is opened, deliberately.</b>
    /// Signing in is a real form POST that redirects home, so signing in <i>after</i>
    /// arriving would discard the very address under test and measure nothing. A person
    /// sharing a link is already signed in, or signs in and then follows it; both land
    /// here.
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
            $"Opening a tree left the address at '{shared}', which names no selection, so there is "
            + "nothing for a person to copy and share.");

        // Step two: entering that address cold must arrive at the same view. The new
        // page is opened in the SAME browser context, so it carries the credential
        // already established rather than needing a sign-in that would redirect away
        // from the address being tested.
        var fresh = await page.Context.NewPageAsync();
        await fresh.GotoAsync(new Uri(BaseUri, shared.TrimStart('/')).ToString());
        await ExplorerShell.WaitForShellReadyAsync(fresh);
        await ExplorerShell.AssertShellRenderedAsync(fresh);
        await ExplorerShell.AssertSignedInAsync(fresh, JourneyWorld.PlatformAdmin);

        // The address must survive the trip: a shell that silently rewrote it back to
        // the bare surface would fail the assertions below for the wrong reason.
        Assert.That(new Uri(fresh.Url).PathAndQuery, Does.Contain(JourneyCatalogReader.OrdersTree),
            "The shared address was rewritten on arrival, so the link no longer names the tree it "
            + "was copied for. Observed: " + await JourneyShell.DescribeCatalogStateAsync(fresh));

        // The surface first, then the catalog's marking, so a partial adoption - the
        // address and the detail panel agreeing while the catalog still shows nothing
        // selected - is distinguishable from the tree never opening at all.
        try
        {
            await Assertions.Expect(fresh.Locator(JourneyShell.DetailTabSelector).First).ToBeVisibleAsync();
        }
        catch (PlaywrightException ex)
        {
            Assert.Fail(
                "The shared address was reproduced, but no detail surface resolved for it, so the "
                + "link lands on an empty panel."
                + Environment.NewLine + await JourneyShell.DescribeCatalogStateAsync(fresh)
                + Environment.NewLine + ex.Message);
        }

        try
        {
            await Assertions
                .Expect(fresh.Locator(JourneyShell.SelectedCatalogRowSelector))
                .ToContainTextAsync(JourneyCatalogReader.OrdersTree);
        }
        catch (PlaywrightException ex)
        {
            Assert.Fail(
                "The address was reproduced and the detail panel opened, but the catalog does not "
                + $"mark '{JourneyCatalogReader.OrdersTree}' as selected, so the shared link lands on "
                + "the surface without showing what it is a surface of."
                + Environment.NewLine + await JourneyShell.DescribeCatalogStateAsync(fresh)
                + Environment.NewLine + ex.Message);
        }
    }
}

