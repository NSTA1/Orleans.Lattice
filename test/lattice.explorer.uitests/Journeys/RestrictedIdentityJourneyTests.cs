using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: restricted identity.</b> A reader who holds none of the administrative
/// grants must be able to see that those areas exist and what to do about them - not
/// find them silently missing, and not be invited into a surface that will refuse.
/// </summary>
/// <remarks>
/// <para>
/// The epic's approved policy is that a refusal is <i>visible but demoted</i>, below a
/// divider, with a stated remedy - never merely greyed out and never simply gone. Grey
/// tells a person nothing; absence tells them the feature does not exist and sends them
/// to support asking why. Demotion with a remedy tells them what to ask for and who to
/// ask.
/// </para>
/// <para>
/// The remedy is the gate's, not the surface's: it names the missing permission and the
/// audience who issues it, rather than repeating the area label the caller can already
/// read. That is asserted here on the one area whose gate can report facts without a
/// cluster - see <see cref="JourneyLedgerGate"/> for why the shipped areas cannot.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class RestrictedIdentityJourneyTests : JourneyTestBase
{
    [Test]
    public async Task A_refused_area_is_demoted_below_a_divider_rather_than_hidden()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);
        await JourneyShell.AssertRailSettledAsync(page);

        // Visible: it is in the rail, in the demoted group.
        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(1);

        // Demoted: the group is announced as a group and is preceded by a separator, so
        // the demotion is structural rather than a visual convention a screen-reader
        // user cannot perceive.
        await Assertions.Expect(page.Locator(JourneyShell.DemotedDividerSelector)).ToBeVisibleAsync();
        await Assertions
            .Expect(page.Locator(JourneyShell.DemotedGroupSelector))
            .ToHaveAttributeAsync("aria-label", new Regex(".+"));
    }

    [Test]
    public async Task A_refused_area_states_the_permission_and_the_audience_rather_than_its_own_name()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);
        await JourneyShell.AssertRailSettledAsync(page);

        var entry = JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel);
        await Assertions.Expect(entry).ToHaveCountAsync(1);

        // The disclosure is a real focusable trigger, not a hover tooltip, so a keyboard
        // or screen-reader user can reach the remedy at all.
        var trigger = entry.Locator("button.lx-help-trigger");
        await Assertions.Expect(trigger).ToBeVisibleAsync();
        await trigger.ClickAsync();
        await Assertions.Expect(trigger).ToHaveAttributeAsync("aria-expanded", "true");

        // And the remedy names the grant and who issues it. A remedy composed from the
        // area label would say "ask for access to Ledger", which tells the caller only
        // what they just clicked.
        await Assertions.Expect(entry).ToContainTextAsync(JourneyLedgerGate.Permission);
        await Assertions.Expect(entry).ToContainTextAsync(JourneyLedgerGate.Audience);
    }

    [Test]
    public async Task A_refused_area_is_not_offered_as_something_to_open()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);
        await JourneyShell.AssertRailSettledAsync(page);

        // Precondition: the area is genuinely present and refused, so the absence below
        // is about how it is offered rather than about it having vanished.
        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(1);

        // Never invited in: it is not a tab, so there is nothing to activate and no
        // disabled control to puzzle over. A disabled tab would also remove the rail's
        // roving-tabindex owner if every tab were disabled.
        await Assertions
            .Expect(page.GetByRole(AriaRole.Tab, new PageGetByRoleOptions
            {
                Name = JourneyLedgerPlugin.AreaLabel,
                Exact = true,
            }))
            .ToHaveCountAsync(0);
    }

    [Test]
    public async Task The_same_area_opens_for_an_identity_that_holds_the_grant()
    {
        // The converse, and it is what makes the three cases above assertions about the
        // gate rather than about an area that is simply always refused.
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(0);

        await JourneyShell.OpenAreaAsync(page, JourneyLedgerPlugin.AreaLabel);
        await Assertions
            .Expect(page.GetByRole(AriaRole.Heading, new PageGetByRoleOptions
            {
                Name = JourneyLedgerView.Heading,
                Exact = true,
            }))
            .ToBeVisibleAsync();
    }

    [Test]
    public async Task A_reader_can_still_do_the_work_the_product_is_for()
    {
        // A restricted identity that can see nothing is not "correctly gated", it is
        // broken. The reader must still reach the catalog and open a tree.
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);

        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);
        await Assertions
            .Expect(page.Locator(JourneyShell.DetailStripSelector + " [role=tab][aria-selected='true']"))
            .ToHaveTextAsync("Data");
    }
}

