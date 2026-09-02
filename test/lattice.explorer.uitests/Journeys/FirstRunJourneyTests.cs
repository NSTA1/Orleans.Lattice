using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: first run, unauthenticated.</b> Someone lands on the Explorer for the
/// first time with no credential. They must be able to tell what the product is, find
/// the way in, and - the part this epic changed - be told that a refused area needs a
/// <i>sign-in</i>, rather than that it "is not available for your account".
/// </summary>
/// <remarks>
/// <para>
/// The distinction is the whole point of #1854's four-state contract: an anonymous
/// caller and an authenticated-but-unauthorized one get the same refusal from a
/// server, so a gate that maps both onto "denied" tells a signed-out visitor their
/// account lacks a grant they were never asked to present. The composed shell must
/// keep an <c>AuthenticationRequired</c> area prominent and clickable - an invitation -
/// and must open the sign-in when it is taken.
/// </para>
/// <para>
/// <b>Measured through the journey head's ledger area.</b> Every area the product
/// ships probes a cluster; with none, each probe throws and
/// <c>ExplorerPluginAccessRefresher</c> contains a faulting gate at <c>Deny</c> by
/// design, so in any cluster-free head every shipped area reads Denied whether or not
/// anyone is signed in. The ledger area's gate derives from the real
/// <c>ExplorerPluginAccessGate</c> and reports facts that cannot throw, so the
/// contract, the visibility policy and the rail rendering under test are all shipped
/// code. See <see cref="JourneyLedgerGate"/>.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class FirstRunJourneyTests : JourneyTestBase
{
    [Test]
    public async Task A_first_visit_names_the_product_and_offers_a_way_in()
    {
        var page = await OpenAtAsync("", ExpandedWidth);

        // What is this? The surface names itself in the one h1 the shell owns, and the
        // rail names the areas - so the answer is on the page, not in the title bar.
        await Assertions.Expect(page.Locator(JourneyShell.SurfaceTitleSelector)).ToBeVisibleAsync();
        await Assertions.Expect(page.Locator(JourneyShell.RailTabSelector).First).ToBeVisibleAsync();

        // How do I get in? The sign-in affordance is present and operable while signed out.
        await ExplorerShell.AssertSignedOutAsync(page);
        await Assertions.Expect(page.Locator(JourneyShell.SignInSelector)).ToBeEnabledAsync();
    }

    [Test]
    public async Task An_area_that_needs_a_credential_stays_prominent_and_opens_the_sign_in()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.AssertSignedOutAsync(page);
        await JourneyShell.AssertRailSettledAsync(page);

        var ledger = page.GetByRole(AriaRole.Tab,
            new PageGetByRoleOptions { Name = JourneyLedgerPlugin.AreaLabel, Exact = true });

        // Prominent: an invitation is a real rail tab, above the divider, not an inert
        // span in the demoted group and not a greyed-out one.
        await Assertions.Expect(ledger).ToBeVisibleAsync();
        await Assertions.Expect(ledger).ToBeEnabledAsync();
        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(0);

        // And taking the invitation does what it promises.
        await ledger.ClickAsync();
        await Assertions.Expect(page.Locator(JourneyShell.SignInDialogSelector)).ToBeVisibleAsync();
    }

    [Test]
    public async Task A_signed_out_visitor_is_never_told_a_credential_free_refusal_is_a_missing_grant()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.AssertSignedOutAsync(page);
        await JourneyShell.AssertRailSettledAsync(page);

        // The demoted group is rendered (asserted above), so this is a statement about
        // a settled rail rather than about one that has not finished probing.
        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(0);

        // The same area, once a credential is presented and refused, IS demoted with a
        // remedy. Asserting both halves in one case is what makes the first half mean
        // "authentication was distinguished" rather than "nothing was demoted at all".
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);

        await Assertions
            .Expect(JourneyShell.DemotedEntry(page, JourneyLedgerPlugin.AreaLabel))
            .ToHaveCountAsync(1);
    }
}
