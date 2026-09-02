using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: adaptive tenant affordance.</b> The tenant control is offered in
/// proportion to the choice the caller actually has. With one reachable tenant there is
/// no picker and the display is quiet; on a cluster with no tenancy at all the chrome is
/// absent entirely, so a single-tenant deployment looks like a non-tenant one.
/// </summary>
/// <remarks>
/// The absence assertions here are the ones most at risk of passing vacuously - a shell
/// that failed to render satisfies every one of them - so each first proves the page it
/// is measuring genuinely rendered and genuinely reached the identity it names.
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class AdaptiveTenantAffordanceJourneyTests : JourneyTestBase
{
    [Test]
    public async Task A_caller_with_one_reachable_tenant_is_offered_no_picker_and_told_quietly()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.DataReader);

        // Precondition: the tenancy chrome is present at all for this caller. Without
        // this, "no picker" would also be satisfied by a shell with no tenancy, which is
        // the neighbouring case and a different answer.
        await Assertions.Expect(page.Locator(JourneyShell.TenantScopeSelector)).ToBeVisibleAsync();

        // Quiet: the tenant is stated, not offered as a choice.
        await Assertions
            .Expect(page.Locator(JourneyShell.QuietTenantSelector))
            .ToHaveTextAsync(JourneyWorld.AcmeTenant);

        await Assertions.Expect(page.Locator(JourneyShell.TenantPickerSelector)).ToHaveCountAsync(0);

        // And no cross-tenant toggle either: only an operator may widen the view, so
        // offering the control to anyone else would promise what the seam refuses.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantScopeSelector + " input[type=checkbox]"))
            .ToHaveCountAsync(0);
    }

    [Test]
    public async Task A_cluster_without_tenancy_renders_no_tenancy_chrome_at_all()
    {
        // Deliberately the default head, not the journey head: it composes
        // AddLatticeExplorerWeb without AddExplorerTenantView, which is exactly a
        // single-tenant deployment. Measuring this on the journey head would be
        // impossible, because that head opts tenancy on.
        var page = await NewPageAsync(ExpandedWidth, Height);
        await page.GotoAsync(ExplorerAppHostSetup.Host.BaseUri.ToString());
        await ExplorerShell.WaitForShellReadyAsync(page);

        // Prove the shell rendered before asserting anything is missing from it.
        await ExplorerShell.AssertShellRenderedAsync(page);
        await Assertions.Expect(page.Locator(JourneyShell.SurfaceTitleSelector)).ToBeVisibleAsync();

        // Signed in, because the control's shape depends on the caller and a signed-out
        // absence would prove only that anonymity hides it.
        await ExplorerShell.SignInAsync(page);

        await Assertions.Expect(page.Locator(JourneyShell.TenantScopeSelector)).ToHaveCountAsync(0);
        await Assertions.Expect(page.Locator(JourneyShell.TenantPickerSelector)).ToHaveCountAsync(0);
        await Assertions.Expect(page.Locator(JourneyShell.QuietTenantSelector)).ToHaveCountAsync(0);
    }

    [Test]
    public async Task An_operator_whose_choice_shrinks_to_one_loses_the_picker_rather_than_keeping_a_dead_one()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await Assertions.Expect(page.Locator(JourneyShell.TenantPickerSelector)).ToBeVisibleAsync();

        await WithdrawSecondTenantAsync();
        var next = await NewSessionAsync(page);

        // The tenancy chrome is still there for this operator - so the absence below is
        // about the picker, not about the control having disappeared.
        await Assertions.Expect(next.Locator(JourneyShell.TenantScopeSelector)).ToBeVisibleAsync();
        await Assertions.Expect(next.Locator(JourneyShell.TenantPickerSelector)).ToHaveCountAsync(0);
        await Assertions
            .Expect(next.Locator(JourneyShell.QuietTenantSelector))
            .ToHaveTextAsync(JourneyWorld.AcmeTenant);
    }
}
