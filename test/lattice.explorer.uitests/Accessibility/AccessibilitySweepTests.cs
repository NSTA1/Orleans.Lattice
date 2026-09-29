using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// The Explorer's automated accessibility gate: an axe-core sweep of every area's
/// primary page, as the administrator of a live cluster, in every appearance - Paper
/// and Board, standard and more contrast, comfortable and compact density.
/// </summary>
/// <remarks>
/// <para>
/// Each case proves its own premises before it sweeps: the page's heading rendered, and
/// the document carries exactly the appearance asked for (<see cref="Shell.SetAppearanceAsync"/>
/// reads the attributes back), so a sweep can never be of a blank page or of the wrong
/// palette. <see cref="AxeConformance"/> then proves the rule set itself was not empty.
/// </para>
/// <para>
/// axe is the net for defects nobody anticipated. The criteria it cannot see - keyboard
/// operation, focus, landmarks, forced colours - are asserted by name in
/// <see cref="KeyboardAccessibilityTests"/> and <see cref="AccessibilityStructureTests"/>.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AccessibilitySweepTests : UiTestBase
{
    /// <summary>Every area, as the page the directory stop opens.</summary>
    [TestCaseSource(typeof(ExplorerAreas), nameof(ExplorerAreas.Keys))]
    public async Task Every_area_primary_page_has_no_serious_violations_in_any_appearance(string areaKey)
    {
        var area = ExplorerAreas.Get(areaKey);
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, area.PrimaryPath, WorldIdentities.Admin);
        await ExpectPrimaryPageAsync(page, area);

        foreach (var appearance in ShellAppearanceChoice.All)
        {
            await Shell.SetAppearanceAsync(page, appearance);
            await AxeConformance.SweepAsync(page, $"the {area.DisplayName} area ({area.PrimaryPath}) in {appearance}");
        }
    }

    /// <summary>
    /// The signed-out Explorer and its sign-in dialog: the first thing every user sees.
    /// </summary>
    [Test]
    public async Task The_signed_out_home_and_the_sign_in_dialog_have_no_serious_violations()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/");
        await Expect(Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First).ToBeVisibleAsync();
        await AxeConformance.SweepAsync(page, "the signed-out home");

        await Shell.OpenSignInAsync(page);
        await AxeConformance.SweepAsync(page, "the sign-in dialog");
    }

    internal static async Task ExpectPrimaryPageAsync(IPage page, ExplorerArea area)
    {
        if (area.ShownToAdmin)
        {
            await Expect(Shell.Heading(page)).ToHaveTextAsync(area.DisplayName);
            await Expect(Shell.Stop(page, area.Key)).ToHaveAttributeAsync("aria-current", "page");
        }
        else
        {
            await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        }
    }
}
