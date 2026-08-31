using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The conformance criteria an automated axe sweep cannot see, asserted explicitly
/// against the live shell.
/// <para>
/// Automated scanning finds a minority of real barriers, and the ones it misses are
/// not the marginal ones. axe did not flag the valueless <c>aria-selected</c> of
/// #1793, because a rule satisfied by an attribute's mere presence cannot tell a
/// valid enumerated value from an empty one. The same blindness covers everything in
/// this fixture: whether a tab is bound to a panel that exists, whether a document has
/// a heading outline a screen-reader user can navigate, whether a keyboard user can
/// skip repeated blocks or drive a tab strip at all, and whether an asynchronous state
/// change is announced. Every one of those is a WCAG conformance requirement, and
/// every one is invisible to a rule engine that only inspects static markup.
/// </para>
/// <para>
/// <b>These assertions are the epic's gate, and several are red today.</b> That is
/// deliberate: issue #1849 lands the standard, and the issues that follow it are
/// measured against it. Each test below names the defect it currently reports and the
/// issue expected to turn it green, so a red run is legible rather than alarming. Do
/// not weaken, skip, or ignore one to get a green run - a conformance gate that is
/// edited until it passes measures nothing. The full standard is published in
/// <c>ConformanceChecklist.md</c> beside this file.
/// </para>
/// <para>
/// Every probe carries a vacuity guard: it reports how many elements it examined, and
/// examining none fails. A structural check without that guard reports an identical
/// clean result for "the contract holds" and for "the elements were never rendered".
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed partial class AccessibilityStructureTests : UiTestBase
{
    /// <summary>Every breakpoint band, narrowest first.</summary>
    public static IEnumerable<LatticeBreakpoint> Breakpoints() => LatticeBreakpoints.All;

    /// <summary>
    /// Every <c>role="tab"</c> must name a real <c>role="tabpanel"</c> through
    /// <c>aria-controls</c>, and that panel must be labelled by a tab.
    /// <para>
    /// <b>Currently red.</b> The shell renders eleven <c>role="tab"</c> elements and
    /// zero <c>role="tabpanel"</c> elements, and no tab carries <c>aria-controls</c>:
    /// the ARIA tabs pattern is announced but not implemented, in all three strips. A
    /// screen-reader user is told a tab is selected and is given nothing to move into.
    /// Expected to go green with #1848 (F3 design-system primitives), which replaces
    /// all three hand-rolled strips with one primitive that renders a real panel.
    /// </para>
    /// </summary>
    /// <param name="breakpoint">The breakpoint band to render in.</param>
    [TestCaseSource(nameof(Breakpoints))]
    public async Task Every_tab_is_bound_to_a_real_tabpanel(LatticeBreakpoint breakpoint)
    {
        var page = await OpenHomeAsync(breakpoint);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var report = await AccessibilityProbe.RunAsync(page, TabPanelBindingProbe);

        Assert.That(report.Examined, Is.GreaterThan(0),
            "The tab/panel binding probe found no role=tab elements to examine, so its clean "
            + "result means nothing. The shell did not render its tab strips.");

        Assert.That(report.Problems, Is.Empty, () =>
            $"{report.Problems.Count} of {report.Examined} tabs at the "
            + $"{LatticeBreakpoints.Name(breakpoint)} band are not bound to a real tab panel. "
            + "WCAG SC 4.1.2 Name, Role, Value requires a custom widget to expose the "
            + "relationships its role implies; a tab that controls nothing leaves a screen-reader "
            + "user with no way to reach the content it selects."
            + Environment.NewLine + report);
    }

    /// <summary>
    /// Every surface must have exactly one level-1 heading and a heading outline that
    /// never skips a level.
    /// <para>
    /// <b>Currently red.</b> The shell renders no <c>h1</c> at all, and exactly one
    /// heading in the whole document - an <c>h3</c>. Heading navigation is the primary
    /// way screen-reader users move around a page, so a document with no outline is one
    /// they can only read linearly. Expected to go green with #1850 (S1 navigation IA
    /// and shell chrome), which owns the shell-level heading structure.
    /// </para>
    /// </summary>
    /// <param name="breakpoint">The breakpoint band to render in.</param>
    [TestCaseSource(nameof(Breakpoints))]
    public async Task Each_surface_has_one_h1_and_no_skipped_heading_levels(LatticeBreakpoint breakpoint)
    {
        var page = await OpenHomeAsync(breakpoint);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var report = await AccessibilityProbe.RunAsync(page, HeadingOutlineProbe);

        Assert.That(report.Problems, Is.Empty, () =>
            $"The heading outline at the {LatticeBreakpoints.Name(breakpoint)} band does not "
            + $"satisfy WCAG SC 1.3.1 Info and Relationships ({report.Examined} headings examined)."
            + Environment.NewLine + report);
    }

    /// <summary>
    /// The shell must expose exactly one main landmark, at least one navigation
    /// landmark, and a banner, so assistive technology can jump between the regions of
    /// the page instead of walking it linearly.
    /// <para>
    /// <b>Expected green on the home surface</b>, and kept as a regression guard. The
    /// same check runs after every area activation in
    /// <c>AccessibilitySweepTests.Every_offered_area_has_no_critical_or_serious_wcag_violations</c>,
    /// because the shell wraps its own home surface in
    /// <c>&lt;main class="lx-shell-main"&gt;</c> but renders an active area plugin
    /// directly with no landmark around it - so the moment a user leaves home, the
    /// working surface is landmark-less. That defect is not reachable in this test host,
    /// where every area gate needs a cluster connection the host deliberately does not
    /// have; folding the check into the area walk is what makes it fire automatically on
    /// the day a plugin area becomes reachable, rather than standing here as a case that
    /// can never run. #1850 (S1 navigation IA and shell chrome) owns the fix.
    /// </para>
    /// </summary>
    /// <param name="breakpoint">The breakpoint band to render in.</param>
    [TestCaseSource(nameof(Breakpoints))]
    public async Task The_shell_exposes_a_main_a_navigation_and_a_banner_landmark(LatticeBreakpoint breakpoint)
    {
        var page = await OpenHomeAsync(breakpoint);
        await ExplorerShell.AssertShellRenderedAsync(page);

        await AccessibilityProbe.AssertLandmarksAsync(
            page, $"the home surface at the {LatticeBreakpoints.Name(breakpoint)} band");
    }

    private const string TabPanelBindingProbe =
        """
        () => {
            const tabs = Array.from(document.querySelectorAll('[role=tab]'));
            const problems = [];
            const name = el => ((el.textContent || '').trim() || el.getAttribute('aria-label') || '<unlabelled>').slice(0, 40);

            for (const tab of tabs) {
                const controls = tab.getAttribute('aria-controls');
                if (!controls) {
                    problems.push('tab "' + name(tab) + '" has no aria-controls, so it names no panel');
                    continue;
                }

                const panel = document.getElementById(controls);
                if (!panel) {
                    problems.push('tab "' + name(tab) + '" has aria-controls="' + controls + '" but no element has that id');
                    continue;
                }

                if (panel.getAttribute('role') !== 'tabpanel') {
                    problems.push('tab "' + name(tab) + '" controls #' + controls + ', whose role is "'
                        + (panel.getAttribute('role') || '<none>') + '" rather than tabpanel');
                    continue;
                }

                const labelledBy = (panel.getAttribute('aria-labelledby') || '').split(/\s+/).filter(Boolean);
                const labelledByATab = labelledBy.some(id => {
                    const source = document.getElementById(id);
                    return source !== null && source.getAttribute('role') === 'tab';
                });

                if (!labelledByATab) {
                    problems.push('the panel #' + controls + ' controlled by tab "' + name(tab)
                        + '" is not aria-labelledby any role=tab, so it has no accessible name');
                }
            }

            return { examined: tabs.length, problems };
        }
        """;

    private const string HeadingOutlineProbe =
        """
        () => {
            const isVisible = el => (typeof el.checkVisibility === 'function' ? el.checkVisibility() : true)
                && el.closest('[aria-hidden="true"]') === null;

            const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6, [role=heading]'))
                .filter(isVisible);

            const levelOf = el => {
                if (el.getAttribute('role') === 'heading') {
                    const declared = parseInt(el.getAttribute('aria-level') || '2', 10);
                    return Number.isNaN(declared) ? 2 : declared;
                }
                return parseInt(el.tagName.substring(1), 10);
            };

            const name = el => ((el.textContent || '').trim() || '<empty>').slice(0, 40);
            const problems = [];
            const levels = headings.map(levelOf);
            const topLevel = levels.filter(level => level === 1).length;

            if (topLevel !== 1) {
                problems.push('the surface has ' + topLevel + ' level-1 headings; exactly one is required '
                    + 'so the document has a single title at the root of its heading outline'
                    + (headings.length === 0
                        ? ' (the document has no headings at all)'
                        : ' (levels present, in document order: ' + levels.join(', ') + ')'));
            }

            let previous = 0;
            for (let i = 0; i < headings.length; i++) {
                if (previous !== 0 && levels[i] > previous + 1) {
                    problems.push('heading "' + name(headings[i]) + '" is level ' + levels[i]
                        + ' but follows level ' + previous + ', skipping level ' + (previous + 1));
                }
                previous = levels[i];
            }

            return { examined: headings.length, problems };
        }
        """;


}
