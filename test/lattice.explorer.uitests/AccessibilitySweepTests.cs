using System.Text;
using Deque.AxeCore.Playwright;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The Explorer's automated accessibility gate: an axe-core sweep of the rendered
/// shell against the WCAG 2.0, 2.1 and 2.2 A and AA rule sets, across both themes,
/// all three breakpoint bands, both identities, and every area the shell offers.
/// <para>
/// <b>Why the matrix.</b> Until issue #1849 this swept one cell - the signed-out home
/// surface, at 1400x900, in the default dark theme, against <c>wcag2a</c> /
/// <c>wcag2aa</c> alone. Each of those four narrowings hid a class of defect by
/// construction rather than by accident: the 2.0-only tag set put SC 1.4.11 Non-text
/// Contrast out of scope, which is exactly why borders measuring 1.21:1 passed; the
/// single viewport never rendered the compact band, where the overflow menu is clipped
/// at every width; the single theme never rendered the light palette, whose own dim
/// text token was below AA and had to be found by hand; and the signed-out-only run
/// never reached the areas the gates admit only once a credential is applied.
/// </para>
/// <para>
/// <b>Why it cannot pass vacuously.</b> axe reports zero violations on a blank
/// document, so a broken app sweeps cleanest. Every case therefore proves its own
/// premises before asserting cleanliness: the shell rendered interactive content, the
/// requested theme genuinely changed what the browser resolved, the design system
/// genuinely classified the viewport into the requested band, the requested identity
/// is genuinely the one rendered, and - the trap specific to widening the tag set -
/// every requested WCAG tag genuinely resolved to rules axe evaluated. See
/// <see cref="AccessibilityConformance"/>, which owns the last of those.
/// </para>
/// <para>
/// <b>There is no allow-list and no mechanism to add one</b> - see
/// <see cref="AccessibilityConformance"/> for that history. A finding is fixed or
/// tracked as its own issue.
/// </para>
/// <para>
/// axe is a net for the defects nobody anticipated, not a substitute for asserting a
/// specific contract: it did not flag the valueless <c>aria-selected</c> of #1793, and
/// it cannot see tab/panel binding, heading structure, a skip link, or keyboard
/// operability at all. Those are asserted explicitly in
/// <see cref="AccessibilityStructureTests"/>, and the standard both fixtures enforce
/// is published in <c>ConformanceChecklist.md</c> beside them.
/// </para>
/// <para>
/// Carries <c>[Category("Integration")]</c> in addition to <c>[Category("UI")]</c>
/// because the suite transitively depends on a running <c>IHost</c> (the in-process
/// Explorer web head).
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class AccessibilitySweepTests : UiTestBase
{
    /// <summary>Every theme x breakpoint x identity cell the home surface is swept in.</summary>
    public static IEnumerable<AccessibilityScenario> Scenarios() => AccessibilityScenario.All();

    /// <summary>Every breakpoint band, narrowest first.</summary>
    public static IEnumerable<LatticeBreakpoint> Breakpoints() => LatticeBreakpoints.All;

    /// <summary>
    /// How many times the area walk re-reads the strip looking for an area whose gate
    /// had not yet reported. Bounded so a strip that somehow never settles fails in
    /// bounded time rather than looping; two passes suffice in practice.
    /// </summary>
    private const int MaxAreaSettlePasses = 4;

    [TestCaseSource(nameof(Scenarios))]
    public async Task Home_surface_has_no_critical_or_serious_wcag_violations(AccessibilityScenario scenario)
    {
        var page = await OpenScenarioAsync(scenario);
        await AssertSweepIsCleanAsync(page, $"the home surface ({scenario})");
    }

    /// <summary>
    /// Sweeps every area the shell offers, not just the default home surface.
    /// <para>
    /// The areas are enumerated from the live strip rather than hard-coded, so an area
    /// added to the catalogue, or newly admitted by a gate, is swept without editing
    /// this file - and one silently withheld cannot be quietly dropped from the gate.
    /// The run is at the expanded band in the dark theme because theme and breakpoint
    /// are already crossed exhaustively on the home surface above; what this case adds
    /// is the surface dimension, held against both identities because which areas are
    /// reachable is exactly what a credential changes.
    /// </para>
    /// <para>
    /// The strip is re-read until the set of offered areas stops growing, rather than
    /// snapshotted once. Every gate reports asynchronously and every plugin's access
    /// defaults to denied until it does, so a single read catches a strip mid-settle:
    /// it can list an area that is about to be withdrawn, and - the reason this matters
    /// for a gate rather than only for flakiness - it can miss one that has not yet been
    /// admitted, silently narrowing "every area" to "every area that had reported by the
    /// time we looked". Walking to a fixed point converges in two passes in practice,
    /// because the first pass spends seconds running axe.
    /// </para>
    /// <para>
    /// Failures are collected across every area rather than thrown at the first, so one
    /// run reports the whole gate's state - which is what the issues downstream of this
    /// one are measured against.
    /// </para>
    /// </summary>
    /// <param name="signedIn">Whether to apply a credential before enumerating areas.</param>
    [TestCase(false)]
    [TestCase(true)]
    public async Task Every_offered_area_has_no_critical_or_serious_wcag_violations(bool signedIn)
    {
        var scenario = new AccessibilityScenario(ExplorerTheme.Dark, LatticeBreakpoint.Expanded, signedIn);
        var page = await OpenScenarioAsync(scenario);
        var identity = signedIn ? "signed in" : "signed out";

        var failures = new StringBuilder();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var swept = new List<string>();
        var unreachable = new List<string>();

        for (var pass = 0; pass < MaxAreaSettlePasses; pass++)
        {
            var discovered = false;

            foreach (var label in await ExplorerShell.OfferedAreaLabelsAsync(page))
            {
                if (!seen.Add(label))
                {
                    continue;
                }

                discovered = true;

                if (!await ExplorerShell.TryActivateAreaAsync(page, label))
                {
                    unreachable.Add(label);
                    continue;
                }

                // Re-prove the premises after each activation: switching area re-renders
                // the whole working surface, and an area that rendered nothing at all
                // would otherwise sweep clean and be counted as covered.
                await ExplorerShell.AssertShellRenderedAsync(page);
                await ExplorerShell.AssertBreakpointAsync(page, scenario.Breakpoint);
                swept.Add(label);

                var surface = $"the '{label}' area ({identity})";

                // Landmarks are checked here rather than only on the home surface,
                // because the shell wraps its own home surface in a main element and
                // renders an active area plugin directly. axe cannot see that: its
                // landmark rules are best-practice tags outside the WCAG rule set this
                // sweep scopes to.
                await AccessibilityProbe.AssertLandmarksAsync(page, surface);

                var results = await page.RunAxe(AccessibilityConformance.RunOptions);
                AccessibilityConformance.AssertRuleSetIsNotVacuous(results, surface);

                var blocking = AccessibilityConformance.BlockingViolations(results);
                if (blocking.Count > 0)
                {
                    failures.AppendLine(AccessibilityConformance.Describe(blocking, surface));
                }
            }

            if (!discovered)
            {
                break;
            }
        }

        var coverage = $"{identity}: swept [{string.Join(", ", swept)}]"
            + (unreachable.Count == 0
                ? string.Empty
                : $"; unreachable [{string.Join(", ", unreachable)}]");

        // Recorded on every run, pass or fail: which areas the gate actually covered is
        // the evidence the epic's later issues are measured against, and a shrinking
        // reachable set is itself a regression worth seeing in the log.
        TestContext.Out.WriteLine(coverage);

        Assert.That(swept, Is.Not.Empty, () =>
            $"No area was reachable {identity}, so this case swept nothing and its result is "
            + $"meaningless. Every area the strip offered was withheld, disabled, or required a "
            + $"sign-in. {coverage}");

        Assert.That(failures.Length, Is.Zero, () => coverage + Environment.NewLine + failures);
    }

    /// <summary>
    /// Directly guards #1793: the ARIA spec defines <c>aria-selected</c> as an
    /// enumerated attribute whose only valid tokens are <c>"true"</c> and
    /// <c>"false"</c>. A bare boolean-attribute form - which Blazor renders when the
    /// value is a C# <c>bool</c> - produces <c>aria-selected</c> with no value, which no
    /// tab may report. axe does not flag this: <c>aria-required-attr</c> is satisfied by
    /// the attribute's mere presence. See <c>AxeMutationProof.md</c> in this directory
    /// for the recorded mutation-test result documenting that honestly.
    /// <para>
    /// Run once per breakpoint band, because the band decides how many tabs render
    /// inline and how many collapse into the overflow menu, so each band presents a
    /// different set of elements to check.
    /// </para>
    /// <para>
    /// Every tab is read from a single DOM snapshot rather than by indexing a live
    /// locator: the strip re-renders as each access gate reports, so an index resolved
    /// against the pre-settle strip can address a tab that has since been withdrawn, and
    /// the test then times out on a missing element instead of reporting on the tabs
    /// that are there. Reading the attribute in the page is still reading the parsed
    /// DOM - <c>getAttribute</c> returns the empty string for a valueless attribute,
    /// which is exactly the #1793 signal.
    /// </para>
    /// </summary>
    /// <param name="breakpoint">The breakpoint band to render in.</param>
    [TestCaseSource(nameof(Breakpoints))]
    public async Task Every_tab_reports_a_valid_enumerated_aria_selected_value(LatticeBreakpoint breakpoint)
    {
        var page = await OpenHomeAsync(breakpoint);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var report = await AccessibilityProbe.RunAsync(page, EnumeratedAriaSelectedProbe);

        Assert.That(report.Examined, Is.GreaterThan(0),
            $"Expected at least one role=tab element at the {LatticeBreakpoints.Name(breakpoint)} "
            + "band; with none, a clean result would say nothing.");

        Assert.That(report.Problems, Is.Empty, () =>
            $"{report.Problems.Count} of {report.Examined} tabs at the "
            + $"{LatticeBreakpoints.Name(breakpoint)} band report an invalid aria-selected value. A "
            + "valueless (boolean-attribute) aria-selected reads as an empty string here and is the "
            + "exact #1793 regression."
            + Environment.NewLine + report);
    }

    /// <summary>
    /// Opens the home surface in <paramref name="scenario"/>'s state and proves every
    /// dimension of that state is genuinely in effect before any assertion is made
    /// about the surface.
    /// </summary>
    private async Task<IPage> OpenScenarioAsync(AccessibilityScenario scenario)
    {
        var page = await OpenHomeAsync(scenario.Breakpoint);

        if (scenario.SignedIn)
        {
            // Sign in before theming: the sign-in POST redirects home, so a second
            // document loads and any attribute written on the first one is gone.
            await ExplorerShell.SignInAsync(page);
            await ExplorerShell.AssertBreakpointAsync(page, scenario.Breakpoint);
        }
        else
        {
            await ExplorerShell.AssertSignedOutAsync(page);
        }

        // Asserted after the identity is settled, not before: a sign-in lands a second
        // document, so a strip proved to have rendered on the first one proves nothing
        // about the one that is about to be swept.
        await ExplorerShell.AssertShellRenderedAsync(page);
        await ExplorerShell.ApplyThemeAsync(page, scenario.Theme);
        return page;
    }

    private static async Task AssertSweepIsCleanAsync(IPage page, string surface)
    {
        var results = await page.RunAxe(AccessibilityConformance.RunOptions);
        AccessibilityConformance.AssertRuleSetIsNotVacuous(results, surface);

        var blocking = AccessibilityConformance.BlockingViolations(results);
        Assert.That(blocking, Is.Empty, () => AccessibilityConformance.Describe(blocking, surface));
    }

    private const string EnumeratedAriaSelectedProbe =
        """
        () => {
            const tabs = Array.from(document.querySelectorAll('[role=tab]'));
            const problems = [];

            for (const tab of tabs) {
                const name = ((tab.textContent || '').trim() || tab.getAttribute('aria-label') || '<unlabelled>').slice(0, 40);

                if (!tab.hasAttribute('aria-selected')) {
                    problems.push('role=tab "' + name + '" is missing aria-selected entirely');
                    continue;
                }

                const value = tab.getAttribute('aria-selected');
                if (value !== 'true' && value !== 'false') {
                    problems.push('role=tab "' + name + '" has aria-selected="' + value
                        + '", which is not one of the enumerated values "true" or "false"');
                }
            }

            return { examined: tabs.length, problems };
        }
        """;
}
