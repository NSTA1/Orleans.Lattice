using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The user-preference half of the structural conformance gate: the adaptations a
/// user's operating-system settings ask for, which only a real browser context can
/// express and therefore only this tier can measure.
/// </summary>
public sealed partial class AccessibilityStructureTests
{
    /// <summary>
    /// The longest transition or animation, in milliseconds, that still counts as
    /// motion having been neutralised. The design system collapses every duration to
    /// 0.01ms under a reduced-motion preference, so the bar is loose enough to survive
    /// a change of that constant and tight enough that a real animation fails it.
    /// </summary>
    private const double NeutralisedMotionMs = 1d;

    /// <summary>
    /// A <c>prefers-reduced-motion: reduce</c> preference must neutralise the shell's
    /// transitions and animations.
    /// <para>
    /// <b>Expected green</b>, and kept as a regression guard: the design system already
    /// honours the preference in <c>lattice-breakpoints.css</c>. The test carries its
    /// own control - it first measures the same element with no preference set and
    /// requires it to genuinely animate - because an element that never animated would
    /// satisfy the reduced-motion assertion trivially, and the guard would quietly stop
    /// guarding the day the transition moved elsewhere.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_reduced_motion_preference_neutralises_shell_motion()
    {
        var normal = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(normal);
        var baseline = await normal.Locator(":root").EvaluateAsync<double>(LongestMotionProbe);

        Assert.That(baseline, Is.GreaterThan(NeutralisedMotionMs),
            $"The control measurement found no motion to neutralise ({baseline}ms), so the "
            + "reduced-motion assertion below would pass whatever the stylesheet did. The element "
            + "this probe measures no longer animates; point it at one that does.");

        var reduced = await OpenHomeAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize
            {
                Width = ExplorerShell.ViewportWidth(LatticeBreakpoint.Expanded),
                Height = ExplorerShell.ViewportHeight,
            },
            ReducedMotion = ReducedMotion.Reduce,
        });
        await ExplorerShell.AssertShellRenderedAsync(reduced);
        var neutralised = await reduced.Locator(":root").EvaluateAsync<double>(LongestMotionProbe);

        Assert.That(neutralised, Is.LessThanOrEqualTo(NeutralisedMotionMs),
            $"With prefers-reduced-motion: reduce the shell still animates for {neutralised}ms "
            + $"(it animates for {baseline}ms with no preference set). WCAG SC 2.3.3 Animation from "
            + "Interactions asks that motion triggered by interaction can be disabled.");
    }

    /// <summary>
    /// The design system must declare adaptations for the two contrast preferences a
    /// user's operating system can express: <c>forced-colors</c> (Windows High Contrast
    /// and its equivalents, which replace the author's palette wholesale) and
    /// <c>prefers-contrast</c>.
    /// <para>
    /// <b>Currently red.</b> The Explorer's stylesheets declare neither, so a user in a
    /// forced-colors mode gets whatever the browser's substitution happens to produce
    /// over a palette that was never designed for it - and the shell's several
    /// colour-only state indicators (an active tab is distinguished from an inactive one
    /// by hue at 1.54:1 plus a same-colour underline) have no defined behaviour there at
    /// all. Expected to go green with #1846 (F1 design tokens), which owns the palette
    /// and is adding both adaptations.
    /// </para>
    /// <para>
    /// The probe reads the real loaded CSSOM rather than the source files, so it
    /// measures what the browser actually received. It carries its own control: the
    /// design system is known to declare a <c>prefers-reduced-motion</c> adaptation, so
    /// a scan that cannot find that one is not reading the shipped stylesheets and its
    /// other findings would be meaningless.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_design_system_declares_contrast_preference_adaptations()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var report = await AccessibilityProbe.RunAsync(page, MediaConditionProbe);

        Assert.That(report.Examined, Is.GreaterThan(0),
            "The stylesheet scan found no media conditions at all, so it is not reading the shell's "
            + "stylesheets and its result means nothing.");

        Assert.That(report.Problems, Is.Empty, () =>
            $"The {report.Examined} media conditions the shell's stylesheets declare do not cover "
            + "every user contrast preference."
            + Environment.NewLine + report);
    }

    private const string LongestMotionProbe =
        """
        () => {
            // Scan the whole rendered shell rather than one named element.
            //
            // The probe used to read '.lx-shell-area-tab', a class the navigation
            // redesign deleted when the hand-rolled area strip moved onto the shared
            // tab primitive; it then fell back to '.lx-shell', which has never
            // animated, so the control measured 0ms and the guard correctly reported
            // that it was no longer guarding anything. Pinning a selector makes this
            // probe fail on a rename rather than on a regression, so it does not pin
            // one: it asks whether the shell animates ANYWHERE, which is the actual
            // subject and is immune to a class moving.
            let longest = 0;
            const toMilliseconds = value => (value || '').split(',')
                .map(part => part.trim())
                .filter(Boolean)
                .map(part => part.endsWith('ms') ? parseFloat(part) : parseFloat(part) * 1000)
                .filter(number => !Number.isNaN(number));

            for (const el of document.querySelectorAll('.lx-shell, .lx-shell *')) {
                const style = getComputedStyle(el);
                for (const duration of toMilliseconds(style.transitionDuration)) {
                    if (duration > longest) { longest = duration; }
                }
                for (const duration of toMilliseconds(style.animationDuration)) {
                    if (duration > longest) { longest = duration; }
                }
            }

            return longest;
        }
        """;

    private const string MediaConditionProbe =
        """
        () => {
            const conditions = [];
            const visit = rules => {
                for (const rule of rules) {
                    if (rule.media && rule.conditionText) {
                        conditions.push(rule.conditionText);
                    }
                    if (rule.cssRules) {
                        visit(rule.cssRules);
                    }
                }
            };

            for (const sheet of Array.from(document.styleSheets)) {
                try {
                    visit(sheet.cssRules);
                } catch (error) {
                    // A stylesheet this document may not read; the control below fails if
                    // that ever applies to the design system's own sheets.
                }
            }

            const declares = needle => conditions.some(condition => condition.indexOf(needle) >= 0);
            const problems = [];

            if (!declares('prefers-reduced-motion')) {
                problems.push('control failure: the scan found no prefers-reduced-motion condition, '
                    + 'which the design system is known to declare, so it is not reading the shipped '
                    + 'stylesheets and every other finding here is worthless');
            }

            if (!declares('forced-colors')) {
                problems.push('no stylesheet declares a forced-colors adaptation, so a user in a '
                    + 'forced-colors mode gets an undefined result over a palette designed without it');
            }

            if (!declares('prefers-contrast')) {
                problems.push('no stylesheet declares a prefers-contrast adaptation, so a user asking '
                    + 'their operating system for more contrast receives none');
            }

            return { examined: conditions.length, problems };
        }
        """;
}

