using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The keyboard and announcement half of the structural conformance gate: whether a
/// keyboard-only user can bypass repeated blocks, see where focus is, drive the custom
/// widgets, and hear that an asynchronous change happened.
/// </summary>
public sealed partial class AccessibilityStructureTests
{
    /// <summary>
    /// How many tab stops the focus walk visits before giving up. High enough to walk
    /// the whole shell chrome and into the working surface, low enough that a focus
    /// trap fails in bounded time rather than looping.
    /// </summary>
    private const int MaxFocusStops = 30;

    /// <summary>The catalog-kind tab strip, the one asynchronous state change this harness can drive.</summary>
    // A DESCENDANT selector, deliberately. The shared tab primitive puts the
    // caller's Class on its host element and role=tablist on the inner strip, so
    // no single element can carry both. The original compound selector encoded
    // the hand-rolled markup #1850 replaced, and would match nothing once the
    // catalog-kind toggle moved onto LatticeAdaptiveTabs.
    private const string CatalogKindStripSelector = ".lx-shell-nav-toggle [role=tablist]";

    /// <summary>The catalog kind switched to, chosen because it is not the default.</summary>
    private const string CatalogKindTarget = "Views";

    private const string PoliteLiveRegionSelector = "[role=status], [aria-live='polite']";

    private const string AnyLiveRegionSelector = "[role=status], [role=alert], [aria-live]";

    /// <summary>
    /// The first stop in the tab order must be a skip link that is visible while
    /// focused, targets the main landmark, and actually moves focus there.
    /// <para>
    /// <b>Currently red.</b> The shell has no skip link at all, so a keyboard user
    /// tabs through the whole chrome - the brand, the cog, the sign-in control, the
    /// area strip, the catalog toggle, the catalog list - before reaching the working
    /// surface, on every single surface change. This is WCAG SC 2.4.1 Bypass Blocks,
    /// a level A criterion. Expected to go green with #1850 (S1 navigation IA and
    /// shell chrome), which owns the shell-level skip link.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        await ResetFocusAsync(page);
        await page.Keyboard.PressAsync("Tab");

        var first = await AccessibilityProbe.RunAsync(page, SkipLinkProbe);
        Assert.That(first.Examined, Is.GreaterThan(0),
            "Pressing Tab from the top of the document moved focus to nothing, so the shell has no "
            + "keyboard-reachable content at all and this case cannot measure a skip link.");

        Assert.That(first.Problems, Is.Empty, () =>
            "The first stop in the tab order is not a working skip link, so a keyboard user cannot "
            + "bypass the shell chrome (WCAG SC 2.4.1 Bypass Blocks, level A)."
            + Environment.NewLine + first);

        // Only reachable once a skip link exists: activating it must actually land the
        // user in the main landmark. A link that announces a bypass and does not perform
        // one satisfies nobody, and is a documented way of failing 2.4.1 while appearing
        // to meet it.
        await page.Keyboard.PressAsync("Enter");
        await page.Keyboard.PressAsync("Tab");

        var landing = await AccessibilityProbe.RunAsync(page, FocusInsideMainProbe);
        Assert.That(landing.Problems, Is.Empty, () =>
            "Activating the skip link did not move the keyboard user into the main landmark."
            + Environment.NewLine + landing);
    }

    /// <summary>
    /// Every tab strip must expose a roving tabindex: exactly one of its tabs is in the
    /// document's tab sequence and the rest are reachable only with the arrow keys.
    /// <para>
    /// <b>Currently red.</b> Only the design system's own <c>LatticeAdaptiveTabs</c>
    /// implements this. The shell's area strip and the catalog-kind toggle are
    /// hand-rolled copies whose tabs carry no <c>tabindex</c> at all, so every tab sits
    /// in the tab sequence and a keyboard user must tab through the entire strip to
    /// reach what follows it. Expected to go green with #1848 (F3 design-system
    /// primitives), which replaces the hand-rolled strips with the one primitive that
    /// implements the pattern.
    /// </para>
    /// <para>
    /// This is the half of criterion 1 that can be checked on a strip whose tabs a gate
    /// has disabled, which is why it exists alongside the arrow-key case below: in this
    /// test host every area gate denies for want of a cluster connection, so the area
    /// strip - the strip that most needs checking - has only one operable tab and cannot
    /// be driven with the keyboard at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_tab_strip_exposes_a_roving_tabindex()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var report = await AccessibilityProbe.RunAsync(page, RovingTabIndexProbe);

        Assert.That(report.Examined, Is.GreaterThan(0),
            "The roving-tabindex probe found no tab strip with two or more tabs, so its clean "
            + "result means nothing.");

        Assert.That(report.Problems, Is.Empty, () =>
            $"{report.Problems.Count} of {report.Examined} tab strips do not expose a roving "
            + "tabindex, so keyboard focus order does not match the ARIA tabs pattern the markup "
            + "claims (WCAG SC 2.4.3 Focus Order, level A)."
            + Environment.NewLine + report);
    }

    /// <summary>
    /// Every tab strip must be operable from the keyboard: arrow keys move focus
    /// between the tabs of the strip that has focus.
    /// <para>
    /// <b>Currently red.</b> Only the design system's own <c>LatticeAdaptiveTabs</c>
    /// implements arrow-key handling. The shell's area strip and the catalog-kind toggle
    /// are hand-rolled with none, so arrow keys do nothing in two of the three strips -
    /// the ARIA tabs pattern the markup claims is not the behaviour the widget has.
    /// Expected to go green with #1848 (F3 design-system primitives).
    /// </para>
    /// <para>
    /// The assertion is that focus <i>moved to another tab in the same strip</i>, not
    /// that it moved to a specific index: a correct implementation skips disabled tabs,
    /// and the shell's gates disable tabs the caller may not use. A strip with fewer
    /// than two operable tabs has no arrow-key behaviour to exercise and is left to
    /// <see cref="Every_tab_strip_exposes_a_roving_tabindex"/>, which does not need one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_operable_tab_strip_moves_focus_with_arrow_keys()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        await Assertions.Expect(page.Locator("[role=tablist]").First).ToBeAttachedAsync();

        // Snapshot the strips by the name each publishes, in one read, and address them
        // by that name afterwards.
        //
        // Walking [role=tablist] by index was a real race rather than a theoretical
        // one: every strip re-renders as a gate reports and as the catalog settles, so
        // an index resolved against one render can address a detached element in the
        // next - whose child count then reads zero, which this case previously
        // interpreted as "no operable strip" and reported as its own vacuity failure.
        var strips = await page.Locator(":root").EvaluateAsync<StripSnapshot[]>(
            """
            () => Array.from(document.querySelectorAll('[role=tablist]')).map(s => ({
                label: s.getAttribute('aria-label') || '',
                vertical: s.getAttribute('aria-orientation') === 'vertical',
                operable: s.querySelectorAll('[role=tab]:not([disabled])').length,
            }))
            """);

        var failures = new List<string>();
        var exercised = new List<string>();

        foreach (var strip in strips)
        {
            if (strip.Operable < 2 || strip.Label.Length == 0)
            {
                continue;
            }

            // The axis comes from the strip's own aria-orientation. The area rail is
            // vertical now, and each axis deliberately leaves the other pair to the
            // page so a rail does not swallow page scrolling - so asserting one fixed
            // key pair would report a false failure for a strip that is behaving
            // exactly as the pattern requires.
            var forward = strip.Vertical ? "ArrowDown" : "ArrowRight";
            var located = page.Locator($"[role=tablist][aria-label='{strip.Label}']");

            await located.Locator("[role=tab]:not([disabled])").First.FocusAsync();

            var before = await FocusedTabIdAsync(page, strip.Label);
            if (before is null)
            {
                failures.Add($"the '{strip.Label}' strip would not accept keyboard focus on its first tab");
                continue;
            }

            await page.Keyboard.PressAsync(forward);
            var after = await WaitForFocusedTabChangeAsync(page, strip.Label, before);
            exercised.Add($"{strip.Label} ({forward})");

            if (after is null)
            {
                failures.Add($"pressing {forward} in the '{strip.Label}' strip moved focus out of the "
                    + "strip entirely rather than to the next tab");
            }
            else if (after == before)
            {
                failures.Add($"the '{strip.Label}' strip did not move focus when {forward} was pressed "
                    + $"(focus stayed on '{before}'), so it is not keyboard operable");
            }
        }

        Assert.That(exercised, Is.Not.Empty, () =>
            $"None of the {strips.Length} tab strips had two or more operable tabs, so this case "
            + "exercised nothing and could not tell an operable strip from an inert one. Strips seen: ["
            + string.Join(", ", strips.Select(s => $"{s.Label}:{s.Operable}")) + "].");

        Assert.That(failures, Is.Empty, () =>
            $"Exercised [{string.Join(", ", exercised)}]. A tab strip that announces role=tablist "
            + "must implement the keyboard behaviour that role implies (WCAG SC 2.1.1 Keyboard, "
            + "level A)."
            + Environment.NewLine + string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// The id (or label) of the tab that currently has focus, or <see langword="null"/>
    /// when focus is not on a tab at all. Identity rather than index, so it stays
    /// meaningful across the re-render a key press can cause.
    /// </summary>
    /// <param name="page">The page to read focus from.</param>
    /// <summary>
    /// The tab the strip currently gives keyboard focus to.
    /// </summary>
    /// <remarks>
    /// Reads the tab carrying the roving
    /// <c>tabindex="0"</c>, corroborated by the focused element. In the tabs pattern those are the same tab: the
    /// strip moves keyboard focus BY moving the roving tabindex, and the accompanying
    /// <c>element.focus()</c> is a best-effort browser affordance. A headless CI runner
    /// does not always honour that call even though the widget performed correctly -
    /// observed directly, with the captured page showing the roving tabindex on the
    /// next tab while document.activeElement had not moved.
    /// <para>
    /// The fallback does not weaken the assertion. A strip that genuinely does not
    /// respond to the key moves neither, so it still reports the tab it started on and
    /// still fails.
    /// </para>
    /// </remarks>
    /// <param name="page">The page to read focus from.</param>
    /// <param name="stripLabel">The aria-label of the strip to read within.</param>
    private static Task<string?> FocusedTabIdAsync(IPage page, string stripLabel) =>
        page.Locator(":root").EvaluateAsync<string?>(
            """
            (root, label) => {
                const id = el => el ? (el.id || (el.textContent || '').trim()) : null;

                const strip = document.querySelector(
                    `[role=tablist][aria-label="${label}"]`);
                if (!strip) { return null; }

                // The roving tabindex is the widget's own statement of which tab it
                // gives keyboard focus to, and is what assistive technology follows,
                // so it is the authority here. document.activeElement is only
                // corroboration: a headless runner does not always honour the
                // accompanying element.focus(), and preferring it would report the
                // runner's behaviour rather than the widget's.
                // Scoped to this strip, because every strip carries its own roving
                // tabindex and an unscoped query would report a different one's.
                const roving = strip.querySelector('[role=tab][tabindex="0"]');
                if (roving) { return id(roving); }

                const active = document.activeElement;
                return active && active.getAttribute('role') === 'tab' && strip.contains(active)
                    ? id(active)
                    : null;
            }
            """,
            stripLabel);

    /// <summary>
    /// Reads the focused tab once the strip has had a chance to move focus, returning
    /// as soon as it changes rather than after a fixed wait.
    /// </summary>
    /// <remarks>
    /// The strips are Blazor Server components, so an arrow key is handled over the
    /// circuit: the key press, a server round trip, a re-render and only then the focus
    /// move. On the area rail it is longer still, because an arrow selects the next
    /// area rather than only moving focus, so the chain includes a route change and the
    /// shell re-rendering around it. Reading focus immediately after the press
    /// therefore samples before the move on any machine slow enough to lose that race,
    /// and reports a strip that works as "not keyboard operable".
    /// <para>
    /// The budget is generous because it is only ever paid in full by a genuine
    /// failure: the poll returns as soon as focus moves, so a healthy strip costs one
    /// interval. A strip that never moves focus still returns the original id when the
    /// window elapses, and still fails.
    /// </para>
    /// </remarks>
    /// <param name="page">The page to read focus from.</param>
    /// <param name="stripLabel">The aria-label of the strip being exercised.</param>
    /// <param name="before">The tab id focused before the key was pressed.</param>
    private static async Task<string?> WaitForFocusedTabChangeAsync(IPage page, string stripLabel, string? before)
    {
        const int budgetMs = 20_000;
        const int intervalMs = 50;

        var current = before;
        for (var waited = 0; waited < budgetMs; waited += intervalMs)
        {
            current = await FocusedTabIdAsync(page, stripLabel);
            if (current != before)
            {
                return current;
            }

            await Task.Delay(intervalMs);
        }

        return current;
    }

    /// <summary>
    /// Every control a keyboard user can reach must paint something while focused.
    /// <para>
    /// <b>Expected green</b>, and kept as a regression guard. The check is deliberately
    /// coarse: it fails a control that paints neither an outline nor a box-shadow while
    /// focused, which is the classic <c>outline: none</c> with no replacement. It does
    /// not measure the indicator's contrast or thickness - that is SC 1.4.11 Non-text
    /// Contrast, which the token layer owns and #1846 (F1) is raising to 3:1.
    /// </para>
    /// <para>
    /// Focus is driven with real Tab presses rather than <c>element.focus()</c>, because
    /// <c>:focus-visible</c> only matches after a keyboard interaction: a programmatic
    /// focus would report every author focus style as missing and make this test lie.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_keyboard_focus_stop_paints_a_visible_focus_indicator()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        await ResetFocusAsync(page);

        var failures = new List<string>();
        var stops = 0;

        for (var i = 0; i < MaxFocusStops; i++)
        {
            await page.Keyboard.PressAsync("Tab");
            var report = await AccessibilityProbe.RunAsync(page, FocusIndicatorProbe);
            if (report.Examined == 0)
            {
                // Focus left the document (it reached the browser chrome), so the walk
                // has covered every stop the page offers.
                break;
            }

            stops++;
            failures.AddRange(report.Problems);
        }

        Assert.That(stops, Is.GreaterThan(2), () =>
            $"The keyboard walk found only {stops} focus stops in the whole shell, so it cannot have "
            + "covered the chrome and its clean result would mean nothing.");

        Assert.That(failures, Is.Empty, () =>
            $"{failures.Count} of {stops} keyboard focus stops paint no focus indicator at all "
            + "(WCAG SC 2.4.7 Focus Visible, level AA)."
            + Environment.NewLine + string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// An asynchronous state change - switching the catalog between trees, views and
    /// tag indexes - must be announced in a polite live region.
    /// <para>
    /// <b>Currently red.</b> The whole application has one live region, the connection
    /// status banner, and it announces only the connection. Switching the catalog kind
    /// replaces the entire list asynchronously with no announcement, so a screen-reader
    /// user who activates the toggle is told nothing about what replaced it. The region
    /// must also pre-exist the message: assistive technology only announces changes to a
    /// live region already in the accessibility tree, so a region rendered at the same
    /// moment as its content is silent - which is why the failure message reports what
    /// live regions were present rather than only that none matched. Expected to go
    /// green with #1850 (S1 navigation IA and shell chrome), which owns
    /// <c>UI/Navigation/</c> and the shell's announcement surface.
    /// </para>
    /// <para>
    /// The catalog toggle is the state change this harness can actually make. Switching
    /// the working <i>area</i> would be the more obvious subject, but every area gate in
    /// this test host denies for want of a cluster connection, so no area beyond home is
    /// reachable and such a case could only ever report that it measured nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_async_catalog_change_is_announced_in_a_polite_live_region()
    {
        var page = await OpenHomeAsync(LatticeBreakpoint.Expanded);
        await ExplorerShell.AssertShellRenderedAsync(page);

        var toggle = page.Locator(CatalogKindStripSelector);
        await Assertions.Expect(toggle).ToHaveCountAsync(1);

        var target = toggle.GetByRole(AriaRole.Tab, new LocatorGetByRoleOptions
        {
            Name = CatalogKindTarget,
            Exact = true,
        });
        await Assertions.Expect(target).ToBeVisibleAsync();
        await target.ClickAsync();

        // Prove the change actually happened before asserting anything about how it was
        // announced: an announcement assertion against a click that did nothing would
        // report a missing announcement for a change that never occurred.
        await Assertions.Expect(target).ToHaveAttributeAsync("aria-selected", "true");

        var politeRegions = page.Locator(PoliteLiveRegionSelector);
        var announcement = politeRegions.Filter(new LocatorFilterOptions
        {
            HasTextString = CatalogKindTarget,
        });

        try
        {
            await Assertions.Expect(announcement.First).ToBeAttachedAsync();
        }
        catch (PlaywrightException ex)
        {
            var present = await page.Locator(AnyLiveRegionSelector).AllInnerTextsAsync();
            Assert.Fail(
                $"Switching the catalog to '{CatalogKindTarget}' was not announced: no polite live "
                + "region mentions it, so a screen-reader user is given no indication that the whole "
                + "catalog changed (WCAG SC 4.1.3 Status Messages, level AA). "
                + $"The {present.Count} live region(s) present in the document say: "
                + $"[{string.Join(" | ", present)}]."
                + Environment.NewLine + ex.Message);
        }
    }

    /// <summary>
    /// Moves focus to the very start of the document, so the next Tab press begins a
    /// fresh sequential walk at the first focusable element.
    /// <para>
    /// This deliberately does not blur. Blurring looks like it should reset the walk
    /// and does not: the HTML sequential focus navigation starting point stays where
    /// the blurred element was, so the next Tab continues from the middle of the page.
    /// The shell moves focus onto its active tab while initialising, so a walk that
    /// began after that had already skipped the chrome - and reported the first stop as
    /// the catalog-kind tab rather than the skip link, or found two stops where there
    /// are dozens. It is a race against initialisation, which is why it passed on a
    /// quiet machine and failed on a loaded CI runner.
    /// </para>
    /// <para>
    /// Focusing the body element is what actually moves the starting point back to the
    /// top. The temporary tabindex is required because body is not focusable by
    /// default, and it is removed immediately so the walk does not count body itself as
    /// a stop.
    /// </para>
    /// </summary>
    private static async Task ResetFocusAsync(IPage page)
    {
        await page.BringToFrontAsync();
        await page.Locator(":root").EvaluateAsync(
            """
            () => {
                if (document.activeElement instanceof HTMLElement) {
                    document.activeElement.blur();
                }

                const body = document.body;
                const had = body.hasAttribute('tabindex');
                if (!had) {
                    body.setAttribute('tabindex', '-1');
                }

                body.focus();

                if (!had) {
                    body.removeAttribute('tabindex');
                }
            }
            """);
    }

    private const string RovingTabIndexProbe =
        """
        () => {
            const strips = Array.from(document.querySelectorAll('[role=tablist]'));
            const problems = [];
            let examined = 0;

            for (let i = 0; i < strips.length; i++) {
                const strip = strips[i];
                const tabs = Array.from(strip.querySelectorAll('[role=tab]'));
                if (tabs.length < 2) {
                    continue;
                }

                examined++;
                const label = strip.getAttribute('aria-label') || ('strip #' + i);
                const inSequence = tabs.filter(tab => tab.getAttribute('tabindex') === '0');
                const declared = tabs.filter(tab => tab.hasAttribute('tabindex'));

                if (declared.length === 0) {
                    problems.push('the "' + label + '" strip declares no tabindex on any of its '
                        + tabs.length + ' tabs, so every tab sits in the document tab sequence and a '
                        + 'keyboard user must tab through the whole strip to pass it');
                } else if (inSequence.length !== 1) {
                    problems.push('the "' + label + '" strip has ' + inSequence.length
                        + ' tabs with tabindex="0"; a roving tabindex keeps exactly one tab in the '
                        + 'document tab sequence');
                } else if (declared.length !== tabs.length) {
                    problems.push('the "' + label + '" strip leaves ' + (tabs.length - declared.length)
                        + ' of its ' + tabs.length + ' tabs without an explicit tabindex, so they '
                        + 'default into the tab sequence alongside the roving one');
                }
            }

            return { examined, problems };
        }
        """;

    private const string SkipLinkProbe =
        """
        () => {
            const el = document.activeElement;
            if (!el || el === document.body || el === document.documentElement) {
                return { examined: 0, problems: [] };
            }

            const text = ((el.textContent || '').trim() || el.getAttribute('aria-label') || '').slice(0, 40);
            const describe = el.tagName.toLowerCase()
                + (el.className ? '.' + String(el.className).trim().split(/\s+/).join('.') : '')
                + ' "' + text + '"';

            const problems = [];
            const href = el.getAttribute('href') || '';

            if (el.tagName.toLowerCase() !== 'a' || !href.startsWith('#')) {
                problems.push('the first stop in the tab order is ' + describe
                    + ', not a same-page skip link (an <a href="#...">) targeting the main landmark');
                return { examined: 1, problems };
            }

            const target = document.getElementById(href.slice(1));
            if (!target) {
                problems.push('the skip link ' + describe + ' targets "' + href
                    + '" but no element has that id, so activating it goes nowhere');
            } else if (!target.matches('main, [role=main]') && target.closest('main, [role=main]') === null) {
                problems.push('the skip link ' + describe + ' targets "' + href
                    + '", which is not the main landmark nor inside it');
            }

            const rect = el.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0) {
                problems.push('the skip link ' + describe + ' is not rendered while it has focus, '
                    + 'so a sighted keyboard user cannot see the stop they are on');
            }

            return { examined: 1, problems };
        }
        """;

    private const string FocusInsideMainProbe =
        """
        () => {
            const el = document.activeElement;
            if (!el || el === document.body || el === document.documentElement) {
                return { examined: 0, problems: ['focus was on no element after activating the skip link'] };
            }

            const text = ((el.textContent || '').trim() || el.getAttribute('aria-label') || '').slice(0, 40);
            const describe = el.tagName.toLowerCase() + ' "' + text + '"';
            const inside = el.matches('main, [role=main]') || el.closest('main, [role=main]') !== null;
            const problems = [];

            if (!inside) {
                problems.push('after activating the skip link and pressing Tab, focus was on ' + describe
                    + ', which is not inside the main landmark, so the bypass did not happen');
            }

            return { examined: 1, problems };
        }
        """;

    private const string FocusIndicatorProbe =
        """
        () => {
            const el = document.activeElement;
            if (!el || el === document.body || el === document.documentElement) {
                return { examined: 0, problems: [] };
            }

            const style = getComputedStyle(el);
            const outlined = style.outlineStyle !== 'none' && parseFloat(style.outlineWidth) > 0;
            const shadowed = style.boxShadow !== 'none' && style.boxShadow !== '';
            const problems = [];

            if (!outlined && !shadowed) {
                const text = ((el.textContent || '').trim() || el.getAttribute('aria-label') || '').slice(0, 40);
                problems.push('the focused control ' + el.tagName.toLowerCase()
                    + (el.className ? '.' + String(el.className).trim().split(/\s+/).join('.') : '')
                    + ' "' + text + '" paints neither an outline nor a box-shadow while focused, '
                    + 'so keyboard focus is invisible on it');
            }

            return { examined: 1, problems };
        }
        """;
}

