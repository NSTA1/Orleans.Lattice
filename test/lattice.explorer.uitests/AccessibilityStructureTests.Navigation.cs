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

        var strips = page.Locator("[role=tablist]");
        await Assertions.Expect(strips.First).ToBeAttachedAsync();

        var stripCount = await strips.CountAsync();
        var failures = new List<string>();
        var exercised = new List<string>();

        for (var i = 0; i < stripCount; i++)
        {
            var strip = strips.Nth(i);
            var tabs = strip.Locator("[role=tab]:not([disabled])");
            if (await tabs.CountAsync() < 2)
            {
                continue;
            }

            var label = await strip.GetAttributeAsync("aria-label") ?? $"strip #{i}";
            await tabs.First.FocusAsync();

            var before = await FocusedTabIndexAsync(page, i);
            if (before < 0)
            {
                failures.Add($"the '{label}' strip would not accept keyboard focus on its first tab");
                continue;
            }

            await page.Keyboard.PressAsync("ArrowRight");
            var after = await FocusedTabIndexAsync(page, i);
            exercised.Add(label);

            if (after == before)
            {
                failures.Add($"the '{label}' strip did not move focus when ArrowRight was pressed "
                    + $"(focus stayed on tab {before}), so it is not keyboard operable");
            }
            else if (after < 0)
            {
                failures.Add($"pressing ArrowRight in the '{label}' strip moved focus out of the strip "
                    + "entirely rather than to the next tab");
            }
        }

        Assert.That(exercised, Is.Not.Empty, () =>
            $"None of the {stripCount} tab strips had two or more operable tabs, so this case "
            + "exercised nothing and could not tell an operable strip from an inert one.");

        Assert.That(failures, Is.Empty, () =>
            $"Exercised [{string.Join(", ", exercised)}]. A tab strip that announces role=tablist "
            + "must implement the keyboard behaviour that role implies (WCAG SC 2.1.1 Keyboard, "
            + "level A)."
            + Environment.NewLine + string.Join(Environment.NewLine, failures));
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
    /// Clears focus so the next Tab press starts a fresh sequential walk from the top of
    /// the document. Blurring is how a browser's sequential focus navigation starting
    /// point is reset without clicking, which would itself move focus somewhere.
    /// </summary>
    private static Task ResetFocusAsync(IPage page) =>
        page.Locator(":root").EvaluateAsync(
            """
            () => {
                if (document.activeElement instanceof HTMLElement) {
                    document.activeElement.blur();
                }
            }
            """);

    /// <summary>
    /// The index of the currently focused tab within the tab strip at
    /// <paramref name="stripIndex"/>, or a negative value when focus is not on one of
    /// its tabs.
    /// </summary>
    private static Task<int> FocusedTabIndexAsync(IPage page, int stripIndex) =>
        page.Locator(":root").EvaluateAsync<int>(
            """
            index => {
                const strips = document.querySelectorAll('[role=tablist]');
                const strip = strips[index];
                if (!strip) {
                    return -2;
                }
                const tabs = Array.from(strip.querySelectorAll('[role=tab]'));
                return tabs.indexOf(document.activeElement);
            }
            """,
            stripIndex);

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

