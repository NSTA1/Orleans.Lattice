using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The operations a journey performs on a live page, each of which proves the state it
/// claims to have reached before it returns.
/// <para>
/// This is the journey tier's answer to the same trap
/// <see cref="ExplorerShell"/> guards at the surface tier: an accessibility scan or an
/// absence assertion passes hardest against a document that never rendered, and a
/// journey compounds that risk because it is a chain - one step that quietly did
/// nothing turns every later assertion into a claim about the wrong page. Every helper
/// here therefore ends on an observable consequence (a selected row, a populated
/// strip, a rendered heading) rather than on the instruction that was issued.
/// </para>
/// </summary>
internal static class JourneyShell
{
    /// <summary>The rail's tabs. A demoted area is deliberately not one of these.</summary>
    internal const string RailTabSelector = ".lx-shell-areastrip [role=tab]";

    /// <summary>The group holding areas a gate has refused this caller.</summary>
    internal const string DemotedGroupSelector = ".lx-shell-rail-demoted";

    /// <summary>The rail once every area gate has reported.</summary>
    internal const string SettledRailSelector = ".lx-shell-rail[data-lx-rail-settled='true']";

    /// <summary>The detail strip once every surface gate has reported.</summary>
    internal const string SettledDetailSelector = ".lx-shell-detail[data-lx-detail-settled='true']";

    /// <summary>One refused area's entry, carrying its label and its remedy disclosure.</summary>
    internal const string DemotedEntrySelector = ".lx-shell-rail-demoted-entry";

    /// <summary>The divider a refused area is demoted below.</summary>
    internal const string DemotedDividerSelector = ".lx-shell-rail-divider[role=separator]";

    /// <summary>The catalog's rows.</summary>
    internal const string CatalogRowSelector = "#lx-shell-catalog button";

    /// <summary>The catalog row currently selected.</summary>
    internal const string SelectedCatalogRowSelector = "#lx-shell-catalog button.is-selected";

    /// <summary>The per-selection detail strip, addressed by the name it publishes.</summary>
    internal const string DetailStripSelector = "[role=tablist][aria-label='Detail tabs']";

    /// <summary>The per-selection detail strip's tabs.</summary>
    internal const string DetailTabSelector = DetailStripSelector + " [role=tab]";

    /// <summary>The compact-band control that reveals the catalog drawer.</summary>
    internal const string CatalogDrawerToggleSelector = ".lx-nav-drawer-toggle";

    /// <summary>The tenant scope control's host.</summary>
    internal const string TenantScopeSelector = ".lx-shell-tenant-switch";

    /// <summary>The tenant picker, offered only to an operator with a genuine choice.</summary>
    internal const string TenantPickerSelector = "#lx-tenant-scope-select";

    /// <summary>The quiet, non-interactive statement of the active tenant.</summary>
    internal const string QuietTenantSelector = ".lx-shell-tenant-id";

    /// <summary>The tenant scope control's polite live region.</summary>
    internal const string TenantScopeStatusSelector = TenantScopeSelector + " [role=status]";

    /// <summary>The shell's sign-in affordance.</summary>
    internal const string SignInSelector = ".lx-shell-auth-signin";

    /// <summary>The sign-in dialog the shell opens.</summary>
    internal const string SignInDialogSelector = ".lx-shell-config-overlay [role=dialog]";

    /// <summary>The surface heading the shell owns, one per area.</summary>
    internal const string SurfaceTitleSelector = "h1.lx-shell-surface-title";

    /// <summary>Any tab strip's overflow trigger.</summary>
    internal const string OverflowToggleSelector = ".lx-tabstrip-overflow-toggle";

    /// <summary>
    /// Opens the catalog if the current band hides it behind a drawer, then returns
    /// once at least one catalog row is genuinely visible.
    /// <para>
    /// The drawer's own <c>aria-expanded</c> is waited on rather than assumed, because
    /// the compact frame renders the catalog only while the drawer is open - so a click
    /// that has not yet round-tripped the circuit leaves nothing to find, and a bare
    /// wait on the rows cannot tell that from an empty catalog.
    /// </para>
    /// </summary>
    /// <param name="page">The page to act on.</param>
    internal static async Task RevealCatalogAsync(IPage page)
    {
        var toggle = page.Locator(CatalogDrawerToggleSelector);
        if (await toggle.CountAsync() > 0)
        {
            if (await toggle.First.GetAttributeAsync("aria-expanded") != "true")
            {
                await toggle.First.ClickAsync();
            }

            await Assertions.Expect(toggle.First).ToHaveAttributeAsync("aria-expanded", "true");
        }

        await Assertions.Expect(page.Locator(CatalogRowSelector).First).ToBeVisibleAsync();
    }

    /// <summary>
    /// Selects the catalog row whose label starts with <paramref name="label"/> and
    /// returns only once the shell has genuinely opened it: the row reads as selected
    /// and the per-selection strip has tabs.
    /// </summary>
    /// <param name="page">The page to act on.</param>
    /// <param name="label">The row's leading label text.</param>
    internal static async Task OpenCatalogItemAsync(IPage page, string label)
    {
        await RevealCatalogAsync(page);

        var row = page.Locator(CatalogRowSelector)
            .Filter(new LocatorFilterOptions { HasTextString = label })
            .First;

        await Assertions.Expect(row).ToBeVisibleAsync();
        await row.ClickAsync();

        // The strip proves the detail panel resolved a surface set for the choice, and
        // it holds at every width.
        await Assertions.Expect(page.Locator(DetailTabSelector).First).ToBeVisibleAsync();

        // The selected-row marking is only assertable while the catalog is on screen.
        // In the compact band the catalog is a drawer that the shell deliberately
        // dismisses once a choice is made - the point of the drawer is to give the
        // detail surface the viewport back - so the list is gone, not unmarked, and
        // asserting on it there would fail a shell behaving exactly as designed.
        if (!await page.Locator("#lx-shell-catalog").IsVisibleAsync())
        {
            return;
        }

        try
        {
            await Assertions.Expect(page.Locator(SelectedCatalogRowSelector)).ToContainTextAsync(label);
        }
        catch (PlaywrightException ex)
        {
            Assert.Fail(
                $"The detail panel opened, but the catalog does not mark '{label}' as the selected "
                + "row, so a caller cannot see which tree they are looking at."
                + Environment.NewLine + await DescribeCatalogStateAsync(page)
                + Environment.NewLine + ex.Message);
        }
    }

    /// <summary>
    /// A one-line description of what the catalog is currently showing: each row, its
    /// selected marking, and the detail surfaces resolved beside it. Best-effort and
    /// bounded, so it can never become a second failure or hold the run open.
    /// </summary>
    /// <param name="page">The page to describe.</param>
    internal static async Task<string> DescribeCatalogStateAsync(IPage page)
    {
        try
        {
            return await page.Locator(":root").EvaluateAsync<string>(
                """
                () => {
                    const rows = Array.from(document.querySelectorAll('#lx-shell-catalog button'))
                        .map(b => (b.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40)
                            + (b.classList.contains('is-selected') ? ' [selected]' : ''));
                    const strip = document.querySelector("[role=tablist][aria-label='Detail tabs']");
                    const tabs = strip
                        ? Array.from(strip.querySelectorAll('[role=tab]'))
                            .map(t => (t.textContent || '').trim()
                                + (t.getAttribute('aria-selected') === 'true' ? '*' : ''))
                        : [];
                    return 'address=' + location.pathname + location.search
                        + ' rows=[' + rows.join(' | ') + ']'
                        + ' detailTabs=[' + tabs.join(' ') + ']';
                }
                """,
                arg: null,
                new LocatorEvaluateOptions { Timeout = DiagnosticTimeoutMs });
        }
        catch (PlaywrightException ex)
        {
            return "catalog state could not be read: " + ex.Message;
        }
    }

    /// <summary>
    /// Activates the rail tab labelled <paramref name="label"/> and returns once that
    /// area is genuinely the active one - the tab reads selected and the surface
    /// heading names it.
    /// </summary>
    /// <param name="page">The page to act on.</param>
    /// <param name="label">The exact area label.</param>
    internal static async Task OpenAreaAsync(IPage page, string label)
    {
        // Wait for the gates before clicking. An area shown plainly while unprobed
        // can demote a moment later, so a click aimed at a tab mid-settle can land
        // on a control that is no longer there - which reads as "the area would not
        // open" rather than as a race.
        await AssertRailSettledAsync(page);

        var tab = page.GetByRole(AriaRole.Tab, new PageGetByRoleOptions { Name = label, Exact = true });
        await Assertions.Expect(tab).ToBeVisibleAsync();
        await tab.ClickAsync();
        await AssertActiveAreaAsync(page, label);
    }

    /// <summary>
    /// Asserts <paramref name="label"/> is the active area, by both signals the shell
    /// publishes: the rail's selected tab and the surface's single heading.
    /// <para>
    /// On failure it reports the address and the whole rail, because "the wrong area is
    /// active" is almost never diagnosable from the expected/actual pair alone - what
    /// distinguishes a click that did not land from a navigation that was reverted is
    /// whether the address moved.
    /// </para>
    /// </summary>
    /// <param name="page">The page to check.</param>
    /// <param name="label">The exact area label.</param>
    internal static async Task AssertActiveAreaAsync(IPage page, string label)
    {
        var selected = page.Locator(RailTabSelector + "[aria-selected='true']");

        try
        {
            await Assertions.Expect(selected).ToHaveTextAsync(label);
        }
        catch (PlaywrightException ex)
        {
            Assert.Fail(
                $"'{label}' did not become the active area." + Environment.NewLine
                + await DescribeNavigationStateAsync(page) + Environment.NewLine
                + ex.Message);
        }

        await Assertions.Expect(page.Locator(SurfaceTitleSelector)).ToHaveTextAsync(label);
    }

    /// <summary>
    /// A one-line description of where the shell currently thinks it is: the address,
    /// the surface heading, and every rail tab with its selected state. Attached to a
    /// navigation failure so the transcript carries the evidence.
    /// </summary>
    /// <remarks>
    /// Read through a locator with an explicit timeout, and never allowed to throw.
    /// <see cref="IPage.EvaluateAsync{T}(string, object?)"/> has <b>no timeout</b>, so a
    /// page whose circuit has wedged blocks it forever - and a diagnostic that hangs the
    /// run is worse than no diagnostic at all. This one is best-effort by construction.
    /// </remarks>
    /// <param name="page">The page to describe.</param>
    internal static async Task<string> DescribeNavigationStateAsync(IPage page)
    {
        try
        {
            return await page.Locator(":root").EvaluateAsync<string>(
                """
                () => {
                    const tabs = Array.from(document.querySelectorAll('.lx-shell-areastrip [role=tab]'))
                        .map(t => (t.textContent || '').trim()
                            + '[selected=' + t.getAttribute('aria-selected')
                            + ',tabindex=' + t.getAttribute('tabindex')
                            + (t.hasAttribute('disabled') ? ',disabled' : '') + ']');
                    const h1 = document.querySelector('h1.lx-shell-surface-title');
                    return 'address=' + location.pathname + location.search
                        + ' heading=' + (h1 ? (h1.textContent || '').trim() : '<none>')
                        + ' rail=[' + tabs.join(' ') + ']';
                }
                """,
                new LocatorEvaluateOptions { Timeout = DiagnosticTimeoutMs });
        }
        catch (PlaywrightException ex)
        {
            return "navigation state could not be read: " + ex.Message;
        }
    }

    /// <summary>
    /// How long a best-effort diagnostic read may take. Short: it runs only on a path
    /// that has already failed, and its job is to add evidence to that failure, never to
    /// become a second failure or to hold the run open.
    /// </summary>
    private const float DiagnosticTimeoutMs = 5_000;

    /// <summary>The demoted entry for <paramref name="label"/>, refused areas only.</summary>
    /// <param name="page">The page to read.</param>
    /// <param name="label">The exact area label.</param>
    internal static ILocator DemotedEntry(IPage page, string label) =>
        page.Locator(DemotedEntrySelector)
            .Filter(new LocatorFilterOptions { HasTextString = label });

    /// <summary>
    /// Waits until the rail has genuinely settled - every area's gate has reported -
    /// and offers at least one area.
    /// </summary>
    /// <remarks>
    /// This used to wait for the demoted group to be visible, on the reasoning that a
    /// settled rail has resolved at least one refusal into it. That was inverted. The
    /// access store reads a fail-closed <c>Denied</c> for a key nobody has probed yet,
    /// so the rail opened with EVERY area demoted and emptied the group as the probes
    /// landed: the group was visible precisely while the rail was unsettled, and on a
    /// signed-out shell - where a refusal is a sign-in prompt and stays prominent
    /// rather than demoted - it was correctly empty once settled, so the wait could
    /// never succeed.
    /// <para>
    /// The shell now publishes the fact directly, so this waits on the rail's own
    /// statement rather than inferring it from a side effect.
    /// </para>
    /// </remarks>
    /// <param name="page">The page to check.</param>
    internal static async Task AssertRailSettledAsync(IPage page)
    {
        await Assertions.Expect(page.Locator(RailTabSelector).First).ToBeVisibleAsync();
        await Assertions.Expect(page.Locator(SettledRailSelector)).ToBeAttachedAsync();
    }

    /// <summary>
    /// Waits until the detail strip's operable set has stopped changing: every surface
    /// gate has reported, and at least one tab is present and not disabled.
    /// </summary>
    /// <remarks>
    /// The strip needs this for the same reason the rail does, and waiting only for a
    /// non-disabled tab is not enough. A surface nobody has probed yet renders as
    /// <c>Pending</c>, which is <b>enabled</b> - deliberately, because "not asked" is
    /// not "refused" - so a caller that samples the strip mid-probe sees more operable
    /// tabs than the strip will settle with, and every one of them can still turn
    /// disabled or disappear. Waiting for the strip's own settled statement removes
    /// that window rather than narrowing it.
    /// </remarks>
    /// <param name="page">The page to check.</param>
    internal static async Task AssertDetailStripSettledAsync(IPage page)
    {
        await Assertions
            .Expect(page.Locator(DetailTabSelector + ":not([disabled])").First)
            .ToBeVisibleAsync();
        await Assertions.Expect(page.Locator(SettledDetailSelector)).ToBeAttachedAsync();
    }

    /// <summary>
    /// Opens the overflow menu behind <paramref name="toggle"/> and returns its bounding
    /// rectangle together with the viewport width, having first proved the menu genuinely
    /// rendered with items in it.
    /// </summary>
    /// <remarks>
    /// Takes the toggle its caller already resolved rather than re-resolving and
    /// re-asserting one. Re-asserting was the defect: it meant the caller waited for the
    /// toggle, then this method waited for it again, so the two waits straddled a window
    /// the toggle could vanish in - which is exactly what kept happening.
    /// </remarks>
    /// <param name="page">The page to act on.</param>
    /// <param name="toggle">The overflow trigger to open, already known to be visible.</param>
    /// <returns>The opened menu's geometry.</returns>
    private static async Task<OverflowGeometry> OpenOverflowMenuAsync(IPage page, ILocator toggle)
    {
        var menuId = await toggle.GetAttributeAsync("aria-controls");
        Assert.That(menuId, Is.Not.Null.And.Not.Empty,
            "The overflow trigger declares no aria-controls, so it names no menu and a "
            + "screen-reader user is told a menu exists that they cannot reach.");

        await toggle.ClickAsync();

        var menu = page.Locator("#" + menuId);
        await Assertions.Expect(menu).ToBeVisibleAsync();

        // Anti-vacuity: a menu with no items has a trivially contained rectangle, so
        // measuring one would report a clean pass for a control that offers nothing.
        await Assertions.Expect(menu.Locator("[role=menuitemradio], [role=menuitem]").First)
            .ToBeVisibleAsync();

        return await menu.EvaluateAsync<OverflowGeometry>(
            """
            element => {
                const r = element.getBoundingClientRect();
                return {
                    left: r.left, right: r.right, width: r.width,
                    viewportWidth: window.innerWidth,
                    items: element.querySelectorAll('[role=menuitemradio],[role=menuitem]').length,
                };
            }
            """);
    }

    /// <summary>
    /// Measures the overflow menu at the current viewport if one is offered, opening it
    /// and closing it again, or returns <see langword="null"/> when nothing overflows at
    /// this width.
    /// </summary>
    /// <remarks>
    /// The thing being measured genuinely flickers, so the measurement tolerates flicker
    /// instead of asserting it away.
    /// <para>
    /// Whether a strip overflows is decided by a client-side layout measurement that
    /// converges asynchronously after a viewport resize, and
    /// <see cref="OverflowToggleSelector"/> matches EVERY strip's toggle, so the answer
    /// depends on the rail and the detail strip together. While that settles, a toggle can
    /// appear and then be withdrawn again. A single wait therefore proves only that a
    /// toggle existed at one instant, which is why "wait for it, then open it" kept
    /// failing on the second step against an element the first step had genuinely seen.
    /// </para>
    /// <para>
    /// So each attempt resolves the toggle and opens it as one unit, and a toggle that
    /// disappears mid-open simply costs an attempt rather than failing the test. The
    /// attempts are bounded: if the layout never settles enough to complete one open, that
    /// is a real defect and is reported as one rather than retried forever. Returning
    /// <see langword="null"/> means no toggle was offered at all, which is the ordinary
    /// answer for a width that does not overflow - callers keep an anti-vacuity assertion
    /// so a run where nothing ever overflowed still fails.
    /// </para>
    /// </remarks>
    /// <param name="page">The page to act on.</param>
    /// <param name="settleMs">How long to wait for a toggle before concluding there is none.</param>
    /// <returns>The measured geometry, or <see langword="null"/> when nothing overflows.</returns>
    internal static async Task<OverflowGeometry?> TryMeasureOverflowMenuAsync(
        IPage page,
        float settleMs = 5_000)
    {
        const int Attempts = 4;

        for (var attempt = 1; attempt <= Attempts; attempt++)
        {
            var toggle = page.Locator(OverflowToggleSelector).First;

            if (!await BecomesVisibleAsync(toggle, settleMs))
            {
                // No toggle at all: this width does not overflow.
                return null;
            }

            try
            {
                var geometry = await OpenOverflowMenuAsync(page, toggle);

                // Close it again, so the next width is measured from the same resting
                // state. The toggle is still the one that was just opened.
                await toggle.ClickAsync();
                return geometry;
            }
            catch (PlaywrightException) when (attempt < Attempts)
            {
                // The toggle or its menu went away while it was being opened - the layout
                // was still converging. Re-resolve and try again; if the strip has since
                // stopped overflowing, the visibility check above will say so and this
                // width is correctly reported as not overflowing.
            }
            catch (TimeoutException) when (attempt < Attempts)
            {
            }
        }

        Assert.Fail(
            $"The overflow menu could not be opened in {Attempts} attempts: a toggle kept "
            + "appearing and then disappearing before its menu could be measured. That is no "
            + "longer the ordinary post-resize settle - it means the strip's overflow layout "
            + "is not converging.");
        return null;
    }

    /// <summary>
    /// Whether <paramref name="locator"/> becomes visible within
    /// <paramref name="timeoutMs"/>, rather than whether it happens to exist right now.
    /// </summary>
    private static async Task<bool> BecomesVisibleAsync(ILocator locator, float timeoutMs)
    {
        try
        {
            await locator.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = timeoutMs,
            });
            return true;
        }
        catch (PlaywrightException)
        {
            return false;
        }
        catch (TimeoutException)
        {
            // Playwright surfaces a wait timeout as System.TimeoutException here rather
            // than as a PlaywrightException, so both have to be caught. This is the
            // ordinary answer, not an error: nothing overflows at this width.
            return false;
        }
    }
}
