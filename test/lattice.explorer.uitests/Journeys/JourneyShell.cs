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

        // Both halves matter. The row proves the catalog registered the choice; the
        // strip proves the detail panel resolved a surface set for it. Asserting only
        // the first would let a journey continue against an empty detail panel.
        await Assertions.Expect(page.Locator(SelectedCatalogRowSelector)).ToContainTextAsync(label);
        await Assertions.Expect(page.Locator(DetailTabSelector).First).ToBeVisibleAsync();
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
    /// Asserts the rail has genuinely settled: it offers at least two areas and has
    /// resolved at least one refusal into the demoted group. Both halves are needed
    /// before any claim about what the rail does or does not contain, because the rail
    /// re-renders as each gate reports and an early read sees neither.
    /// </summary>
    /// <param name="page">The page to check.</param>
    internal static async Task AssertRailSettledAsync(IPage page)
    {
        await Assertions.Expect(page.Locator(RailTabSelector).First).ToBeVisibleAsync();
        await Assertions.Expect(page.Locator(DemotedGroupSelector)).ToBeVisibleAsync();
    }

    /// <summary>
    /// Opens the first overflow menu on the page and returns its bounding rectangle
    /// together with the viewport width, having first proved the menu genuinely
    /// rendered with items in it.
    /// </summary>
    /// <param name="page">The page to act on.</param>
    /// <returns>The opened menu's geometry.</returns>
    internal static async Task<OverflowGeometry> OpenOverflowMenuAsync(IPage page)
    {
        var toggle = page.Locator(OverflowToggleSelector).First;
        await Assertions.Expect(toggle).ToBeVisibleAsync();

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
}
