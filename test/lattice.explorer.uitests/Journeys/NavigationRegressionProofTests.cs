using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The epic's permanent regression proofs: one case per defect it fixed, each written
/// so that it fails against the code as it stood before the epic.
/// <para>
/// These are deliberately separate from the journeys. A journey describes something a
/// person does and may be re-shaped as the product grows; a proof pins one defect and
/// exists to stay red the day it returns. They run over the journey head because that
/// is the only composition in this suite that offers more than one area, a real
/// selection and a strip that genuinely overflows - the conditions under which the
/// original defects were measured.
/// </para>
/// <para>
/// <b>How each fails against pre-epic code</b> is recorded on each case, from the audit
/// in memory topic <c>epic-explorer-ux</c>.
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class NavigationRegressionProofTests : JourneyTestBase
{
    /// <summary>Widths from the narrowest supported viewport up through every band.</summary>
    private static readonly int[] SampledWidths = [320, 360, 430, 520, 599, 720, 1024, 1400];

    /// <summary>
    /// <b>Proof: the overflow menu lies wholly within the viewport at every width from
    /// 320px up.</b>
    /// <para>
    /// Pre-epic this failed by construction. The menu was absolutely positioned against
    /// whichever ancestor happened to be positioned, with a fixed offset and no clamp,
    /// so the audit measured its leading edge a constant 25.2px outside the viewport
    /// right across the compact band - the same number at every width, which is the
    /// signature of an offset rather than of a layout that merely ran out of room.
    /// #1848 made the strip host the menu's containing block and clamped its width to
    /// the gutters. This measures the rendered rectangle, so it fails on any regression
    /// of either half.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_overflow_menu_never_renders_outside_the_viewport()
    {
        var page = await OpenAtAsync("", NarrowWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        var measured = new List<string>();
        var clipped = new List<string>();

        foreach (var width in SampledWidths)
        {
            await page.SetViewportSizeAsync(width, Height);
            await Assertions.Expect(page.Locator(JourneyShell.DetailTabSelector).First).ToBeVisibleAsync();

            if (await page.Locator(JourneyShell.OverflowToggleSelector).CountAsync() == 0)
            {
                continue;
            }

            var geometry = await JourneyShell.OpenOverflowMenuAsync(page);
            measured.Add($"{width}px: {geometry}");
            if (!geometry.IsContained)
            {
                clipped.Add($"{width}px: {geometry}");
            }

            await page.Locator(JourneyShell.OverflowToggleSelector).First.ClickAsync();
        }

        Assert.That(measured, Is.Not.Empty,
            "No overflow menu rendered at any sampled width, so this proof measured nothing and "
            + "could not have detected the clip it exists for.");

        Assert.That(clipped, Is.Empty,
            "An overflow menu rendered outside the viewport."
            + Environment.NewLine + string.Join(Environment.NewLine, clipped)
            + Environment.NewLine + "All measured:" + Environment.NewLine
            + string.Join(Environment.NewLine, measured));
    }

    /// <summary>
    /// <b>Proof: an explicit tenant switch is not reverted by the identity resolver.</b>
    /// <para>
    /// Pre-epic the resolver re-asserted the identity's default tenant on every resolve,
    /// and the scope control calls a resolve on every refresh - so switching tenant
    /// applied, then was silently overwritten before the next render, and the picker
    /// snapped back. The audit recorded it as gotcha
    /// <c>explorer-tenant-switch-reverted-by-resolver</c>. #1851 changed the resolver to
    /// <i>establish</i> once per sign-in and never overwrite an explicit switch. This
    /// drives the switch through the real control and then forces several further
    /// resolves - a reload and a fresh circuit - which is exactly what used to revert it.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_explicit_tenant_switch_is_not_reverted_by_the_identity_resolver()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        // The resolver's own default for this identity is the first reachable tenant, so
        // switching away from it is what makes a revert observable.
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.AcmeTenant);

        await page.Locator(JourneyShell.TenantPickerSelector)
            .SelectOptionAsync(JourneyWorld.GlobexTenant);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        // Provoke further resolves. Each of these ran the resolver again and, pre-fix,
        // put the default back.
        await ReloadAsync(page);
        await Assertions
            .Expect(page.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);

        var next = await NewSessionAsync(page);
        await Assertions
            .Expect(next.Locator(JourneyShell.TenantPickerSelector))
            .ToHaveValueAsync(JourneyWorld.GlobexTenant);
    }

    /// <summary>
    /// <b>Proof: the active area survives a reload.</b>
    /// <para>
    /// Pre-epic the shell held the active area in a component field with nothing
    /// durable behind it, so any reload landed the caller back on the home surface no
    /// matter where they had been working. #1847 added the route grammar and the
    /// remembered-route preference and #1850 made the shell read them through a single
    /// arbitrator. Both an addressed reload and a bare re-entry are checked, because
    /// they take different paths through that arbitrator and only the second consults
    /// what was remembered.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_active_area_survives_a_reload_and_a_bare_re_entry()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        await ReloadAsync(page);
        await JourneyShell.AssertActiveAreaAsync(page, JourneyWorkbenchPlugin.AreaLabel);

        var next = await NewSessionAsync(page);
        await JourneyShell.AssertActiveAreaAsync(next, JourneyWorkbenchPlugin.AreaLabel);
    }

    /// <summary>
    /// <b>Proof: every <c>role=tab</c> is bound to a real <c>role=tabpanel</c>.</b>
    /// <para>
    /// Pre-epic this failed outright: #1849 measured 9 of 9 tabs carrying no
    /// <c>aria-controls</c> and <i>zero</i> <c>role=tabpanel</c> elements in the whole
    /// document, because the shell's strips were hand-rolled markup that claimed the
    /// tabs role without implementing the pattern. A tab that controls nothing tells a
    /// screen-reader user a region exists and gives them no way to reach it.
    /// </para>
    /// <para>
    /// This runs over the journey head's richer surface - three areas, a populated
    /// per-selection strip and the catalog-kind strip - so it covers strips the
    /// disconnected head never renders.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_tab_is_bound_to_a_real_tabpanel()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        var report = await page.Locator(":root").EvaluateAsync<TabBindingReport>(
            """
            () => {
                const tabs = Array.from(document.querySelectorAll('[role=tab]'));
                const problems = [];
                for (const tab of tabs) {
                    const label = (tab.textContent || '').trim() || tab.id || '(unlabelled)';
                    const controls = tab.getAttribute('aria-controls');
                    if (!controls) { problems.push(label + ': no aria-controls'); continue; }
                    const panel = document.getElementById(controls);
                    if (!panel) { problems.push(label + ": aria-controls names '" + controls + "', which is not in the document"); continue; }
                    if (panel.getAttribute('role') !== 'tabpanel') {
                        problems.push(label + ": aria-controls names '" + controls + "', whose role is '" + (panel.getAttribute('role') || 'none') + "' rather than tabpanel");
                    }
                }
                return { examined: tabs.length, problems };
            }
            """);

        Assert.That(report.Examined, Is.GreaterThan(0),
            "No element declared role=tab, so this proof examined nothing.");

        Assert.That(report.Problems, Is.Empty, () =>
            $"{report.Problems.Length} of {report.Examined} tabs are not bound to a real tabpanel "
            + "(WCAG SC 1.3.1 Info and Relationships, and the ARIA tabs pattern the markup claims)."
            + Environment.NewLine + string.Join(Environment.NewLine, report.Problems));
    }

    /// <summary>
    /// <b>Proof: arrow keys move focus in every tab strip.</b>
    /// <para>
    /// Pre-epic the shell's own strips were hand-rolled with no key handling at all, so
    /// arrow keys did nothing in the area strip and the catalog-kind toggle: the widget
    /// announced <c>role=tablist</c> and did not behave like one, and a keyboard user
    /// had to tab through every tab to get past the strip. #1848 moved every strip onto
    /// the one primitive that implements the pattern, including a roving tabindex, which
    /// this asserts alongside - the two together are the pattern, and either alone can
    /// pass while the strip is unusable.
    /// </para>
    /// <para>
    /// The axis is read from each strip's own <c>aria-orientation</c>, so the vertical
    /// rail is driven with Up/Down and the horizontal strips with Left/Right - a strip
    /// that swallowed the other axis would be a different defect, and asserting one
    /// fixed key pair would report a false failure for the rail.
    /// </para>
    /// </summary>
    [Test]
    public async Task Arrow_keys_move_focus_in_every_tab_strip()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        // Wait for the catalog-kind strip to leave its busy state before snapshotting.
        // Selecting a tree publishes the selection into the address, which reloads the
        // catalog, and its tabs are disabled while that is in flight - so a snapshot
        // taken immediately records a strip that is operable now and inert a moment
        // later, and the walk then waits out a full action timeout on it.
        await Assertions
            .Expect(page.Locator("[role=tablist][aria-label='Catalog kind'] [role=tab]:not([disabled])").First)
            .ToBeVisibleAsync();

        // The detail strip has the same busy state for the same reason - its surface
        // tabs are disabled while the selected tree's surfaces resolve - and the walk
        // exercises it too, so it must be waited for on the same terms rather than
        // only the catalog strip.
        await Assertions
            .Expect(page.Locator("[role=tablist][aria-label='Detail tabs'] [role=tab]:not([disabled])").First)
            .ToBeVisibleAsync();

        // The rail settles independently of both, as its area gates report.
        await JourneyShell.AssertRailSettledAsync(page);

        await Assertions.Expect(page.Locator("[role=tablist]").First).ToBeAttachedAsync();

        // Snapshot the strips by the name each publishes, in one read, and address them
        // by that name afterwards. Walking [role=tablist] by index is a real race here
        // rather than a theoretical one: the rail and the detail strip re-render as
        // gates report and as a surface resolves, so an index taken before a re-render
        // can address an element that no longer exists - which is how this proof first
        // failed, in the harness rather than in the product.
        var strips = await page.Locator(":root").EvaluateAsync<StripSnapshot[]>(
            """
            () => Array.from(document.querySelectorAll('[role=tablist]')).map(s => ({
                label: s.getAttribute('aria-label') || '',
                vertical: s.getAttribute('aria-orientation') === 'vertical',
                operable: s.querySelectorAll('[role=tab]:not([disabled])').length,
            }))
            """);

        var exercised = new List<string>();
        var failures = new List<string>();

        foreach (var strip in strips)
        {
            if (strip.Operable < 2 || strip.Label.Length == 0)
            {
                // A strip with one operable tab has no arrow-key behaviour to exercise;
                // the roving-tabindex half below still covers it. An unlabelled strip
                // cannot be addressed by name, and is a different defect.
                continue;
            }

            var located = page.Locator($"[role=tablist][aria-label='{strip.Label}']");
            var tabs = located.Locator("[role=tab]:not([disabled])");
            var forward = strip.Vertical ? "ArrowDown" : "ArrowRight";

            await tabs.First.FocusAsync();
            var before = await FocusedTabIdAsync(page);
            if (before is null)
            {
                failures.Add($"'{strip.Label}' would not accept keyboard focus on its first tab");
                continue;
            }

            await page.Keyboard.PressAsync(forward);
            var after = await FocusedTabIdAsync(page);
            exercised.Add($"{strip.Label} ({forward})");

            if (after is null)
            {
                failures.Add($"pressing {forward} in '{strip.Label}' moved focus off the tabs entirely");
            }
            else if (after == before)
            {
                failures.Add($"pressing {forward} in '{strip.Label}' left focus on '{before}', so the "
                    + "strip is not keyboard operable");
            }

            // The other half of the pattern: exactly one tab of the strip is in the
            // document's tab sequence, so a caller tabs past the strip rather than
            // through it.
            var inSequence = await located.Locator("[role=tab][tabindex='0']").CountAsync();
            if (inSequence != 1)
            {
                failures.Add($"'{strip.Label}' puts {inSequence} tabs in the tab sequence; a roving "
                    + "tabindex requires exactly one");
            }
        }

        Assert.That(exercised, Is.Not.Empty,
            $"None of the {strips.Length} strips had two or more operable tabs, so this proof could "
            + "not tell an operable strip from an inert one.");

        Assert.That(failures, Is.Empty, () =>
            $"Exercised [{string.Join(", ", exercised)}]. A strip that announces role=tablist must "
            + "implement the keyboard behaviour that role implies (WCAG SC 2.1.1 Keyboard, level A)."
            + Environment.NewLine + string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// <b>Proof: no navigation path contains an upper-case character.</b>
    /// <para>
    /// Pre-epic the shell had no route grammar at all - the whole console lived at one
    /// address - so there was no lower-case contract to keep and nothing addressable to
    /// share. #1847 made every area, kind and surface a canonical lower-case slug and
    /// added a hygiene gate over the declared templates. That gate reads source; this
    /// reads the address bar after real navigation, which is the only place a slug
    /// composed at run time from a plugin id can be seen.
    /// </para>
    /// </summary>
    [Test]
    public async Task No_navigation_path_contains_an_upper_case_character()
    {
        var page = await OpenAtAsync("", ExpandedWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);

        var visited = new List<string>();
        var offenders = new List<string>();

        void Record(string label, string url)
        {
            var path = new Uri(url).AbsolutePath;
            visited.Add($"{label} -> {path}");
            if (path.Any(char.IsUpper))
            {
                offenders.Add($"{label} -> {path}");
            }
        }

        Record("entry", page.Url);

        var tabs = page.Locator(JourneyShell.RailTabSelector);
        var count = await tabs.CountAsync();
        Assert.That(count, Is.GreaterThan(1),
            $"Only {count} area was offered, so walking 'every navigation path' would visit one "
            + "address and prove nothing about slug composition.");

        for (var i = 0; i < count; i++)
        {
            var label = (await tabs.Nth(i).TextContentAsync() ?? string.Empty).Trim();
            await JourneyShell.OpenAreaAsync(page, label);
            Record(label, page.Url);
        }

        Assert.That(offenders, Is.Empty, () =>
            "A navigation path contains an upper-case character. Area, kind and surface slugs are "
            + "canonical lower case, so an upper-case segment means a slug was composed from a "
            + "display label or a plugin id instead of through ExplorerRouteSlug."
            + Environment.NewLine + string.Join(Environment.NewLine, offenders)
            + Environment.NewLine + "Visited:" + Environment.NewLine
            + string.Join(Environment.NewLine, visited));
    }

    private static Task<string?> FocusedTabIdAsync(IPage page) =>
        page.Locator(":root").EvaluateAsync<string?>(
            """
            () => {
                const el = document.activeElement;
                if (!el || el.getAttribute('role') !== 'tab') { return null; }
                return el.id || (el.textContent || '').trim();
            }
            """);
}

