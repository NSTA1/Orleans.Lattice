using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// A small driver over a live Explorer page: the operations the accessibility
/// harness needs to put the shell into a named state (a theme, a breakpoint band,
/// a signed-in or signed-out identity, a chosen area) and - crucially - to
/// <b>prove that state is genuinely in effect</b> before anything is asserted about
/// it.
/// <para>
/// The proving is the point. axe reports zero violations on a blank document, so an
/// accessibility sweep passes hardest exactly when the app is most broken; the same
/// trap reappears once per dimension the sweep is widened along. A run that thought
/// it was measuring the light palette but silently kept the dark one, or thought it
/// was compact but rendered expanded, would report a clean pass for a surface it
/// never looked at. Every helper here therefore asserts an observable consequence of
/// the state - a resolved token, the measured breakpoint attribute, a rendered
/// username - rather than trusting the instruction it just issued.
/// </para>
/// <para>
/// Every wait is a Playwright web-first assertion or locator wait, so the driver
/// auto-retries against a settling DOM and contains no fixed delay, no polling loop,
/// and no wall-clock dependence.
/// </para>
/// </summary>
internal static class ExplorerShell
{
    /// <summary>The viewport height every sweep uses; only width selects a band.</summary>
    internal const int ViewportHeight = 900;

    /// <summary>
    /// The time to allow for the Blazor Server circuit to connect and report the
    /// viewport breakpoint. Generous relative to a local connect (well under a
    /// second) so a loaded CI agent does not flake, while still failing in bounded
    /// time if the circuit never establishes.
    /// </summary>
    internal const float CircuitReadyTimeoutMs = 30_000;

    /// <summary>
    /// The username the harness signs in as. The seeded endpoint is deliberately
    /// unreachable, so nothing validates this pair against a server: the sign-in
    /// exercises the shell's authenticated <i>rendering</i> path, which is what the
    /// sweep needs. These are placeholders, not credentials.
    /// </summary>
    private const string TestUsername = "ui-test-operator";

    private const string TestPassword = "not-a-secret-unreachable-endpoint";

    private const string ShellSelector = ".lx-shell";
    private const string SignInButtonSelector = ".lx-shell-auth-signin";
    private const string SignedInNameSelector = ".lx-shell-auth-name";
    private const string LoginFormSelector = ".lx-shell-config-overlay form";
    private const string LoginDialogSelector = ".lx-shell-config-overlay [role=dialog]";
    private const string AreaStripTabSelector = ".lx-shell-areastrip [role=tab]";
    private const string AreaStripSelectedTabSelector = ".lx-shell-areastrip [role=tab][aria-selected='true']";
    private const string AreaOverflowToggleSelector = ".lx-shell-areastrip .lx-tabstrip-overflow-toggle";

    // The three band selectors are composed once as constants rather than interpolated
    // per assertion: the sweep asserts the band on every case and after every area
    // activation, and the selector never varies for a given band.
    private const string CompactRootSelector = ".lx-root[data-lx-measured='true'][data-lx-breakpoint='compact']";
    private const string MediumRootSelector = ".lx-root[data-lx-measured='true'][data-lx-breakpoint='medium']";
    private const string ExpandedRootSelector = ".lx-root[data-lx-measured='true'][data-lx-breakpoint='expanded']";

    /// <summary>
    /// Reads a document-element custom property and the painted background of the
    /// shell in one style recalculation, after applying <c>data-theme</c>.
    /// </summary>
    private const string ApplyThemeScript =
        """
        value => {
            document.documentElement.setAttribute('data-theme', value);
            const shell = document.querySelector('.lx-shell');
            const token = getComputedStyle(document.documentElement)
                .getPropertyValue('--lx-color-canvas').trim();
            const painted = shell ? getComputedStyle(shell).backgroundColor : '';
            return token + ' / ' + painted;
        }
        """;

    /// <summary>Reads every area-strip tab label from a single DOM snapshot.</summary>
    private const string AreaLabelsScript =
        """
        () => Array.from(document.querySelectorAll('.lx-shell-areastrip [role=tab]'))
            .map(tab => (tab.textContent || '').trim())
        """;

    /// <summary>The viewport width that lands squarely inside <paramref name="breakpoint"/>'s band.</summary>
    /// <param name="breakpoint">The band to size for.</param>
    internal static int ViewportWidth(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        // Representative phone / tablet / desktop widths, each well inside its band
        // rather than on a boundary, so a one-pixel rounding difference in the
        // browser's matchMedia evaluation cannot reclassify the run.
        LatticeBreakpoint.Compact => 390,
        LatticeBreakpoint.Medium => 800,
        LatticeBreakpoint.Expanded => 1400,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };

    /// <summary>The <c>data-theme</c> attribute value that selects <paramref name="theme"/>.</summary>
    /// <param name="theme">The theme to name.</param>
    internal static string ThemeAttributeValue(ExplorerTheme theme) => theme switch
    {
        ExplorerTheme.Dark => "dark",
        ExplorerTheme.Light => "light",
        _ => throw new ArgumentOutOfRangeException(nameof(theme), theme, "Unknown theme."),
    };

    /// <summary>
    /// Waits for the shell to have rendered and for the interactive circuit to have
    /// reported the real viewport, which is when the adaptive root marks itself
    /// measured. This is the deterministic, web-first way to synchronize on "circuit
    /// connected and viewport classified" without any fixed delay, and its failure is
    /// the earliest clear signal that the app did not render at all.
    /// </summary>
    /// <param name="page">The page to wait on.</param>
    internal static Task WaitForShellReadyAsync(IPage page) =>
        page.Locator(".lx-root[data-lx-measured='true']").First.WaitForAsync(new LocatorWaitForOptions
        {
            State = WaitForSelectorState.Attached,
            Timeout = CircuitReadyTimeoutMs,
        });

    /// <summary>
    /// Asserts the shell rendered interactive content, so nothing downstream can pass
    /// against a blank or half-rendered document. This is the original false-pass
    /// guard the sweep has always carried, hoisted so every dimension inherits it.
    /// </summary>
    /// <param name="page">The page to check.</param>
    internal static Task AssertShellRenderedAsync(IPage page) =>
        Assertions.Expect(page.Locator("[role=tab]").First).ToBeAttachedAsync();

    /// <summary>
    /// Applies <paramref name="theme"/> and proves the palette genuinely took effect
    /// by measuring what the browser resolved, not by trusting the attribute written.
    /// <para>
    /// Both palettes are measured, so the check also fails if the two themes resolve
    /// to the same canvas - which would mean the "light theme" run was silently a
    /// second dark run, and every light-theme result in the suite was worthless. That
    /// is a real hazard here rather than a hypothetical one: the light palette's
    /// surface, raised and sunken tokens are currently all <c>#ffffff</c>, so palette
    /// collapse is a live failure mode in this codebase.
    /// </para>
    /// </summary>
    /// <param name="page">The page to theme.</param>
    /// <param name="theme">The theme to apply.</param>
    internal static async Task ApplyThemeAsync(IPage page, ExplorerTheme theme)
    {
        var dark = await ResolveThemeAsync(page, ExplorerTheme.Dark);
        var light = await ResolveThemeAsync(page, ExplorerTheme.Light);

        Assert.That(light, Is.Not.EqualTo(dark), () =>
            "The light and dark palettes resolved to the same canvas colour and painted background "
            + $"('{dark}'), so selecting a theme changes nothing that renders. Every light-theme "
            + "result in this suite would be a duplicate dark-theme result reported as light-theme "
            + "coverage.");

        var expected = theme == ExplorerTheme.Dark ? dark : light;
        var applied = await ResolveThemeAsync(page, theme);

        Assert.That(applied, Is.EqualTo(expected), () =>
            $"Selecting the {theme} theme did not put its palette in effect: the document resolved "
            + $"'{applied}' where the {theme} palette resolves '{expected}'.");
    }

    /// <summary>
    /// Asserts the design system classified the viewport as <paramref name="breakpoint"/>
    /// and has finished measuring it.
    /// <para>
    /// The band is read from the adaptive root's own <c>data-lx-breakpoint</c>, which
    /// is written from the <c>matchMedia</c> observer's answer. Asserting the viewport
    /// size we asked for would prove nothing: the whole reason these tests need a real
    /// browser is that the breakpoint comes from the browser, not from the request.
    /// </para>
    /// </summary>
    /// <param name="page">The page to check.</param>
    /// <param name="breakpoint">The band expected to be in effect.</param>
    internal static Task AssertBreakpointAsync(IPage page, LatticeBreakpoint breakpoint) =>
        Assertions.Expect(page.Locator(RootSelectorFor(breakpoint))).ToHaveCountAsync(1);

    private static string RootSelectorFor(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => CompactRootSelector,
        LatticeBreakpoint.Medium => MediumRootSelector,
        LatticeBreakpoint.Expanded => ExpandedRootSelector,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };

    /// <summary>Asserts no credential is applied, so the shell offers its sign-in affordance.</summary>
    /// <param name="page">The page to check.</param>
    internal static Task AssertSignedOutAsync(IPage page) =>
        Assertions.Expect(page.Locator(SignInButtonSelector)).ToBeVisibleAsync();

    /// <summary>Asserts a credential is applied, so the shell renders the signed-in identity.</summary>
    /// <param name="page">The page to check.</param>
    internal static Task AssertSignedInAsync(IPage page) => AssertSignedInAsync(page, TestUsername);

    /// <summary>
    /// Asserts the shell renders <paramref name="username"/> as the signed-in identity.
    /// </summary>
    /// <param name="page">The page to check.</param>
    /// <param name="username">The identity expected to be in effect.</param>
    internal static Task AssertSignedInAsync(IPage page, string username) =>
        Assertions.Expect(page.Locator(SignedInNameSelector)).ToHaveTextAsync(username);

    /// <summary>
    /// Signs in through the shell's own affordance - open the dialog, fill the form,
    /// submit it - rather than by reaching past the UI into the auth session.
    /// <para>
    /// The web head configures a server form post, so the submit is a real
    /// antiforgery-guarded <c>POST /auth/login</c> that writes the encrypted credential
    /// cookie and redirects home. The browser context keeps that cookie, so every later
    /// navigation on the same page stays signed in. Driving the real path is what makes
    /// this a genuine signed-in sweep rather than a signed-out sweep with a flag set.
    /// </para>
    /// </summary>
    /// <param name="page">The page to sign in on.</param>
    internal static Task SignInAsync(IPage page) => SignInAsync(page, TestUsername);

    /// <summary>
    /// Signs in as <paramref name="username"/> through the shell's own affordance, and
    /// proves the shell rendered that identity before returning.
    /// <para>
    /// The endpoint is deliberately unreachable so nothing validates the pair against a
    /// server; the username is what the head's own seams key their answers off, which
    /// is how the journey suite tells a platform operator from a restricted reader
    /// without a cluster.
    /// </para>
    /// </summary>
    /// <param name="page">The page to sign in on.</param>
    /// <param name="username">The identity to sign in as.</param>
    internal static async Task SignInAsync(IPage page, string username)
    {
        await Assertions.Expect(page.Locator(SignInButtonSelector)).ToBeVisibleAsync();
        await page.Locator(SignInButtonSelector).ClickAsync();

        var form = page.Locator(LoginFormSelector);
        await Assertions.Expect(form).ToBeVisibleAsync();

        await form.Locator("input[name='username']").FillAsync(username);
        await form.Locator("input[name='password']").FillAsync(TestPassword);
        await form.Locator("button[type='submit']").ClickAsync();

        // The POST redirects home, so a second document loads and a fresh circuit
        // connects. Wait for that circuit rather than for the navigation itself, then
        // assert the identity actually rendered - a sign-in that silently failed would
        // otherwise hand every "signed in" test case a signed-out surface.
        await WaitForShellReadyAsync(page);
        await AssertSignedInAsync(page, username);
    }

    /// <summary>
    /// The labels of every area the shell currently offers in its strip, in render
    /// order.
    /// <para>
    /// The set is read from the live strip rather than hard-coded, so an area added,
    /// removed, or newly admitted by a gate is swept without editing this file. It is
    /// also asserted to be complete for the moment it is read: if the strip has
    /// overflowed, some areas are hidden behind a menu and enumerating the inline tabs
    /// would silently sweep a subset, so that case fails loudly instead. Callers
    /// therefore enumerate at a width wide enough to render every area inline.
    /// </para>
    /// <para>
    /// <b>The set is a snapshot, not a settled answer.</b> Every plugin's access
    /// defaults to denied until its gate reports, and a gate that reports
    /// <i>unavailable</i> withdraws its area from the strip entirely - so an area can
    /// appear in one read and be gone from the next, and vice versa once a credential
    /// is applied. There is no DOM signal for "every gate has reported", so a caller
    /// that needs the settled set re-reads until the set stops changing rather than
    /// trusting one read. <see cref="TryActivateAreaAsync"/> is written to make that
    /// cheap: it reports an area that has since been withdrawn as simply unreachable,
    /// without waiting for it to come back.
    /// </para>
    /// </summary>
    /// <param name="page">The page whose area strip to enumerate.</param>
    internal static async Task<IReadOnlyList<string>> OfferedAreaLabelsAsync(IPage page)
    {
        await AssertShellRenderedAsync(page);

        await Assertions
            .Expect(page.Locator(AreaOverflowToggleSelector))
            .ToHaveCountAsync(0);

        // Read every label in one evaluation. Taking a count and then indexing the
        // locator per element is a real race here rather than a theoretical one: the
        // strip re-renders whenever a gate reports, so an index resolved against the
        // pre-settle strip can address an element that no longer exists by the time it
        // is read, and Playwright then waits out its whole action timeout on a tab that
        // is never coming back.
        var labels = await page.EvaluateAsync<string[]>(AreaLabelsScript);

        Assert.That(labels.Length, Is.GreaterThan(1),
            "The area strip offered fewer than two areas, so an 'every area' sweep would be "
            + "indistinguishable from a home-only sweep. Either the plugin catalogue failed to "
            + "register or every gate withheld its area.");

        return labels;
    }

    /// <summary>
    /// Activates the area labelled <paramref name="label"/> and reports whether it
    /// became the active area.
    /// <para>
    /// Three outcomes are legitimate rather than failures, and all three report
    /// <see langword="false"/>: the area has been withdrawn since it was enumerated (its
    /// gate reported unavailable), it is rendered disabled because its gate denied the
    /// caller, or it stays clickable but opens the sign-in dialog because its gate
    /// requires authentication. The caller records the area as unreachable for this
    /// identity. The first two are decided by a point-in-time read rather than by
    /// waiting, so a withdrawn area costs nothing instead of burning a locator timeout.
    /// </para>
    /// <para>
    /// Waiting on "either the tab became selected or the sign-in dialog opened" keeps
    /// the third decision web-first: there is no timeout race and no fixed delay
    /// deciding which happened.
    /// </para>
    /// </summary>
    /// <param name="page">The page to act on.</param>
    /// <param name="label">The exact area label to activate.</param>
    /// <returns><see langword="true"/> when the area became active.</returns>
    internal static async Task<bool> TryActivateAreaAsync(IPage page, string label)
    {
        var tab = page.GetByRole(AriaRole.Tab, new PageGetByRoleOptions { Name = label, Exact = true });

        if (await tab.CountAsync() == 0 || !await tab.IsEnabledAsync())
        {
            return false;
        }

        await tab.ClickAsync();

        var selected = page.Locator(AreaStripSelectedTabSelector)
            .Filter(new LocatorFilterOptions { HasTextString = label });
        var loginDialog = page.Locator(LoginDialogSelector);

        await Assertions.Expect(selected.Or(loginDialog).First).ToBeVisibleAsync();

        if (await loginDialog.CountAsync() > 0)
        {
            await loginDialog.GetByRole(AriaRole.Button, new LocatorGetByRoleOptions { Name = "Cancel" })
                .ClickAsync();
            await Assertions.Expect(loginDialog).ToHaveCountAsync(0);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Reads the resolved canvas token and the shell's painted background after
    /// selecting <paramref name="theme"/>. Both are read in one evaluation so they
    /// come from a single style recalculation.
    /// </summary>
    private static async Task<string> ResolveThemeAsync(IPage page, ExplorerTheme theme)
    {
        await Assertions.Expect(page.Locator(ShellSelector)).ToBeAttachedAsync();
        return await page.EvaluateAsync<string>(ApplyThemeScript, ThemeAttributeValue(theme));
    }
}
