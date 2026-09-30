using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// How the suite reads and drives the Explorer's chrome: the readiness signal, the
/// landmarks, sign-in, the address line and the appearance controls. Every locator
/// here is a role, a name or a stable <c>data-lt-*</c> hook the Explorer ships, never
/// a styling class, so a restyle cannot break the suite and a broken role cannot pass
/// it.
/// </summary>
internal static class Shell
{
    /// <summary>A phone: below the medium breakpoint, so the compact chrome renders.</summary>
    public const int SmallWidth = 360;

    /// <summary>A tablet: the medium band, where the directory is a rail.</summary>
    public const int MediumWidth = 768;

    /// <summary>A desktop: the expanded band.</summary>
    public const int LargeWidth = 1280;

    /// <summary>The viewport height every page uses; only the width selects a band.</summary>
    public const int Height = 900;

    /// <summary>The key the chrome writes the first-paint appearance record under.</summary>
    public const string FirstPaintKey = "orleans.lattice.explorer.appearance.v2";

    /// <summary>
    /// Runs in every document and frame before any page script. It records three facts
    /// the suite cannot otherwise observe without a delay:
    /// <list type="bullet">
    /// <item>when the interactive circuit has applied the operator's appearance, which the
    /// chrome does only once the circuit is live (the readiness signal);</item>
    /// <item>every Content-Security-Policy violation the page reports, and every text message posted to it,
    /// and - as the promise <c>window.__ltFrameBlocked</c> - the first navigation of a frame the
    /// document's own <c>frame-src</c> refused;</item>
    /// <item>inside an app frame, how many <c>lattice.hello</c> messages the frame was sent.</item>
    /// </list>
    /// It adds nothing a page could use: it only records.
    /// </summary>
    public const string InitScript =
        """
        (() => {
          let ready;
          window.__ltReady = new Promise((resolve) => { ready = resolve; });
          try {
            const key = 'orleans.lattice.explorer.appearance.v2';
            const setItem = Storage.prototype.setItem;
            Storage.prototype.setItem = function (name, value) {
              const result = setItem.apply(this, arguments);
              if (name === key) { window.__ltShellInteractive = true; ready(true); }
              return result;
            };
          } catch (e) { }
          window.__ltViolations = [];
          let frameBlocked;
          window.__ltFrameBlocked = new Promise((resolve) => { frameBlocked = resolve; });
          window.__ltMessages = [];
          window.addEventListener('message', (e) => {
            if (typeof e.data === 'string') { window.__ltMessages.push(e.data); }
          });
          document.addEventListener('securitypolicyviolation', (e) => {
            const directive = e.effectiveDirective || e.violatedDirective;
            window.__ltViolations.push({ directive: directive, blocked: String(e.blockedURI) });
            if (directive === 'frame-src' || directive === 'child-src') { frameBlocked(directive); }
          });
          if (location.pathname.indexOf('/_apps/frame/') >= 0) {
            window.__ltHellos = 0;
            window.addEventListener('message', (e) => {
              if (e.data && e.data.type === 'lattice.hello') { window.__ltHellos += 1; }
            });
          }
        })();
        """;

    /// <summary>
    /// Waits until the interactive circuit is live on <paramref name="page"/>'s current
    /// document: the chrome has applied the operator's appearance, which it only does
    /// from the live circuit, never from the prerender.
    /// </summary>
    /// <param name="page">The page.</param>
    public static async Task WaitForInteractiveAsync(IPage page)
    {
        // Awaits the init script's promise rather than polling a predicate: the
        // Explorer's policy forbids eval, which Playwright's predicate polling needs. A
        // document that is being left never answers, so the wait moves on to the next
        // document when this one is torn down.
        using var deadline = new CancellationTokenSource(ReadyTimeout);
        while (true)
        {
            try
            {
                await page.EvaluateAsync<bool>("() => window.__ltLeaving === true ? new Promise(() => {}) : window.__ltReady")
                    .WaitAsync(deadline.Token);
                return;
            }
            catch (PlaywrightException ex) when (IsDocumentGone(ex))
            {
                deadline.Token.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"The Explorer at {page.Url} did not start its interactive circuit within {ReadyTimeout.TotalSeconds:0} seconds.");
            }
        }
    }

    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(60);

    private static bool IsDocumentGone(PlaywrightException ex) =>
        ex.Message.Contains("Execution context was destroyed", StringComparison.Ordinal)
        || ex.Message.Contains("navigation", StringComparison.OrdinalIgnoreCase)
        || ex.Message.Contains("Cannot find context", StringComparison.Ordinal)
        || ex.Message.Contains("context", StringComparison.OrdinalIgnoreCase);

    /// <summary>Navigates to <paramref name="path"/> on <paramref name="head"/> and waits for the live circuit.</summary>
    /// <param name="page">The page.</param>
    /// <param name="head">The head.</param>
    /// <param name="path">A head-relative path.</param>
    public static async Task GotoAsync(IPage page, ExplorerHead head, string path)
    {
        await page.GotoAsync(head.Url(path));
        await WaitForInteractiveAsync(page);
    }

    /// <summary>The header landmark.</summary>
    public static ILocator Banner(IPage page) => page.GetByRole(AriaRole.Banner);

    /// <summary>The main landmark.</summary>
    public static ILocator Content(IPage page) => page.Locator("main#lt-shell-content");

    /// <summary>The page's level-one heading, inside the main landmark.</summary>
    public static ILocator Heading(IPage page) => Content(page).Locator("h1").First;

    /// <summary>The directory spine, wherever it is shown (beside the content, or in its sheet).</summary>
    public static ILocator Directory(IPage page) => page.GetByRole(AriaRole.Navigation, new() { Name = "Estate" });

    /// <summary>The directory stop for <paramref name="areaKey"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="areaKey">The area key, such as <c>data</c>.</param>
    public static ILocator Stop(IPage page, string areaKey) => Directory(page).Locator($"a[data-lt-command=\"go.{areaKey}\"]");

    /// <summary>The address line's combobox, once the line is open.</summary>
    public static ILocator AddressInput(IPage page) => page.GetByRole(AriaRole.Combobox, new() { Name = "Address, search or command" });

    /// <summary>The address line's suggestions.</summary>
    public static ILocator Suggestions(IPage page) => page.GetByRole(AriaRole.Listbox, new() { Name = "Suggestions" }).GetByRole(AriaRole.Option);

    /// <summary>The toast region's toasts.</summary>
    public static ILocator Toasts(IPage page) => page.Locator(".lt-toast");

    /// <summary>The message text of each toast.</summary>
    public static ILocator ToastMessages(IPage page) => page.Locator(".lt-toast .lt-toast__message");

    /// <summary>The live region's latest announcement: read out by a screen reader, never drawn.</summary>
    public static ILocator Announcement(IPage page) => page.Locator(".lt-toasts > .lt-visually-hidden");

    /// <summary>
    /// Signs <paramref name="user"/> in through the Explorer's own sign-in dialog and its
    /// server form post, then waits for the redirected document's live circuit.
    /// </summary>
    /// <param name="page">A page showing the Explorer, signed out.</param>
    /// <param name="user">The user name.</param>
    public static async Task SignInAsync(IPage page, string user)
    {
        await OpenSignInAsync(page);
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await dialog.GetByLabel("Username").FillAsync(user);
        await dialog.GetByLabel("Password").FillAsync(WorldIdentities.Password);

        await SubmitAndWaitForNextDocumentAsync(page, dialog.Locator("form[method=post] button[type=submit]"));
        await ExpectSignedInAsync(page, user);
    }

    /// <summary>
    /// Activates <paramref name="control"/>, which leaves the current document (a form post
    /// and its redirect, a forced reload), and waits for the next document's live circuit.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="control">The control that navigates.</param>
    public static async Task SubmitAndWaitForNextDocumentAsync(IPage page, ILocator control)
    {
        await page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await control.ClickAsync();
        await WaitForInteractiveAsync(page);
    }
    /// <summary>Opens the sign-in dialog from the header (or, when compact, from its overflow menu).</summary>
    /// <param name="page">A page showing the Explorer, signed out.</param>
    public static async Task OpenSignInAsync(IPage page)
    {
        var menu = Banner(page).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });
        if (await menu.IsVisibleAsync())
        {
            await menu.ClickAsync();
            await page.GetByRole(AriaRole.Dialog, new() { Name = "Menu" }).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First.ClickAsync();
        }
        else
        {
            await Banner(page).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First.ClickAsync();
        }

        await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" })).ToBeVisibleAsync();
    }

    /// <summary>Asserts the header names <paramref name="user"/> as the signed-in identity.</summary>
    /// <param name="page">The page.</param>
    /// <param name="user">The user name.</param>
    public static async Task ExpectSignedInAsync(IPage page, string user)
    {
        var menu = Banner(page).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });
        if (await menu.IsVisibleAsync())
        {
            return;
        }

        await Expect(Banner(page).GetByRole(AriaRole.Button, new() { Name = $"Your session: {user}" })).ToBeVisibleAsync();
    }

    /// <summary>
    /// Opens the address line by its keyboard shortcut and waits for the combobox to take
    /// focus.
    /// </summary>
    /// <param name="page">The page.</param>
    public static async Task OpenAddressLineAsync(IPage page)
    {
        await page.Locator("body").FocusAsync();
        await page.Keyboard.PressAsync("Control+k");
        await Expect(AddressInput(page)).ToBeFocusedAsync();
    }

    /// <summary>
    /// Chooses the Explorer's appearance through its own appearance controls, then proves
    /// the document carries the choice.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="appearance">The appearance.</param>
    public static async Task SetAppearanceAsync(IPage page, ShellAppearanceChoice appearance)
    {
        foreach (var command in appearance.Commands)
        {
            var control = page.Locator($"button[data-lt-command=\"{command}\"]");
            if (!await control.IsVisibleAsync())
            {
                await OpenAppearanceControlsAsync(page);
            }

            await control.ClickAsync();
            await CloseAppearanceControlsAsync(page);
        }

        var root = page.Locator("html");
        await Expect(root).ToHaveAttributeAsync("data-bs-theme", appearance.ThemeAttribute);
        await Expect(root).ToHaveAttributeAsync("data-lt-contrast", appearance.ContrastAttribute);
        if (appearance.Compact)
        {
            await Expect(root).ToHaveAttributeAsync("data-lt-density", "compact");
        }
        else
        {
            await Expect(root).Not.ToHaveAttributeAsync("data-lt-density", new Regex(".*"));
        }
    }

    /// <summary>
    /// Waits for every running finite animation on the page to finish, so a measurement
    /// of computed style (axe's colour contrast, a bounding box) never reads a frame part
    /// way through a transition. Bounded, and event-driven rather than a delay.
    /// </summary>
    /// <param name="page">The page.</param>
    public static async Task WaitForMotionToSettleAsync(IPage page)
    {
        var moving = await page.EvaluateAsync<int>(
            """
            async () => {
              const deadline = performance.now() + 10000;
              const running = () => document.getAnimations().filter(a => a.playState === 'running'
                && a.effect !== null && Number.isFinite(a.effect.getComputedTiming().endTime));
              for (let now = running(); now.length > 0; now = running()) {
                const left = deadline - performance.now();
                if (left <= 0) { return now.length; }
                await Promise.race([
                  Promise.all(now.map(a => a.finished.catch(() => undefined))),
                  new Promise(resolve => setTimeout(resolve, left)),
                ]);
              }
              return 0;
            }
            """);

        Assert.That(moving, Is.Zero, $"{moving} animation(s) were still running ten seconds after the page was asked to settle.");
    }

    /// <summary>
    /// Asserts the page does not scroll horizontally (WCAG 2.2 SC 1.4.10 Reflow): the
    /// document is no wider than the viewport, so a table that is wider scrolls inside its
    /// own frame.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="where">What is being measured, for the failure message.</param>
    public static async Task AssertNoHorizontalPageScrollAsync(IPage page, string where)
    {
        var overflow = await page.EvaluateAsync<OverflowReport>(
            """
            () => {
              const root = document.documentElement;
              const width = root.clientWidth;
              const offenders = [];
              for (const element of document.body.querySelectorAll('*')) {
                const box = element.getBoundingClientRect();
                if (box.width === 0 || box.height === 0) { continue; }
                if (box.right > width + 1 || box.left < -1) {
                  let clipped = false;
                  for (let parent = element.parentElement; parent && parent !== document.body; parent = parent.parentElement) {
                    const style = getComputedStyle(parent);
                    if (/(auto|scroll|hidden|clip)/.test(style.overflowX)) {
                      const frame = parent.getBoundingClientRect();
                      if (frame.right <= width + 1 && frame.left >= -1) { clipped = true; break; }
                    }
                  }
                  if (!clipped) {
                    offenders.push(element.tagName.toLowerCase() + (element.id ? '#' + element.id : '') + '.' + String(element.className).split(' ').join('.') + ' right=' + Math.round(box.right));
                  }
                }
              }
              return { scrollWidth: root.scrollWidth, clientWidth: width, offenders: offenders.slice(0, 10) };
            }
            """);

        Assert.That(overflow.ScrollWidth, Is.LessThanOrEqualTo(overflow.ClientWidth), () =>
            $"{where} scrolls horizontally: the document is {overflow.ScrollWidth}px wide in a {overflow.ClientWidth}px viewport. "
            + "Content wider than the viewport must scroll inside its own frame (WCAG 2.2 SC 1.4.10). Elements past the edge: "
            + string.Join(", ", overflow.Offenders));
    }

    private static async Task OpenAppearanceControlsAsync(IPage page)
    {
        var menu = Banner(page).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });
        if (await menu.IsVisibleAsync())
        {
            await menu.ClickAsync();
            await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "Menu" })).ToBeVisibleAsync();
            return;
        }

        await page.Locator("button[data-lt-command=\"appearance.menu\"]").ClickAsync();
        await Expect(page.GetByRole(AriaRole.Group, new() { Name = "Appearance" })).ToBeVisibleAsync();
    }

    private static async Task CloseAppearanceControlsAsync(IPage page)
    {
        var toggle = page.Locator("button[data-lt-command=\"appearance.menu\"]");
        if (await toggle.IsVisibleAsync() && await toggle.GetAttributeAsync("aria-expanded") == "true")
        {
            await toggle.ClickAsync();
            await Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        }

        var sheet = page.GetByRole(AriaRole.Dialog, new() { Name = "Menu" });
        if (await sheet.IsVisibleAsync())
        {
            await page.Keyboard.PressAsync("Escape");
            await Expect(sheet).ToBeHiddenAsync();
        }
    }

    private sealed class OverflowReport
    {
        public int ScrollWidth { get; set; }

        public int ClientWidth { get; set; }

        public string[] Offenders { get; set; } = [];
    }
}
