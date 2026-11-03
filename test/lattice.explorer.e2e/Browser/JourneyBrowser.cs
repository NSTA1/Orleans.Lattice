using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;

/// <summary>
/// The smallest browser surface a journey needs from the estate harness: a Chromium engine,
/// a fresh context on an Explorer head, the readiness signal, and the Explorer's own sign-in
/// form. Personas, sessions and the screenshot manifest build on it (#5360).
/// </summary>
internal static class JourneyBrowser
{
    /// <summary>A desktop viewport width.</summary>
    public const int DesktopWidth = 1280;

    /// <summary>The viewport height.</summary>
    public const int Height = 900;

    /// <summary>
    /// Runs in every document before any page script, and records the moment the interactive
    /// circuit first writes the operator's appearance - which the chrome does only once the
    /// circuit is live - as the promise <c>window.__ltReady</c>.
    /// </summary>
    internal const string ReadyScript =
        """
        (() => {
          let ready;
          window.__ltReady = new Promise((resolve) => { ready = resolve; });
          try {
            const setItem = Storage.prototype.setItem;
            Storage.prototype.setItem = function (name, value) {
              const result = setItem.apply(this, arguments);
              if (name === 'orleans.lattice.explorer.appearance.v2') { ready(true); }
              return result;
            };
          } catch (e) { }
        })();
        """;

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static IPlaywright? _playwright;
    private static IBrowser? _browser;

    /// <summary>The shared Chromium instance, launched on first use.</summary>
    public static async Task<IBrowser> ChromiumAsync()
    {
        await Gate.WaitAsync();
        try
        {
            if (_browser is { IsConnected: true })
            {
                return _browser;
            }

            _playwright ??= await Playwright.CreateAsync();
            _browser = await _playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions { Headless = true });
            return _browser;
        }
        finally
        {
            Gate.Release();
        }
    }

    /// <summary>A fresh, isolated context on <paramref name="explorer"/>, with tracing on.</summary>
    /// <param name="explorer">The Explorer head's base address.</param>
    /// <param name="width">The viewport width.</param>
    public static async Task<IBrowserContext> NewContextAsync(Uri explorer, int width = DesktopWidth)
    {
        var browser = await ChromiumAsync();
        var context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            BaseURL = explorer.ToString(),
            IgnoreHTTPSErrors = true,
            ViewportSize = new ViewportSize { Width = width, Height = Height },
        });
        context.SetDefaultNavigationTimeout(90_000);
        context.SetDefaultTimeout(30_000);
        await context.AddInitScriptAsync(ReadyScript);
        await context.Tracing.StartAsync(new TracingStartOptions { Screenshots = true, Snapshots = true });
        return context;
    }

    /// <summary>Navigates to <paramref name="path"/> and waits for the live circuit.</summary>
    /// <param name="page">The page.</param>
    /// <param name="path">A head-relative path.</param>
    public static async Task GotoAsync(IPage page, string path)
    {
        await page.GotoAsync(path.TrimStart('/'));
        await WaitForInteractiveAsync(page);
    }

    /// <summary>Waits until the Explorer's interactive circuit is live on the current document.</summary>
    /// <param name="page">The page.</param>
    public static async Task WaitForInteractiveAsync(IPage page)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        while (true)
        {
            try
            {
                await page.EvaluateAsync<bool>("() => window.__ltLeaving === true ? new Promise(() => {}) : window.__ltReady")
                    .WaitAsync(deadline.Token);
                return;
            }
            catch (PlaywrightException) when (!deadline.IsCancellationRequested)
            {
                // The document was replaced mid-wait; wait on the next one.
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException($"The Explorer at {page.Url} did not start its interactive circuit within 60 seconds.");
            }
        }
    }

    /// <summary>
    /// Signs <paramref name="user"/> in through the Explorer's own sign-in form (its server form
    /// post and redirect) and waits until the header names them.
    /// </summary>
    /// <param name="page">A page showing the Explorer, signed out, at desktop width.</param>
    /// <param name="user">The user name.</param>
    /// <param name="password">The password.</param>
    public static async Task SignInAsync(IPage page, string user, string password)
    {
        var dialog = await OpenSignInAsync(page);
        await dialog.GetByLabel("Username").FillAsync(user);
        await dialog.GetByLabel("Password").FillAsync(password);
        await page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await dialog.Locator("form[method=post] button[type=submit]").ClickAsync();
        await WaitForInteractiveAsync(page);
        await Expect(SessionButton(page, user)).ToBeVisibleAsync();
    }

    /// <summary>Opens the sign-in dialog from the header.</summary>
    /// <param name="page">A page showing the Explorer, signed out.</param>
    public static async Task<ILocator> OpenSignInAsync(IPage page)
    {
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        var trigger = page.GetByRole(AriaRole.Banner).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First;
        await OpenDialogAsync(page, dialog, () => trigger.ClickAsync(new LocatorClickOptions { Timeout = (float)DialogAttempt.TotalMilliseconds }), "the sign-in dialog");
        return dialog;
    }

    /// <summary>How long one attempt waits for a dialog to open after its trigger is activated.</summary>
    internal static readonly TimeSpan DialogAttempt = TimeSpan.FromSeconds(15);

    /// <summary>The absolute deadline for a dialog to open, however many attempts it takes.</summary>
    internal static readonly TimeSpan DialogCeiling = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Opens <paramref name="dialog"/> with <paramref name="open"/> and waits until it is shown
    /// (#5744). A dialog is opened by the server over the interactive circuit, so on a loaded
    /// estate the render can lag the click, and a click made while the circuit reconnects is
    /// dropped. Each attempt waits for the live circuit, activates the trigger unless the dialog
    /// already opened, and waits <see cref="DialogAttempt"/> for it; the wait ends at
    /// <see cref="DialogCeiling"/>, and a dialog that never opens fails with the page's state
    /// and a screenshot of it.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="dialog">The dialog.</param>
    /// <param name="open">Activates the dialog's trigger.</param>
    /// <param name="what">The dialog, for the failure message.</param>
    /// <exception cref="TimeoutException">The dialog did not open by the ceiling.</exception>
    public static async Task OpenDialogAsync(IPage page, ILocator dialog, Func<Task> open, string what)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(dialog);
        ArgumentNullException.ThrowIfNull(open);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var attempts = new List<string>();
        while (true)
        {
            if (await dialog.IsVisibleAsync())
            {
                return;
            }

            try
            {
                await WaitForInteractiveAsync(page);
                if (!await dialog.IsVisibleAsync())
                {
                    await open();
                }

                await Expect(dialog).ToBeVisibleAsync(new LocatorAssertionsToBeVisibleOptions { Timeout = (float)DialogAttempt.TotalMilliseconds });
                return;
            }
            catch (Exception ex) when (ex is PlaywrightException or TimeoutException && clock.Elapsed < DialogCeiling)
            {
                attempts.Add($"{FirstLine(ex.Message)} by {clock.Elapsed.TotalSeconds:0} s");
            }
            catch (Exception ex) when (ex is PlaywrightException or TimeoutException)
            {
                attempts.Add($"{FirstLine(ex.Message)} by {clock.Elapsed.TotalSeconds:0} s");
                throw new TimeoutException(
                    $"{what} did not open within {DialogCeiling.TotalSeconds:0} seconds. Attempts: {string.Join("; ", attempts)}. Page: {await DescribeAsync(page)}",
                    ex);
            }
        }
    }

    /// <summary>The page's state for a failure message: its address, circuit, reconnect UI and open dialogs, and a screenshot's path.</summary>
    private static async Task<string> DescribeAsync(IPage page)
    {
        var parts = new List<string> { $"url {page.Url}" };
        try
        {
            parts.Add(await page.EvaluateAsync<string>(
                """
                async () => {
                  const ready = await Promise.race([window.__ltReady, new Promise((r) => setTimeout(() => r('pending'), 1000))]);
                  const reconnect = document.querySelector('#components-reconnect-modal, .components-reconnect-show, .components-reconnect-failed, .components-reconnect-rejected');
                  const dialogs = [...document.querySelectorAll('[role=dialog]')].map((d) => d.getAttribute('aria-label') || d.getAttribute('aria-labelledby') || '?');
                  return `circuit ${ready === true ? 'live' : ready}; reconnect UI ${reconnect ? (reconnect.className || reconnect.id) : 'none'}; dialogs [${dialogs.join(', ')}]; title ${document.title}`;
                }
                """));
        }
        catch (PlaywrightException ex)
        {
            parts.Add($"state unreadable ({FirstLine(ex.Message)})");
        }

        try
        {
            var shot = Path.Combine(Estate.JourneyPaths.ProjectDirectory, "artifacts", "dialog-failures", $"{DateTime.UtcNow:yyyyMMdd-HHmmss-fff}.png");
            await ScreenshotAsync(page, shot);
            parts.Add($"screenshot {shot}");
        }
        catch (PlaywrightException ex)
        {
            parts.Add($"no screenshot ({FirstLine(ex.Message)})");
        }

        return string.Join("; ", parts);
    }

    private static string FirstLine(string message)
    {
        var end = message.IndexOf('\n');
        return (end < 0 ? message : message[..end]).Trim();
    }

    /// <summary>The header's session button for <paramref name="user"/>, shown once they are signed in.</summary>
    /// <param name="page">The page.</param>
    /// <param name="user">The user name.</param>
    public static ILocator SessionButton(IPage page, string user) =>
        page.GetByRole(AriaRole.Banner).GetByRole(AriaRole.Button, new() { Name = $"Your session: {user}" });

    /// <summary>The main landmark's level-one heading.</summary>
    /// <param name="page">The page.</param>
    public static ILocator Heading(IPage page) => page.Locator("main#lt-shell-content h1").First;

    /// <summary>The directory stop for an area, such as <c>data</c>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="area">The area key.</param>
    public static ILocator Stop(IPage page, string area) =>
        page.GetByRole(AriaRole.Navigation, new() { Name = "Estate" }).Locator($"a[data-lt-command=\"go.{area}\"]");

    /// <summary>Writes a full-page screenshot to <paramref name="path"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="path">The PNG file.</param>
    public static Task<byte[]> ScreenshotAsync(IPage page, string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return page.ScreenshotAsync(new PageScreenshotOptions { Path = path, FullPage = true });
    }
}
