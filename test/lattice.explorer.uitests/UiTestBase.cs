using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Base class for every UI fixture. Provides a fresh, isolated Playwright browser
/// context and page per test (so no cookie, storage, or circuit state leaks between
/// tests) opened at a caller-chosen viewport, and navigates it to the running
/// Explorer web head's home surface with the interactive Blazor Server circuit
/// connected.
/// <para>
/// Every context records a Playwright trace, and on a failed test the trace, a
/// screenshot, and the page HTML are written into the artifact directories the CI
/// workflow uploads (<c>playwright-traces/**</c>, <c>screenshots/**</c>). A browser
/// failure therefore leaves a concrete, inspectable record rather than only a bare
/// locator timeout in the test log.
/// </para>
/// <para>
/// Every derived fixture must carry <c>[Category("UI")]</c>; the
/// <see cref="UiCategoryHygieneTests"/> gate enforces this so browser tests never
/// leak into a browser-free default filter.
/// </para>
/// </summary>
public abstract class UiTestBase
{
    private readonly List<IBrowserContext> _contexts = [];
    private readonly List<IPage> _pages = [];

    /// <summary>
    /// The base address of the running Explorer web head this fixture drives.
    /// Defaults to the shared disconnected, signed-out head every accessibility
    /// fixture measures; the end-to-end journey suite overrides it to point at its own
    /// head, which composes the same shell over a demo cluster's facts.
    /// </summary>
    protected virtual Uri BaseUri => ExplorerAppHostSetup.Host.BaseUri;

    /// <summary>
    /// Opens a fresh browser context sized to <paramref name="width"/> x
    /// <paramref name="height"/> and returns a new page in it. The viewport is set
    /// before any navigation, so the design system's <c>matchMedia</c>-based
    /// breakpoint observer classifies the correct band on the first circuit
    /// connection - the capability a headless server-render or a stubbed
    /// <c>window.innerWidth</c> cannot provide. The context records a trace so a
    /// failure can be replayed.
    /// </summary>
    protected Task<IPage> NewPageAsync(int width, int height) =>
        NewPageAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = width, Height = height },
        });

    /// <summary>
    /// Opens a fresh browser context configured by <paramref name="options"/> and
    /// returns a new page in it. The overload exists so a fixture can emulate a user
    /// preference that only a browser context can express - <c>prefers-reduced-motion</c>
    /// and <c>forced-colors</c> among them - which is exactly the class of conformance
    /// criterion no renderer-based test can reach. The context records a trace so a
    /// failure can be replayed.
    /// </summary>
    /// <param name="options">The context options, including the viewport size.</param>
    protected async Task<IPage> NewPageAsync(BrowserNewContextOptions options)
    {
        var context = await ExplorerAppHostSetup.Browser.NewContextAsync(options);
        _contexts.Add(context);

        await context.Tracing.StartAsync(new TracingStartOptions
        {
            Screenshots = true,
            Snapshots = true,
            Sources = true,
        });

        var page = await context.NewPageAsync();

        // Every fixture shares one browser, so a page created earlier can still hold
        // the browser's focus target. Claim it on creation: a page without focus
        // reports document.hasFocus() === false, and its sequential (Tab) navigation
        // does not advance, which turns a keyboard assertion into a confusing claim
        // that the shell has nothing focusable.
        await page.BringToFrontAsync();

        _pages.Add(page);
        return page;
    }

    /// <summary>
    /// Opens a page at the given viewport, navigates to the Explorer home surface,
    /// and waits for the interactive Blazor Server circuit to establish so the
    /// viewport-driven layout has been applied. Returns the ready page.
    /// </summary>
    protected async Task<IPage> OpenHomeAsync(int width, int height)
    {
        var page = await NewPageAsync(width, height);
        await NavigateHomeAsync(page);
        return page;
    }

    /// <summary>
    /// Opens a page in a context configured by <paramref name="options"/>, navigates to
    /// the Explorer home surface, and waits for the interactive circuit. Use it when the
    /// state under test is a browser-level user preference rather than a page-level one.
    /// </summary>
    /// <param name="options">The context options, including the viewport size.</param>
    protected async Task<IPage> OpenHomeAsync(BrowserNewContextOptions options)
    {
        var page = await NewPageAsync(options);
        await NavigateHomeAsync(page);
        return page;
    }

    private async Task NavigateHomeAsync(IPage page)
    {
        await page.GotoAsync(BaseUri.ToString());

        // The shell frame is server-rendered, but the compact/expanded layout is only
        // settled once the interactive circuit connects and the breakpoint observer
        // reports the real viewport. ExplorerShell.WaitForShellReadyAsync waits on the
        // adaptive root's data-lx-measured="true", the deterministic, web-first signal
        // for "circuit connected and viewport classified", with no fixed delay. It is
        // shared with the post-sign-in redirect, which lands a second document and a
        // fresh circuit that must be waited on the same way.
        await ExplorerShell.WaitForShellReadyAsync(page);
    }

    /// <summary>
    /// Opens the Explorer home surface at the viewport width that lands squarely
    /// inside <paramref name="breakpoint"/>'s band, and asserts the design system
    /// genuinely classified the viewport into that band before returning.
    /// </summary>
    /// <param name="breakpoint">The breakpoint band to render in.</param>
    protected async Task<IPage> OpenHomeAsync(LatticeBreakpoint breakpoint)
    {
        var page = await OpenHomeAsync(ExplorerShell.ViewportWidth(breakpoint), ExplorerShell.ViewportHeight);
        await ExplorerShell.AssertBreakpointAsync(page, breakpoint);
        return page;
    }

    /// <summary>
    /// Stops tracing, writes failure artifacts when the test did not pass, and disposes
    /// every browser context opened during the test.
    /// </summary>
    [TearDown]
    public async Task DisposeContextsAsync()
    {
        var failed = TestContext.CurrentContext.Result.Outcome.Status
            == NUnit.Framework.Interfaces.TestStatus.Failed;
        var slug = Slug(TestContext.CurrentContext.Test.Name);

        for (var i = 0; i < _contexts.Count; i++)
        {
            var context = _contexts[i];
            var tracePath = failed
                ? Path.Combine(TraceDirectory, $"{slug}-{i}.zip")
                : null;

            if (failed)
            {
                await DumpPageArtifactsAsync(i, slug);
            }

            await context.Tracing.StopAsync(new TracingStopOptions { Path = tracePath });
            await context.DisposeAsync();
        }

        _contexts.Clear();
        _pages.Clear();
    }

    private async Task DumpPageArtifactsAsync(int index, string slug)
    {
        if (index >= _pages.Count)
        {
            return;
        }

        var page = _pages[index];
        try
        {
            var html = await page.ContentAsync();
            await File.WriteAllTextAsync(
                Path.Combine(ScreenshotDirectory, $"{slug}-{index}.html"), html);

            await page.ScreenshotAsync(new PageScreenshotOptions
            {
                Path = Path.Combine(ScreenshotDirectory, $"{slug}-{index}.png"),
                FullPage = true,
            });
        }
        catch (PlaywrightException)
        {
            // A page that never rendered may refuse a screenshot; the HTML dump and the
            // trace are the primary artifacts, so a screenshot failure is non-fatal.
        }
    }

    // The CI workflow uploads playwright-traces/** and screenshots/**. Resolve them
    // against the current working directory (where `dotnet test` runs), so the globs
    // match regardless of the build output layout.
    private static string TraceDirectory => EnsureDirectory("playwright-traces");

    private static string ScreenshotDirectory => EnsureDirectory("screenshots");

    private static string EnsureDirectory(string name)
    {
        var dir = Path.Combine(Directory.GetCurrentDirectory(), name);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static string Slug(string name)
    {
        var cleaned = Regex.Replace(name, "[^a-zA-Z0-9._-]", "_");
        return cleaned.Length > 120 ? cleaned[..120] : cleaned;
    }
}
