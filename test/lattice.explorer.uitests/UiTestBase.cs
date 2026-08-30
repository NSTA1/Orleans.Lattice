using System.Text.RegularExpressions;
using Microsoft.Playwright;

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

    /// <summary>The base address of the running Explorer web head.</summary>
    protected static Uri BaseUri => ExplorerAppHostSetup.Host.BaseUri;

    /// <summary>
    /// Opens a fresh browser context sized to <paramref name="width"/> x
    /// <paramref name="height"/> and returns a new page in it. The viewport is set
    /// before any navigation, so the design system's <c>matchMedia</c>-based
    /// breakpoint observer classifies the correct band on the first circuit
    /// connection - the capability a headless server-render or a stubbed
    /// <c>window.innerWidth</c> cannot provide. The context records a trace so a
    /// failure can be replayed.
    /// </summary>
    protected async Task<IPage> NewPageAsync(int width, int height)
    {
        var context = await ExplorerAppHostSetup.Browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = width, Height = height },
        });
        _contexts.Add(context);

        await context.Tracing.StartAsync(new TracingStartOptions
        {
            Screenshots = true,
            Snapshots = true,
            Sources = true,
        });

        var page = await context.NewPageAsync();
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
        await page.GotoAsync(BaseUri.ToString());

        // The shell frame is server-rendered, but the compact/expanded layout is only
        // settled once the interactive circuit connects and the breakpoint observer
        // reports the real viewport. The adaptive root marks itself measured only after
        // the circuit is live and the breakpoint observer has run, so waiting for
        // data-lx-measured="true" is the deterministic, web-first way to synchronize on
        // "circuit connected and viewport classified" without any fixed delay. This
        // wait failing is also the earliest, clearest signal that the app did not
        // render at all (for example because the framework asset 404'd) - which the
        // host's own startup probe should already have caught.
        await page.Locator(".lx-root[data-lx-measured='true']").First.WaitForAsync(new LocatorWaitForOptions
        {
            State = WaitForSelectorState.Attached,
            Timeout = CircuitReadyTimeoutMs,
        });

        return page;
    }

    /// <summary>
    /// The time to allow for the Blazor Server circuit to connect and report the
    /// viewport breakpoint. Generous relative to a local connect (which is well under
    /// a second) so a loaded CI agent does not flake, while still failing in bounded
    /// time if the circuit never establishes.
    /// </summary>
    private const float CircuitReadyTimeoutMs = 30_000;

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
