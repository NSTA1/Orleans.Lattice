using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Base class for every UI fixture. Provides a fresh, isolated Playwright browser
/// context and page per test (so no cookie, storage, or circuit state leaks between
/// tests) opened at a caller-chosen viewport, and navigates it to the running
/// Explorer web head's home surface with the interactive Blazor Server circuit
/// connected.
/// <para>
/// Every derived fixture must carry <c>[Category("UI")]</c>; the
/// <see cref="UiCategoryHygieneTests"/> gate enforces this so browser tests never
/// leak into a browser-free default filter.
/// </para>
/// </summary>
public abstract class UiTestBase
{
    private readonly List<IBrowserContext> _contexts = [];

    /// <summary>The base address of the running Explorer web head.</summary>
    protected static Uri BaseUri => ExplorerAppHostSetup.Host.BaseUri;

    /// <summary>
    /// Opens a fresh browser context sized to <paramref name="width"/> x
    /// <paramref name="height"/> and returns a new page in it. The viewport is set
    /// before any navigation, so the design system's <c>matchMedia</c>-based
    /// breakpoint observer classifies the correct band on the first circuit
    /// connection - the capability a headless server-render or a stubbed
    /// <c>window.innerWidth</c> cannot provide.
    /// </summary>
    protected async Task<IPage> NewPageAsync(int width, int height)
    {
        var context = await ExplorerAppHostSetup.Browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = width, Height = height },
        });
        _contexts.Add(context);
        return await context.NewPageAsync();
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
        // "circuit connected and viewport classified" without any fixed delay.
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

    /// <summary>Disposes every browser context opened during the test.</summary>
    [TearDown]
    public async Task DisposeContextsAsync()
    {
        foreach (var context in _contexts)
        {
            await context.DisposeAsync();
        }

        _contexts.Clear();
    }
}
