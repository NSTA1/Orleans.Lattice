using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Assembly-level fixture that owns the two run-wide singletons the UI suite shares:
/// the in-process Explorer web head (<see cref="ExplorerAppHost"/>) and the Playwright
/// driver plus its launched Chromium browser. Starting the head and the browser once
/// per run - rather than per fixture - keeps the suite fast as it grows.
/// <para>
/// If the Chromium browser binary is not installed, the launch fails here with an
/// actionable message telling the developer exactly which command to run, rather than
/// surfacing an opaque Playwright driver error deep inside a test.
/// </para>
/// </summary>
[SetUpFixture]
public sealed class ExplorerAppHostSetup
{
    private ExplorerAppHost? _host;
    private IPlaywright? _playwright;
    private IBrowser? _browser;

    /// <summary>The running Explorer web head shared by every UI fixture.</summary>
    public static ExplorerAppHost Host =>
        Instance._host ?? throw new InvalidOperationException(NotStarted);

    /// <summary>The launched Chromium browser shared by every UI fixture.</summary>
    public static IBrowser Browser =>
        Instance._browser ?? throw new InvalidOperationException(NotStarted);

    private static ExplorerAppHostSetup Instance =>
        _instance ?? throw new InvalidOperationException(NotStarted);

    private static ExplorerAppHostSetup? _instance;

    private const string NotStarted =
        "The UI-test host is not running. This property is only valid while the "
        + nameof(ExplorerAppHostSetup)
        + " one-time setup has completed and before its teardown.";

    /// <summary>
    /// Starts the Explorer web head and launches Chromium once, before any UI fixture
    /// runs.
    /// </summary>
    [OneTimeSetUp]
    public async Task StartAsync()
    {
        _instance = this;
        _host = await ExplorerAppHost.StartAsync();

        _playwright = await Microsoft.Playwright.Playwright.CreateAsync();
        _browser = await LaunchChromiumOrExplainAsync(_playwright);

        // Web-first assertions (Assertions.Expect(...)) default to a 5s timeout. That
        // is ample for a DOM already settled, but a compact-reflow assertion fires
        // immediately after the interactive Blazor Server circuit connects, and the
        // shell's re-render to the stacked layout can trail the circuit handshake by
        // more than 5s on a loaded agent. Raising the default (rather than sprinkling
        // per-assertion timeouts, or worse, a fixed delay) keeps every assertion
        // web-first and auto-waiting while giving the circuit room to settle.
        Assertions.SetDefaultExpectTimeout(15_000);
    }

    /// <summary>Stops the browser, the Playwright driver, and the web head.</summary>
    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_browser is not null)
        {
            await _browser.DisposeAsync();
        }

        _playwright?.Dispose();

        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        _instance = null;
    }

    private static async Task<IBrowser> LaunchChromiumOrExplainAsync(IPlaywright playwright)
    {
        try
        {
            return await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = true,
            });
        }
        catch (PlaywrightException ex)
        {
            // The most common cause is a missing browser payload. Playwright's own
            // message is developer-hostile ("Executable doesn't exist at ..."), so
            // wrap it in the exact install command for this repo's layout.
            throw new InvalidOperationException(
                "Playwright could not launch Chromium. This almost always means the browser "
                + "binaries are not installed. Build this project, then install Chromium with:"
                + Environment.NewLine
                + Environment.NewLine
                + "    pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium"
                + Environment.NewLine
                + Environment.NewLine
                + "(add --with-deps on CI Linux agents). See the original Playwright error below."
                + Environment.NewLine
                + ex.Message,
                ex);
        }
    }
}
