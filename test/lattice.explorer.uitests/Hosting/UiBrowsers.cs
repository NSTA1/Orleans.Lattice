using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The browsers the suite drives, launched on first use and shared by every fixture.
/// </summary>
/// <remarks>
/// Most fixtures run in Chromium only. The pilot and isolation fixtures run in
/// Chromium, Firefox and WebKit, because frame isolation is enforced by each engine's
/// own sandbox and policy implementation and the suite has to show it holds in all of
/// them. A browser that is not installed fails with the install command rather than
/// Playwright's own message.
/// </remarks>
internal static class UiBrowsers
{
    /// <summary>The engine name every fixture runs in unless it asks for more.</summary>
    public const string Chromium = "chromium";

    /// <summary>The Gecko engine.</summary>
    public const string Firefox = "firefox";

    /// <summary>The WebKit engine.</summary>
    public const string WebKit = "webkit";

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static readonly Dictionary<string, IBrowser> Launched = new(StringComparer.Ordinal);
    private static IPlaywright? _playwright;

    /// <summary>The browser for <paramref name="engine"/>, launching it on first use.</summary>
    /// <param name="engine">One of <see cref="Chromium"/>, <see cref="Firefox"/> or <see cref="WebKit"/>.</param>
    public static async Task<IBrowser> GetAsync(string engine)
    {
        await Gate.WaitAsync();
        try
        {
            if (Launched.TryGetValue(engine, out var browser))
            {
                return browser;
            }

            _playwright ??= await Microsoft.Playwright.Playwright.CreateAsync();
            var type = engine switch
            {
                Chromium => _playwright.Chromium,
                Firefox => _playwright.Firefox,
                WebKit => _playwright.Webkit,
                _ => throw new ArgumentOutOfRangeException(nameof(engine), engine, "Not a Playwright engine this suite runs."),
            };

            try
            {
                browser = await type.LaunchAsync(new BrowserTypeLaunchOptions { Headless = true });
            }
            catch (PlaywrightException ex)
            {
                throw new InvalidOperationException(
                    $"Playwright could not launch {engine}. This almost always means the browser is not installed. "
                    + "Build this project, then run:" + Environment.NewLine + Environment.NewLine
                    + "    pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium firefox webkit"
                    + Environment.NewLine + Environment.NewLine
                    + "(add --with-deps on a Linux agent). Playwright said: " + ex.Message,
                    ex);
            }

            Launched[engine] = browser;
            return browser;
        }
        finally
        {
            Gate.Release();
        }
    }

    /// <summary>Closes every launched browser and the Playwright driver.</summary>
    public static async Task DisposeAsync()
    {
        foreach (var browser in Launched.Values)
        {
            await browser.DisposeAsync();
        }

        Launched.Clear();
        _playwright?.Dispose();
        _playwright = null;
    }
}
