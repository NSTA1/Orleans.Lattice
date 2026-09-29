using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Base class for every browser fixture: a fresh, isolated browser context per page
/// (no cookie, storage or circuit leaks between tests), opened in the fixture's
/// engine, with the suite's recording init script, a trace, and - on failure - the
/// trace, a screenshot, the page's HTML and what the heads and the browser logged,
/// written where the CI workflow uploads them.
/// </summary>
/// <remarks>
/// Every derived fixture carries <c>[Category("UI")]</c>; <see cref="UiCategoryHygieneTests"/>
/// enforces it.
/// </remarks>
public abstract class UiTestBase
{
    private static readonly ConcurrentDictionary<string, Task<string>> SignedIn = new(StringComparer.Ordinal);

    private readonly List<IBrowserContext> _contexts = [];
    private readonly List<IPage> _pages = [];
    private readonly List<string> _clientFaults = [];
    private readonly HashSet<ExplorerHead> _heads = [];

    /// <summary>Runs the fixture in Chromium.</summary>
    protected UiTestBase()
        : this(UiBrowsers.Chromium)
    {
    }

    /// <summary>Runs the fixture in <paramref name="engine"/>.</summary>
    /// <param name="engine">A Playwright engine name: <c>chromium</c>, <c>firefox</c> or <c>webkit</c>.</param>
    protected UiTestBase(string engine)
    {
        Engine = engine;
    }

    /// <summary>The engine this fixture runs in.</summary>
    protected string Engine { get; }

    /// <summary>
    /// Opens a page on <paramref name="head"/> in a fresh context, signed in as
    /// <paramref name="user"/> when one is given, navigates to <paramref name="path"/> and
    /// waits for the live circuit.
    /// </summary>
    /// <param name="head">The head.</param>
    /// <param name="path">A head-relative path.</param>
    /// <param name="user">The user to be signed in as, or <see langword="null"/> for signed out.</param>
    /// <param name="width">The viewport width.</param>
    /// <param name="configure">Further context options, such as an emulated preference.</param>
    private protected async Task<IPage> OpenAsync(
        ExplorerHead head,
        string path,
        string? user = null,
        int width = Shell.LargeWidth,
        Action<BrowserNewContextOptions>? configure = null)
    {
        var page = await NewPageAsync(head, user, width, configure);
        await Shell.GotoAsync(page, head, path);
        return page;
    }

    /// <summary>
    /// Opens a page on <paramref name="head"/> in a fresh context without navigating,
    /// signed in as <paramref name="user"/> when one is given.
    /// </summary>
    /// <param name="head">The head.</param>
    /// <param name="user">The user to be signed in as, or <see langword="null"/> for signed out.</param>
    /// <param name="width">The viewport width.</param>
    /// <param name="configure">Further context options.</param>
    private protected async Task<IPage> NewPageAsync(
        ExplorerHead head,
        string? user = null,
        int width = Shell.LargeWidth,
        Action<BrowserNewContextOptions>? configure = null)
    {
        _heads.Add(head);
        var options = ContextOptions(head, width);
        if (user is not null)
        {
            options.StorageState = await SignedInStateAsync(head, user);
        }

        configure?.Invoke(options);
        var browser = await UiBrowsers.GetAsync(Engine);
        var context = await browser.NewContextAsync(options);
        _contexts.Add(context);
        await context.AddInitScriptAsync(Shell.InitScript);
        await context.Tracing.StartAsync(new TracingStartOptions { Screenshots = true, Snapshots = true, Sources = true });

        var page = await context.NewPageAsync();
        page.Console += (_, message) =>
        {
            if (message.Type is "error" or "warning")
            {
                _clientFaults.Add($"  [console.{message.Type}] {message.Text}");
            }
        };
        page.PageError += (_, error) => _clientFaults.Add($"  [pageerror] {error}");

        await page.BringToFrontAsync();
        _pages.Add(page);
        return page;
    }

    /// <summary>Stops tracing, writes failure artifacts and disposes every context.</summary>
    [TearDown]
    public async Task DisposeContextsAsync()
    {
        var failed = TestContext.CurrentContext.Result.Outcome.Status == NUnit.Framework.Interfaces.TestStatus.Failed;
        var slug = Slug(TestContext.CurrentContext.Test.FullName);

        foreach (var head in _heads)
        {
            var faults = head.DescribeFaults();
            if (failed && faults is not null)
            {
                TestContext.Out.WriteLine(faults);
            }
        }

        if (failed && _clientFaults.Count > 0)
        {
            TestContext.Out.WriteLine("The browser reported:" + Environment.NewLine + string.Join(Environment.NewLine, _clientFaults));
        }

        for (var i = 0; i < _contexts.Count; i++)
        {
            if (failed && i < _pages.Count)
            {
                await DumpPageAsync(_pages[i], $"{slug}-{i}");
            }

            try
            {
                await _contexts[i].Tracing.StopAsync(new TracingStopOptions
                {
                    Path = failed ? Path.Combine(ArtifactDirectory("playwright-traces"), $"{slug}-{i}.zip") : null,
                });
            }
            catch (PlaywrightException)
            {
                // A context the test closed itself has no trace left to stop.
            }

            await _contexts[i].DisposeAsync();
        }

        _contexts.Clear();
        _pages.Clear();
        _clientFaults.Clear();
        _heads.Clear();
    }

    private BrowserNewContextOptions ContextOptions(ExplorerHead head, int width) => new()
    {
        ViewportSize = new ViewportSize { Width = width, Height = Shell.Height },
        IgnoreHTTPSErrors = true,
        BaseURL = head.BaseUri.ToString(),
    };

    // Signing in through the dialog once per head, engine and user, and reusing the
    // resulting encrypted cookie, keeps each test to what it is about. The sign-in flow
    // itself is a journey of its own.
    private Task<string> SignedInStateAsync(ExplorerHead head, string user) =>
        SignedIn.GetOrAdd($"{head.BaseUri}|{Engine}|{user}", _ => SignInOnceAsync(head, user));

    private async Task<string> SignInOnceAsync(ExplorerHead head, string user)
    {
        var browser = await UiBrowsers.GetAsync(Engine);
        await using var context = await browser.NewContextAsync(ContextOptions(head, Shell.LargeWidth));
        await context.AddInitScriptAsync(Shell.InitScript);
        var page = await context.NewPageAsync();
        await Shell.GotoAsync(page, head, "/");
        await Shell.SignInAsync(page, user);
        return await context.StorageStateAsync();
    }

    private static async Task DumpPageAsync(IPage page, string name)
    {
        try
        {
            await File.WriteAllTextAsync(Path.Combine(ArtifactDirectory("screenshots"), name + ".html"), await page.ContentAsync());
            await page.ScreenshotAsync(new PageScreenshotOptions
            {
                Path = Path.Combine(ArtifactDirectory("screenshots"), name + ".png"),
                FullPage = true,
            });
        }
        catch (PlaywrightException)
        {
            // A page that never rendered may refuse a screenshot; the trace remains.
        }
    }

    private static string ArtifactDirectory(string name)
    {
        var directory = Path.Combine(System.IO.Directory.GetCurrentDirectory(), name);
        System.IO.Directory.CreateDirectory(directory);
        return directory;
    }

    private static string Slug(string name)
    {
        var cleaned = Regex.Replace(name, "[^a-zA-Z0-9._-]", "_");
        return cleaned.Length > 120 ? cleaned[^120..] : cleaned;
    }
}
