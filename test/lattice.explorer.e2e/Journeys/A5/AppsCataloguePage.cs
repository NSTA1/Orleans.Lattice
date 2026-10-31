using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>
/// The Apps area's landing (<c>/apps</c>) and its source catalogue (<c>/apps/catalogue</c>) as
/// journey A5 drives them: the empty state, the installed list, the source selector, the
/// All/Installed/Available/Updates filter, text search and Load more.
/// </summary>
/// <param name="shell">The Explorer's chrome on the session's page.</param>
internal sealed class AppsCataloguePage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The catalogue's count line: "{n} sources, {m}[+] apps".</summary>
    public ILocator Count => Content.GetByRole(AriaRole.Status).Filter(new() { HasTextRegex = new Regex(@"\d+ sources?, \d+\+? apps?") });

    /// <summary>The catalogue's rows.</summary>
    public ILocator Rows => Content.Locator("table tbody tr");

    /// <summary>Opens the Apps landing and asserts nothing is installed yet: "No apps yet".</summary>
    public async Task ExpectNoAppsYetAsync()
    {
        await shell.GotoAsync("/apps");
        await shell.ExpectHeadingAsync("Apps");
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "No apps yet", Exact = true })).ToBeVisibleAsync();
        await Expect(Content).ToContainTextAsync("No app is installed yet. Install one from the catalogue.");
        Coverage.Hit("route:/apps");
        Coverage.Hit("use-case:apps/area");
    }

    /// <summary>Runs the palette's <b>Install app...</b> command, which opens the catalogue on its Available filter.</summary>
    public async Task InstallFromPaletteAsync()
    {
        await shell.RunCommandAsync("Install app...");
        await Expect(Page).ToHaveURLAsync(new Regex(@"/apps/catalogue\?filter=available$"));
        await ExpectAvailableFilterAsync();
        Coverage.Hit("command:apps.install");
        Coverage.Hit("route:/apps/catalogue");
        Coverage.Hit("subroute:apps/catalogue");
        Coverage.Hit("query:apps.filter");
    }

    /// <summary>Asserts the pressed filter is Available and the first page is loaded.</summary>
    private async Task ExpectAvailableFilterAsync()
    {
        await Expect(FilterButton("Available")).ToHaveAttributeAsync("aria-pressed", "true");
        await Expect(Count).ToHaveTextAsync("2 sources, 50+ apps");
        await Expect(Rows).ToHaveCountAsync(50);
        Coverage.Hit("query-value:apps.filter=available");
    }

    /// <summary>Loads the catalogue's next page and asserts every app is now listed.</summary>
    /// <param name="total">The number of apps the selection holds.</param>
    public async Task LoadMoreAsync(int total)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Load more apps", Exact = true }).ClickAsync();
        await Expect(Count).ToHaveTextAsync($"2 sources, {total} apps");
        await Expect(Rows).ToHaveCountAsync(total);
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Load more apps", Exact = true })).ToHaveCountAsync(0);
    }

    /// <summary>Chooses <paramref name="source"/> in the source selector and waits for its listing.</summary>
    /// <param name="source">The source key.</param>
    public async Task SelectSourceAsync(string source)
    {
        await Content.GetByLabel("Source", new() { Exact = true }).SelectOptionAsync(source);
        await Expect(Page).ToHaveURLAsync(new Regex($@"[?&]source={Regex.Escape(source)}(&|$)"));
        await Expect(Content.GetByLabel("Source", new() { Exact = true })).ToHaveValueAsync(source);
        Coverage.Hit("query:apps.source");
    }

    /// <summary>Searches the catalogue for <paramref name="text"/>.</summary>
    /// <param name="text">The text.</param>
    public async Task SearchAsync(string text)
    {
        var search = Content.GetByRole(AriaRole.Searchbox, new() { Name = "Search apps" });
        await Expect(search).ToBeEnabledAsync();
        await search.FillAsync(text);
        await search.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex($@"[?&]q={Regex.Escape(text)}(&|$)"));
        Coverage.Hit("query:apps.q");
    }

    /// <summary>Presses one of the All/Installed/Available/Updates filter buttons.</summary>
    /// <param name="text">The button's text.</param>
    public async Task FilterAsync(string text)
    {
        await FilterButton(text).ClickAsync();
        await Expect(FilterButton(text)).ToHaveAttributeAsync("aria-pressed", "true");
        if (!string.Equals(text, "All", StringComparison.Ordinal))
        {
            Coverage.Hit($"query-value:apps.filter={text.ToLowerInvariant()}");
        }
    }

    /// <summary>Asserts the listing is empty and says <paramref name="why"/>.</summary>
    /// <param name="why">The empty state's sentence.</param>
    public async Task ExpectNoMatchAsync(string why)
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "No apps match", Exact = true })).ToBeVisibleAsync();
        await Expect(Content).ToContainTextAsync(why);
    }

    /// <summary>The catalogue row of <paramref name="displayName"/> offered by <paramref name="source"/>.</summary>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="source">The source key.</param>
    public ILocator Row(string displayName, string source) =>
        Rows.Filter(new() { HasText = displayName }).Filter(new() { Has = Page.Locator(".lt-apps-source", new() { HasTextString = source }) });

    /// <summary>Opens the row's primary action, such as <b>Review</b>, which opens the app's review page.</summary>
    /// <param name="action">The action's text.</param>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="source">The source key.</param>
    public async Task OpenRowActionAsync(string action, string displayName, string source)
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = $"{action} {displayName} from {source}", Exact = true }).ClickAsync();
        await shell.ExpectHeadingAsync(displayName);
    }

    /// <summary>Opens the Apps landing and asserts <paramref name="displayName"/> is installed in <paramref name="state"/>.</summary>
    /// <param name="slug">The app's slug.</param>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="version">The installed version.</param>
    /// <param name="state">The state pill's text.</param>
    public async Task ExpectInstalledAsync(string slug, string displayName, string version, string state)
    {
        await shell.GotoAsync("/apps");
        await shell.ExpectHeadingAsync("Apps");
        var installed = Content.GetByRole(AriaRole.Region, new() { Name = "Installed apps" });
        await Expect(installed.Locator($"[data-lt-installed-app='{slug}']")).ToHaveTextAsync(displayName);
        var row = installed.Locator("tbody tr").Filter(new() { HasText = displayName });
        await Expect(row).ToContainTextAsync(version);
        await Expect(row).ToContainTextAsync(state);
        Coverage.Hit("route:/apps");
    }

    private ILocator FilterButton(string text) =>
        Content.GetByRole(AriaRole.Group, new() { Name = "Show", Exact = true }).GetByRole(AriaRole.Button, new() { Name = text, Exact = true });
}
