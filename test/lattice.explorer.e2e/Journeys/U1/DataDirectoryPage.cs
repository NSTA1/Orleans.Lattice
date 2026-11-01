using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>
/// The Data directory as a data steward drives it (U1): the filters, Refresh, the listed
/// trees and views, and New tree from file....
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class DataDirectoryPage(PersonaSession session)
{
    private const string NameCell = "th:first-child, td:first-child";

    private ILocator Content => session.Shell.Content;

    /// <summary>The text filter.</summary>
    public ILocator Filter => Content.GetByLabel("Filter trees and views", new() { Exact = true });

    /// <summary>The kind filter's segmented group.</summary>
    public ILocator Kinds => Content.GetByRole(AriaRole.Group, new() { Name = "Show" });

    /// <summary>The Refresh control.</summary>
    public ILocator Refresh => Content.Locator("[data-lt-command=\"data.refresh\"]");

    /// <summary>The New tree from file... control.</summary>
    public ILocator NewTreeFromFile => Content.Locator("[data-lt-data-new-from-file]");

    /// <summary>The listing's rows.</summary>
    public ILocator Rows => Content.Locator("tbody tr.lt-table__row");

    /// <summary>Opens the directory from the spine and waits for its listing.</summary>
    public async Task OpenAsync()
    {
        await session.Shell.GoToAreaAsync("data");
        await session.Shell.ExpectHeadingAsync("Data");
        await WaitForListingAsync();
    }

    /// <summary>Waits until the listing has rows.</summary>
    public async Task WaitForListingAsync()
    {
        await Expect(Content.Locator(".lt-skeleton")).ToHaveCountAsync(0, new() { Timeout = 60_000 });
        await Expect(Rows.First).ToBeVisibleAsync();
    }

    /// <summary>The names in the listing, in order.</summary>
    public async Task<IReadOnlyList<string>> NamesAsync() =>
        [.. (await Rows.Locator(NameCell).AllInnerTextsAsync()).Select(name => name.Trim())];

    /// <summary>Types into the text filter and waits until every row's name contains it.</summary>
    /// <param name="text">The filter text.</param>
    public async Task FilterAsync(string text)
    {
        await Filter.FillAsync(text);
        await Expect(Rows.Locator(NameCell).Filter(new() { HasNotText = text })).ToHaveCountAsync(0);
        await Expect(Rows.First).ToBeVisibleAsync();
    }

    /// <summary>Chooses a kind (All, Trees, Views) and waits for it to be pressed.</summary>
    /// <param name="kind">The kind's label.</param>
    public async Task ShowAsync(string kind)
    {
        var button = Kinds.GetByRole(AriaRole.Button, new() { Name = kind, Exact = true });
        await button.ClickAsync();
        await Expect(button).ToHaveAttributeAsync("aria-pressed", "true");
    }

    /// <summary>Refreshes the listing through its <c>data.refresh</c> control.</summary>
    public async Task RefreshAsync()
    {
        await Expect(Refresh).ToHaveTextAsync("Refresh");
        await Refresh.ClickAsync();
        await WaitForListingAsync();
        Coverage.Hit("command:data.refresh");
    }

    /// <summary>The row whose name is exactly <paramref name="name"/>.</summary>
    /// <param name="name">The tree or view name.</param>
    public ILocator Row(string name) =>
        Rows.Filter(new() { Has = session.Page.Locator(NameCell, new() { HasTextRegex = new Regex("^\\s*" + Regex.Escape(name) + "\\s*$") }) });
}
