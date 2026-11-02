using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// The Replication area's estate page, <c>/replication</c>, and the parts it shares with the
/// enrolled-trees page: the section row, the filter toolbar (<c>?health=</c>, <c>?region=</c>,
/// <c>?app=</c>) and the link table.
/// </summary>
internal sealed class ReplicationEstateScreen(ShellPage shell)
{
    /// <summary>The command that re-reads every peer link.</summary>
    public const string RefreshCommandTitle = "Refresh replication status";

    /// <summary>The command that shows the enrolled trees.</summary>
    public const string TreesCommandTitle = "Show enrolled trees";

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The filter toolbar.</summary>
    public ILocator Toolbar => Shell.Content.GetByRole(AriaRole.Group, new() { Name = "Filter replication links" });

    /// <summary>The health filter.</summary>
    public ILocator HealthFilter => Toolbar.GetByLabel("Health", new() { Exact = true });

    /// <summary>The peer-region filter.</summary>
    public ILocator RegionFilter => Toolbar.GetByLabel("Peer region", new() { Exact = true });

    /// <summary>The owning-app filter.</summary>
    public ILocator AppFilter => Toolbar.GetByLabel("App", new() { Exact = true });

    /// <summary>The toolbar's <b>Clear filters</b>, drawn only while a filter is set.</summary>
    public ILocator ClearFilters => Toolbar.GetByRole(AriaRole.Button, new() { Name = "Clear filters", Exact = true });

    /// <summary>The status line: how many links are shown, and when they were read.</summary>
    public ILocator StatusLine => Shell.Content.GetByRole(AriaRole.Status).Filter(new() { HasTextRegex = new Regex("Read at") });

    /// <summary>The visible <b>Refresh</b> button beside the status line.</summary>
    public ILocator RefreshButton => Shell.Content.Locator("button[data-lt-command=\"replication.refresh\"]");

    /// <summary>The estate's order diagram: this region and its peers.</summary>
    public ILocator Map => Shell.Content.Locator("figure").Filter(new() { HasText = "This region" });

    /// <summary>The section row's link to the estate.</summary>
    public ILocator EstateSection => Sections.GetByRole(AriaRole.Link, new() { Name = "Estate", Exact = true });

    /// <summary>The section row's link to the enrolled trees.</summary>
    public ILocator TreesSection => Sections.GetByRole(AriaRole.Link, new() { Name = "Enrolled trees", Exact = true });

    /// <summary>The area's section row (or, on one tree's page, its way back).</summary>
    public ILocator Sections => Shell.Content.GetByRole(AriaRole.Navigation, new() { Name = "Replication sections" });

    /// <summary>The map's node for peer region <paramref name="region"/>.</summary>
    /// <param name="region">The peer region id.</param>
    public ILocator PeerNode(string region) =>
        Map.GetByRole(AriaRole.List, new() { Name = "Peer regions" }).GetByRole(AriaRole.Link, new() { NameRegex = new Regex("^" + Regex.Escape(region) + "\\b") });

    /// <summary>The map's node for this region.</summary>
    /// <param name="region">This region's id.</param>
    public ILocator LocalNode(string region) => Map.GetByText(region, new() { Exact = true }).First;

    /// <summary>Goes to the estate page from the directory, and waits for its map.</summary>
    public async Task OpenAsync()
    {
        await Shell.GoToAreaAsync("replication");
        await Shell.ExpectHeadingAsync("Replication");
        await Expect(Map).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>
    /// Waits until the map draws <paramref name="region"/> as a peer, pressing <b>Refresh</b>
    /// between reads: links appear once a replicated tree's shipper has reported.
    /// </summary>
    /// <param name="region">The peer region id.</param>
    /// <param name="timeout">How long to wait.</param>
    public async Task WaitForPeerAsync(string region, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await PeerNode(region).IsVisibleAsync())
        {
            if (DateTime.UtcNow > deadline)
            {
                await Expect(PeerNode(region)).ToBeVisibleAsync();
            }

            await RefreshButton.ClickAsync();
            await Expect(RefreshButton).ToHaveTextAsync("Refresh", new() { Timeout = 30_000 });
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
    }

    /// <summary>Re-reads the estate through the palette command, and waits for the status line to change.</summary>
    public async Task RefreshFromPaletteAsync()
    {
        var before = await StatusLine.InnerTextAsync();

        // The read time is to the second: let it move before asking again.
        await Task.Delay(TimeSpan.FromMilliseconds(1_100));
        await Shell.RunCommandAsync(RefreshCommandTitle);
        await Expect(StatusLine).Not.ToHaveTextAsync(before, new() { Timeout = 30_000 });
        await Expect(RefreshButton).ToHaveTextAsync("Refresh", new() { Timeout = 30_000 });
    }

    /// <summary>Chooses <paramref name="label"/> in <paramref name="filter"/> and waits for the address to carry it.</summary>
    /// <param name="filter">One of the toolbar's selects.</param>
    /// <param name="label">The option's text.</param>
    /// <param name="query">The query key the filter writes.</param>
    /// <param name="value">The value it writes, or <see langword="null"/> when the option clears it.</param>
    public async Task FilterAsync(ILocator filter, string label, string query, string? value)
    {
        await filter.SelectOptionAsync(new SelectOptionValue { Label = label });
        var pattern = new Regex("[?&]" + Regex.Escape(query) + "=");
        if (value is null)
        {
            await Expect(Page).Not.ToHaveURLAsync(pattern);
        }
        else
        {
            await Expect(Page).ToHaveURLAsync(new Regex("[?&]" + Regex.Escape(query) + "=" + Regex.Escape(value) + "(&|$)"));
        }
    }

    /// <summary>The link rows of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree id.</param>
    /// <param name="compact">Whether the page draws its compact chrome.</param>
    public ILocator LinkRows(string treeId, bool compact) => ReplicationTables.Rows(Shell.Content, treeId, compact);

    /// <summary>Re-reads the page by reloading its document.</summary>
    public async Task ReloadAsync()
    {
        await Page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await Page.ReloadAsync();
        await JourneyBrowser.WaitForInteractiveAsync(Page);
    }
}
