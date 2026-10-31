using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>The Telemetry area: its list of boards at <c>/telemetry</c>, and one board at <c>/telemetry/{board}</c>.</summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class TelemetryBoardPage(ShellPage shell) : A8PageObject(shell)
{
    /// <summary>The board whose charts show writes per tree.</summary>
    public const string ThroughputBoard = "Throughput";

    /// <summary>The chart of write operations per tree on the <see cref="ThroughputBoard"/>.</summary>
    public const string WriteOperationsChart = "Write operations per second, by tree";

    /// <summary>The board's and the list's Refresh button: the visible control of <c>telemetry.refresh</c>.</summary>
    public ILocator Refresh => Content.Locator("button[data-lt-command=\"telemetry.refresh\"]");

    /// <summary>Opens the Telemetry area from its spine stop and sees its list of boards.</summary>
    public async Task OpenAsync()
    {
        await Shell.GoToAreaAsync("telemetry");
        await Shell.ExpectHeadingAsync("Telemetry");
        await Expect(Content.GetByText(ThroughputBoard, new() { Exact = true }).First).ToBeVisibleAsync();
        Coverage.Hit("route:/telemetry");
    }

    /// <summary>Opens the <see cref="ThroughputBoard"/> from the list (its address when the list is compact).</summary>
    public async Task OpenThroughputAsync()
    {
        if (await Shell.IsCompactAsync())
        {
            await Shell.GotoAsync("/telemetry/throughput");
        }
        else
        {
            await Link(ThroughputBoard).ClickAsync();
        }

        await Expect(Page).ToHaveURLAsync(new Regex("/telemetry/throughput(\\?|$)"));
        await Shell.ExpectHeadingAsync(ThroughputBoard);
        Coverage.Hit("route:/telemetry/{p1}");
        Coverage.Hit("subroute:telemetry/throughput");
    }

    /// <summary>The chart titled <paramref name="title"/>.</summary>
    /// <param name="title">The chart's title.</param>
    public ILocator Chart(string title) =>
        Content.Locator("section.lt-telemetry-chart")
            .Filter(new() { Has = Page.Locator("h2.lt-telemetry-chart__title", new() { HasTextRegex = new Regex("^" + Regex.Escape(title) + "$") }) });

    /// <summary>The legend entry of <paramref name="tree"/>'s series on the chart titled <paramref name="chart"/>.</summary>
    /// <param name="chart">The chart's title.</param>
    /// <param name="tree">The tree.</param>
    public ILocator Series(string chart, string tree) =>
        Chart(chart).Locator(".lt-telemetry-legend__item").Filter(new() { HasText = tree });

    /// <summary>
    /// Waits for <paramref name="tree"/>'s series on the write-operations chart, pressing
    /// Refresh while Prometheus has not yet scraped the writes (each press re-reads the charts).
    /// </summary>
    /// <param name="tree">The tree whose writes are awaited.</param>
    /// <param name="timeout">How long to wait.</param>
    public async Task ExpectWritesAsync(string tree, TimeSpan timeout)
    {
        var series = Series(WriteOperationsChart, tree);
        await UntilAsync($"a write-operations series for {tree}", async () =>
        {
            try
            {
                await series.First.WaitForAsync(new() { Timeout = 15_000 });
                return true;
            }
            catch (PlaywrightException)
            {
                // Prometheus has not scraped the writes into this window yet: read the charts again.
                await Refresh.ClickAsync();
                return false;
            }
        }, timeout);

        await Expect(series.First).ToBeVisibleAsync();
    }

    /// <summary>Presses Refresh and sees the board re-read with <paramref name="tree"/>'s series still drawn.</summary>
    /// <param name="tree">The tree whose writes are drawn.</param>
    public async Task RefreshAsync(string tree)
    {
        await Refresh.ClickAsync();
        await Expect(Series(WriteOperationsChart, tree).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
        Coverage.Hit("command:telemetry.refresh");
    }
}
