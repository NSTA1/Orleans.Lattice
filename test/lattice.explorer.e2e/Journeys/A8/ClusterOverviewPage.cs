using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// The Cluster overview at <c>/cluster</c>: the estate's identity and storage figures, the
/// re-measure of every shard (a tracked operation it follows and can stop), and the
/// Administer stops.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class ClusterOverviewPage(ShellPage shell) : A8PageObject(shell)
{
    /// <summary>What the overview says when a re-measure is stopped before it finishes.</summary>
    public const string StoppedText = "The re-measure was stopped before it finished.";

    /// <summary>The "Re-measure every shard..." button.</summary>
    public ILocator Measure => Content.Locator("button[data-lt-cluster=\"measure\"]");

    /// <summary>The "Stop re-measuring" button, drawn while a re-measure is starting or running.</summary>
    public ILocator Stop => Content.Locator("button[data-lt-cluster=\"stop-measure\"]");

    /// <summary>The re-measure's progress.</summary>
    public ILocator Progress => Content.Locator("[data-lt-cluster=\"remeasure-progress\"]");

    /// <summary>Why the last re-measure ended without figures.</summary>
    public ILocator RefreshError => Content.Locator("[data-lt-cluster=\"remeasure-error\"]");

    /// <summary>
    /// The note a tenant-scoped overview draws in place of the cluster-wide controls, with its
    /// "See the cluster-wide overview" link.
    /// </summary>
    public ILocator ClusterWideNote => Content.Locator("[data-lt-cluster-wide=\"true\"]");

    /// <summary>
    /// Opens the Cluster area from its spine stop, and the cluster-wide overview when the shell
    /// lands on a tenant's: a tenant-scoped overview offers no re-measure and only its first
    /// Administer stop.
    /// </summary>
    public async Task OpenAsync()
    {
        await Shell.GoToAreaAsync("cluster");
        await Shell.ExpectHeadingAsync("Cluster");
        await Expect(Term("Cluster id")).Not.ToBeEmptyAsync();
        await Expect(Measure.Or(ClusterWideNote)).ToBeVisibleAsync();
        if (await ClusterWideNote.IsVisibleAsync())
        {
            await ClusterWideNote.GetByRole(AriaRole.Link, new() { Name = "See the cluster-wide overview" }).ClickAsync();
            await Expect(ClusterWideNote).ToHaveCountAsync(0);
        }

        await Expect(Measure).ToBeVisibleAsync();
        Coverage.Hit("route:/cluster");
    }

    /// <summary>The cluster id the overview shows, which the re-measure's confirmation asks for.</summary>
    public async Task<string> ClusterIdAsync() => (await Term("Cluster id").TextContentAsync())!.Trim();

    /// <summary>Opens the re-measure's confirmation and types the cluster id.</summary>
    public async Task<ConfirmDialog> AskToMeasureAsync()
    {
        var clusterId = await ClusterIdAsync();
        await Measure.ClickAsync();
        var dialog = new ConfirmDialog(Page, "Re-measure every shard");
        await dialog.TypeAsync(clusterId, "Re-measure");
        return dialog;
    }

    /// <summary>Confirms the re-measure and follows it to its figures, which the overview then says a leaf walk measured.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task MeasureToTheEndAsync(ConfirmDialog dialog)
    {
        var before = await MeasuredAsync();
        await dialog.ConfirmAsync("Re-measure");
        await UntilAsync("the re-measured figures", async () => await FinishedSinceAsync(before), Structural);
        await Expect(Term("Measured")).ToContainTextAsync("Re-measured by a leaf walk at");
        await Expect(RefreshError).ToHaveCountAsync(0);
        Coverage.Hit("use-case:cluster/overview-re-measure");
    }

    /// <summary>
    /// Confirms a re-measure and presses Stop re-measuring as soon as the overview offers it.
    /// </summary>
    /// <param name="dialog">The open confirmation.</param>
    /// <returns>
    /// <see langword="true"/> when the stop was pressed while the re-measure ran and the overview
    /// says why it ended; <see langword="false"/> when the leaf walk finished first.
    /// </returns>
    public async Task<bool> MeasureAndStopAsync(ConfirmDialog dialog)
    {
        var before = await MeasuredAsync();
        await dialog.ConfirmAsync("Re-measure");
        var pressed = false;
        await UntilAsync("the re-measure running, or finished", async () =>
        {
            if (await Stop.IsVisibleAsync() && await Stop.IsEnabledAsync())
            {
                await Stop.ClickAsync();
                pressed = true;
                return true;
            }

            return await FinishedSinceAsync(before);
        }, TimeSpan.FromMinutes(2));

        // The cluster honours the stop at the walk's next progress report, so the walk can
        // still finish first; either way the overview ends with the measure button back.
        await Expect(Measure).ToBeVisibleAsync(new() { Timeout = (float)Structural.TotalMilliseconds });
        await Expect(Stop).ToHaveCountAsync(0);
        return pressed && await RefreshError.IsVisibleAsync();
    }

    // A walk has finished when the measure button is back and the Measured line has moved on
    // from what it said before the walk was asked for (it names the instant it measured).
    private async Task<bool> FinishedSinceAsync(string before) =>
        await Measure.IsVisibleAsync() && await Stop.CountAsync() == 0 && await MeasuredAsync() != before;

    private async Task<string> MeasuredAsync() => (await Term("Measured").TextContentAsync())?.Trim() ?? string.Empty;

    /// <summary>Sees the overview say the re-measure was stopped.</summary>
    public Task ExpectStoppedAsync() => Expect(RefreshError).ToHaveTextAsync(StoppedText);

    /// <summary>Follows the Administer stop named <paramref name="stop"/>, such as <c>Trees</c> or <c>WAL placement</c>.</summary>
    /// <param name="stop">The stop's name.</param>
    public Task FollowStopAsync(string stop) =>
        Content.Locator("a.lt-cluster-stops__link").Filter(new() { Has = Page.Locator(".lt-cluster-stops__name", new() { HasText = stop }) }).ClickAsync();
}
