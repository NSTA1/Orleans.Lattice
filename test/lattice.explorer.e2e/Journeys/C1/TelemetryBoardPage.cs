using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// The Telemetry area's Throughput board as C1 reads it: the write-operation rate of one tree
/// over the last fifteen minutes at a fifteen-second step, drawn as a table so its readings
/// can be read, where a burst of writes shows as a spike.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed partial class TelemetryBoardPage(PersonaSession session)
{
    /// <summary>How much higher than the median the peak must read to count as a spike.</summary>
    public const double SpikeFactor = 3;

    private IPage Page => session.Page;

    /// <summary>The write-operations chart's table of readings.</summary>
    public ILocator WriteOperations => Page.GetByRole(AriaRole.Table, new() { NameRegex = WriteOperationsCaption() });

    /// <summary>Opens the Throughput board narrowed to <paramref name="tree"/> as tables.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenAsync(string tree)
    {
        await session.Shell.GotoAsync($"telemetry/throughput?tree={Uri.EscapeDataString(tree)}&range=15m&step=15s&view=table");
        await Expect(WriteOperations).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>
    /// Waits until the write-operations readings show a spike - a peak at least
    /// <see cref="SpikeFactor"/> times the median reading - re-reading the board while the
    /// metrics backend has not scraped it yet, and returns the median and the peak.
    /// </summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="timeout">How long to wait.</param>
    public async Task<(double Median, double Peak)> ExpectSpikeAsync(string tree, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            await OpenAsync(tree);
            var readings = await ReadingsAsync();
            if (readings.Count >= 3)
            {
                var sorted = readings.Order().ToList();
                var median = sorted[sorted.Count / 2];
                var peak = sorted[^1];
                if (peak >= Math.Max(median * SpikeFactor, median + 3))
                {
                    return (median, peak);
                }

                if (DateTime.UtcNow > deadline)
                {
                    throw new AssertionException($"The write-operations board shows no spike for {tree}: median {median}, peak {peak} over {readings.Count} readings.");
                }
            }
            else if (DateTime.UtcNow > deadline)
            {
                throw new AssertionException($"The write-operations board shows only {readings.Count} readings for {tree}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(10));
        }
    }

    private async Task<IReadOnlyList<double>> ReadingsAsync()
    {
        var readings = new List<double>();
        foreach (var cell in await WriteOperations.Locator("tbody td").AllInnerTextsAsync())
        {
            var match = Reading().Match(cell);
            if (match.Success && double.TryParse(match.Groups["n"].Value.Replace(",", string.Empty, StringComparison.Ordinal), NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                readings.Add(value);
            }
        }

        return readings;
    }

    [GeneratedRegex("^Write operations")]
    private static partial Regex WriteOperationsCaption();

    [GeneratedRegex(@"^\s*(?<n>[0-9][0-9,]*(\.[0-9]+)?)")]
    private static partial Regex Reading();
}
