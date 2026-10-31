using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>The Compliance tab: a scan's progress, stopping it, and its result.</summary>
internal sealed partial class SchemaPage
{
    /// <summary>The sentence the tab shows once a scan was stopped before it finished.</summary>
    public const string ScanStoppedText = "The scan was stopped before it finished.";

    /// <summary>The scan's result section.</summary>
    public ILocator ScanResult => Content.Locator(".lt-schema-result");

    /// <summary>Asserts a scan of <paramref name="tree"/> is running: the tab says so and offers <b>Stop scanning</b>.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ExpectScanningAsync(string tree)
    {
        await Expect(Content.Locator("p.lt-schema-status[role=status]")).ToHaveTextAsync($"Scanning every value of {tree}...");
        await Expect(Button("Stop scanning")).ToBeEnabledAsync();
    }

    /// <summary>Stops the running scan, and asserts it ended cancelled, before it finished.</summary>
    public async Task StopScanAsync()
    {
        await Button("Stop scanning").ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Alert).Filter(new() { HasText = ScanStoppedText })).ToBeVisibleAsync();
        await Expect(Button("Stop scanning")).ToHaveCountAsync(0);
        await Expect(Button("Scan compliance")).ToBeVisibleAsync();
    }

    /// <summary>Runs the scan again from the tab's own button, and waits for its result.</summary>
    public async Task ScanAgainAsync()
    {
        await Button("Scan again").ClickAsync();
        await Expect(ScanResult).ToBeVisibleAsync(new() { Timeout = (float)OperationPatience.TotalMilliseconds });
    }

    /// <summary>
    /// Asserts the last scan's result: <paramref name="nonCompliant"/> non-compliant values, the
    /// pill saying so, and - when any fail - the reasons accounting for every one of them.
    /// </summary>
    /// <param name="nonCompliant">How many values fail the policy.</param>
    /// <returns>The reasons and their counts, as the table lists them.</returns>
    public async Task<IReadOnlyList<(string Reason, int Count)>> ExpectScanResultAsync(int nonCompliant)
    {
        await Expect(ScanResult).ToBeVisibleAsync(new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        var pill = nonCompliant == 0 ? "Compliant" : "Non-compliant";
        await Expect(ScanResult.Locator(".lt-schema-heading").GetByText(pill, new() { Exact = true })).ToBeVisibleAsync();
        await Expect(Definition(ScanResult, "Non-compliant")).ToHaveTextAsync(Values(nonCompliant));

        var reasons = new List<(string, int)>();
        var rows = ScanResult.Locator("table tbody tr");
        if (nonCompliant == 0)
        {
            await Expect(rows).ToHaveCountAsync(0);
            return reasons;
        }

        await Expect(rows).Not.ToHaveCountAsync(0);
        for (var index = 0; index < await rows.CountAsync(); index++)
        {
            var cells = rows.Nth(index).Locator("th, td");
            reasons.Add((
                (await cells.First.InnerTextAsync()).Trim(),
                int.Parse(await cells.Last.InnerTextAsync(), NumberStyles.AllowThousands, CultureInfo.InvariantCulture)));
        }

        Assert.That(reasons.Sum(reason => reason.Item2), Is.EqualTo(nonCompliant), "the reasons account for every non-compliant value");
        return reasons;
    }

    /// <summary>
    /// Opens the deep link that starts a scan (<c>?tab=compliance&amp;scan=start</c>), and asserts
    /// the address drops <c>scan</c> once the scan has started, so a reload does not scan again.
    /// </summary>
    /// <param name="tree">The tree.</param>
    public async Task StartScanByAddressAsync(string tree)
    {
        await Shell.GotoAsync($"/schema/{tree}?tab=compliance&scan=start");
        await Expect(Shell.Heading).ToHaveTextAsync(tree);
        await Expect(Page).ToHaveURLAsync(new Regex($@"/schema/{Regex.Escape(tree)}\?tab=compliance$"));
        await Expect(Tab("Compliance")).ToHaveAttributeAsync("aria-selected", "true");
        Coverage.Hit("query:schema.scan");
        Coverage.Hit("query-value:schema.scan=start");
        Coverage.Hit("query:schema.tab");
        Coverage.Hit("tab:schema/compliance");
        Coverage.Hit("use-case:schema/addresses");
    }

    /// <summary>The description of <paramref name="term"/> in a definition list inside <paramref name="scope"/>.</summary>
    /// <param name="scope">Where the list is.</param>
    /// <param name="term">The term.</param>
    public static ILocator Definition(ILocator scope, string term) =>
        scope.Locator($"dt:text-is(\"{term}\") + dd").First;

    /// <summary>A count of values as the Schema area writes it, such as <c>2 values</c>.</summary>
    /// <param name="count">The count.</param>
    public static string Values(int count) => count == 1 ? "1 value" : $"{count.ToString("N0", CultureInfo.InvariantCulture)} values";
}
