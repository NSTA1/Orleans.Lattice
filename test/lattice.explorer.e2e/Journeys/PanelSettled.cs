using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys;

/// <summary>
/// Bounded waits for what an Explorer panel draws once a server read or write behind it ends. A
/// panel either draws its result or its own failure state ("The keys did not load", "The entry did
/// not load", ...), so a wait ends on either, never on a fixed guess shorter than the read: the
/// bound is three 30-second server timeouts (the first attempt and the state client's two transient
/// retries) and margin, the bound U4's waits use (#5737). On a healthy estate the result draws at
/// once; the bound only matters under the replay-permit starvation of #5569.
/// </summary>
internal static class PanelSettled
{
    /// <summary>How long a server read or write behind a panel may take to end.</summary>
    public static readonly TimeSpan Bound = TimeSpan.FromSeconds(120);

    /// <summary><see cref="Bound"/> in milliseconds, for Playwright options.</summary>
    public static float BoundMs => (float)Bound.TotalMilliseconds;

    private static readonly Regex DidNotLoad = new("(did not load|could not be listed)$");

    /// <summary>Any panel's failure state in <paramref name="scope"/>: its "... did not load" heading.</summary>
    /// <param name="scope">Where the panel is drawn.</param>
    public static ILocator Failure(ILocator scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.GetByRole(AriaRole.Heading, new() { NameRegex = DidNotLoad });
    }

    /// <summary>
    /// Waits until <paramref name="drawn"/> or <paramref name="failed"/> is visible, within
    /// <see cref="Bound"/>, and returns whether <paramref name="drawn"/> was.
    /// </summary>
    /// <param name="drawn">What the panel shows once its read or write succeeds.</param>
    /// <param name="failed">The panel's failure state.</param>
    public static async Task<bool> SettledAsync(ILocator drawn, ILocator failed)
    {
        ArgumentNullException.ThrowIfNull(drawn);
        ArgumentNullException.ThrowIfNull(failed);
        await Expect(drawn.Or(failed).First).ToBeVisibleAsync(new() { Timeout = BoundMs });
        return !await failed.First.IsVisibleAsync();
    }

    /// <summary>As <see cref="SettledAsync"/>, failing with the panel's own words when it did not draw.</summary>
    /// <param name="drawn">What the panel shows once its read or write succeeds.</param>
    /// <param name="failed">The panel's failure state.</param>
    /// <param name="what">What was awaited, for the failure.</param>
    public static async Task ExpectDrawnAsync(ILocator drawn, ILocator failed, string what)
    {
        if (!await SettledAsync(drawn, failed))
        {
            Assert.Fail($"{what}: the panel ended in its failure state: {(await failed.First.Locator("xpath=..").InnerTextAsync()).ReplaceLineEndings(" ")}");
        }
    }
}
