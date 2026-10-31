using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// The regions every checkpoint screenshot masks because they change from run to run - so a
/// screenshot of a step shows the same pixels on every run of the same commit.
/// </summary>
/// <remarks>
/// <para>
/// The Explorer has no single clock or rate component to target, so the defaults match
/// volatile <em>text</em> in the leaf elements that carry values (<c>code</c>, <c>kbd</c>,
/// <c>samp</c>, <c>time</c>, table and definition cells, and a dialog's description, which names the endpoint): the run's loopback endpoints (their
/// ports are free ports), wall times and dates (clocks and HLC stamps, which the Explorer
/// writes as <c>yyyy-MM-dd HH:mm:ss UTC</c>), durations ago, rates, and GUID-shaped operation
/// ids. Any element carrying <c>data-lt-volatile</c> is masked too, which is the hook to add
/// to an Explorer component whose value cannot be matched by its text.
/// </para>
/// <para>A substep masks anything else through its own locators.</para>
/// </remarks>
public static partial class JourneyMasks
{
    /// <summary>The colour masked regions are painted, so a mask is unmistakable in a screenshot.</summary>
    public const string Colour = "#FF00FF";

    /// <summary>The leaf elements whose text the defaults match.</summary>
    public const string ValueElements = "code, kbd, samp, time, td, th, dd, .lt-mono-cell__value, .lt-pill__text, .lt-dialog__description";

    /// <summary>Volatile text: what makes an element a mask under the defaults.</summary>
    public static Regex VolatileText { get; } = VolatileTextPattern();

    /// <summary>The default masks on <paramref name="page"/>.</summary>
    /// <param name="page">The page about to be captured.</param>
    public static IReadOnlyList<ILocator> Default(IPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        return
        [
            page.Locator("[data-lt-volatile]"),
            page.Locator(ValueElements).Filter(new LocatorFilterOptions { HasTextRegex = VolatileText }),
        ];
    }

    // IgnoreCase only: Playwright hands the pattern to the browser, and refuses any flag JavaScript has no equivalent for.
    [GeneratedRegex(
        @"(127\.0\.0\.1|localhost|\[::1\]):\d{2,5}"
        + @"|\b\d{4}-\d{2}-\d{2}\b"
        + @"|\b\d{1,2}:\d{2}(:\d{2})?\b"
        + @"|\b\d+(\.\d+)?\s*(s|sec|seconds?|m|min|minutes?|h|hours?|ms)\s+ago\b"
        + @"|\b(just now|moments? ago)\b"
        + @"|\d[\d.,]*\s*(/\s?s|/\s?min|per second|per minute|ops/s|B/s|KiB/s|MiB/s)"
        + @"|\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
        + @"|\b[0-9a-f]{32}\b",
        RegexOptions.IgnoreCase)]
    private static partial Regex VolatileTextPattern();
}
