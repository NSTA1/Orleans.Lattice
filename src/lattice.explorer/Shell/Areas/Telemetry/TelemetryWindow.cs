using System.Globalization;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// Everything a Telemetry address says about how to draw a board, read from its
/// query: the time range (<c>?range=</c>, or <c>?from=&amp;to=</c>), the step,
/// the tree filter, the tenant scope and the chart or table view. Every chart
/// state is therefore a link.
/// </summary>
/// <remarks>
/// A value the area cannot read is dropped with a <see cref="Notice"/> rather than
/// failing the page: a mistyped link still lands on a board.
/// </remarks>
internal sealed record TelemetryWindow
{
    /// <summary>The query key naming a relative range, such as <c>1h</c>.</summary>
    public const string RangeQuery = "range";

    /// <summary>The query key naming the start of an absolute window.</summary>
    public const string FromQuery = "from";

    /// <summary>The query key naming the end of an absolute window.</summary>
    public const string ToQuery = "to";

    /// <summary>The query key naming the step.</summary>
    public const string StepQuery = "step";

    /// <summary>The query key naming the one tree to narrow to.</summary>
    public const string TreeQuery = "tree";

    /// <summary>The query key naming the tenant scope.</summary>
    public const string ScopeQuery = "scope";

    /// <summary>The query key naming the view.</summary>
    public const string ViewQuery = "view";

    /// <summary>The scope value asking for every tenant.</summary>
    public const string AllTenantsScope = "all";

    /// <summary>The view value drawing every chart as a table.</summary>
    public const string TableView = "table";

    /// <summary>The compact, unreserved instant format the area writes into the address.</summary>
    public const string InstantFormat = "yyyyMMdd'T'HHmmss'Z'";

    private static readonly string[] InstantFormats =
    [
        InstantFormat,
        "yyyyMMdd'T'HHmm'Z'",
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm'Z'",
        "yyyy-MM-dd'T'HH:mm:ssK",
        "yyyy-MM-dd'T'HH:mmK",
    ];

    /// <summary>The relative range, or <see langword="null"/>.</summary>
    public TimeSpan? Range { get; init; }

    /// <summary>The start of the absolute window, or <see langword="null"/>.</summary>
    public DateTimeOffset? From { get; init; }

    /// <summary>The end of the absolute window, or <see langword="null"/>.</summary>
    public DateTimeOffset? To { get; init; }

    /// <summary>The requested step, or <see langword="null"/> for automatic.</summary>
    public TimeSpan? Step { get; init; }

    /// <summary>The logical tree id to narrow to, or <see langword="null"/>.</summary>
    public string? Tree { get; init; }

    /// <summary>Whether the address asks for every tenant.</summary>
    public bool AllTenants { get; init; }

    /// <summary>Whether the address asks for tables rather than charts.</summary>
    public bool ShowTable { get; init; }

    /// <summary>What the address named that the area could not read, or <see langword="null"/>.</summary>
    public string? Notice { get; init; }

    /// <summary>Whether the address names no range at all, so each chart shows its own default window.</summary>
    public bool IsDefault => Range is null && From is null;

    /// <summary>Whether the address names an absolute window.</summary>
    public bool IsAbsolute => From is not null;

    /// <summary>Reads the window an address names.</summary>
    /// <param name="address">The address.</param>
    /// <returns>The window.</returns>
    public static TelemetryWindow FromAddress(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        var problems = new List<string>(2);
        TimeSpan? range = null;
        DateTimeOffset? from = null;
        DateTimeOffset? to = null;
        TimeSpan? step = null;

        var fromText = address.GetQuery(FromQuery);
        var toText = address.GetQuery(ToQuery);
        if (fromText is not null || toText is not null)
        {
            if (TryParseInstant(fromText, out var start) && TryParseInstant(toText, out var end) && start < end)
            {
                from = start;
                to = end;
            }
            else
            {
                problems.Add("the time window");
            }
        }
        else if (address.GetQuery(RangeQuery) is { } rangeText)
        {
            if (TelemetryDurations.TryParse(rangeText, out var parsed))
            {
                range = parsed;
            }
            else
            {
                problems.Add("the time range");
            }
        }

        if (address.GetQuery(StepQuery) is { } stepText)
        {
            if (TelemetryDurations.TryParse(stepText, out var parsed))
            {
                step = parsed;
            }
            else
            {
                problems.Add("the step");
            }
        }

        var tree = address.GetQuery(TreeQuery);

        return new TelemetryWindow
        {
            Range = range,
            From = from,
            To = to,
            Step = step,
            Tree = string.IsNullOrWhiteSpace(tree) ? null : tree,
            AllTenants = string.Equals(address.GetQuery(ScopeQuery), AllTenantsScope, StringComparison.Ordinal),
            ShowTable = string.Equals(address.GetQuery(ViewQuery), TableView, StringComparison.Ordinal),
            Notice = problems.Count == 0
                ? null
                : $"This address names {string.Join(" and ", problems)} in a form the Explorer cannot read, so each chart uses its default instead.",
        };
    }

    /// <summary>Formats an instant in the compact form the address carries.</summary>
    /// <param name="instant">The instant.</param>
    /// <returns>The text, such as <c>20260101T120000Z</c>.</returns>
    public static string FormatInstant(DateTimeOffset instant) =>
        instant.UtcDateTime.ToString(InstantFormat, CultureInfo.InvariantCulture);

    /// <summary>Parses an instant in any form the area accepts.</summary>
    /// <param name="text">The text.</param>
    /// <param name="instant">The instant, in UTC.</param>
    /// <returns>Whether the text named an instant.</returns>
    public static bool TryParseInstant(string? text, out DateTimeOffset instant)
    {
        if (!string.IsNullOrEmpty(text)
            && DateTimeOffset.TryParseExact(
                text,
                InstantFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            instant = parsed.ToUniversalTime();
            return true;
        }

        instant = default;
        return false;
    }

    /// <summary>The concrete start and end of the window at <paramref name="now"/>, or <see langword="null"/> for the default window.</summary>
    /// <param name="now">The current instant.</param>
    /// <returns>The window's bounds.</returns>
    public (DateTimeOffset Start, DateTimeOffset End)? Resolve(DateTimeOffset now)
    {
        if (From is { } from && To is { } to)
        {
            return (from, to);
        }

        if (Range is { } range)
        {
            return (now - range, now);
        }

        return null;
    }
}
