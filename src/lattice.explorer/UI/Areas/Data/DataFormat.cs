using System.Globalization;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The Data area's fixed formats for times and counts, culture-invariant so a page reads the same everywhere.</summary>
internal static class DataFormat
{
    /// <summary>The <c>?at=</c> format: an ISO 8601 UTC instant to the second.</summary>
    public const string InstantFormat = "yyyy-MM-ddTHH:mm:ssZ";

    /// <summary>A hybrid-logical clock's wall time, as <c>2026-09-28 14:02:11 UTC</c>, or a dash for none.</summary>
    /// <param name="hlc">The clock.</param>
    public static string Time(HybridLogicalClock hlc) =>
        hlc.WallClockTicks <= 0 || hlc.WallClockTicks > DateTimeOffset.MaxValue.UtcTicks
            ? "-"
            : Time(new DateTimeOffset(hlc.WallClockTicks, TimeSpan.Zero));

    /// <summary>An instant as <c>2026-09-28 14:02:11 UTC</c>.</summary>
    /// <param name="instant">The instant.</param>
    public static string Time(DateTimeOffset instant) =>
        instant.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", CultureInfo.InvariantCulture);

    /// <summary>A count with group separators.</summary>
    /// <param name="value">The count.</param>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>Reads an <c>?at=</c> value: an ISO 8601 instant, taken as UTC when it names no offset.</summary>
    /// <param name="text">The query value.</param>
    /// <param name="instant">The instant read.</param>
    public static bool TryParseInstant(string? text, out DateTimeOffset instant) =>
        DateTimeOffset.TryParse(
            text,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out instant);

    /// <summary>An instant in the <c>?at=</c> format.</summary>
    /// <param name="instant">The instant.</param>
    public static string Instant(DateTimeOffset instant) =>
        instant.ToUniversalTime().ToString(InstantFormat, CultureInfo.InvariantCulture);
}
