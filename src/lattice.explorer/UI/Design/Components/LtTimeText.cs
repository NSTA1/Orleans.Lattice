using System.Globalization;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The text forms of the date, time and duration primitives: an instant is typed and
/// shown as an ISO 8601 UTC instant to the second, read back culture-invariant, and a
/// duration is named in the largest whole units it holds.
/// </summary>
internal static class LtTimeText
{
    /// <summary>The typed and stored form of an instant: ISO 8601, UTC, to the second.</summary>
    public const string IsoFormat = "yyyy-MM-ddTHH:mm:ssZ";

    /// <summary><paramref name="instant"/> in UTC, cut to the whole second below it.</summary>
    /// <param name="instant">The instant.</param>
    public static DateTimeOffset ToSecond(DateTimeOffset instant)
    {
        var utc = instant.ToUniversalTime();
        return new DateTimeOffset(utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond), TimeSpan.Zero);
    }

    /// <summary><paramref name="instant"/> in the typed form, such as <c>2026-09-28T14:05:00Z</c>.</summary>
    /// <param name="instant">The instant.</param>
    public static string Iso(DateTimeOffset instant) =>
        instant.ToUniversalTime().ToString(IsoFormat, CultureInfo.InvariantCulture);

    /// <summary><paramref name="instant"/> as a reader sees it, such as <c>2026-09-28 14:05:00 UTC</c>.</summary>
    /// <param name="instant">The instant.</param>
    public static string Readable(DateTimeOffset instant) =>
        instant.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", CultureInfo.InvariantCulture);

    /// <summary>
    /// Reads a typed instant: ISO 8601 with or without the <c>T</c>, taken as UTC when it
    /// names no offset and converted to UTC when it names one, cut to the second.
    /// </summary>
    /// <param name="text">The typed text.</param>
    /// <param name="instant">The instant read, in UTC.</param>
    /// <returns>Whether the text named an instant.</returns>
    public static bool TryParseInstant(string? text, out DateTimeOffset instant)
    {
        if (!string.IsNullOrWhiteSpace(text)
            && DateTimeOffset.TryParse(
                text.Trim(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            instant = ToSecond(parsed);
            return true;
        }

        instant = default;
        return false;
    }

    /// <summary>
    /// <paramref name="instant"/> as the wall time of <paramref name="zone"/>, named with the
    /// zone and its offset from UTC, such as <c>2026-09-28 15:05:00 Europe/London (UTC+01:00)</c>.
    /// </summary>
    /// <param name="instant">The instant.</param>
    /// <param name="zone">The zone.</param>
    public static string InZone(DateTimeOffset instant, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(zone);
        var local = TimeZoneInfo.ConvertTime(instant, zone);
        return local.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " " + zone.Id + " (" + Offset(local.Offset) + ")";
    }

    /// <summary>An offset from UTC, such as <c>UTC+01:00</c> or <c>UTC-05:30</c>.</summary>
    /// <param name="offset">The offset.</param>
    public static string Offset(TimeSpan offset) =>
        "UTC" + (offset < TimeSpan.Zero ? "-" : "+") + offset.Duration().ToString(@"hh\:mm", CultureInfo.InvariantCulture);

    /// <summary>A duration in the largest whole units it holds, such as <c>1 h 30 min</c>, or <c>0 s</c>.</summary>
    /// <param name="duration">The duration.</param>
    public static string Duration(TimeSpan duration)
    {
        if (duration <= TimeSpan.Zero)
        {
            return "0 s";
        }

        var parts = new List<string>(4);
        Add(parts, (long)duration.TotalDays, "d");
        Add(parts, duration.Hours, "h");
        Add(parts, duration.Minutes, "min");
        Add(parts, duration.Seconds, "s");
        return parts.Count == 0 ? "0 s" : string.Join(' ', parts);

        static void Add(List<string> parts, long value, string unit)
        {
            if (value > 0)
            {
                parts.Add(value.ToString(CultureInfo.InvariantCulture) + " " + unit);
            }
        }
    }
}
