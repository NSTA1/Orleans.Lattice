using System.Globalization;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>The Cluster area's number formats: counts with separators, sizes in binary units.</summary>
internal static class ClusterFormat
{
    private static readonly string[] Units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

    /// <summary>A count with thousands separators: <c>48,210</c>.</summary>
    /// <param name="value">The count.</param>
    /// <returns>The text.</returns>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>A rate to one decimal place: <c>3.2</c>.</summary>
    /// <param name="value">The rate.</param>
    /// <returns>The text.</returns>
    public static string Rate(double value) => value.ToString("0.0", CultureInfo.InvariantCulture);

    /// <summary>A size in binary units: <c>4.2 GiB</c>.</summary>
    /// <param name="bytes">The size in bytes.</param>
    /// <returns>The text.</returns>
    public static string Bytes(long bytes)
    {
        if (bytes < 1024)
        {
            return bytes.ToString(CultureInfo.InvariantCulture) + " B";
        }

        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < Units.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return value.ToString("0.0", CultureInfo.InvariantCulture) + " " + Units[unit];
    }

    /// <summary>A UTC instant: <c>2026-09-28 14:02:11 UTC</c>.</summary>
    /// <param name="value">The instant.</param>
    /// <returns>The text.</returns>
    public static string Instant(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " UTC";

    /// <summary>A duration in the largest whole unit that fits: <c>7 days</c>, <c>3 hours</c>, <c>90 seconds</c>.</summary>
    /// <param name="value">The duration.</param>
    /// <returns>The text.</returns>
    public static string Duration(TimeSpan value) => value switch
    {
        { Ticks: <= 0 } => "none",
        { TotalDays: >= 1 } when value.TotalDays % 1 == 0 => Plural((long)value.TotalDays, "day"),
        { TotalHours: >= 1 } when value.TotalHours % 1 == 0 => Plural((long)value.TotalHours, "hour"),
        { TotalMinutes: >= 1 } when value.TotalMinutes % 1 == 0 => Plural((long)value.TotalMinutes, "minute"),
        _ => Plural((long)Math.Ceiling(value.TotalSeconds), "second"),
    };

    /// <summary>"1 tree" or "3 trees".</summary>
    /// <param name="count">The count.</param>
    /// <param name="noun">The singular noun.</param>
    /// <param name="plural">The plural noun, when it is not the singular plus "s".</param>
    /// <returns>The text.</returns>
    public static string Plural(long count, string noun, string? plural = null) =>
        Count(count) + " " + (count == 1 ? noun : plural ?? noun + "s");
}
