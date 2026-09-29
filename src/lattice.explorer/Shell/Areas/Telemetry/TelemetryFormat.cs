using System.Globalization;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// How the Telemetry area writes a reading: a byte count scaled to its unit, a
/// ratio as a percentage, a duration in milliseconds, and a rate with its unit,
/// with the catalogue's UCUM annotations (<c>{op}/s</c>) read as plain words.
/// </summary>
internal static class TelemetryFormat
{
    /// <summary>What a missing or non-finite reading reads as.</summary>
    public const string NoReading = "no reading";

    private static readonly string[] ByteUnits = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

    /// <summary>Writes <paramref name="value"/> in the query's unit.</summary>
    /// <param name="value">The reading.</param>
    /// <param name="unit">The query's UCUM unit.</param>
    /// <param name="semantic">What the reading measures.</param>
    /// <returns>The reading with its unit, such as <c>1.5 MiB</c> or <c>93.1 %</c>.</returns>
    public static string Value(double value, string? unit, TelemetryMeasurementSemantic semantic)
    {
        if (!double.IsFinite(value))
        {
            return NoReading;
        }

        if (string.Equals(unit, "By", StringComparison.Ordinal))
        {
            return Bytes(value);
        }

        if (string.Equals(unit, "1", StringComparison.Ordinal) && semantic == TelemetryMeasurementSemantic.Ratio)
        {
            return (value * 100).ToString("0.#", CultureInfo.CurrentCulture) + " %";
        }

        var number = Number(value);
        var words = Unit(unit);
        return words.Length == 0 ? number : number + " " + words;
    }

    /// <summary>Reads a UCUM unit as words: braces dropped, <c>1</c> as nothing.</summary>
    /// <param name="unit">The unit.</param>
    /// <returns>The unit as it is written beside a number.</returns>
    public static string Unit(string? unit)
    {
        if (string.IsNullOrWhiteSpace(unit) || string.Equals(unit, "1", StringComparison.Ordinal))
        {
            return string.Empty;
        }

        return string.Equals(unit, "By", StringComparison.Ordinal) ? "bytes" : unit.Replace("{", string.Empty, StringComparison.Ordinal).Replace("}", string.Empty, StringComparison.Ordinal);
    }

    /// <summary>A time for an axis or a table row, in UTC.</summary>
    /// <param name="instant">The instant.</param>
    /// <param name="withDate">Whether the date is included.</param>
    /// <returns>The time, such as <c>14:05</c> or <c>2026-01-01 14:05</c>.</returns>
    public static string Time(DateTimeOffset instant, bool withDate) =>
        instant.UtcDateTime.ToString(withDate ? "yyyy-MM-dd HH:mm" : "HH:mm", CultureInfo.InvariantCulture);

    private static string Number(double value)
    {
        var magnitude = Math.Abs(value);
        var format = magnitude >= 100 ? "N0" : magnitude >= 10 ? "0.#" : magnitude == 0 ? "0" : "0.##";
        return value.ToString(format, CultureInfo.CurrentCulture);
    }

    private static string Bytes(double value)
    {
        var scaled = value;
        var index = 0;
        while (Math.Abs(scaled) >= 1024 && index < ByteUnits.Length - 1)
        {
            scaled /= 1024;
            index++;
        }

        return (index == 0 ? scaled.ToString("0", CultureInfo.CurrentCulture) : scaled.ToString("0.#", CultureInfo.CurrentCulture)) + " " + ByteUnits[index];
    }
}
