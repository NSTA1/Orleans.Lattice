using System.Globalization;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Formats a telemetry reading for display, using the unit and semantic the
/// <em>server</em> published for the catalogue entry rather than a guess made
/// from the number.
/// </summary>
/// <remarks>
/// <para>
/// <b>The unit is the server's word, and it is never overridden.</b> A panel
/// that decided for itself that a figure was milliseconds would be wrong the
/// moment the catalogue said otherwise. The only interpretation applied here is
/// the number of decimal places, which is chosen from the semantic so a ratio
/// does not read as an integer and a count does not read as a fraction.
/// </para>
/// </remarks>
public static class TelemetryValueFormat
{
    /// <summary>The text rendered where a reading is absent rather than zero.</summary>
    public const string NoReadingText = "no reading";

    /// <summary>
    /// Formats <paramref name="value"/> for an entry whose declared semantic is
    /// <paramref name="semantic"/>, without its unit.
    /// </summary>
    /// <param name="value">The reading, which may be non-finite.</param>
    /// <param name="semantic">What one measurement counts.</param>
    /// <returns>The formatted number, or <see cref="NoReadingText"/>.</returns>
    public static string Value(double? value, ExplorerTelemetrySemantic semantic)
    {
        if (value is not { } reading || !double.IsFinite(reading))
        {
            return NoReadingText;
        }

        return semantic switch
        {
            // A ratio is read as a proportion, where two decimals is the
            // difference between 0.99 and 0.994 mattering and not.
            ExplorerTelemetrySemantic.Ratio => reading.ToString("N2", CultureInfo.CurrentCulture),

            // A duration is usually sub-unit, so truncating it to a whole
            // number would turn every fast operation into a zero.
            ExplorerTelemetrySemantic.Duration => reading.ToString("N3", CultureInfo.CurrentCulture),

            // A level is a gauge and can be fractional; a rate is often below
            // one per second on a quiet cluster.
            ExplorerTelemetrySemantic.Level or ExplorerTelemetrySemantic.Unspecified =>
                reading.ToString("N2", CultureInfo.CurrentCulture),

            // Counting semantics: whole things, unless the reading is small
            // enough that rounding it would lose the whole signal.
            _ => Math.Abs(reading) >= 10
                ? reading.ToString("N0", CultureInfo.CurrentCulture)
                : reading.ToString("N2", CultureInfo.CurrentCulture),
        };
    }

    /// <summary>
    /// Formats <paramref name="value"/> with the unit the catalogue entry
    /// declares appended, or without one when the entry declared none.
    /// </summary>
    /// <param name="value">The reading, which may be non-finite.</param>
    /// <param name="semantic">What one measurement counts.</param>
    /// <param name="unit">The server-authored unit.</param>
    /// <returns>The formatted reading.</returns>
    public static string WithUnit(double? value, ExplorerTelemetrySemantic semantic, string? unit)
    {
        var formatted = Value(value, semantic);
        if (string.IsNullOrWhiteSpace(unit) || string.Equals(formatted, NoReadingText, StringComparison.Ordinal))
        {
            return formatted;
        }

        return string.Create(CultureInfo.CurrentCulture, $"{formatted} {unit}");
    }
}
