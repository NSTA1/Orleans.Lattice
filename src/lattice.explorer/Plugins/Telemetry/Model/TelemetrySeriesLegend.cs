using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Turns a series' labels into the short text a chart legend renders.
/// </summary>
/// <remarks>
/// <para>
/// <b>The label set is the server's, and it is rendered rather than
/// interpreted.</b> A series is named by the labels it actually carries, in a
/// stable priority order, so a catalogue entry that starts emitting a new
/// dimension is legible immediately without a client change. Only two names are
/// treated specially, and only to make a reserved id readable: the platform
/// sentinel and the tenancy-off default.
/// </para>
/// <para>
/// A series carrying no labels at all is legal - a query aggregating everything
/// away returns one - and is named by its position rather than left blank.
/// </para>
/// </remarks>
public static class TelemetrySeriesLegend
{
    // Ordered by how much a reader needs it: which tree, then whose, then
    // whatever else distinguishes one series from its neighbour.
    private static readonly string[] PreferredLabels = [TelemetryLabelNames.Tree, TelemetryLabelNames.Tenant];

    /// <summary>The most labels a legend entry names before it stops being readable.</summary>
    private const int MaxLabelsRendered = 3;

    /// <summary>
    /// The legend label for <paramref name="series"/>, falling back to
    /// <c>Series N</c> when it carries nothing to name it by.
    /// </summary>
    /// <param name="series">The series to name.</param>
    /// <param name="index">The series' zero-based position, used for the fallback.</param>
    /// <returns>The legend text.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="series"/> is <see langword="null"/>.</exception>
    public static string LabelFor(ExplorerTelemetrySeries series, int index)
    {
        ArgumentNullException.ThrowIfNull(series);

        if (series.Labels.Count == 0)
        {
            return Fallback(index);
        }

        var builder = new StringBuilder(48);
        var rendered = 0;

        for (var i = 0; i < PreferredLabels.Length && rendered < MaxLabelsRendered; i++)
        {
            var name = PreferredLabels[i];
            if (series.TryGetLabel(name, out var value))
            {
                Append(builder, name, value);
                rendered++;
            }
        }

        var labels = series.Labels;
        for (var i = 0; i < labels.Count && rendered < MaxLabelsRendered; i++)
        {
            var label = labels[i];
            if (IsPreferred(label.Name))
            {
                continue;
            }

            Append(builder, label.Name, label.Value);
            rendered++;
        }

        return builder.Length == 0 ? Fallback(index) : builder.ToString();
    }

    private static bool IsPreferred(string name)
    {
        for (var i = 0; i < PreferredLabels.Length; i++)
        {
            if (string.Equals(PreferredLabels[i], name, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static void Append(StringBuilder builder, string name, string value)
    {
        if (builder.Length > 0)
        {
            builder.Append(" / ");
        }

        builder.Append(string.Equals(name, TelemetryLabelNames.Tenant, StringComparison.Ordinal)
            ? TelemetryLabelNames.DisplayTenant(value)
            : value);
    }

    private static string Fallback(int index) =>
        string.Create(CultureInfo.InvariantCulture, $"Series {index + 1}");
}
