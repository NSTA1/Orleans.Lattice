using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One series the backend returned: its labels and its points, in the order the
/// backend produced them.
/// </summary>
/// <remarks>
/// The seam returns every series the facade returned. It never drops one because
/// of a label - in particular a tenant label - because deciding which tenant's
/// series a caller may see is the facade's job and the one a desktop head could
/// otherwise be edited to bypass.
/// </remarks>
public sealed record ExplorerTelemetrySeries
{
    private static readonly ExplorerTelemetrySeries EmptySeries = new() { Labels = [], Points = [] };

    /// <summary>The series' labels, exactly as the backend produced them.</summary>
    public required IReadOnlyList<ExplorerTelemetryLabel> Labels { get; init; }

    /// <summary>The series' points, in the order the backend produced them.</summary>
    public required IReadOnlyList<ExplorerTelemetryPoint> Points { get; init; }

    /// <summary>The shared empty series.</summary>
    public static ExplorerTelemetrySeries Empty => EmptySeries;

    /// <summary>The number of points in the series.</summary>
    public int PointCount => Points.Count;

    /// <summary><see langword="true"/> when the series carries no points.</summary>
    public bool IsEmpty => Points.Count == 0;

    /// <summary>Finds the value of the label named <paramref name="name"/>.</summary>
    /// <param name="name">The label name, compared ordinally.</param>
    /// <param name="value">The label value when found.</param>
    /// <returns><see langword="true"/> when the series carries the label.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    public bool TryGetLabel(string name, [NotNullWhen(true)] out string? value)
    {
        ArgumentNullException.ThrowIfNull(name);

        var labels = Labels;
        for (var i = 0; i < labels.Count; i++)
        {
            var label = labels[i];
            if (string.Equals(label.Name, name, StringComparison.Ordinal))
            {
                value = label.Value;
                return true;
            }
        }

        value = null;
        return false;
    }
}
