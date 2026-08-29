namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// One time series in a query result: its label set and its samples. An instant
/// query yields one sample per series; a range query yields one per resolution
/// step; a scalar result is modelled as a single series with no labels and one
/// sample.
/// </summary>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryTimeSeries)]
[Immutable]
public sealed record TelemetryTimeSeries
{
    private static readonly TelemetryTimeSeries EmptySeries =
        new() { Labels = [], Points = [] };

    /// <summary>
    /// The series' labels in the order the backend returned them, including the
    /// derived <c>tenant</c> label the cluster always emits. Empty for a scalar
    /// result.
    /// </summary>
    [Id(0)] public required IReadOnlyList<TelemetryLabel> Labels { get; init; }

    /// <summary>
    /// The series' samples in ascending timestamp order. Empty when the query
    /// matched the series but it carried no sample in the window.
    /// </summary>
    [Id(1)] public required IReadOnlyList<TelemetryDataPoint> Points { get; init; }

    /// <summary>
    /// The label-free, sample-free series. A cached singleton, so representing an
    /// empty scalar result allocates nothing.
    /// </summary>
    public static TelemetryTimeSeries Empty => EmptySeries;

    /// <summary>
    /// Looks up the value of the label named <paramref name="name"/>, compared
    /// ordinally. Scans by index, so a lookup allocates nothing.
    /// </summary>
    /// <param name="name">The label name to resolve, for example <c>tree</c>.</param>
    /// <param name="value">The label value, or <see langword="null"/> when the label is absent.</param>
    /// <returns><see langword="true"/> when the series carries the label.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    public bool TryGetLabel(string name, out string? value)
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
