using System.Globalization;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// One series of a telemetry answer as the area presents it: its display name,
/// the logical tree it belongs to (when it carries one), and its readings aligned
/// to the answer's shared timeline.
/// </summary>
/// <param name="Name">What the series is called on the chart and in the table.</param>
/// <param name="Tree">The logical tree id the series measures, or <see langword="null"/>.</param>
/// <param name="Values">The readings at each of the chart's times; <see cref="double.NaN"/> where there is none.</param>
/// <param name="Latest">The last finite reading, or <see cref="double.NaN"/>.</param>
internal sealed record TelemetrySeriesView(string Name, string? Tree, IReadOnlyList<double> Values, double Latest)
{
    /// <summary>The label a series is named by: which tree, whose tenant, then whatever else tells it apart.</summary>
    public const string TreeLabel = "tree";

    /// <summary>The label naming the tenant a series belongs to.</summary>
    public const string TenantLabel = "tenant";

    private const int MaxLabelsNamed = 3;

    /// <summary>Names a series from its labels.</summary>
    /// <param name="series">The series.</param>
    /// <param name="index">Its position in the answer, for an unlabelled series.</param>
    /// <returns>The name and the logical tree it measures.</returns>
    public static (string Name, string? Tree) Describe(TelemetryTimeSeries series, int index)
    {
        ArgumentNullException.ThrowIfNull(series);

        var parts = new List<string>(MaxLabelsNamed);
        string? tree = null;
        if (series.TryGetLabel(TreeLabel, out var treeValue) && !string.IsNullOrEmpty(treeValue))
        {
            tree = treeValue;
            parts.Add(treeValue);
        }

        if (series.TryGetLabel(TenantLabel, out var tenant))
        {
            parts.Add("tenant " + DisplayTenant(tenant));
        }

        foreach (var label in series.Labels)
        {
            if (parts.Count >= MaxLabelsNamed)
            {
                break;
            }

            // A physical tree id is an implementation detail the Explorer never
            // shows: the tree dimension is the logical id, and anything naming the
            // physical copy is left out of the name.
            if (label.Name is TreeLabel or TenantLabel
                || string.IsNullOrEmpty(label.Value)
                || label.Name.Contains("physical", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            parts.Add(label.Name + " " + label.Value);
        }

        return (parts.Count == 0
            ? string.Create(CultureInfo.InvariantCulture, $"Series {index + 1}")
            : string.Join(" / ", parts), tree);
    }

    private static string DisplayTenant(string? tenant) => tenant switch
    {
        null or "" => "unattributed",
        "_platform_" => "platform",
        _ => tenant,
    };
}
