namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>One series drawn as a line on a telemetry chart.</summary>
/// <param name="Series">The series.</param>
/// <param name="Index">Its position among the drawn lines.</param>
/// <param name="Ink">Whether it is drawn in ink (chalk on Board) rather than link blue (chalk blue).</param>
/// <param name="Dash">Its SVG dash pattern; empty for a solid line.</param>
/// <param name="Path">Its SVG path data.</param>
/// <param name="LabelY">Where its direct label sits, clear of its neighbours.</param>
/// <param name="Label">Its direct label, shortened to fit the margin.</param>
internal sealed record TelemetryPlotLine(
    TelemetrySeriesView Series,
    int Index,
    bool Ink,
    string Dash,
    string Path,
    double LabelY,
    string Label)
{
    /// <summary>The stylesheet class that paints the line's pigment.</summary>
    public string PigmentClass => Ink ? "lt-telemetry-series--ink" : "lt-telemetry-series--blue";

    /// <summary>The dash pattern in words, for the legend's accessible name.</summary>
    public string DashName => Dash.Length == 0 ? "solid" : (Index / 2) switch
    {
        1 => "dashed",
        2 => "dotted",
        _ => "dash-dot",
    };

    /// <summary>The pigment in words, for the legend's accessible name.</summary>
    public string PigmentName => Ink ? "dark" : "blue";
}
