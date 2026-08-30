namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One plotted series: its legend label, the <c>points</c> attribute of the SVG
/// polyline that draws it, the palette slot it is drawn in, and its most recent
/// reading.
/// </summary>
/// <remarks>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> over strings the chart already composed, so the
/// component that renders it allocates nothing per frame.
/// </remarks>
/// <param name="Label">The legend label, derived from the series' own labels.</param>
/// <param name="Points">
/// The polyline's <c>points</c> attribute in the chart's view-box coordinate
/// space, or <see cref="string.Empty"/> when the series had nothing plottable.
/// </param>
/// <param name="PaletteIndex">
/// The palette slot, so the stylesheet - not the component - owns the colours.
/// </param>
/// <param name="Reading">
/// The most recent finite value, already formatted with the entry's unit.
/// <para>
/// Formatted here rather than in the component on purpose: a legend entry is
/// re-rendered on every refresh, every gate change, and every breakpoint change,
/// and formatting a <see cref="double"/> and appending a unit on each of those
/// is a per-frame allocation for a value that only changes when a new result
/// arrives.
/// </para>
/// </param>
public readonly record struct TelemetryPlot(
    string Label,
    string Points,
    int PaletteIndex,
    string Reading)
{
    /// <summary><see langword="true"/> when the series contributed no drawable geometry.</summary>
    public bool IsEmpty => Points.Length == 0;
}
