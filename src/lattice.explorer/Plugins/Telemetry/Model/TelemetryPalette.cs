namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The pre-composed CSS class names a chart's palette slots resolve to.
/// </summary>
/// <remarks>
/// <para>
/// A component that wrote <c>class="lxt-series lxt-series-@plot.PaletteIndex"</c>
/// would concatenate a string from an <see langword="int"/> for every plotted
/// series on every render - and a metrics panel re-renders on every refresh,
/// every gate change, and every breakpoint change. Handing out an interned
/// literal from a fixed table costs nothing instead.
/// </para>
/// <para>
/// The table's length is the palette the stylesheet publishes classes for, and
/// <see cref="TelemetryChart"/> already reduces a slot modulo that length, so an
/// index is always in range. An out-of-range one still resolves to slot zero
/// rather than throwing, because a chart is not worth a render fault.
/// </para>
/// </remarks>
public static class TelemetryPalette
{
    private static readonly string[] SeriesClasses =
    [
        "lxt-series lxt-series-0",
        "lxt-series lxt-series-1",
        "lxt-series lxt-series-2",
        "lxt-series lxt-series-3",
        "lxt-series lxt-series-4",
        "lxt-series lxt-series-5",
    ];

    private static readonly string[] SwatchClasses =
    [
        "lxt-swatch lxt-series-0",
        "lxt-swatch lxt-series-1",
        "lxt-swatch lxt-series-2",
        "lxt-swatch lxt-series-3",
        "lxt-swatch lxt-series-4",
        "lxt-swatch lxt-series-5",
    ];

    /// <summary>The number of palette slots the stylesheet publishes.</summary>
    public static int Size => SeriesClasses.Length;

    /// <summary>The polyline class for palette slot <paramref name="index"/>.</summary>
    /// <param name="index">The palette slot.</param>
    /// <returns>The class attribute value.</returns>
    public static string SeriesClass(int index) =>
        SeriesClasses[(uint)index < (uint)SeriesClasses.Length ? index : 0];

    /// <summary>The legend swatch class for palette slot <paramref name="index"/>.</summary>
    /// <param name="index">The palette slot.</param>
    /// <returns>The class attribute value.</returns>
    public static string SwatchClass(int index) =>
        SwatchClasses[(uint)index < (uint)SwatchClasses.Length ? index : 0];
}
