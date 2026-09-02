namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// One overflow menu's measured geometry, read from the browser in a single layout
/// read so the numbers all describe the same frame.
/// </summary>
internal sealed record OverflowGeometry
{
    /// <summary>The menu's left edge, in CSS pixels from the viewport's left edge.</summary>
    public double Left { get; init; }

    /// <summary>The menu's right edge, in CSS pixels from the viewport's left edge.</summary>
    public double Right { get; init; }

    /// <summary>The menu's width in CSS pixels.</summary>
    public double Width { get; init; }

    /// <summary>The viewport width the menu was measured against.</summary>
    public double ViewportWidth { get; init; }

    /// <summary>How many menu items the menu holds; zero means nothing was measured.</summary>
    public int Items { get; init; }

    /// <summary>
    /// How far the menu's leading edge falls outside the viewport, in CSS pixels; zero
    /// when it is inside. The audit measured a constant 25.2px here right across the
    /// compact band.
    /// </summary>
    public double LeadingOverflow => Left < 0 ? -Left : 0;

    /// <summary>How far the menu's trailing edge falls outside the viewport, in CSS pixels.</summary>
    public double TrailingOverflow => Right > ViewportWidth ? Right - ViewportWidth : 0;

    /// <summary><see langword="true"/> when the menu lies wholly inside the viewport.</summary>
    public bool IsContained => LeadingOverflow == 0 && TrailingOverflow == 0;

    /// <inheritdoc />
    public override string ToString() =>
        $"left={Left:0.##} right={Right:0.##} width={Width:0.##} viewport={ViewportWidth:0.##} "
        + $"items={Items} leadingOverflow={LeadingOverflow:0.##} trailingOverflow={TrailingOverflow:0.##}";
}
