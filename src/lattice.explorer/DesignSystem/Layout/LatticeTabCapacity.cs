namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// How many items of a strip fit inline in a given width, measured from the
/// items' own labels rather than assumed from a fixed per-breakpoint constant.
/// </summary>
/// <remarks>
/// <para>
/// A fixed capacity cannot be right: it does not know whether the strip holds
/// <c>Data</c> or <c>Retention and residency</c>, so it either clips a long
/// label or wastes half the row on short ones. This measures instead - label
/// width from <see cref="LatticeTextMetrics"/>, item padding, gaps, and the
/// overflow control's own width from <see cref="LatticeStripMetrics"/> - and
/// answers how many items actually fit.
/// </para>
/// <para>
/// The result is the <em>capacity</em>, not the split.
/// <see cref="LatticeOverflowLayout.Resolve"/> turns it into the split, and
/// keeps the rule that the active item is always inline.
/// </para>
/// <para>
/// Both measurements walk the strip by index and read each label as a span, so
/// a render pass may call this per pass without allocating.
/// </para>
/// </remarks>
public static class LatticeTabCapacity
{
    /// <summary>
    /// How many of <paramref name="tabs"/> fit inline in
    /// <paramref name="availableWidthPx"/>, using the shipped tab strip's
    /// geometry.
    /// </summary>
    /// <param name="tabs">The strip's items, in display order.</param>
    /// <param name="availableWidthPx">
    /// The inline extent the strip has to fill, in CSS pixels.
    /// </param>
    /// <returns>
    /// The number of items that fit: <c>0</c> for an empty strip, otherwise at
    /// least one and never more than the strip holds.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="tabs"/> is null.</exception>
    public static int Measure(IReadOnlyList<LatticeTabItem> tabs, double availableWidthPx) =>
        Measure(tabs, availableWidthPx, LatticeStripMetrics.Default);

    /// <summary>
    /// How many of <paramref name="tabs"/> fit inline in
    /// <paramref name="availableWidthPx"/>, using
    /// <paramref name="metrics"/>.
    /// </summary>
    /// <param name="tabs">The strip's items, in display order.</param>
    /// <param name="availableWidthPx">
    /// The inline extent the strip has to fill, in CSS pixels. A non-positive
    /// width yields one item for a non-empty strip, because a strip always
    /// shows where the caller is.
    /// </param>
    /// <param name="metrics">The strip's geometry.</param>
    /// <returns>
    /// The number of items that fit: <c>0</c> for an empty strip, otherwise at
    /// least one and never more than the strip holds.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="tabs"/> is null.</exception>
    public static int Measure(
        IReadOnlyList<LatticeTabItem> tabs,
        double availableWidthPx,
        LatticeStripMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(tabs);

        var count = tabs.Count;
        if (count == 0)
        {
            return 0;
        }

        var available = availableWidthPx - metrics.GutterPx;
        if (available <= 0)
        {
            return 1;
        }

        // First ask whether the whole strip fits, because a strip that fits
        // renders no overflow control and therefore does not have to pay for
        // one. Asking the other way round would collapse a strip that fits by
        // a hair into an overflow menu it never needed.
        var whole = 0.0;
        for (var i = 0; i < count; i++)
        {
            whole += metrics.MeasureItemWidth(tabs[i].Label);
            if (i > 0)
            {
                whole += metrics.ItemGapPx;
            }
        }

        if (whole <= available)
        {
            return count;
        }

        // It does not fit, so the overflow control is rendered and claims its
        // width from the same row.
        var budget = available - metrics.OverflowControlPx - metrics.ItemGapPx;
        var used = 0.0;
        var fitted = 0;

        for (var i = 0; i < count; i++)
        {
            var next = used + metrics.MeasureItemWidth(tabs[i].Label) + (i > 0 ? metrics.ItemGapPx : 0);
            if (next > budget)
            {
                break;
            }

            used = next;
            fitted++;
        }

        // Never zero: a strip that cannot fit even its first label still shows
        // it, clipped by the browser, rather than collapsing to an overflow
        // control with nothing beside it.
        return fitted < 1 ? 1 : fitted;
    }
}
