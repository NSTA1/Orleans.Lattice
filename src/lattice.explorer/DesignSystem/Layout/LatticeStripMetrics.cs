namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// The geometry of one adaptive strip: the type size its items are set in, the
/// space each item adds around its label, the gap between items, the width the
/// overflow control claims, and any gutter the strip must leave clear.
/// </summary>
/// <remarks>
/// <para>
/// This is the input <see cref="LatticeTabCapacity"/> measures against. Its
/// defaults describe the shipped tab strip - the type size, inline padding and
/// gap the primitive stylesheet gives a <c>.lx-tab</c> - so the common caller
/// supplies nothing. A surface that knows its strip is set differently (a
/// denser sub-surface control, a strip inside a padded pane) passes its own.
/// </para>
/// <para>
/// A <see langword="readonly" /> <see langword="record" />
/// <see langword="struct" /> of five doubles: passing one costs no allocation,
/// so a render pass may build one per call.
/// </para>
/// </remarks>
/// <param name="FontSizePx">
/// The rendered type size of an item's label, in CSS pixels.
/// </param>
/// <param name="ItemPaddingInlinePx">
/// The padding an item adds on <em>each</em> side of its label, in CSS pixels.
/// </param>
/// <param name="ItemGapPx">The gap between two adjacent items, in CSS pixels.</param>
/// <param name="OverflowControlPx">
/// The width the overflow control claims once the strip overflows, in CSS
/// pixels. Reserved only when the strip does not fit, because a strip that fits
/// renders no overflow control.
/// </param>
/// <param name="GutterPx">
/// Space the strip must leave clear inside its own box - a host's padding, or a
/// trailing control sharing the row - in CSS pixels.
/// </param>
public readonly record struct LatticeStripMetrics(
    double FontSizePx,
    double ItemPaddingInlinePx,
    double ItemGapPx,
    double OverflowControlPx,
    double GutterPx)
{
    /// <summary>
    /// The type size of a tab label: <c>--lx-text-body</c> (0.9rem) at the
    /// browser's 16px root.
    /// </summary>
    public const double TabFontSizePx = 14.4;

    /// <summary>
    /// The inline padding a tab adds on each side of its label:
    /// <c>--lx-space-5</c>.
    /// </summary>
    public const double TabPaddingInlinePx = 12;

    /// <summary>The gap between two tabs: <c>--lx-space-2</c>.</summary>
    public const double TabGapPx = 4;

    /// <summary>
    /// The width the overflow control claims: its own inline padding either
    /// side of a short label.
    /// </summary>
    public const double OverflowControlWidthPx = 64;

    /// <summary>
    /// The type size of a segmented option, which is set smaller than a tab so
    /// a toggle reads as subordinate to the strip above it.
    /// </summary>
    public const double SegmentFontSizePx = 12.8;

    /// <summary>
    /// The inline padding a segmented option adds on each side of its label:
    /// <c>--lx-space-4</c>.
    /// </summary>
    public const double SegmentPaddingInlinePx = 8;

    /// <summary>
    /// The width a segmented control's overflow control claims, sized for its
    /// smaller type.
    /// </summary>
    public const double SegmentOverflowControlWidthPx = 56;

    /// <summary>
    /// The padding a segmented control's track adds inside its border, on both
    /// sides together.
    /// </summary>
    public const double SegmentTrackPaddingPx = 4;

    /// <summary>
    /// The geometry of the shipped tab strip, and the value
    /// <see cref="LatticeTabCapacity"/> measures against when a caller supplies
    /// none.
    /// </summary>
    public static LatticeStripMetrics Default { get; } = new(
        TabFontSizePx,
        TabPaddingInlinePx,
        TabGapPx,
        OverflowControlWidthPx,
        GutterPx: 0);

    /// <summary>
    /// The geometry of a segmented control: smaller type, tighter padding, and
    /// no gap, because its options abut inside one bordered track.
    /// </summary>
    public static LatticeStripMetrics Segment { get; } = new(
        SegmentFontSizePx,
        SegmentPaddingInlinePx,
        ItemGapPx: 0,
        SegmentOverflowControlWidthPx,
        SegmentTrackPaddingPx);

    /// <summary>
    /// The width one item occupies: its label at
    /// <see cref="FontSizePx"/> plus <see cref="ItemPaddingInlinePx"/> on each
    /// side.
    /// </summary>
    /// <param name="label">The item's label. <see langword="null"/> measures as empty.</param>
    /// <returns>The item's estimated width in CSS pixels.</returns>
    public double MeasureItemWidth(string? label) =>
        LatticeTextMetrics.Measure(label, FontSizePx) + (2 * ItemPaddingInlinePx);
}
