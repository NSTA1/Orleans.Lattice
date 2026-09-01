namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// A deterministic estimate of how wide a run of text renders, in the absence
/// of a browser to measure it.
/// </summary>
/// <remarks>
/// <para>
/// The Explorer's adaptive primitives decide how many items fit inline before
/// the remainder moves into an overflow menu. Deciding that from a fixed
/// per-breakpoint count cannot adapt to label length, so a long label overflows
/// its slot while a short one wastes space. Deciding it from a real browser
/// measurement would put a JavaScript round trip on the render path and make
/// the layout untestable without a browser.
/// </para>
/// <para>
/// This is the third option: a per-character advance table for a typical UI
/// sans-serif, expressed in units of 1/256 em, summed over the text. It is an
/// estimate, not a rasteriser - a proportional face will differ by a few
/// percent - but it is monotone in label length, stable across runs, identical
/// on every platform, and good enough to decide how many tabs fit. Every
/// primitive that uses it also clamps to at least one item, so an estimate that
/// is slightly wrong costs a little wasted space, never a broken strip.
/// </para>
/// <para>
/// It allocates nothing: the table is a <see cref="ReadOnlySpan{T}"/> over
/// static data and the measurement walks the text as a span.
/// </para>
/// </remarks>
public static class LatticeTextMetrics
{
    /// <summary>
    /// The advance used for a character outside the table's range - accented
    /// Latin, Greek, Cyrillic and the like. Deliberately a little generous, so
    /// an unknown character is more likely to reserve too much space than too
    /// little.
    /// </summary>
    public const double FallbackAdvanceEm = 0.60;

    /// <summary>
    /// The advance used for a character from a full-width script (CJK, Hangul,
    /// the fullwidth forms), which occupies a whole em rather than a fraction
    /// of one.
    /// </summary>
    public const double WideAdvanceEm = 1.0;

    /// <summary>
    /// The lowest code point treated as full-width. Chosen at the start of the
    /// Hangul Jamo block, above every Latin, Greek and Cyrillic form.
    /// </summary>
    private const int WideRangeStart = 0x1100;

    private const int TableStart = ' ';
    private const int TableEnd = '~';
    private const double AdvanceUnit = 1.0 / 256.0;

    // Advances for U+0020..U+007E in units of 1/256 em, in code-point order.
    // A ReadOnlySpan over a constant byte array compiles to a reference into
    // the assembly's data section, so reading it never allocates and never runs
    // a static constructor.
    private static ReadOnlySpan<byte> Advances =>
    [
        67, 77, 102, 154, 141, 205, 174, 56, 84, 84, 115, 148, 67, 84, 67, 102,
        146, 146, 146, 146, 146, 146, 146, 146, 146, 146, 67, 67, 148, 148, 148, 128,
        235, 166, 166, 174, 179, 154, 148, 184, 184, 72, 128, 166, 141, 218, 187, 192,
        161, 192, 166, 159, 154, 184, 166, 243, 161, 154, 154, 84, 102, 84, 128, 128,
        84, 141, 148, 128, 148, 141, 90, 148, 146, 64, 64, 136, 64, 223, 146, 146,
        148, 148, 95, 123, 95, 146, 133, 205, 133, 133, 123, 90, 67, 90, 148,
    ];

    /// <summary>
    /// The width of <paramref name="text"/> in ems: the sum of its characters'
    /// advances, independent of any font size.
    /// </summary>
    /// <param name="text">The text to measure. An empty span measures zero.</param>
    /// <returns>The estimated width in ems.</returns>
    public static double MeasureEm(ReadOnlySpan<char> text)
    {
        var advances = Advances;
        var total = 0.0;

        foreach (var character in text)
        {
            if (character >= TableStart && character <= TableEnd)
            {
                total += advances[character - TableStart] * AdvanceUnit;
            }
            else if (character >= WideRangeStart)
            {
                total += WideAdvanceEm;
            }
            else
            {
                total += FallbackAdvanceEm;
            }
        }

        return total;
    }

    /// <summary>
    /// The width of <paramref name="text"/> in CSS pixels at
    /// <paramref name="fontSizePx"/>.
    /// </summary>
    /// <param name="text">The text to measure. An empty span measures zero.</param>
    /// <param name="fontSizePx">
    /// The rendered font size in CSS pixels. A non-positive size measures zero,
    /// so a caller can pass a size it has not resolved yet without guarding it.
    /// </param>
    /// <returns>The estimated width in CSS pixels.</returns>
    public static double Measure(ReadOnlySpan<char> text, double fontSizePx) =>
        fontSizePx <= 0 ? 0 : MeasureEm(text) * fontSizePx;

    /// <summary>
    /// The width of <paramref name="text"/> in CSS pixels at
    /// <paramref name="fontSizePx"/>.
    /// </summary>
    /// <param name="text">
    /// The text to measure. <see langword="null"/> and empty both measure zero.
    /// </param>
    /// <param name="fontSizePx">The rendered font size in CSS pixels.</param>
    /// <returns>The estimated width in CSS pixels.</returns>
    public static double Measure(string? text, double fontSizePx) =>
        Measure(text.AsSpan(), fontSizePx);
}
