using System.IO;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// The box an overflow menu lands in, computed from the declarations the
/// shipped stylesheet actually carries.
/// </summary>
/// <param name="Left">The box's leading edge, in CSS pixels from the viewport's leading edge.</param>
/// <param name="Width">The box's width, in CSS pixels.</param>
internal readonly record struct CssBox(double Left, double Width)
{
    /// <summary>The box's trailing edge.</summary>
    public double Right => Left + Width;
}

/// <summary>
/// Resolves where an absolutely positioned overflow menu lands, from the real
/// declarations of the real stylesheet.
/// </summary>
/// <remarks>
/// <para>
/// This models the one part of CSS layout the fault lived in: which ancestor is
/// the menu's containing block, how wide the menu resolves to once
/// <c>min-width</c> and <c>max-width</c> are applied, and where its edges land
/// once it is offset from that containing block. Everything else about layout
/// is out of scope, deliberately - the narrower the model, the more confidently
/// it can be checked against the audit's measured numbers.
/// </para>
/// <para>
/// It is faithful enough to reproduce the audit exactly: given the stylesheet
/// as it shipped before this fix, it computes a leading edge of -25.2px at
/// every compact width, which is what was measured in the browser.
/// </para>
/// </remarks>
internal static class OverflowMenuGeometry
{
    /// <summary>
    /// The Explorer's primitive stylesheet, relative to the repository root.
    /// </summary>
    public const string PrimitiveStylesheet =
        "src/lattice.explorer/DesignSystem/wwwroot/lattice-primitives.css";

    /// <summary>
    /// The Explorer's token stylesheet, relative to the repository root, which
    /// the primitives' <c>var()</c> references resolve against.
    /// </summary>
    public const string TokenStylesheet =
        "src/lattice.explorer/DesignSystem/wwwroot/lattice-tokens.css";

    /// <summary>
    /// The width of the tab strip's overflow-toggle box at compact, in CSS
    /// pixels.
    /// </summary>
    /// <remarks>
    /// From the pre-epic audit's browser measurement: at
    /// <c>CompactTabInlineCapacity = 1</c> the toggle sits immediately behind a
    /// single tab, and its trailing edge was measured at 166.8px, constant
    /// across the whole compact band. Reproducing those numbers is what makes
    /// this model trustworthy rather than merely self-consistent.
    /// </remarks>
    public const double CompactOverflowToggleWidthPx = 50;

    /// <summary>
    /// The trailing edge of the tab strip's overflow-toggle box at compact, in
    /// CSS pixels from the viewport's leading edge.
    /// </summary>
    public const double CompactOverflowToggleRightPx = 166.8;

    private static readonly string[] PositionedKeywords = ["relative", "absolute", "fixed", "sticky"];

    /// <summary>Loads the shipped primitive stylesheet.</summary>
    /// <returns>The parsed stylesheet.</returns>
    public static CssStylesheet LoadPrimitives() => Load(PrimitiveStylesheet);

    /// <summary>Loads the shipped token stylesheet.</summary>
    /// <returns>The parsed stylesheet.</returns>
    public static CssStylesheet LoadTokens() => Load(TokenStylesheet);

    private static CssStylesheet Load(string relativePath) =>
        CssStylesheet.Load(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            relativePath.Replace('/', Path.DirectorySeparatorChar)));

    /// <summary>
    /// The declarations in effect for an element matched by several rules,
    /// applied in cascade order: pass the least specific rule first and the
    /// most specific last.
    /// </summary>
    /// <param name="rulesInCascadeOrder">The matching rules, least specific first.</param>
    /// <returns>The merged declarations.</returns>
    public static IReadOnlyDictionary<string, string> Effective(
        params IReadOnlyDictionary<string, string>[] rulesInCascadeOrder)
    {
        ArgumentNullException.ThrowIfNull(rulesInCascadeOrder);

        var merged = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var rule in rulesInCascadeOrder)
        {
            foreach (var declaration in rule)
            {
                merged[declaration.Key] = declaration.Value;
            }
        }

        return merged;
    }

    /// <summary>
    /// Whether <paramref name="declarations"/> make the element a containing
    /// block for an absolutely positioned descendant.
    /// </summary>
    /// <param name="declarations">The element's declarations.</param>
    /// <returns><see langword="true"/> when the element is positioned.</returns>
    public static bool IsPositioned(IReadOnlyDictionary<string, string> declarations) =>
        declarations.TryGetValue("position", out var position)
        && Array.Exists(PositionedKeywords, keyword =>
            string.Equals(position.Trim(), keyword, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The containing block an absolutely positioned menu resolves against:
    /// the nearest positioned ancestor, or the viewport when none is
    /// positioned.
    /// </summary>
    /// <param name="ancestors">
    /// The candidate ancestors and their boxes, nearest first.
    /// </param>
    /// <param name="viewport">The viewport's own box.</param>
    /// <returns>The containing block's box.</returns>
    public static CssBox ContainingBlock(
        IReadOnlyList<(IReadOnlyDictionary<string, string> Declarations, CssBox Box)> ancestors,
        CssBox viewport)
    {
        ArgumentNullException.ThrowIfNull(ancestors);

        for (var i = 0; i < ancestors.Count; i++)
        {
            if (IsPositioned(ancestors[i].Declarations))
            {
                return ancestors[i].Box;
            }
        }

        return viewport;
    }

    /// <summary>
    /// Where the menu lands inside <paramref name="containingBlock"/>.
    /// </summary>
    /// <param name="menu">The menu rule's declarations.</param>
    /// <param name="containingBlock">The containing block's box.</param>
    /// <param name="intrinsicWidthPx">
    /// The width the menu's content would take if nothing constrained it. The
    /// invariant is asserted across a range of these, so it does not depend on
    /// any one guess about how wide a label set happens to be.
    /// </param>
    /// <param name="tokens">The custom properties <c>var()</c> resolves against.</param>
    /// <returns>
    /// The menu's box, or <see langword="null"/> when the rule offsets it from
    /// neither edge, which this model cannot place.
    /// </returns>
    public static CssBox? Resolve(
        IReadOnlyDictionary<string, string> menu,
        CssBox containingBlock,
        double intrinsicWidthPx,
        IReadOnlyDictionary<string, string> tokens)
    {
        ArgumentNullException.ThrowIfNull(menu);

        var width = intrinsicWidthPx;

        var maxWidth = Length(menu, "max-width", containingBlock.Width, tokens);
        if (maxWidth is double cap)
        {
            width = Math.Min(width, cap);
        }

        // A min-width beats a max-width, which is exactly why an unclamped one
        // would reintroduce the overhang on a narrow strip.
        var minWidth = Length(menu, "min-width", containingBlock.Width, tokens);
        if (minWidth is double floorWidth)
        {
            width = Math.Max(width, floorWidth);
        }

        // An over-constrained box resolves in favour of the leading edge, so
        // `left` is consulted first and `right` only when `left` is `auto`.
        var left = Length(menu, "left", containingBlock.Width, tokens);
        if (left is double leadingInset)
        {
            return new CssBox(containingBlock.Left + leadingInset, width);
        }

        var right = Length(menu, "right", containingBlock.Width, tokens);
        if (right is double trailingInset)
        {
            return new CssBox(containingBlock.Right - trailingInset - width, width);
        }

        return null;
    }

    private static double? Length(
        IReadOnlyDictionary<string, string> declarations,
        string property,
        double containingBlockWidthPx,
        IReadOnlyDictionary<string, string> tokens) =>
        declarations.TryGetValue(property, out var value)
            ? CssStylesheet.ResolveLength(value, containingBlockWidthPx, tokens)
            : null;
}
