namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// The single source of the Explorer's breakpoint values on the .NET side:
/// the minimum viewport width of each <see cref="LatticeBreakpoint"/>, the
/// stable lowercase names the markup and stylesheets key off, and the inline
/// capacities the adaptive primitives use before they overflow.
/// </summary>
/// <remarks>
/// <para>
/// These numbers are mirrored exactly once in the stylesheet layer
/// (<c>lattice-breakpoints.css</c>), which is the only file in the product
/// permitted to carry a width media query. A hygiene guard
/// (<c>BreakpointTokenHygieneTests</c>) fails the build both when another file
/// grows a width media query and when the stylesheet's declared widths drift
/// from the constants below, so the two copies can never disagree silently.
/// </para>
/// <para>
/// Every member is a pure function over its arguments and allocates nothing:
/// the name accessors return interned literals and the numeric accessors return
/// constants, so a render path may call them per item without cost.
/// </para>
/// </remarks>
public static class LatticeBreakpoints
{
    /// <summary>
    /// The narrowest viewport width, in CSS pixels, that resolves to
    /// <see cref="LatticeBreakpoint.Medium"/>. Anything below this is
    /// <see cref="LatticeBreakpoint.Compact"/>.
    /// </summary>
    public const int MediumMinimumWidth = 600;

    /// <summary>
    /// The narrowest viewport width, in CSS pixels, that resolves to
    /// <see cref="LatticeBreakpoint.Expanded"/>.
    /// </summary>
    public const int ExpandedMinimumWidth = 1024;

    /// <summary>
    /// The breakpoint assumed before a viewport measurement arrives: static
    /// server rendering, a prerender pass, and a host without JavaScript all
    /// land here. Expanded is chosen deliberately, because it is the layout the
    /// Explorer shipped with, so a head that never reports a width is
    /// byte-for-byte unchanged.
    /// </summary>
    public const LatticeBreakpoint Default = LatticeBreakpoint.Expanded;

    /// <summary>
    /// The maximum number of navigation destinations rendered inline in the
    /// compact bottom bar before the remainder moves into the overflow menu.
    /// </summary>
    public const int CompactNavigationInlineCapacity = 4;

    /// <summary>
    /// The maximum number of tabs rendered inline in a tab strip at
    /// <see cref="LatticeBreakpoint.Compact"/> before the strip collapses to an
    /// overflow menu. One, so the active tab is always the visible one.
    /// </summary>
    public const int CompactTabInlineCapacity = 1;

    /// <summary>
    /// The maximum number of tabs rendered inline in a tab strip at
    /// <see cref="LatticeBreakpoint.Medium"/>.
    /// </summary>
    public const int MediumTabInlineCapacity = 4;

    /// <summary>
    /// The maximum number of tabs rendered inline in a tab strip at
    /// <see cref="LatticeBreakpoint.Expanded"/>. A strip wider than this still
    /// overflows rather than scrolling off-screen.
    /// </summary>
    public const int ExpandedTabInlineCapacity = 8;

    /// <summary>The stable name of <see cref="LatticeBreakpoint.Compact"/>.</summary>
    public const string CompactName = "compact";

    /// <summary>The stable name of <see cref="LatticeBreakpoint.Medium"/>.</summary>
    public const string MediumName = "medium";

    /// <summary>The stable name of <see cref="LatticeBreakpoint.Expanded"/>.</summary>
    public const string ExpandedName = "expanded";

    /// <summary>
    /// The custom property the breakpoint stylesheet publishes
    /// <see cref="MediumMinimumWidth"/> under, so the value can be read back
    /// from CSS, from script, and by the drift guard.
    /// </summary>
    public const string MediumMinimumWidthCustomProperty = "--lx-breakpoint-medium-min";

    /// <summary>
    /// The custom property the breakpoint stylesheet publishes
    /// <see cref="ExpandedMinimumWidth"/> under.
    /// </summary>
    public const string ExpandedMinimumWidthCustomProperty = "--lx-breakpoint-expanded-min";

    // A single shared array, handed out as IReadOnlyList so callers cannot
    // mutate it. Enumerating `All` therefore allocates only the enumerator the
    // interface forces, and indexing it allocates nothing at all.
    private static readonly LatticeBreakpoint[] AllOrdered =
    [
        LatticeBreakpoint.Compact,
        LatticeBreakpoint.Medium,
        LatticeBreakpoint.Expanded,
    ];

    /// <summary>
    /// Every breakpoint, ordered narrowest first. Useful for exhaustive tests
    /// and for a host that renders a breakpoint picker.
    /// </summary>
    public static IReadOnlyList<LatticeBreakpoint> All => AllOrdered;

    /// <summary>
    /// Resolves a viewport width in CSS pixels to its breakpoint. A negative
    /// width is treated as zero, so a bogus measurement degrades to
    /// <see cref="LatticeBreakpoint.Compact"/> rather than throwing on a render
    /// path.
    /// </summary>
    /// <param name="viewportWidth">The viewport width in CSS pixels.</param>
    /// <returns>The breakpoint the width falls in.</returns>
    public static LatticeBreakpoint Resolve(int viewportWidth) => viewportWidth switch
    {
        >= ExpandedMinimumWidth => LatticeBreakpoint.Expanded,
        >= MediumMinimumWidth => LatticeBreakpoint.Medium,
        _ => LatticeBreakpoint.Compact,
    };

    /// <summary>
    /// The narrowest viewport width, in CSS pixels, that resolves to
    /// <paramref name="breakpoint"/>. Zero for
    /// <see cref="LatticeBreakpoint.Compact"/>, which has no lower bound.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to measure.</param>
    /// <returns>The breakpoint's inclusive minimum width.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="breakpoint"/> is not a declared breakpoint.
    /// </exception>
    public static int MinimumWidth(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => 0,
        LatticeBreakpoint.Medium => MediumMinimumWidth,
        LatticeBreakpoint.Expanded => ExpandedMinimumWidth,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };

    /// <summary>
    /// The stable lowercase name of <paramref name="breakpoint"/>, as used by
    /// the <c>data-lx-breakpoint</c> attribute and the stylesheet selectors.
    /// Returns an interned literal, so this allocates nothing.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to name.</param>
    /// <returns>The breakpoint's stable name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="breakpoint"/> is not a declared breakpoint.
    /// </exception>
    public static string Name(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => CompactName,
        LatticeBreakpoint.Medium => MediumName,
        LatticeBreakpoint.Expanded => ExpandedName,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };

    /// <summary>
    /// Parses a stable breakpoint name produced by <see cref="Name"/>. Matching
    /// is ordinal and case-insensitive.
    /// </summary>
    /// <param name="name">The breakpoint name to parse.</param>
    /// <param name="breakpoint">The parsed breakpoint when parsing succeeds.</param>
    /// <returns><see langword="true"/> when <paramref name="name"/> is a known breakpoint name.</returns>
    public static bool TryParseName(string? name, out LatticeBreakpoint breakpoint)
    {
        if (string.Equals(name, CompactName, StringComparison.OrdinalIgnoreCase))
        {
            breakpoint = LatticeBreakpoint.Compact;
            return true;
        }

        if (string.Equals(name, MediumName, StringComparison.OrdinalIgnoreCase))
        {
            breakpoint = LatticeBreakpoint.Medium;
            return true;
        }

        if (string.Equals(name, ExpandedName, StringComparison.OrdinalIgnoreCase))
        {
            breakpoint = LatticeBreakpoint.Expanded;
            return true;
        }

        breakpoint = Default;
        return false;
    }

    /// <summary>
    /// Whether <paramref name="breakpoint"/> is <paramref name="minimum"/> or
    /// wider. Reads better than an ordinal comparison at a call site and states
    /// the intent the enum ordering encodes.
    /// </summary>
    /// <param name="breakpoint">The breakpoint under test.</param>
    /// <param name="minimum">The narrowest breakpoint that satisfies the test.</param>
    /// <returns><see langword="true"/> when the breakpoint is at least as wide as the minimum.</returns>
    public static bool IsAtLeast(this LatticeBreakpoint breakpoint, LatticeBreakpoint minimum) =>
        breakpoint >= minimum;

    /// <summary>
    /// The maximum number of tabs an adaptive tab strip renders inline at
    /// <paramref name="breakpoint"/> before it collapses the remainder into an
    /// overflow menu.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to size for.</param>
    /// <returns>The inline tab capacity, always at least one.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="breakpoint"/> is not a declared breakpoint.
    /// </exception>
    public static int TabInlineCapacity(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => CompactTabInlineCapacity,
        LatticeBreakpoint.Medium => MediumTabInlineCapacity,
        LatticeBreakpoint.Expanded => ExpandedTabInlineCapacity,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };

    /// <summary>
    /// The maximum number of navigation destinations rendered inline at
    /// <paramref name="breakpoint"/>. Only the compact bottom bar overflows;
    /// the medium drawer and the expanded sidebar are vertical lists that scroll
    /// and therefore render every destination.
    /// </summary>
    /// <param name="breakpoint">The breakpoint to size for.</param>
    /// <returns>
    /// The inline navigation capacity, or <see cref="int.MaxValue"/> when the
    /// breakpoint renders every destination.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="breakpoint"/> is not a declared breakpoint.
    /// </exception>
    public static int NavigationInlineCapacity(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => CompactNavigationInlineCapacity,
        LatticeBreakpoint.Medium or LatticeBreakpoint.Expanded => int.MaxValue,
        _ => throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint."),
    };
}
