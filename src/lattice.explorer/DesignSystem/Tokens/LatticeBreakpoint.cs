namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// The Explorer's named viewport size classes. There are exactly three, they
/// are declared once here and once in the breakpoint stylesheet, and every
/// adaptive component refers to them <em>by name</em> rather than restating a
/// width. A component that needs a width has taken a wrong turn: add a token,
/// not a media query.
/// </summary>
/// <remarks>
/// The ordinal values are ascending by viewport width, so the enum compares
/// naturally (<c>breakpoint &gt;= LatticeBreakpoint.Medium</c> reads as "medium
/// or wider"). Use <see cref="LatticeBreakpoints.IsAtLeast"/> when you want that
/// intent spelled out.
/// </remarks>
public enum LatticeBreakpoint
{
    /// <summary>
    /// The narrowest class: phones and narrow split panes. Navigation collapses
    /// to a bottom bar with an overflow menu, tab strips collapse to a single
    /// tab plus an overflow menu, and tabular data reflows to a card list.
    /// </summary>
    Compact = 0,

    /// <summary>
    /// The middle class: tablets, small laptops, and half-screen windows.
    /// Navigation becomes a dismissible drawer and tab strips keep a few tabs
    /// inline with the remainder in an overflow menu.
    /// </summary>
    Medium = 1,

    /// <summary>
    /// The widest class: desktop windows. Navigation is a persistent sidebar,
    /// tab strips render inline, and tabular data renders as a table.
    /// </summary>
    Expanded = 2,
}
