namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// The axis an adaptive tab strip runs along.
/// </summary>
/// <remarks>
/// The axis is not merely cosmetic: it decides the strip's
/// <c>aria-orientation</c> and therefore which arrow keys move between tabs,
/// which the WAI-ARIA tabs pattern defines per axis.
/// </remarks>
public enum LatticeTabsOrientation
{
    /// <summary>
    /// A row of tabs. Left and Right move between them. The default, and the
    /// shape a tab tier above its panel wears.
    /// </summary>
    Horizontal = 0,

    /// <summary>
    /// A column of tabs - a rail beside its panel. Up and Down move between
    /// them.
    /// </summary>
    /// <remarks>
    /// A vertical strip renders every tab inline by default and scrolls, as a
    /// rail does, rather than collapsing the remainder into an overflow menu;
    /// the same reasoning applies to the vertical shapes of
    /// <see cref="Components.LatticeAdaptiveNav"/>. Pin
    /// <c>InlineCapacity</c> if you want a vertical strip to overflow anyway.
    /// </remarks>
    Vertical = 1,
}
