namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// When moving between tabs with the arrow keys also selects the tab moved to.
/// </summary>
/// <remarks>
/// <para>
/// Both are conformant WAI-ARIA tabs behaviours. Automatic activation is the
/// better default: with one keypress the caller both moves and sees the panel,
/// and for a cheap, always-permitted switch there is nothing to be gained by
/// asking for a second key.
/// </para>
/// <para>
/// Manual activation is required whenever selecting a tab is expensive or can be
/// refused. Under automatic activation an arrow key is a selection attempt, so a
/// tab that cannot be selected cannot be reached either - the strip restores
/// focus to whatever stayed active and the key appears to do nothing. That makes
/// an area a keyboard caller cannot reach at all, which matters most for exactly
/// the areas they most need to reach: the ones that are refused and carry the
/// explanation of why.
/// </para>
/// </remarks>
public enum LatticeTabsActivation
{
    /// <summary>
    /// An arrow key moves focus and selects the tab it lands on. The default.
    /// </summary>
    Automatic = 0,

    /// <summary>
    /// An arrow key moves focus only; Enter or Space selects the focused tab.
    /// </summary>
    Manual = 1,
}
