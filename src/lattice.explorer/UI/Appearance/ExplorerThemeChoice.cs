namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// The palette the operator has asked for. One half of the appearance contract;
/// contrast is the other, orthogonal half
/// (<see cref="ExplorerContrastChoice"/>).
/// </summary>
/// <remarks>
/// There is deliberately no <c>HighContrast</c> member. The token layer treats
/// contrast as an axis that layers over whichever palette is active, so a
/// high-contrast theme would be a fourth palette to keep in step with the other
/// three rather than a modifier of them. Choosing "high contrast" therefore sets
/// <see cref="ExplorerContrastChoice.More"/> and leaves the palette alone.
/// </remarks>
public enum ExplorerThemeChoice
{
    /// <summary>
    /// Take the palette from the environment: the browser's
    /// <c>prefers-color-scheme</c>, or the desktop head's own application theme
    /// when it reports one. The default, so the Explorer respects the machine it
    /// is running on before anybody has expressed a preference.
    /// </summary>
    FollowSystem = 0,

    /// <summary>The light palette, whatever the environment reports.</summary>
    Light = 1,

    /// <summary>The dark palette, whatever the environment reports.</summary>
    Dark = 2,
}
