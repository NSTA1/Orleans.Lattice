namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// One resolved appearance: the three attribute-bearing values the document is
/// to carry, as a value type so passing it around a render path allocates
/// nothing.
/// </summary>
/// <param name="Theme">The palette, or <see cref="ExplorerThemeChoice.FollowSystem"/> to leave it to the document.</param>
/// <param name="Contrast">The contrast overlay, or <see cref="ExplorerContrastChoice.FollowSystem"/> to leave it to the operating system.</param>
/// <param name="Density">The density, or <see cref="ExplorerDensityChoice.FollowLayout"/> to leave it to each adaptive root.</param>
public readonly record struct ExplorerAppearanceState(
    ExplorerThemeChoice Theme,
    ExplorerContrastChoice Contrast,
    ExplorerDensityChoice Density)
{
    /// <summary>
    /// The out-of-the-box appearance: follow the environment on every axis. What
    /// an operator who has never expressed a preference gets, and what a reset
    /// returns them to.
    /// </summary>
    public static ExplorerAppearanceState Default => default;

    /// <summary>
    /// Whether every axis is left to the environment, so applying this state
    /// removes all three attributes rather than setting any.
    /// </summary>
    public bool IsFollowingEverything =>
        Theme == ExplorerThemeChoice.FollowSystem
        && Contrast == ExplorerContrastChoice.FollowSystem
        && Density == ExplorerDensityChoice.FollowLayout;
}
