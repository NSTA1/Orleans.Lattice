namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// How much contrast the operator has asked for. Orthogonal to
/// <see cref="ExplorerThemeChoice"/>: the high-contrast overlay layers over
/// whichever palette is active, so the two axes compose instead of multiplying
/// into a combinatorial set of palettes.
/// </summary>
public enum ExplorerContrastChoice
{
    /// <summary>
    /// Take the answer from the environment's own <c>prefers-contrast</c> hint.
    /// The default, and the only value that leaves the attribute off the document
    /// entirely so the operating system's setting is honoured.
    /// </summary>
    FollowSystem = 0,

    /// <summary>
    /// The standard overlay, explicitly. This is not the same as
    /// <see cref="FollowSystem"/>: it opts <em>out</em> of an operating-system
    /// hint that would otherwise raise the contrast, for an operator who has that
    /// setting on system-wide but does not want it here.
    /// </summary>
    Standard = 1,

    /// <summary>
    /// The high-contrast overlay, whatever the environment reports. Pairs with
    /// whichever palette <see cref="ExplorerThemeChoice"/> selected.
    /// </summary>
    More = 2,
}
