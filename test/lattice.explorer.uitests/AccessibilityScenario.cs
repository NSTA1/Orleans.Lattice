using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One cell of the accessibility sweep matrix: the theme, breakpoint band,
/// identity, and contrast overlay a surface is swept under.
/// <para>
/// A struct rather than a class because NUnit materialises the whole case source at
/// discovery and holds it for the run; the record's generated <see cref="ToString"/>
/// is overridden so the test name reads as the state under test rather than as a
/// type dump.
/// </para>
/// </summary>
/// <param name="Theme">The colour theme in effect.</param>
/// <param name="Breakpoint">The breakpoint band the viewport is classified into.</param>
/// <param name="SignedIn">Whether a credential is applied before sweeping.</param>
/// <param name="Contrast">
/// The contrast overlay in effect. Defaults to
/// <see cref="ExplorerContrastChoice.FollowSystem"/>, which writes no attribute at
/// all and so leaves the environment's own answer standing - the state every cell
/// swept before the contrast axis existed.
/// </param>
public readonly record struct AccessibilityScenario(
    ExplorerTheme Theme,
    LatticeBreakpoint Breakpoint,
    bool SignedIn,
    ExplorerContrastChoice Contrast = ExplorerContrastChoice.FollowSystem)
{
    /// <summary>
    /// The band the high-contrast cells are swept at.
    /// <para>
    /// Contrast is a palette-level concern: the overlay changes colours and nothing
    /// about layout, so sweeping it at every band would re-prove what the twelve
    /// standard cells already establish about layout, at three times the cost. The
    /// expanded band is chosen because it renders the most surface at once, so one
    /// run puts the most tokens in front of axe.
    /// </para>
    /// </summary>
    private const LatticeBreakpoint ContrastBreakpoint = LatticeBreakpoint.Expanded;

    /// <summary>
    /// Every cell of the sweep: the full theme x breakpoint x identity matrix at the
    /// environment's own contrast, plus a targeted high-contrast pass.
    /// <para>
    /// The standard cross product is taken in full rather than sampled. It is only
    /// twelve cases, and the interactions are exactly where the defects live: the
    /// compact band is the one that overflows, the light palette is the one no one
    /// has ever looked at, and signing in is what changes which areas the gates
    /// admit. A pairwise reduction would drop cells such as compact-and-light, which
    /// is precisely the combination nothing in this repository has ever rendered.
    /// </para>
    /// <para>
    /// The contrast axis is added as four cells rather than by multiplying the
    /// matrix to twenty-four. High contrast is an overlay of colour tokens over
    /// whichever palette is active, so what it can regress is contrast ratios, not
    /// layout - and the lane is already close to its budget. Both palettes are
    /// covered because the overlay is written twice, once per palette, and both
    /// identities because a credential changes which areas render and therefore
    /// which tokens are on screen at all.
    /// </para>
    /// </summary>
    public static IEnumerable<AccessibilityScenario> All()
    {
        foreach (var theme in Enum.GetValues<ExplorerTheme>())
        {
            foreach (var breakpoint in LatticeBreakpoints.All)
            {
                yield return new AccessibilityScenario(theme, breakpoint, SignedIn: false);
                yield return new AccessibilityScenario(theme, breakpoint, SignedIn: true);
            }

            yield return new AccessibilityScenario(
                theme, ContrastBreakpoint, SignedIn: false, ExplorerContrastChoice.More);
            yield return new AccessibilityScenario(
                theme, ContrastBreakpoint, SignedIn: true, ExplorerContrastChoice.More);
        }
    }

    /// <summary>A short human description used in the test name and failure messages.</summary>
    /// <remarks>
    /// The contrast overlay is named only when one is actually requested, so the
    /// twelve standard cells keep the names they have always had rather than every
    /// one of them gaining a redundant qualifier.
    /// </remarks>
    public override string ToString() =>
        $"{Theme} / {LatticeBreakpoints.Name(Breakpoint)} / {(SignedIn ? "signed in" : "signed out")}"
        + (Contrast == ExplorerContrastChoice.FollowSystem ? string.Empty : $" / {Contrast} contrast");
}
