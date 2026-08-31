using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One cell of the accessibility sweep matrix: the theme, breakpoint band, and
/// identity a surface is swept under.
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
public readonly record struct AccessibilityScenario(
    ExplorerTheme Theme,
    LatticeBreakpoint Breakpoint,
    bool SignedIn)
{
    /// <summary>
    /// Every cell of the theme x breakpoint x identity matrix.
    /// <para>
    /// The cross product is taken in full rather than sampled. It is only twelve
    /// cases, and the interactions are exactly where the defects live: the compact
    /// band is the one that overflows, the light palette is the one no one has ever
    /// looked at, and signing in is what changes which areas the gates admit. A
    /// pairwise reduction would drop cells such as compact-and-light, which is
    /// precisely the combination nothing in this repository has ever rendered.
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
        }
    }

    /// <summary>A short human description used in the test name and failure messages.</summary>
    public override string ToString() =>
        $"{Theme} / {LatticeBreakpoints.Name(Breakpoint)} / {(SignedIn ? "signed in" : "signed out")}";
}
