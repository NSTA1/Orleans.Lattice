namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The width band a scenario renders the Explorer at, ascending by viewport width.
/// A local stand-in for the retired design system's breakpoint enum, kept so this
/// suite compiles after the cutover (issue #3831) until the browser suite is
/// rewritten against the new Explorer UI (issue #3832).
/// </summary>
public enum LatticeBreakpoint
{
    /// <summary>Below the medium width.</summary>
    Compact = 0,

    /// <summary>From the medium width up to the expanded width.</summary>
    Medium = 1,

    /// <summary>The expanded width and up.</summary>
    Expanded = 2,
}

/// <summary>
/// The width bands a scenario can render at, and their names. A local stand-in for
/// the retired design system's helper (see <see cref="LatticeBreakpoint"/>).
/// </summary>
public static class LatticeBreakpoints
{
    /// <summary>Every band, ascending by width.</summary>
    public static IReadOnlyList<LatticeBreakpoint> All { get; } =
        [LatticeBreakpoint.Compact, LatticeBreakpoint.Medium, LatticeBreakpoint.Expanded];

    /// <summary>The band's lower-case name.</summary>
    /// <param name="breakpoint">The band.</param>
    /// <returns>The name.</returns>
    public static string Name(LatticeBreakpoint breakpoint) => breakpoint switch
    {
        LatticeBreakpoint.Compact => "compact",
        LatticeBreakpoint.Medium => "medium",
        _ => "expanded",
    };
}