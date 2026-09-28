using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// How the session chrome's header controls are presented: inline in the header,
/// or folded into the header's overflow menu below the small breakpoint (the
/// responsive contract of epic #3807).
/// </summary>
/// <remarks>
/// The layout cascades the width band it is laid out in as an
/// <see cref="LtBreakpoint"/> under <see cref="BreakpointCascadeName"/>. With
/// nothing cascaded - outside the layout, or before the width is known - the
/// controls render inline, so the header never depends on script to be usable.
/// </remarks>
internal static class SessionPresentation
{
    /// <summary>
    /// The name the layout cascades its width band under. It is the value of the
    /// navigation chrome's <c>LtBreakpointCascade.Name</c> (S1, issue #3815).
    /// </summary>
    public const string BreakpointCascadeName = "Orleans.Lattice.Explorer.Breakpoint";

    /// <summary>Whether the header controls are folded into the overflow menu at <paramref name="breakpoint"/>.</summary>
    /// <param name="breakpoint">The cascaded width band, or <see langword="null"/> when none is cascaded.</param>
    /// <returns><see langword="true"/> below the small breakpoint.</returns>
    public static bool IsFolded(LtBreakpoint? breakpoint) => breakpoint == LtBreakpoint.Compact;
}
