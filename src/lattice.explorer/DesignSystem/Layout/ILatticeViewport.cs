using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// The Explorer's viewport seam: the one place that knows the current
/// <see cref="LatticeBreakpoint"/>, and the only thing a head has to drive to
/// make the whole shell adapt.
/// </summary>
/// <remarks>
/// <para>
/// This is the .NET half of design decision D7. A component never measures a
/// viewport and never writes a media query; it takes the cascaded breakpoint
/// and renders the layout that breakpoint names. A head decides how the
/// breakpoint is obtained: <see cref="Components.LatticeAdaptiveRoot"/> observes
/// the browser through <c>matchMedia</c>, and a native head can instead push a
/// window size straight into <see cref="SetViewportWidth"/>.
/// </para>
/// <para>
/// Until a measurement arrives the value is
/// <see cref="LatticeBreakpoints.Default"/>, so a static render, a prerender
/// pass, and a host without JavaScript all produce the layout the Explorer
/// shipped with rather than a collapsed one.
/// </para>
/// </remarks>
public interface ILatticeViewport
{
    /// <summary>The breakpoint the shell is currently rendering for.</summary>
    LatticeBreakpoint Breakpoint { get; }

    /// <summary>
    /// Whether a real viewport measurement has been supplied. False while
    /// <see cref="Breakpoint"/> is still the assumed default, which lets a
    /// component distinguish "known to be expanded" from "not measured yet".
    /// </summary>
    bool IsMeasured { get; }

    /// <summary>
    /// Raised after <see cref="Breakpoint"/> changes to a different value.
    /// A measurement that resolves to the breakpoint already in effect does not
    /// raise it, so a resize within a breakpoint costs no re-render.
    /// </summary>
    event Action<LatticeBreakpoint>? BreakpointChanged;

    /// <summary>
    /// Sets the breakpoint directly, for a head that classifies its own window.
    /// </summary>
    /// <param name="breakpoint">The breakpoint now in effect.</param>
    /// <returns><see langword="true"/> when the value changed.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="breakpoint"/> is not a declared breakpoint.
    /// </exception>
    bool SetBreakpoint(LatticeBreakpoint breakpoint);

    /// <summary>
    /// Sets the breakpoint from a viewport width in CSS pixels.
    /// </summary>
    /// <param name="viewportWidth">The measured viewport width in CSS pixels.</param>
    /// <returns><see langword="true"/> when the resolved breakpoint changed.</returns>
    bool SetViewportWidth(int viewportWidth);
}
