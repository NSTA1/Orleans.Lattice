using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// The ambient adaptive state <see cref="Components.LatticeAdaptiveRoot"/>
/// cascades to everything beneath it: the breakpoint the shell is rendering for
/// and the density in effect.
/// </summary>
/// <remarks>
/// <para>
/// This is what a plugin reads to adapt. Take it as a cascading parameter and
/// branch on <see cref="Breakpoint"/> by name; never measure a viewport and
/// never write a media query (epic decision D7).
/// </para>
/// <para>
/// It is cascaded as a single immutable object rather than as two loose values
/// so a component takes one cascading parameter, and so a later addition to the
/// shell context does not force every consumer to declare another one.
/// </para>
/// </remarks>
/// <param name="Breakpoint">The breakpoint the shell is currently rendering for.</param>
/// <param name="Density">The density in effect for the shell.</param>
/// <param name="IsMeasured">
/// Whether a real viewport measurement has arrived. False while the breakpoint
/// is still the assumed default, which lets a component distinguish "known to
/// be expanded" from "not measured yet" - for example to defer an expensive
/// wide-only surface until the shell is sure.
/// </param>
/// <param name="ViewportWidth">
/// The measured viewport width in CSS pixels, or <see langword="null"/> when
/// the head reports a band rather than a width. A primitive that measures its
/// own layout uses this when it is there and
/// <see cref="LatticeBreakpoints.NominalWidth"/> when it is not; nothing may
/// use it to write a media query or to re-derive a breakpoint, which is
/// <see cref="Breakpoint"/>'s job.
/// </param>
public sealed record LatticeAdaptiveContext(
    LatticeBreakpoint Breakpoint,
    LatticeDensity Density,
    bool IsMeasured,
    int? ViewportWidth = null)
{
    /// <summary>
    /// The context assumed when no <see cref="Components.LatticeAdaptiveRoot"/>
    /// is present: the default breakpoint, the standard density, and no
    /// measurement.
    /// </summary>
    public static LatticeAdaptiveContext Unmeasured { get; } =
        new(LatticeBreakpoints.Default, LatticeDensity.Cosy, IsMeasured: false);

    /// <summary>
    /// The width a layout beneath this context measures itself against: the
    /// measured viewport width when the head reported one, and the
    /// breakpoint's nominal width otherwise.
    /// </summary>
    public int LayoutWidth => ViewportWidth ?? LatticeBreakpoints.NominalWidth(Breakpoint);
}
