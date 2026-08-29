using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// The default <see cref="ILatticeViewport"/>: a small piece of observable
/// state holding the current breakpoint.
/// </summary>
/// <remarks>
/// Registered per circuit (scoped), because a viewport belongs to one rendered
/// shell: two browser windows attached to the same server must not share a
/// breakpoint. It holds no timer and no clock, so its behaviour is a pure
/// function of the measurements pushed into it.
/// </remarks>
public sealed class LatticeViewport : ILatticeViewport
{
    private LatticeBreakpoint _breakpoint = LatticeBreakpoints.Default;

    /// <inheritdoc />
    public LatticeBreakpoint Breakpoint => _breakpoint;

    /// <inheritdoc />
    public bool IsMeasured { get; private set; }

    /// <inheritdoc />
    public event Action<LatticeBreakpoint>? BreakpointChanged;

    /// <inheritdoc />
    public bool SetBreakpoint(LatticeBreakpoint breakpoint)
    {
        if (breakpoint is not (LatticeBreakpoint.Compact or LatticeBreakpoint.Medium or LatticeBreakpoint.Expanded))
        {
            throw new ArgumentOutOfRangeException(nameof(breakpoint), breakpoint, "Unknown breakpoint.");
        }

        IsMeasured = true;

        if (_breakpoint == breakpoint)
        {
            return false;
        }

        _breakpoint = breakpoint;
        BreakpointChanged?.Invoke(breakpoint);
        return true;
    }

    /// <inheritdoc />
    public bool SetViewportWidth(int viewportWidth) =>
        SetBreakpoint(LatticeBreakpoints.Resolve(viewportWidth));
}
