using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// How the session chrome is presented at each width band (the responsive
/// contract of epic #3807): its header controls inline or folded into the
/// header's overflow menu, and its dialogs centred or as full-screen sheets.
/// </summary>
/// <remarks>
/// The layout cascades the width band it is laid out in as an
/// <see cref="LtBreakpoint"/> under <see cref="LtBreakpointCascade.Name"/>. With
/// nothing cascaded - outside the layout, or before the width is known - the
/// chrome renders its wide form, so it never depends on script to be usable.
/// </remarks>
internal static class SessionPresentation
{
    /// <summary>Whether the header controls are folded into the overflow menu at <paramref name="breakpoint"/>.</summary>
    /// <param name="breakpoint">The cascaded width band, or <see langword="null"/> when none is cascaded.</param>
    /// <returns><see langword="true"/> below the small breakpoint.</returns>
    public static bool IsFolded(LtBreakpoint? breakpoint) => breakpoint == LtBreakpoint.Compact;

    /// <summary>
    /// Where a session dialog sits at <paramref name="breakpoint"/>: a full-screen
    /// sheet below the small breakpoint, so the connection gate, sign-in and
    /// re-authentication work at a phone's width, and centred otherwise.
    /// </summary>
    /// <param name="breakpoint">The cascaded width band, or <see langword="null"/> when none is cascaded.</param>
    /// <returns>The dialog placement.</returns>
    public static LtDialogPlacement DialogPlacement(LtBreakpoint? breakpoint) =>
        IsFolded(breakpoint) ? LtDialogPlacement.End : LtDialogPlacement.Center;
}
