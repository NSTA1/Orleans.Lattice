namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Records whether this session has already had its remembered view restored.
/// </summary>
/// <remarks>
/// <para>
/// The restore is a once-per-session act: arriving at the bare home address with
/// something remembered should take you back where you were, but a later,
/// deliberate navigation to <c>/</c> must be taken at face value. A user who asks
/// for the plain home surface has to be able to reach it.
/// </para>
/// <para>
/// That distinction cannot be drawn by the page that performs the restore,
/// because it is destroyed and recreated by the router on every navigation away
/// and back. A flag on the page therefore means "once per page instance", which
/// silently becomes "every time you return to <c>/</c>" - so pressing Back out of
/// an area bounced the caller straight back into it, and browser history could
/// not be walked at all. Scoping the claim to the session is what makes the
/// stated rule the enforced one.
/// </para>
/// </remarks>
public interface IExplorerShellEntryGate
{
    /// <summary>
    /// Claims this session's single restore opportunity, returning
    /// <see langword="true"/> to the first caller only.
    /// </summary>
    bool TryClaimEntry();
}
