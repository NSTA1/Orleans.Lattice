namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// How the rail presents one area, given what the caller may do with it.
/// </summary>
/// <remarks>
/// The three values are the whole of the shell's visibility policy: an entry is
/// offered, offered-but-set-aside, or not offered. Everything else - the
/// wording of a refusal, whether a sign-in is invited - follows from the access
/// state the policy read, not from a fourth presentation.
/// </remarks>
public enum ExplorerAreaEntryPresentation
{
    /// <summary>
    /// Not offered. The area contributes no entry at all: either the cluster
    /// does not have the capability it surfaces, so there is nothing to be
    /// granted, or the caller has asked not to be shown what they cannot open.
    /// The default, so an unrecognised state is withheld rather than offered.
    /// </summary>
    Hidden = 0,

    /// <summary>
    /// Offered in the rail proper: reachable, or reachable once the caller
    /// signs in.
    /// </summary>
    Primary = 1,

    /// <summary>
    /// Offered below the divider at lower visual weight, inert, and stating why
    /// and what to do about it. Visible on purpose: an area a caller cannot see
    /// is an area they cannot ask to be granted.
    /// </summary>
    Demoted = 2,
}
