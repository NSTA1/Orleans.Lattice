using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// <b>The shell's area visibility policy.</b> Turns one access decision into
/// one presentation, in a pure function the rail calls and a test can read.
/// </summary>
/// <remarks>
/// <para>
/// The four access states are not four shades of the same grey-out, and the
/// policy is where that stops being an implementation detail:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// <see cref="ExplorerPluginAccessState.Allowed"/> is offered.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> stays
/// <em>prominent and clickable</em>. It is an invitation, not a wall: the
/// caller is one sign-in away, and demoting it would hide the remedy behind
/// the refusal.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="ExplorerPluginAccessState.Denied"/> is <em>demoted, not
/// hidden</em>. A caller who cannot see that the product has a Backups area
/// cannot ask an administrator for it, so the refusal is kept visible, set
/// aside below a divider, and made to state its remedy.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="ExplorerPluginAccessState.Unavailable"/> is hidden. The cluster
/// does not have the capability, so there is nothing to sign in for and
/// nothing to be granted; the absence is explained once in the rail's
/// capabilities affordance rather than repeated as a row of dead entries.
/// </description>
/// </item>
/// </list>
/// <para>
/// <b>This is a usability policy, not a security control.</b> The gate is
/// advisory and the server remains the sole enforcement point, which is what
/// makes it legitimate to name a capability the caller may not use. It governs
/// navigation areas only: a capability name is safe to reveal, an instance name
/// - a tenant id, a tree id - is not, and none appear here.
/// </para>
/// </remarks>
public static class ExplorerAreaVisibilityPolicy
{
    /// <summary>
    /// Decides how an area with <paramref name="state"/> is presented.
    /// </summary>
    /// <param name="state">The access decision the gate reported.</param>
    /// <param name="hideInaccessible">
    /// The caller's "hide what I cannot use" preference. Defaults to
    /// <see langword="false"/> in the shell, which is what makes a refusal
    /// discoverable; when <see langword="true"/> a denial is withheld instead of
    /// demoted.
    /// </param>
    /// <returns>The presentation the rail applies.</returns>
    public static ExplorerAreaEntryPresentation Decide(
        ExplorerPluginAccessState state,
        bool hideInaccessible) => state switch
        {
            ExplorerPluginAccessState.Allowed => ExplorerAreaEntryPresentation.Primary,

            // An invitation outranks the preference: hiding it would hide the
            // remedy along with the refusal, and there is nothing to be granted -
            // the caller only has to sign in.
            ExplorerPluginAccessState.AuthenticationRequired => ExplorerAreaEntryPresentation.Primary,

            ExplorerPluginAccessState.Denied => hideInaccessible
                ? ExplorerAreaEntryPresentation.Hidden
                : ExplorerAreaEntryPresentation.Demoted,

            ExplorerPluginAccessState.Unavailable => ExplorerAreaEntryPresentation.Hidden,

            // Fails closed: a state this shell does not recognise is withheld
            // rather than offered.
            _ => ExplorerAreaEntryPresentation.Hidden,
        };

    /// <summary>
    /// Whether activating an entry in <paramref name="state"/> does something:
    /// opens the area, or offers the sign-in that would open it.
    /// </summary>
    /// <param name="state">The access decision the gate reported.</param>
    public static bool IsActivable(ExplorerPluginAccessState state) =>
        state is ExplorerPluginAccessState.Allowed or ExplorerPluginAccessState.AuthenticationRequired;

    /// <summary>
    /// Whether an area in <paramref name="state"/> is absent because the cluster
    /// does not have the capability, rather than because the caller may not use
    /// it. These are the absences the capabilities affordance explains.
    /// </summary>
    /// <param name="state">The access decision the gate reported.</param>
    public static bool IsUnavailableOnCluster(ExplorerPluginAccessState state) =>
        state == ExplorerPluginAccessState.Unavailable;
}
