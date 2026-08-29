namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The four states a plugin access probe can resolve to. This generalises the
/// single boolean the Explorer's capability map collapsed every gate into, so
/// "you may not" and "you are not signed in" and "this cluster does not have
/// that capability" stop being the same silent grey-out.
/// <para>
/// Gating is advisory on the client: the server remains the sole enforcement
/// point, so a plugin action must still handle a runtime denial whatever this
/// says. <see cref="Denied"/> is <c>0</c> so an unset or defaulted value fails
/// closed.
/// </para>
/// </summary>
public enum ExplorerPluginAccessState
{
    /// <summary>
    /// The caller may not use the plugin. The default, so an unprobed, failed,
    /// or defaulted decision is a denial and never an admission. The plugin
    /// renders disabled and visible rather than hidden.
    /// </summary>
    Denied = 0,

    /// <summary>The caller may use the plugin; it renders and is interactive.</summary>
    Allowed = 1,

    /// <summary>
    /// The probe could not admit the caller because no accepted credential was
    /// presented, rather than because an authenticated caller was refused. The
    /// shell offers a sign-in instead of an inert grey-out, so a recoverable
    /// state is not collapsed into a permanent-looking one.
    /// </summary>
    AuthenticationRequired = 2,

    /// <summary>
    /// The capability the plugin surfaces is not installed on this cluster, so
    /// there is nothing to sign in for and nothing to be granted. The plugin
    /// degrades to nothing: the shell renders no entry at all.
    /// </summary>
    Unavailable = 3,
}
