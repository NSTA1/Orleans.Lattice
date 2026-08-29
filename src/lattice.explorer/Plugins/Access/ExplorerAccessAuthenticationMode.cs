namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The classification of a cluster's active authentication mode, as reported by
/// the Access plugin's own access-model probe.
/// <para>
/// This lives in the plugin rather than in a shared navigation record because
/// nothing outside the Access surface reads it: it is advisory display state
/// the create forms and the mode banner render, not an access decision and not
/// a fact any other plugin needs. The plugin maps the auth-API value onto this
/// enum before publishing it through <see cref="IAccessDomain"/>.
/// </para>
/// </summary>
public enum ExplorerAccessAuthenticationMode
{
    /// <summary>The authentication mode could not be determined (unprobed, denied, or unreachable).</summary>
    Unknown,

    /// <summary>No credential authenticator is registered; callers are anonymous.</summary>
    Anonymous,

    /// <summary>A claims / token credential authenticator is in force.</summary>
    Claims,

    /// <summary>A flat username / password (Basic) credential authenticator is in force.</summary>
    Basic,
}
