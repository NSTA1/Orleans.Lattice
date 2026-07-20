namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The head-agnostic classification of a cluster's active authentication mode,
/// as reported by the Access-area capability probe. A Core-local mirror of the
/// auth-API access-model's authentication-mode vocabulary, so the pure navigation
/// / capability model stays free of any control-plane dependency (the Access
/// feature project maps the auth-API value onto this enum before publishing it).
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
