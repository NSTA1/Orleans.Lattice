namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// The transport-security posture recorded in the explorer's config store.
/// Captures whether the connection is held to the secure-by-default rules or has
/// been explicitly opted in to the interim insecure, loopback-only development
/// path, so the secure posture is never silently lost.
/// </summary>
public enum ExplorerTransportMode
{
    /// <summary>
    /// Secure by default: a non-loopback endpoint must use TLS (<c>https</c>),
    /// and anonymous access to a non-loopback endpoint is rejected (the user must
    /// sign in). This is the default for any newly configured endpoint.
    /// </summary>
    Secure = 0,

    /// <summary>
    /// An explicit opt-in to the interim anonymous / plaintext path, allowed only
    /// against a loopback endpoint (for example <c>http://localhost:5199</c>) for
    /// local development against a state API with authorization disabled.
    /// </summary>
    InsecureLoopbackDev = 1,
}
