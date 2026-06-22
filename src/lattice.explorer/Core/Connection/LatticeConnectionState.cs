namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Lifecycle state of the explorer's single connection to the read-only state API.
/// </summary>
public enum LatticeConnectionState
{
    /// <summary>No endpoint has been configured yet.</summary>
    Disconnected,

    /// <summary>An endpoint is configured and the first connection attempt is in progress.</summary>
    Connecting,

    /// <summary>The endpoint is reachable and the last call or probe succeeded.</summary>
    Connected,

    /// <summary>
    /// A previously healthy connection hit a transient failure and is being
    /// recovered within the degrade grace window. The UI may keep showing the
    /// last data with a subtle reconnecting indicator.
    /// </summary>
    Reconnecting,

    /// <summary>
    /// The connection is unhealthy: either a non-transient failure occurred or
    /// the reconnect grace window elapsed. The UI degrades to a visual
    /// disconnected state and should offer a manual reconnect.
    /// </summary>
    Faulted,
}
