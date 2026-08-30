namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The health of the Explorer's connection to the cluster, as much of it as a
/// plugin is entitled to see. It deliberately carries no endpoint address and
/// no transport handle: a plugin renders a degraded state from this, and reads
/// data through its declared domain contract.
/// </summary>
public enum ExplorerPluginConnectionState
{
    /// <summary>No connection is established. The fail-closed default.</summary>
    Disconnected = 0,

    /// <summary>The first connection attempt is in progress.</summary>
    Connecting = 1,

    /// <summary>The cluster is reachable and the last call succeeded.</summary>
    Connected = 2,

    /// <summary>
    /// A previously healthy connection hit a transient failure and is being
    /// recovered. A plugin may keep showing its last data with a subtle
    /// reconnecting indicator.
    /// </summary>
    Reconnecting = 3,

    /// <summary>
    /// The connection is unhealthy. A plugin should degrade to a disconnected
    /// presentation rather than spin.
    /// </summary>
    Faulted = 4,
}
