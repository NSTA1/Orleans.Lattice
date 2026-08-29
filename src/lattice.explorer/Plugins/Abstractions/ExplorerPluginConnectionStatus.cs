namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// An immutable snapshot of the connection health a plugin may read.
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a plugin reading it once per render allocates
/// nothing. <c>default</c> is
/// <see cref="ExplorerPluginConnectionState.Disconnected"/>, so an
/// uninitialised status reads as unusable rather than healthy.
/// </para>
/// </summary>
/// <param name="State">The current lifecycle state.</param>
/// <param name="RequiresAuthentication">
/// <see langword="true"/> when the endpoint rejected the call as
/// unauthenticated, so the shell should offer a sign-in.
/// </param>
public readonly record struct ExplorerPluginConnectionStatus(
    ExplorerPluginConnectionState State,
    bool RequiresAuthentication = false)
{
    /// <summary>The initial, unconnected status.</summary>
    public static ExplorerPluginConnectionStatus Disconnected { get; }
        = new(ExplorerPluginConnectionState.Disconnected);

    /// <summary>
    /// <see langword="true"/> when the connection is usable for reads:
    /// connected, or reconnecting within the grace window.
    /// </summary>
    public bool IsUsable => State is ExplorerPluginConnectionState.Connected
        or ExplorerPluginConnectionState.Reconnecting;

    /// <summary>
    /// <see langword="true"/> when a plugin should present a degraded,
    /// disconnected experience.
    /// </summary>
    public bool IsDisconnected => State is ExplorerPluginConnectionState.Faulted
        or ExplorerPluginConnectionState.Disconnected;
}
