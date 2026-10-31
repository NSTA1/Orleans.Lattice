namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// An immutable snapshot of the connection's health, raised on every transition
/// so the UI can render connection state (including a degraded, disconnected
/// banner with a reconnect affordance).
/// </summary>
/// <param name="State">The current lifecycle state.</param>
/// <param name="Endpoint">The configured endpoint address, or <see langword="null"/> when unconfigured.</param>
/// <param name="Message">A human-readable description of the current state, suitable for display.</param>
/// <param name="RequiresAuthentication">
/// <see langword="true"/> when the connection faulted because the endpoint
/// rejected the call as unauthenticated / forbidden, so the UI should offer a
/// "Sign in" action.
/// </param>
public sealed record LatticeConnectionStatus(
    LatticeConnectionState State,
    string? Endpoint,
    string? Message,
    bool RequiresAuthentication = false)
{
    /// <summary>The initial, unconfigured status.</summary>
    public static readonly LatticeConnectionStatus Disconnected =
        new(LatticeConnectionState.Disconnected, Endpoint: null, Message: "No endpoint configured.");

    /// <summary>
    /// <see langword="true"/> when the endpoint refused the credential the
    /// connection presented as unauthenticated - the cluster does not accept this
    /// sign-in, as opposed to a caller it recognised but forbids. The UI then reads
    /// the session as needing a sign-in rather than as signed in. Always
    /// <see langword="false"/> for a call that carried no credential.
    /// </summary>
    public bool CredentialRefused { get; init; }

    /// <summary>
    /// <see langword="true"/> when the connection is usable for reads
    /// (connected, or reconnecting within the grace window).
    /// </summary>
    public bool IsUsable => State is LatticeConnectionState.Connected or LatticeConnectionState.Reconnecting;

    /// <summary>
    /// <see langword="true"/> when the UI should present a degraded, disconnected
    /// experience and offer a manual reconnect.
    /// </summary>
    public bool IsDisconnected => State is LatticeConnectionState.Faulted or LatticeConnectionState.Disconnected;
}
