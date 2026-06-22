namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The explorer's single, shared gateway to the read-only state API. Every
/// cluster read in the application flows through this connection, which owns the
/// channel lifecycle, surfaces connection health, retries transient failures,
/// and degrades to a disconnected state when an endpoint stops responding.
/// </summary>
public interface ILatticeStateConnection : ILatticeStateClient, IAsyncDisposable
{
    /// <summary>The current connection health snapshot.</summary>
    LatticeConnectionStatus Status { get; }

    /// <summary>
    /// Raised whenever <see cref="Status"/> changes, on a thread-pool context.
    /// The UI subscribes to drive connection banners and the reconnect button.
    /// </summary>
    event Action<LatticeConnectionStatus>? StatusChanged;

    /// <summary>
    /// Points the connection at <paramref name="settings"/>, rebuilding the
    /// underlying channel if the endpoint changed, and performs an initial
    /// reachability probe. Safe to call repeatedly as configuration changes.
    /// </summary>
    Task ConfigureAsync(LatticeConnectionSettings settings, CancellationToken cancellationToken = default);

    /// <summary>
    /// Manually rebuilds the channel for the currently configured endpoint and
    /// re-probes it. Backs the UI's reconnect button shown in the disconnected
    /// state. Returns <see langword="true"/> when the endpoint is reachable.
    /// </summary>
    Task<bool> ReconnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes the current endpoint with a lightweight call and updates
    /// <see cref="Status"/>. Returns <see langword="true"/> when reachable.
    /// </summary>
    Task<bool> ProbeAsync(CancellationToken cancellationToken = default);
}
