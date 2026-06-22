namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Immutable connection configuration for a single state-API endpoint. Supplied
/// to <see cref="ILatticeStateConnection.ConfigureAsync"/>; changing the
/// <see cref="Address"/> rebuilds the underlying channel.
/// </summary>
public sealed record LatticeConnectionSettings
{
    /// <summary>
    /// The state-API endpoint, for example <c>https://host:443</c> or, for
    /// local development, <c>http://localhost:5199</c>.
    /// </summary>
    public required string Address { get; init; }

    /// <summary>
    /// Enables unencrypted HTTP/2 (h2c) so a plain <c>http://</c> endpoint can be
    /// used for local development. Has no effect on <c>https://</c> endpoints.
    /// </summary>
    public bool AllowUnencryptedHttp2 { get; init; }

    /// <summary>
    /// The authentication seam. <see langword="null"/> (the default) connects
    /// anonymously, which is only appropriate against a development endpoint with
    /// authorization disabled.
    /// </summary>
    public LatticeCallAuthentication? Authentication { get; init; }

    /// <summary>
    /// How long a previously healthy connection may stay in
    /// <see cref="LatticeConnectionState.Reconnecting"/> before degrading to
    /// <see cref="LatticeConnectionState.Faulted"/> (the visual disconnected
    /// state). Defaults to 5 seconds.
    /// </summary>
    public TimeSpan DegradeAfter { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the background health monitor probes a degraded or reconnecting
    /// endpoint to recover it. Defaults to 1 second.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Backoff applied between inline transient retries of a single call.
    /// Defaults to 250 milliseconds.
    /// </summary>
    public TimeSpan TransientRetryBackoff { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Maximum number of inline retries attempted for a single call before it
    /// surfaces a transient failure to the caller (the background monitor then
    /// continues recovery). Defaults to 2.
    /// </summary>
    public int MaxTransientRetries { get; init; } = 2;
}
