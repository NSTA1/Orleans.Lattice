using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Coordinates the config store and the live connection: the single entry point
/// the UI uses to discover whether the explorer is configured, to drive the
/// first-run flow, and to apply endpoint changes from the configuration window.
/// </summary>
public interface IExplorerSession
{
    /// <summary>The shared connection every cluster read flows through.</summary>
    ILatticeStateConnection Connection { get; }

    /// <summary><see langword="true"/> once a valid endpoint has been loaded or applied.</summary>
    bool IsConfigured { get; }

    /// <summary>The currently applied configuration, or <see langword="null"/> when unconfigured.</summary>
    ExplorerConfiguration? Current { get; }

    /// <summary>Raised after the configuration is applied or changed.</summary>
    event Action? ConfigurationChanged;

    /// <summary>
    /// Loads any persisted configuration and, when present and valid, connects.
    /// Idempotent: the first call performs the work and later calls return the
    /// current state. Returns <see langword="true"/> when configured.
    /// </summary>
    Task<bool> InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates and persists <paramref name="configuration"/>, then reconfigures
    /// the live connection (rebuilding the channel). Throws
    /// <see cref="ArgumentException"/> when the endpoint is invalid.
    /// </summary>
    Task ApplyAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default);
}
