using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// The default <see cref="IExplorerSession"/>. Owns the load-then-connect
/// startup flow and the apply-then-reconfigure path used by the configuration
/// window.
/// </summary>
public sealed class ExplorerSession : IExplorerSession
{
    private readonly IExplorerConfigStore _store;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _initialized;

    /// <summary>Creates a session over the config store and shared connection.</summary>
    public ExplorerSession(IExplorerConfigStore store, ILatticeStateConnection connection)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(connection);
        _store = store;
        Connection = connection;
    }

    /// <inheritdoc />
    public ILatticeStateConnection Connection { get; }

    /// <inheritdoc />
    public bool IsConfigured { get; private set; }

    /// <inheritdoc />
    public ExplorerConfiguration? Current { get; private set; }

    /// <inheritdoc />
    public event Action? ConfigurationChanged;

    /// <inheritdoc />
    public async Task<bool> InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return IsConfigured;
            }

            _initialized = true;

            var configuration = await _store.LoadAsync(cancellationToken).ConfigureAwait(false);
            if (configuration is not null && EndpointValidation.TryValidate(configuration.Endpoint, out _))
            {
                await Connection.ConfigureAsync(configuration.ToConnectionSettings(), cancellationToken).ConfigureAwait(false);
                Current = configuration;
                IsConfigured = true;
            }

            return IsConfigured;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task ApplyAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (!EndpointValidation.TryValidate(configuration.Endpoint, out var error))
        {
            throw new ArgumentException(error, nameof(configuration));
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _initialized = true;
            await _store.SaveAsync(configuration, cancellationToken).ConfigureAwait(false);
            await Connection.ConfigureAsync(configuration.ToConnectionSettings(), cancellationToken).ConfigureAwait(false);
            Current = configuration;
            IsConfigured = true;
        }
        finally
        {
            _gate.Release();
        }

        ConfigurationChanged?.Invoke();
    }
}
