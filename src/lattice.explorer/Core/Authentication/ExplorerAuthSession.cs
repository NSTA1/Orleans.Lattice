using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The default <see cref="IExplorerAuthSession"/>. Holds the current credential
/// in memory, persists it through the injected <see cref="ICredentialStore"/>,
/// and reconfigures the shared <see cref="ILatticeStateConnection"/> so every
/// call carries (or, on sign-out, drops) an <c>authorization: Basic</c> header.
/// </summary>
public sealed class ExplorerAuthSession : IExplorerAuthSession, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly ICredentialStore _store;
    private readonly IExplorerCredentialSeed? _seed;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private StoredCredential? _credential;
    private bool _initialized;

    /// <summary>Creates the auth session over the explorer session and credential store.</summary>
    /// <param name="session">The explorer session that owns the endpoint and connection.</param>
    /// <param name="store">The per-user credential store.</param>
    /// <param name="seed">
    /// Optional launcher-friendly sign-in seed. When the credential store is
    /// empty, the seed supplies a username/password applied in memory for the
    /// current process only (never written back to the store). Resolved from DI
    /// when registered; <see langword="null"/> otherwise.
    /// </param>
    public ExplorerAuthSession(
        IExplorerSession session,
        ICredentialStore store,
        IExplorerCredentialSeed? seed = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(store);
        _session = session;
        _store = store;
        _seed = seed;
        _session.ConfigurationChanged += OnConfigurationChanged;
    }

    /// <inheritdoc />
    public bool IsAuthenticated => _credential is not null;

    /// <inheritdoc />
    public string? Username => _credential?.Username;

    /// <inheritdoc />
    public event Action? AuthenticationChanged;

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            _initialized = true;
            _credential = await _store.GetAsync(cancellationToken).ConfigureAwait(false);

            // No stored credential: fall back to the launcher-friendly sign-in
            // seed (username/password passed via environment variable). Applied
            // in memory only - it is never written back to the credential store.
            _credential ??= _seed?.TrySeed();

            if (_credential is not null)
            {
                await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }

        if (_credential is not null)
        {
            AuthenticationChanged?.Invoke();
        }
    }

    /// <inheritdoc />
    public async Task LoginAsync(string username, string password, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(username);
        ArgumentNullException.ThrowIfNull(password);

        var credential = new StoredCredential(username, password);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _initialized = true;
            await _store.SetAsync(credential, cancellationToken).ConfigureAwait(false);
            _credential = credential;
            await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        AuthenticationChanged?.Invoke();
    }

    /// <inheritdoc />
    public async Task LogoutAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _initialized = true;
            await _store.ClearAsync(cancellationToken).ConfigureAwait(false);
            _credential = null;
            await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        AuthenticationChanged?.Invoke();
    }

    /// <summary>
    /// Reconfigures the connection with the current endpoint and credential.
    /// Assumes the caller holds <see cref="_gate"/>. No-op when no endpoint is
    /// configured yet.
    /// </summary>
    private Task ReconfigureAsync(CancellationToken cancellationToken)
    {
        var configuration = _session.Current;
        if (configuration is null)
        {
            return Task.CompletedTask;
        }

        var settings = configuration.ToConnectionSettings();
        if (_credential is { } credential)
        {
            settings = settings with
            {
                Authentication = LatticeCallAuthentication.Basic(credential.Username, credential.Password),
            };
        }

        return _session.Connection.ConfigureAsync(settings, cancellationToken);
    }

    private void OnConfigurationChanged()
    {
        // When the endpoint changes the session reconfigures the connection
        // anonymously; re-apply the live credential to the new endpoint.
        if (_credential is null)
        {
            return;
        }

        _ = ReapplyCredentialAsync();
    }

    private async Task ReapplyCredentialAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            await ReconfigureAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Reconfiguration faults are surfaced through the connection status;
            // re-applying the credential must never throw to the event source.
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _session.ConfigurationChanged -= OnConfigurationChanged;
}
