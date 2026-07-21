using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The default <see cref="IExplorerAuthSession"/>. Holds the current sign-in in
/// memory and reconfigures the shared <see cref="ILatticeStateConnection"/> so
/// every call carries (or, on sign-out, drops) the credential. Sign-in is
/// expressed through pluggable <see cref="IExplorerAuthMethod"/> providers: the
/// original username/password flow is the built-in Basic provider, and other
/// schemes (Entra, generic OIDC, custom) plug in without changing this class.
/// </summary>
/// <remarks>
/// Only the Basic credential is persisted (through the injected
/// <see cref="ICredentialStore"/>, which each head backs with an OS-encrypted
/// store); it is never written to the plaintext config store. Token-based
/// sign-ins are session/in-memory only and are never persisted here - the
/// token provider owns any opt-in persistence of its own refresh material.
/// </remarks>
public sealed class ExplorerAuthSession : IExplorerAuthSession, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly ICredentialStore _store;
    private readonly IExplorerCredentialSeed? _seed;
    private readonly IExplorerAuthSchemeProbe? _probe;
    private readonly TimeProvider _timeProvider;
    private readonly IReadOnlyList<IExplorerAuthMethod> _methods;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private ExplorerAuthSignIn? _signIn;
    private StoredCredential? _credential;
    private ExplorerAuthSchemeAdvertisement _advertisement = ExplorerAuthSchemeAdvertisement.Empty;
    private bool _initialized;

    /// <summary>Creates the auth session over the explorer session and credential store.</summary>
    /// <param name="session">The explorer session that owns the endpoint and connection.</param>
    /// <param name="store">The per-user credential store (Basic credential only).</param>
    /// <param name="seed">
    /// Optional launcher-friendly sign-in seed. When the credential store is
    /// empty, the seed supplies a Basic username/password applied in memory for
    /// the current process only (never written back to the store). Resolved from
    /// DI when registered; <see langword="null"/> otherwise.
    /// </param>
    /// <param name="methods">
    /// The registered auth-method providers. Resolved from DI; when none handle
    /// the Basic scheme a built-in <see cref="BasicExplorerAuthMethod"/> is added
    /// so the original username/password flow always works.
    /// </param>
    /// <param name="probe">
    /// Optional scheme-discovery probe. When registered, <see cref="DiscoverAsync"/>
    /// asks the endpoint which scheme it requires; when absent, discovery yields
    /// an empty advertisement and the explorer falls back to manual/Basic.
    /// </param>
    /// <param name="timeProvider">The clock passed to token-based challenges. Defaults to the system clock.</param>
    public ExplorerAuthSession(
        IExplorerSession session,
        ICredentialStore store,
        IExplorerCredentialSeed? seed = null,
        IEnumerable<IExplorerAuthMethod>? methods = null,
        IExplorerAuthSchemeProbe? probe = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(store);
        _session = session;
        _store = store;
        _seed = seed;
        _probe = probe;
        _timeProvider = timeProvider ?? TimeProvider.System;

        var list = methods?.ToList() ?? new List<IExplorerAuthMethod>();
        if (!list.Any(m => m.CanHandle(ExplorerAuthSchemes.Basic)))
        {
            list.Add(new BasicExplorerAuthMethod());
        }

        _methods = list;
        _session.ConfigurationChanged += OnConfigurationChanged;
    }

    /// <inheritdoc />
    public bool IsAuthenticated => _signIn is not null;

    /// <inheritdoc />
    public string? Username => _signIn?.DisplayName;

    /// <summary>The scheme id of the current sign-in, or <see langword="null"/> when anonymous.</summary>
    public string? CurrentScheme => _signIn?.SchemeId;

    /// <inheritdoc />
    public LatticeCallAuthentication? CurrentAuthentication => _signIn?.Authentication;

    /// <summary>The scheme ids the registered auth-method providers can service.</summary>
    public IReadOnlyCollection<string> AvailableSchemes => _methods.Select(m => m.SchemeId).ToArray();

    /// <inheritdoc />
    public event Action? AuthenticationChanged;

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var changed = false;
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

            if (_credential is { } credential)
            {
                _signIn = await ChallengeBasicAsync(credential, cancellationToken).ConfigureAwait(false);
                await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
                changed = true;
            }
        }
        finally
        {
            _gate.Release();
        }

        if (changed)
        {
            AuthenticationChanged?.Invoke();
        }
    }

    /// <inheritdoc />
    public Task LoginAsync(string username, string password, CancellationToken cancellationToken = default)
    {
        var inputs = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [ExplorerAuthSchemes.UsernameInput] = username,
            [ExplorerAuthSchemes.PasswordInput] = password,
        };

        return LoginWithMethodAsync(ExplorerAuthSchemes.Basic, inputs, cancellationToken);
    }

    /// <summary>
    /// Signs in with the provider identified by <paramref name="schemeId"/>: runs
    /// its interactive challenge with <paramref name="inputs"/> (and any
    /// discovered scheme parameters), applies the resulting credential to the
    /// connection, and reconnects. The Basic credential is persisted for the next
    /// launch; token-based sign-ins are session-only and are never persisted.
    /// </summary>
    /// <param name="schemeId">The scheme to sign in with (an <see cref="AvailableSchemes"/> value).</param>
    /// <param name="inputs">Interactive inputs for the challenge, or <see langword="null"/> for schemes that take none.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="schemeId"/> is null/whitespace, or no provider handles it.</exception>
    public async Task LoginWithMethodAsync(
        string schemeId,
        IReadOnlyDictionary<string, string?>? inputs = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schemeId);
        var method = SelectMethod(schemeId);

        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = schemeId,
            Parameters = ParametersFor(schemeId),
            Inputs = inputs ?? new Dictionary<string, string?>(StringComparer.Ordinal),
            Endpoint = _session.Current?.Endpoint,
            TimeProvider = _timeProvider,
        };

        // Runs the (possibly interactive) challenge before any state is mutated,
        // so an invalid input or a cancelled login leaves the session untouched.
        var signIn = await method.ChallengeAsync(context, cancellationToken).ConfigureAwait(false);
        var isBasic = string.Equals(schemeId, ExplorerAuthSchemes.Basic, StringComparison.OrdinalIgnoreCase);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _initialized = true;
            DisposeCurrentProvider();

            if (isBasic)
            {
                var credential = new StoredCredential(
                    inputs?.GetValueOrDefault(ExplorerAuthSchemes.UsernameInput) ?? signIn.DisplayName,
                    inputs?.GetValueOrDefault(ExplorerAuthSchemes.PasswordInput) ?? string.Empty);
                await _store.SetAsync(credential, cancellationToken).ConfigureAwait(false);
                _credential = credential;
            }
            else
            {
                // Token schemes are never persisted; clear any stale Basic credential.
                await _store.ClearAsync(cancellationToken).ConfigureAwait(false);
                _credential = null;
            }

            _signIn = signIn;
            await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        AuthenticationChanged?.Invoke();
    }

    /// <summary>
    /// Probes the current endpoint for the auth scheme(s) it advertises and
    /// caches the result so a subsequent <see cref="LoginWithMethodAsync"/> can
    /// supply the discovered parameters. Returns
    /// <see cref="ExplorerAuthSchemeAdvertisement.Empty"/> when no probe is
    /// registered or the endpoint does not advertise.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<ExplorerAuthSchemeAdvertisement> DiscoverAsync(CancellationToken cancellationToken = default)
    {
        var configuration = _session.Current;
        if (_probe is null || configuration is null)
        {
            _advertisement = ExplorerAuthSchemeAdvertisement.Empty;
            return _advertisement;
        }

        var advertisement = await _probe
            .ProbeAsync(configuration.Endpoint, configuration.AllowUnencryptedHttp2, configuration.TransportHeaders, cancellationToken)
            .ConfigureAwait(false);

        _advertisement = advertisement;
        return advertisement;
    }

    /// <summary>
    /// Selects the auth-method provider that handles the first advertised scheme,
    /// or <see langword="null"/> when nothing was advertised or no provider can
    /// service it (the caller shows an actionable message or falls back to Basic).
    /// </summary>
    /// <param name="advertisement">The advertisement to select against.</param>
    public IExplorerAuthMethod? SelectMethodForAdvertisement(ExplorerAuthSchemeAdvertisement advertisement)
    {
        ArgumentNullException.ThrowIfNull(advertisement);
        foreach (var scheme in advertisement.Schemes)
        {
            var method = _methods.FirstOrDefault(m => m.CanHandle(scheme.SchemeId));
            if (method is not null)
            {
                return method;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public async Task LogoutAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _initialized = true;
            DisposeCurrentProvider();
            await _store.ClearAsync(cancellationToken).ConfigureAwait(false);
            _credential = null;
            _signIn = null;
            await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        AuthenticationChanged?.Invoke();
    }

    private async Task<ExplorerAuthSignIn> ChallengeBasicAsync(StoredCredential credential, CancellationToken cancellationToken)
    {
        var method = SelectMethod(ExplorerAuthSchemes.Basic);
        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Basic,
            Inputs = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.UsernameInput] = credential.Username,
                [ExplorerAuthSchemes.PasswordInput] = credential.Password,
            },
            Endpoint = _session.Current?.Endpoint,
            TimeProvider = _timeProvider,
        };

        return await method.ChallengeAsync(context, cancellationToken).ConfigureAwait(false);
    }

    private IExplorerAuthMethod SelectMethod(string schemeId)
    {
        var method = _methods.FirstOrDefault(m => string.Equals(m.SchemeId, schemeId, StringComparison.OrdinalIgnoreCase))
            ?? _methods.FirstOrDefault(m => m.CanHandle(schemeId));
        return method ?? throw new ArgumentException(
            $"No auth-method provider is registered for scheme '{schemeId}'. Registered schemes: "
            + string.Join(", ", _methods.Select(m => m.SchemeId)) + ".",
            nameof(schemeId));
    }

    private IReadOnlyDictionary<string, string> ParametersFor(string schemeId)
    {
        foreach (var scheme in _advertisement.Schemes)
        {
            if (string.Equals(scheme.SchemeId, schemeId, StringComparison.OrdinalIgnoreCase))
            {
                return scheme.Parameters;
            }
        }

        return new Dictionary<string, string>(StringComparer.Ordinal);
    }

    /// <summary>
    /// Reconfigures the connection with the current endpoint and sign-in.
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
        if (_signIn is { } signIn)
        {
            settings = settings with { Authentication = signIn.Authentication };
        }

        return _session.Connection.ConfigureAsync(settings, cancellationToken);
    }

    private void OnConfigurationChanged()
    {
        // When the endpoint changes the session reconfigures the connection
        // anonymously; re-apply the live sign-in to the new endpoint.
        if (_signIn is null)
        {
            return;
        }

        _ = ReapplySignInAsync();
    }

    private async Task ReapplySignInAsync()
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

    private void DisposeCurrentProvider()
    {
        if (_signIn?.Authentication.CredentialProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _session.ConfigurationChanged -= OnConfigurationChanged;
        DisposeCurrentProvider();
        _gate.Dispose();
    }
}
