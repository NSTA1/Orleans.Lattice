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
    /// <summary>
    /// The message a sign-in fails with when the console was repointed at another
    /// endpoint while its challenge ran.
    /// </summary>
    internal const string EndpointChangedDuringSignInMessage =
        "The endpoint changed while signing in, so the sign-in was not applied. Sign in to the new endpoint.";

    private readonly IExplorerSession _session;
    private readonly ICredentialStore _store;
    private readonly IExplorerCredentialSeed? _seed;
    private readonly IExplorerAuthSchemeProbe? _probe;
    private readonly TimeProvider _timeProvider;
    private readonly IReadOnlyList<IExplorerAuthMethod> _methods;
    private readonly SemaphoreSlim _gate = new(1, 1);

    // The sign-in and the endpoint it was minted for, published together as one
    // immutable pair, so a lock-free reader (GetAuthenticationFor, on every Shell
    // call) can never pair a credential with another sign-in's endpoint.
    private volatile MintedSignIn? _minted;
    private StoredCredential? _credential;
    private ExplorerAuthSchemeAdvertisement _advertisement = ExplorerAuthSchemeAdvertisement.Empty;
    private bool _initialized;
    private IReauthRequiredSource? _reauthSource;

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
    /// an empty advertisement and the explorer falls back to the Basic
    /// (username and password) sign-in.
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
    public bool IsAuthenticated => _minted is not null;

    /// <inheritdoc />
    public string? Username => _minted?.SignIn.DisplayName;

    /// <summary>The scheme id of the current sign-in, or <see langword="null"/> when anonymous.</summary>
    public string? CurrentScheme => _minted?.SignIn.SchemeId;

    /// <inheritdoc />
    public LatticeCallAuthentication? CurrentAuthentication => _minted?.SignIn.Authentication;

    /// <inheritdoc />
    public LatticeCallAuthentication? GetAuthenticationFor(string endpoint)
    {
        var minted = _minted;
        return minted is not null && IsSameEndpoint(minted.Endpoint, endpoint) ? minted.SignIn.Authentication : null;
    }

    /// <summary>The scheme ids the registered auth-method providers can service.</summary>
    public IReadOnlyCollection<string> AvailableSchemes => _methods.Select(m => m.SchemeId).ToArray();

    /// <inheritdoc />
    public event Action? AuthenticationChanged;

    /// <inheritdoc />
    public event Action? ReauthRequired;

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
                // The endpoint is read once: the challenge and the binding use the same
                // value, so the sign-in is never recorded against an endpoint it was not
                // minted for.
                var endpoint = _session.Current?.Endpoint;
                var signIn = await ChallengeBasicAsync(credential, endpoint, cancellationToken).ConfigureAwait(false);
                if (EndpointMoved(endpoint, _session.Current?.Endpoint))
                {
                    // Repointed while the stored credential was being replayed: never
                    // carry it to the new endpoint; stay anonymous there.
                    DisposeProvider(signIn);
                    return;
                }

                _minted = new MintedSignIn(signIn, endpoint);
                HookReauthSource();
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

        // Read once: the challenge, the check below and the binding all use this value.
        var endpoint = _session.Current?.Endpoint;
        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = schemeId,
            Parameters = ParametersFor(schemeId),
            Inputs = inputs ?? new Dictionary<string, string?>(StringComparer.Ordinal),
            Endpoint = endpoint,
            TimeProvider = _timeProvider,
        };

        // Runs the (possibly interactive) challenge before any state is mutated,
        // so an invalid input or a cancelled login leaves the session untouched.
        var signIn = await method.ChallengeAsync(context, cancellationToken).ConfigureAwait(false);
        var isBasic = string.Equals(schemeId, ExplorerAuthSchemes.Basic, StringComparison.OrdinalIgnoreCase);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (EndpointMoved(endpoint, _session.Current?.Endpoint))
            {
                // The console was repointed while the challenge ran. The credential was
                // entered for (and, for a token scheme, issued to) the endpoint the
                // challenge named, so it is neither applied to the new one nor persisted.
                DisposeProvider(signIn);
                throw new InvalidOperationException(EndpointChangedDuringSignInMessage);
            }

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

            _minted = new MintedSignIn(signIn, endpoint);
            HookReauthSource();
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
            _minted = null;
            await ReconfigureAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        AuthenticationChanged?.Invoke();
    }

    private async Task<ExplorerAuthSignIn> ChallengeBasicAsync(StoredCredential credential, string? endpoint, CancellationToken cancellationToken)
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
            Endpoint = endpoint,
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
    /// configured yet. The credential is attached only when the sign-in was minted
    /// for the configured endpoint; otherwise the connection is configured
    /// anonymously.
    /// </summary>
    private Task ReconfigureAsync(CancellationToken cancellationToken)
    {
        var configuration = _session.Current;
        if (configuration is null)
        {
            return Task.CompletedTask;
        }

        var settings = configuration.ToConnectionSettings();
        if (GetAuthenticationFor(configuration.Endpoint) is { } authentication)
        {
            settings = settings with { Authentication = authentication };
        }

        return _session.Connection.ConfigureAsync(settings, cancellationToken);
    }

    private void OnConfigurationChanged()
    {
        var minted = _minted;
        if (minted is null)
        {
            return;
        }

        // A sign-in is minted against one endpoint and is only ever valid there.
        // The connection reconfigures anonymously on a configuration change, so
        // re-applying the credential is right for a change that keeps the same
        // endpoint (transport posture, headers) - and is a credential leak for a
        // change that repoints the console at a different one, which would hand
        // this endpoint's Basic password, or a silently-renewed bearer token
        // minted for its audience, to an operator who never held either. When
        // the endpoint moves, sign out instead of re-applying.
        if (IsSameEndpoint(minted.Endpoint, _session.Current?.Endpoint))
        {
            _ = ReapplySignInAsync();
            return;
        }

        _ = SignOutForEndpointChangeAsync();
    }

    /// <summary>
    /// Compares the endpoint a sign-in was minted for against another one.
    /// Deliberately conservative: anything that is not recognisably the same
    /// endpoint is treated as a different one, so an unparseable or absent value
    /// drops the credential rather than carrying it across. Allocation-free, as it
    /// runs on every Shell call.
    /// </summary>
    private static bool IsSameEndpoint(string? signInEndpoint, string? current)
    {
        if (string.IsNullOrWhiteSpace(signInEndpoint) || string.IsNullOrWhiteSpace(current))
        {
            return false;
        }

        return signInEndpoint.AsSpan().TrimEnd('/').Equals(current.AsSpan().TrimEnd('/'), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Whether the configured endpoint moved while a challenge ran. Unlike
    /// <see cref="IsSameEndpoint"/>, two absent endpoints have not moved: a sign-in
    /// taken before any endpoint is configured is bound to none, and is attached
    /// nowhere until one is.
    /// </summary>
    private static bool EndpointMoved(string? challenged, string? current) =>
        (challenged is not null || current is not null) && !IsSameEndpoint(challenged, current);

    /// <summary>
    /// Drops the sign-in when the console is repointed at a different endpoint,
    /// and clears the persisted Basic credential with it - leaving it behind
    /// would let <see cref="InitializeAsync"/> replay it to the new endpoint on
    /// the next launch. The connection is left configured anonymously, so the
    /// operator is re-challenged against the endpoint they moved to.
    /// </summary>
    private async Task SignOutForEndpointChangeAsync()
    {
        var changed = false;
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_minted is not { } minted || IsSameEndpoint(minted.Endpoint, _session.Current?.Endpoint))
            {
                return;
            }

            DisposeCurrentProvider();
            await _store.ClearAsync(CancellationToken.None).ConfigureAwait(false);
            _credential = null;
            _minted = null;
            changed = true;
            await ReconfigureAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Reconfiguration faults are surfaced through the connection status;
            // dropping the credential must never throw to the event source.
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
        UnhookReauthSource();
        if (_minted is { } minted)
        {
            DisposeProvider(minted.SignIn);
        }
    }

    private static void DisposeProvider(ExplorerAuthSignIn signIn)
    {
        if (signIn.Authentication.CredentialProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Subscribes to the current sign-in's re-authentication signal, when its
    /// credential provider exposes one, so a latched revoked state is surfaced to
    /// the UI through <see cref="ReauthRequired"/>. A no-op for static credentials
    /// (Basic) whose provider never revokes.
    /// </summary>
    private void HookReauthSource()
    {
        if (_minted?.SignIn.Authentication.CredentialProvider is IReauthRequiredSource source)
        {
            _reauthSource = source;
            source.ReauthRequired += OnReauthRequired;
        }
    }

    /// <summary>Detaches the current re-authentication subscription, if any.</summary>
    private void UnhookReauthSource()
    {
        if (_reauthSource is { } source)
        {
            source.ReauthRequired -= OnReauthRequired;
            _reauthSource = null;
        }
    }

    private void OnReauthRequired() => ReauthRequired?.Invoke();

    /// <summary>A sign-in and the endpoint it was minted for; <see langword="null"/> when none was configured.</summary>
    /// <param name="SignIn">The sign-in.</param>
    /// <param name="Endpoint">The endpoint its challenge named.</param>
    private sealed record MintedSignIn(ExplorerAuthSignIn SignIn, string? Endpoint);

    /// <inheritdoc />
    public void Dispose()
    {
        _session.ConfigurationChanged -= OnConfigurationChanged;
        DisposeCurrentProvider();
        _gate.Dispose();
    }
}
