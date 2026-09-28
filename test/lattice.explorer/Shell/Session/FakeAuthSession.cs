using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// A directly driven <see cref="IExplorerAuthSession"/>: a test sets what the
/// endpoint advertises, signs in and out explicitly, and raises the
/// re-authentication latch itself, so nothing waits on a real challenge.
/// </summary>
internal sealed class FakeAuthSession : IExplorerAuthSession
{
    /// <inheritdoc />
    public bool IsAuthenticated { get; private set; }

    /// <inheritdoc />
    public string? Username { get; private set; }

    /// <inheritdoc />
    public string? CurrentScheme { get; private set; }

    /// <inheritdoc />
    public LatticeCallAuthentication? CurrentAuthentication => null;

    /// <inheritdoc />
    public IReadOnlyCollection<string> AvailableSchemes { get; set; } = [ExplorerAuthSchemes.Basic];

    /// <summary>What <see cref="DiscoverAsync"/> answers.</summary>
    public ExplorerAuthSchemeAdvertisement Advertisement { get; set; } = ExplorerAuthSchemeAdvertisement.Empty;

    /// <summary>When set, <see cref="DiscoverAsync"/> throws it.</summary>
    public Exception? DiscoveryFailure { get; set; }

    /// <summary>When set, both sign-in methods throw it.</summary>
    public Exception? SignInFailure { get; set; }

    /// <summary>Every username passed to <see cref="LoginAsync"/>.</summary>
    public List<(string Username, string Password)> PasswordSignIns { get; } = [];

    /// <summary>Every scheme passed to <see cref="LoginWithMethodAsync"/>.</summary>
    public List<string> MethodSignIns { get; } = [];

    /// <summary>How many times <see cref="LogoutAsync"/> ran.</summary>
    public int SignOuts { get; private set; }

    /// <summary>How many times <see cref="InitializeAsync"/> ran.</summary>
    public int Initializations { get; private set; }

    /// <summary>How many handlers are subscribed to <see cref="ReauthRequired"/>.</summary>
    public int ReauthSubscribers => ReauthRequired?.GetInvocationList().Length ?? 0;

    /// <summary>How many handlers are subscribed to <see cref="AuthenticationChanged"/>.</summary>
    public int AuthenticationSubscribers => AuthenticationChanged?.GetInvocationList().Length ?? 0;

    /// <inheritdoc />
    public event Action? AuthenticationChanged;

    /// <inheritdoc />
    public event Action? ReauthRequired;

    /// <summary>Signs in as <paramref name="username"/> and announces it.</summary>
    /// <param name="username">The display name.</param>
    /// <param name="scheme">The scheme id.</param>
    public void SignIn(string username, string scheme = ExplorerAuthSchemes.Basic)
    {
        IsAuthenticated = true;
        Username = username;
        CurrentScheme = scheme;
        AuthenticationChanged?.Invoke();
    }

    /// <summary>Raises the re-authentication latch, as a revoked token source does.</summary>
    public void RaiseReauthRequired() => ReauthRequired?.Invoke();

    /// <inheritdoc />
    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        Initializations++;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task LoginAsync(string username, string password, CancellationToken cancellationToken = default)
    {
        if (SignInFailure is not null)
        {
            return Task.FromException(SignInFailure);
        }

        PasswordSignIns.Add((username, password));
        SignIn(username);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task LoginWithMethodAsync(string schemeId, IReadOnlyDictionary<string, string?>? inputs = null, CancellationToken cancellationToken = default)
    {
        if (SignInFailure is not null)
        {
            return Task.FromException(SignInFailure);
        }

        MethodSignIns.Add(schemeId);
        SignIn("someone@example.com", schemeId);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<ExplorerAuthSchemeAdvertisement> DiscoverAsync(CancellationToken cancellationToken = default) =>
        DiscoveryFailure is not null ? Task.FromException<ExplorerAuthSchemeAdvertisement>(DiscoveryFailure) : Task.FromResult(Advertisement);

    /// <inheritdoc />
    public Task LogoutAsync(CancellationToken cancellationToken = default)
    {
        SignOuts++;
        IsAuthenticated = false;
        Username = null;
        CurrentScheme = null;
        AuthenticationChanged?.Invoke();
        return Task.CompletedTask;
    }
}
