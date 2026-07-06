using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Owns the explorer's sign-in state: the current in-memory credential, the
/// load-on-startup flow, and the apply-then-reconfigure path that attaches (or
/// clears) the <c>authorization</c> header on the shared connection. The
/// plaintext credential lives only in memory here and at rest in the injected
/// <see cref="ICredentialStore"/>; it is never written to the config store.
/// </summary>
public interface IExplorerAuthSession
{
    /// <summary><see langword="true"/> when a credential is currently applied.</summary>
    bool IsAuthenticated { get; }

    /// <summary>The signed-in username, or <see langword="null"/> when anonymous.</summary>
    string? Username { get; }

    /// <summary>The scheme id of the current sign-in, or <see langword="null"/> when anonymous.</summary>
    string? CurrentScheme { get; }

    /// <summary>
    /// The authentication seam currently applied to the connection, or
    /// <see langword="null"/> when signed out. Exposed so sibling API clients
    /// (for example the backup control-API client) can attach the same
    /// credential to their own channel. Reading it has no side effects.
    /// </summary>
    LatticeCallAuthentication? CurrentAuthentication { get; }

    /// <summary>The scheme ids the registered auth-method providers can service.</summary>
    IReadOnlyCollection<string> AvailableSchemes { get; }

    /// <summary>Raised after a successful sign-in or sign-out.</summary>
    event Action? AuthenticationChanged;

    /// <summary>
    /// Loads any stored credential and, when present, applies it to the
    /// connection. Idempotent: the first call performs the work and later calls
    /// are no-ops.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Signs in with <paramref name="username"/> / <paramref name="password"/>:
    /// persists the credential to the store, applies the <c>authorization</c>
    /// header to the connection, and reconnects.
    /// </summary>
    /// <param name="username">The credential username. Must be non-empty.</param>
    /// <param name="password">The credential password. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task LoginAsync(string username, string password, CancellationToken cancellationToken = default);

    /// <summary>
    /// Signs in with the auth-method provider identified by
    /// <paramref name="schemeId"/>, running its (possibly interactive) challenge
    /// with <paramref name="inputs"/> and any discovered scheme parameters. The
    /// Basic credential is persisted; token-based sign-ins are session-only.
    /// </summary>
    /// <param name="schemeId">The scheme to sign in with (an <see cref="AvailableSchemes"/> value).</param>
    /// <param name="inputs">Interactive inputs for the challenge, or <see langword="null"/> for schemes that take none.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task LoginWithMethodAsync(string schemeId, IReadOnlyDictionary<string, string?>? inputs = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes the current endpoint for the auth scheme(s) it advertises. Returns
    /// <see cref="ExplorerAuthSchemeAdvertisement.Empty"/> when the endpoint does
    /// not advertise or no probe is registered.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ExplorerAuthSchemeAdvertisement> DiscoverAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Signs out: clears the stored credential and drops the connection back to an
    /// unauthenticated state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task LogoutAsync(CancellationToken cancellationToken = default);
}
