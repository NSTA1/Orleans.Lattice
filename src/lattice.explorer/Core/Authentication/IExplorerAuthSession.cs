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
    /// Signs out: clears the stored credential and drops the connection back to an
    /// unauthenticated state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task LogoutAsync(CancellationToken cancellationToken = default);
}
