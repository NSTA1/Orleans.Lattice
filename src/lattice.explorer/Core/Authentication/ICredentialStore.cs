namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Persists the explorer's sign-in credential for the current user. The plaintext
/// JSON config store never holds a secret; each head supplies a platform-specific
/// implementation that keeps the credential at rest in an OS-backed or encrypted
/// store (DPAPI on the Windows desktop head, a Data-Protection-encrypted server
/// cookie on the web head). The in-memory default is used for tests and as a safe
/// fallback when no platform store is registered.
/// </summary>
public interface ICredentialStore
{
    /// <summary>
    /// Returns the stored credential, or <see langword="null"/> when none is
    /// stored (the user is signed out).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default);

    /// <summary>Stores <paramref name="credential"/>, replacing any existing one.</summary>
    /// <param name="credential">The credential to persist.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default);

    /// <summary>Clears any stored credential (sign-out).</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ClearAsync(CancellationToken cancellationToken = default);
}
