namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// Abstracts the Entra/MSAL token acquisition behind a testable seam: the
/// interactive first acquisition and the silent renewal. The concrete
/// <see cref="MsalEntraInteractiveTokenAcquirer"/> drives real MSAL; tests
/// substitute a fake with a controllable clock so the refresh, single-flight,
/// and re-challenge logic is verified without any network or Azure dependency.
/// </summary>
public interface IEntraInteractiveTokenAcquirer
{
    /// <summary>
    /// Runs the interactive sign-in (browser auth-code + PKCE, or device-code)
    /// and returns the acquired token. Throws when the user cancels or the flow
    /// fails.
    /// </summary>
    /// <param name="request">The resolved authority, client id, and scopes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts a silent renewal from cached refresh material. Returns
    /// <see langword="null"/> when silent acquisition is no longer possible (the
    /// refresh token expired or was revoked, or consent was withdrawn), signalling
    /// that the user must be re-challenged interactively.
    /// </summary>
    /// <param name="request">The resolved authority, client id, and scopes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default);
}
