namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Abstracts hosted-web token acquisition behind a testable seam. The concrete
/// <see cref="IdentityWebExplorerTokenAcquirer"/> drives Microsoft.Identity.Web's
/// <c>ITokenAcquisition</c> on behalf of the signed-in browser user; tests
/// substitute a fake so the auth method's challenge, silent renewal, and
/// re-challenge logic is verified without any network or Entra dependency.
/// </summary>
/// <remarks>
/// Unlike the interactive desktop acquirer this seam has a single acquisition
/// method: Microsoft.Identity.Web serves the first and every subsequent token
/// silently from its token cache (the browser already holds the session cookie),
/// so there is no separate interactive step. When the cache can no longer satisfy
/// the request the implementation throws
/// <see cref="ExplorerWebReauthRequiredException"/>.
/// </remarks>
public interface IExplorerWebTokenAcquirer
{
    /// <summary>
    /// Acquires an access token for the given <paramref name="scopes"/> on behalf
    /// of the currently signed-in browser user.
    /// </summary>
    /// <param name="scopes">The downstream State API scopes. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The acquired token.</returns>
    /// <exception cref="ExplorerWebReauthRequiredException">
    /// The user must complete an interactive sign-in again before a token can be
    /// acquired.
    /// </exception>
    Task<ExplorerWebToken> AcquireTokenAsync(IReadOnlyList<string> scopes, CancellationToken cancellationToken = default);
}
