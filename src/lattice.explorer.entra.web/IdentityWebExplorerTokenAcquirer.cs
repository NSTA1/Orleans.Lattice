using System.Security.Claims;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.Identity.Client;
using Microsoft.Identity.Web;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// The Microsoft.Identity.Web-backed <see cref="IExplorerWebTokenAcquirer"/>.
/// Acquires a downstream State API token for the signed-in browser user by
/// passing the circuit's <see cref="ClaimsPrincipal"/> explicitly to
/// <c>ITokenAcquisition</c> - a remote Blazor Server circuit has no ambient
/// <c>HttpContext</c>, so the user cannot be inferred and must be supplied.
/// </summary>
internal sealed class IdentityWebExplorerTokenAcquirer : IExplorerWebTokenAcquirer
{
    private readonly ITokenAcquisition _tokenAcquisition;
    private readonly AuthenticationStateProvider _authenticationStateProvider;

    public IdentityWebExplorerTokenAcquirer(
        ITokenAcquisition tokenAcquisition,
        AuthenticationStateProvider authenticationStateProvider)
    {
        ArgumentNullException.ThrowIfNull(tokenAcquisition);
        ArgumentNullException.ThrowIfNull(authenticationStateProvider);
        _tokenAcquisition = tokenAcquisition;
        _authenticationStateProvider = authenticationStateProvider;
    }

    public async Task<ExplorerWebToken> AcquireTokenAsync(
        IReadOnlyList<string> scopes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        if (scopes.Count == 0)
        {
            throw new ArgumentException("At least one scope is required.", nameof(scopes));
        }

        var state = await _authenticationStateProvider.GetAuthenticationStateAsync().ConfigureAwait(false);
        var user = state.User;
        if (user?.Identity is not { IsAuthenticated: true })
        {
            throw new ExplorerWebReauthRequiredException(
                "The browser session is not authenticated; the OpenID Connect sign-in must complete before a State API token can be acquired.");
        }

        try
        {
            var result = await _tokenAcquisition
                .GetAuthenticationResultForUserAsync(
                    scopes,
                    user: user,
                    tokenAcquisitionOptions: new TokenAcquisitionOptions { CancellationToken = cancellationToken })
                .ConfigureAwait(false);

            return new ExplorerWebToken
            {
                AccessToken = result.AccessToken,
                ExpiresOn = result.ExpiresOn,
                Username = user.Identity.Name ?? result.Account?.Username,
            };
        }
        catch (MsalUiRequiredException ex)
        {
            throw new ExplorerWebReauthRequiredException(
                "Microsoft.Identity.Web could not acquire a token silently; interactive sign-in is required.", ex);
        }
        catch (MicrosoftIdentityWebChallengeUserException ex)
        {
            throw new ExplorerWebReauthRequiredException(
                "Microsoft.Identity.Web signalled that the user must be challenged again before a token can be acquired.", ex);
        }
    }
}
