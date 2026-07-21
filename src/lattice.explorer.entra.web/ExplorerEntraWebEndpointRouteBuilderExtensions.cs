using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Endpoint helpers for the hosted-web Entra sign-in provider.
/// </summary>
public static class ExplorerEntraWebEndpointRouteBuilderExtensions
{
    /// <summary>The default sign-out endpoint pattern.</summary>
    public const string DefaultSignOutPattern = "/explorer-entra/signout";

    /// <summary>
    /// Maps a sign-out endpoint that clears the OpenID Connect cookie and signs
    /// the user out of Entra, redirecting back to <paramref name="redirectUri"/>
    /// afterwards. Point a "Sign out" link or button at
    /// <paramref name="pattern"/> to end the browser session (distinct from the
    /// explorer's own State API sign-out, which only drops the API credential).
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The route pattern. Defaults to <see cref="DefaultSignOutPattern"/>.</param>
    /// <param name="redirectUri">Where Entra returns the browser after sign-out. Defaults to <c>/</c>.</param>
    /// <returns>The endpoint convention builder for further configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <see langword="null"/>.</exception>
    public static IEndpointConventionBuilder MapLatticeExplorerEntraWebSignOut(
        this IEndpointRouteBuilder endpoints,
        string pattern = DefaultSignOutPattern,
        string redirectUri = "/")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapGet(pattern, (HttpContext _) =>
            Results.SignOut(
                new AuthenticationProperties { RedirectUri = redirectUri },
                new[]
                {
                    CookieAuthenticationDefaults.AuthenticationScheme,
                    OpenIdConnectDefaults.AuthenticationScheme,
                }));
    }
}
