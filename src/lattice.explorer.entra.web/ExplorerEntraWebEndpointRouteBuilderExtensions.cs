using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Endpoint helpers for the hosted-web Entra sign-in provider.
/// </summary>
public static class ExplorerEntraWebEndpointRouteBuilderExtensions
{
    /// <summary>The default sign-out endpoint pattern.</summary>
    public const string DefaultSignOutPattern = "/explorer-entra/signout";

    /// <summary>The default forced-interactive re-authentication endpoint pattern.</summary>
    public const string DefaultReauthPattern = "/explorer-entra/reauth";

    /// <summary>
    /// The default OpenID Connect <c>prompt</c> value used by the re-authentication
    /// endpoint. <c>login</c> forces the user to re-enter credentials, redeeming a
    /// fresh authorization code even when a valid session cookie already exists.
    /// </summary>
    public const string DefaultReauthPrompt = "login";

    /// <summary>The default name of the query-string parameter carrying the local return URL.</summary>
    public const string DefaultReturnUrlParameter = "returnUrl";

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

    /// <summary>
    /// Maps a forced-interactive re-authentication endpoint that issues an OpenID
    /// Connect challenge with <c>prompt=<paramref name="prompt"/></c>, so a
    /// <b>new</b> authorization code is redeemed even when a valid session cookie
    /// already exists. This is what repopulates a failover replica's token cache
    /// after a mid-session move: a plain page refresh sees the still-valid cookie
    /// and never redeems a fresh code, leaving the new replica unable to acquire a
    /// downstream token; a <c>prompt=login</c> challenge forces the redemption.
    /// Point the explorer's re-authentication interstitial at
    /// <paramref name="pattern"/>.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The route pattern. Defaults to <see cref="DefaultReauthPattern"/>.</param>
    /// <param name="prompt">
    /// The OpenID Connect <c>prompt</c> value. Defaults to
    /// <see cref="DefaultReauthPrompt"/> (<c>login</c>); <c>select_account</c> is a
    /// common alternative that lets the user pick a different account.
    /// </param>
    /// <param name="returnUrlParameter">
    /// The query-string parameter carrying the post-sign-in return URL. Defaults
    /// to <see cref="DefaultReturnUrlParameter"/>. The value is honoured only when
    /// it is a local path (an absolute or protocol-relative URL is rejected and
    /// the browser returns to <c>/</c>), so the endpoint cannot be abused as an
    /// open redirect.
    /// </param>
    /// <returns>The endpoint convention builder for further configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="pattern"/>, <paramref name="prompt"/>, or <paramref name="returnUrlParameter"/> is null or whitespace.</exception>
    public static IEndpointConventionBuilder MapLatticeExplorerEntraWebReauth(
        this IEndpointRouteBuilder endpoints,
        string pattern = DefaultReauthPattern,
        string prompt = DefaultReauthPrompt,
        string returnUrlParameter = DefaultReturnUrlParameter)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        ArgumentException.ThrowIfNullOrWhiteSpace(prompt);
        ArgumentException.ThrowIfNullOrWhiteSpace(returnUrlParameter);

        return endpoints.MapGet(pattern, (HttpContext context) =>
        {
            var requested = context.Request.Query[returnUrlParameter].ToString();
            var returnUrl = ResolveLocalReturnUrl(requested);

            var properties = new AuthenticationProperties { RedirectUri = returnUrl };

            // Carried through to the OpenID Connect handler, which reads it from
            // AuthenticationProperties.Parameters and adds it to the authorize
            // request, forcing a fresh interactive sign-in and code redemption.
            properties.Parameters[OpenIdConnectParameterNames.Prompt] = prompt;

            return Results.Challenge(
                properties,
                new[] { OpenIdConnectDefaults.AuthenticationScheme });
        });
    }

    /// <summary>
    /// Returns <paramref name="candidate"/> when it is a safe local path, else
    /// <c>/</c>. Fail-closed open-redirect guard: a local URL is a single leading
    /// slash not followed by a slash or backslash (so <c>//evil.com</c> and
    /// <c>/\evil.com</c> are rejected), or a <c>~/</c> app-relative path. Anything
    /// else - an absolute URL, a scheme, a backslash, or empty - returns the site
    /// root.
    /// </summary>
    /// <param name="candidate">The caller-supplied return URL (never trusted).</param>
    internal static string ResolveLocalReturnUrl(string? candidate)
        => IsLocalUrl(candidate) ? candidate! : "/";

    /// <summary>
    /// Mirrors the local-URL rules ASP.NET Core's <c>Url.IsLocalUrl</c> applies,
    /// so a caller-supplied return URL cannot redirect the browser off-site.
    /// </summary>
    /// <param name="url">The candidate URL.</param>
    internal static bool IsLocalUrl(string? url)
    {
        if (string.IsNullOrEmpty(url))
        {
            return false;
        }

        // Allows "/" or "/foo" but not "//" or "/\".
        if (url[0] == '/')
        {
            return url.Length == 1 || (url[1] != '/' && url[1] != '\\');
        }

        // Allows "~/" or "~/foo" but not "~//" or "~/\".
        if (url.Length > 1 && url[0] == '~' && url[1] == '/')
        {
            return url.Length == 2 || (url[2] != '/' && url[2] != '\\');
        }

        return false;
    }
}

