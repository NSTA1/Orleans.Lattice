using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Orleans.Lattice.Explorer.Core.Authentication;

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
    /// Maps a federated sign-out endpoint that clears the local State API
    /// credential, clears the OpenID Connect cookie, and signs the user out of
    /// Entra, redirecting back to <paramref name="redirectUri"/> afterwards. Point
    /// the explorer's "Sign out" button at <paramref name="pattern"/> (the
    /// registration publishes it as <see cref="ExplorerSignOutOptions"/> so the
    /// core UI posts to it automatically).
    /// <para>
    /// This is a <b>POST</b> endpoint guarded by antiforgery validation: signing a
    /// user out mutates session state, so it must not be reachable by a cross-site
    /// <c>GET</c> (a logout-CSRF via an image tag or link) - the button renders an
    /// HTML form carrying a <c>RequestVerificationToken</c>. It also drops the
    /// in-circuit API credential via <see cref="IExplorerAuthSession.LogoutAsync"/>
    /// (when the session is registered), so federated sign-out and local
    /// credential clear happen together. Distinct from the explorer's own
    /// in-process State API sign-out, which only drops the API credential and
    /// leaves the browser session in place.
    /// </para>
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The route pattern. Defaults to <see cref="DefaultSignOutPattern"/>.</param>
    /// <param name="redirectUri">Where Entra returns the browser after sign-out. Defaults to <c>/</c>.</param>
    /// <returns>The endpoint convention builder for further configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="pattern"/> is null or whitespace.</exception>
    public static IEndpointConventionBuilder MapLatticeExplorerEntraWebSignOut(
        this IEndpointRouteBuilder endpoints,
        string pattern = DefaultSignOutPattern,
        string redirectUri = "/")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapPost(pattern, async (HttpContext context, IAntiforgery antiforgery) =>
        {
            // Logout mutates the shared browser/session state, so a cross-site
            // POST must not be able to drive it: fail closed on a missing or
            // invalid antiforgery token, exactly as the cookie web head's own
            // /auth/logout endpoint does.
            if (!await IsRequestValidAsync(antiforgery, context).ConfigureAwait(false))
            {
                return Results.BadRequest();
            }

            // Drop the in-circuit State API credential too, so a federated
            // sign-out and the local credential clear happen together and the
            // explorer does not keep a live cluster credential after the browser
            // session ends. Resolved from the request scope (never a captured
            // singleton) and optional so the endpoint still signs the browser out
            // when no explorer session is registered.
            var session = context.RequestServices.GetService<IExplorerAuthSession>();
            if (session is not null)
            {
                await session.LogoutAsync(context.RequestAborted).ConfigureAwait(false);
            }

            return Results.SignOut(
                new AuthenticationProperties { RedirectUri = redirectUri },
                new[]
                {
                    CookieAuthenticationDefaults.AuthenticationScheme,
                    OpenIdConnectDefaults.AuthenticationScheme,
                });
        });
    }

    private static async Task<bool> IsRequestValidAsync(IAntiforgery antiforgery, HttpContext context)
    {
        try
        {
            await antiforgery.ValidateRequestAsync(context).ConfigureAwait(false);
            return true;
        }
        catch (AntiforgeryValidationException)
        {
            return false;
        }
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

