using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Maps the web head's server-side sign-in / sign-out endpoints. The login form
/// posts here so the password is handled on the server and stored in the
/// encrypted cookie rather than round-tripped over the SignalR circuit.
/// Antiforgery is validated on every POST - the forms embed a
/// <c>RequestVerificationToken</c> - so a cross-site form post cannot drive a
/// victim's browser to sign the shared process-global session in or out
/// (login / logout CSRF).
/// </summary>
public static class AuthEndpoints
{
    /// <summary>
    /// Registers the <c>auth/login</c> and <c>auth/logout</c> POST endpoints,
    /// each guarded by antiforgery-token validation.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <param name="redirectTo">
    /// The path a successful sign-in / sign-out redirects to. Defaults to
    /// <c>/</c>; pass the explorer's base href when it is mounted under a subpath.
    /// </param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    public static IEndpointRouteBuilder MapExplorerAuthEndpoints(
        this IEndpointRouteBuilder endpoints,
        string redirectTo = "/")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrEmpty(redirectTo);

        endpoints.MapPost("/auth/login", async (HttpContext context, IExplorerAuthSession auth, IAntiforgery antiforgery) =>
        {
            if (!await IsRequestValidAsync(antiforgery, context))
            {
                return Results.BadRequest();
            }

            var form = await context.Request.ReadFormAsync();
            var username = form["username"].ToString();
            var password = form["password"].ToString();
            if (!string.IsNullOrWhiteSpace(username))
            {
                await auth.LoginAsync(username.Trim(), password);
            }

            return Results.Redirect(redirectTo);
        });

        endpoints.MapPost("/auth/logout", async (HttpContext context, IExplorerAuthSession auth, IAntiforgery antiforgery) =>
        {
            if (!await IsRequestValidAsync(antiforgery, context))
            {
                return Results.BadRequest();
            }

            await auth.LogoutAsync();
            return Results.Redirect(redirectTo);
        });

        return endpoints;
    }

    private static async Task<bool> IsRequestValidAsync(IAntiforgery antiforgery, HttpContext context)
    {
        try
        {
            await antiforgery.ValidateRequestAsync(context);
            return true;
        }
        catch (AntiforgeryValidationException)
        {
            return false;
        }
    }
}
