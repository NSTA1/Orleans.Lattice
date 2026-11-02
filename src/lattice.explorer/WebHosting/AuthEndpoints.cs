using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.WebUtilities;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Maps the web head's server-side sign-in / sign-out endpoints. The login form
/// posts here so the password is handled on the server and stored in the
/// encrypted cookie rather than round-tripped over the SignalR circuit.
/// Antiforgery is validated on every POST - the forms embed a
/// <c>RequestVerificationToken</c> - so a cross-site form post cannot drive a
/// victim's browser to sign the victim's own session in or out
/// (login / logout CSRF).
/// </summary>
/// <remarks>
/// <para>
/// A sign-in the cluster refuses (a wrong password) establishes no session: no
/// cookie is written, and the redirect carries a
/// <c>signin=refused</c> notice so the next document reopens the sign-in dialog
/// and says the cluster did not accept the username and password. The notice
/// carries neither the user name nor the password.
/// </para>
/// <para>
/// A sign-in, refused or accepted, returns to the address the form was posted
/// from, which the sign-in dialog writes into the form's return-path field
/// (issue #5481). That field is client-supplied, so it is honoured only when it is
/// a plain rooted path under the base href: no scheme or host, no <c>//</c> or
/// <c>\</c> prefix, no backslash, whitespace, control or non-ASCII character, and
/// no <c>.</c> or <c>..</c> segment; and its path, percent-decoded, must still have
/// no <c>//</c> or <c>\</c> prefix, backslash, control character or dot segment.
/// Anything else, or more than one value, falls back to the base href, so the
/// endpoint can never be used as an open redirect. Sign-out always returns to the
/// base href.
/// </para>
/// </remarks>
public static class AuthEndpoints
{
    /// <summary>The longest return path honoured; a longer one falls back to the base href.</summary>
    internal const int MaxReturnPathLength = 2048;

    /// <summary>How many rounds of percent-decoding a return path may need before it is refused.</summary>
    private const int MaxUnescapeRounds = 3;
    /// <summary>
    /// Registers the <c>auth/login</c> and <c>auth/logout</c> POST endpoints,
    /// each guarded by antiforgery-token validation.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <param name="redirectTo">
    /// The base href: where a sign-out redirects, and where a sign-in redirects when
    /// the form carries no acceptable return path. Defaults to <c>/</c>; pass the
    /// explorer's base href when it is mounted under a subpath. A sign-in returns
    /// only to a local path under it.
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
            var returnPath = form.TryGetValue(SessionSignInOptions.ReturnPathFieldName, out var returnValues) && returnValues.Count == 1
                ? ResolveReturnPath(returnValues[0], redirectTo)
                : redirectTo;
            if (!string.IsNullOrWhiteSpace(username))
            {
                try
                {
                    await auth.LoginAsync(username.Trim(), password);
                }
                catch (ExplorerSignInRefusedException)
                {
                    return Results.Redirect(QueryHelpers.AddQueryString(
                        returnPath,
                        SessionSignInOptions.NoticeQueryKey,
                        SessionSignInOptions.RefusedNoticeValue));
                }
            }

            return Results.Redirect(returnPath);
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

    /// <summary>
    /// Returns <paramref name="candidate"/> when it is a plain local path under
    /// <paramref name="basePath"/>, and <paramref name="basePath"/> otherwise.
    /// </summary>
    /// <param name="candidate">The client-supplied return path.</param>
    /// <param name="basePath">The base href, which every refusal falls back to.</param>
    /// <returns>The path to redirect to.</returns>
    internal static string ResolveReturnPath(string? candidate, string basePath) =>
        candidate is { Length: > 0 and <= MaxReturnPathLength } && IsLocalPathUnder(candidate, basePath)
            ? candidate
            : basePath;

    private static bool IsLocalPathUnder(string candidate, string basePath)
    {
        // Only a rooted base can be proven to contain a local path.
        if (basePath.Length == 0 || basePath[0] != '/' || !IsPlainRootedPath(candidate))
        {
            return false;
        }

        // The path must be the base itself or lie beneath it, segment-aligned.
        var path = PathPart(candidate);
        var root = basePath.AsSpan().TrimEnd('/');
        if (!path.StartsWith(root, StringComparison.OrdinalIgnoreCase)
            || (path.Length > root.Length && path[root.Length] != '/'))
        {
            return false;
        }

        // Every percent-decoded form of the path must be just as safe, so an encoded
        // "//", backslash, control character or dot segment cannot slip through. The
        // query is left encoded: it never decides where the browser goes. The
        // decoding allocates, but only for a path that carries a '%'.
        if (!path.Contains('%'))
        {
            return true;
        }

        var decoded = path.ToString();
        for (var round = 0; decoded.Contains('%'); round++)
        {
            if (round == MaxUnescapeRounds)
            {
                return false;
            }

            var next = Uri.UnescapeDataString(decoded);
            if (string.Equals(next, decoded, StringComparison.Ordinal))
            {
                break;
            }

            if (!IsSafeDecodedPath(next))
            {
                return false;
            }

            decoded = next;
        }

        return true;
    }

    /// <summary>
    /// The raw form, which becomes the <c>Location</c> header: rooted, not
    /// <c>//</c> or <c>/\</c>, printable ASCII only, no backslash, no dot segment.
    /// </summary>
    private static bool IsPlainRootedPath(string value)
    {
        if (!IsRootedNotAuthority(value))
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c <= ' ' || c >= '\u007f' || c == '\\')
            {
                return false;
            }
        }

        return HasNoDotSegment(PathPart(value));
    }

    /// <summary>
    /// A decoded path: still rooted and not <c>//</c> or <c>/\</c>, with no
    /// backslash, control character or dot segment. A decoded space or non-ASCII
    /// letter is an ordinary path character, such as a tree id, and is allowed.
    /// </summary>
    private static bool IsSafeDecodedPath(string path)
    {
        if (!IsRootedNotAuthority(path))
        {
            return false;
        }

        foreach (var c in path)
        {
            if (char.IsControl(c) || c == '\\')
            {
                return false;
            }
        }

        return HasNoDotSegment(path);
    }

    private static bool IsRootedNotAuthority(string value) =>
        value.Length > 0 && value[0] == '/' && !(value.Length > 1 && value[1] is '/' or '\\');

    private static bool HasNoDotSegment(ReadOnlySpan<char> path)
    {
        var rest = path[1..];
        while (true)
        {
            var slash = rest.IndexOf('/');
            var segment = slash < 0 ? rest : rest[..slash];
            if (segment is "." or "..")
            {
                return false;
            }

            if (slash < 0)
            {
                return true;
            }

            rest = rest[(slash + 1)..];
        }
    }

    private static ReadOnlySpan<char> PathPart(string value)
    {
        var end = value.AsSpan().IndexOfAny('?', '#');
        return end < 0 ? value : value.AsSpan(0, end);
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
