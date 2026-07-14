using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Default <see cref="ILatticeApiMcpCredentialBridge"/> that lifts the
/// authenticated ASP.NET Core principal onto a <see cref="LatticeCredential"/>.
/// The bridge is fail-closed: an unauthenticated request yields
/// <see langword="null"/> (an anonymous caller the access gate denies). For an
/// authenticated request it resolves the caller's principal id from the standard
/// name claims and reads the opaque token from the header named by
/// <see cref="LatticeApiMcpOptions.CredentialHeaderName"/> (default
/// <c>authorization</c>), stripping a leading
/// <see cref="LatticeApiMcpOptions.CredentialScheme"/> prefix (default
/// <c>Bearer</c>) when present, so a registered
/// <c>ILatticeCredentialAuthenticator</c> can resolve the caller's subject.
/// </summary>
/// <remarks>
/// A registered <c>ILatticeCredentialAuthenticator</c> is responsible for
/// validating the token; this bridge performs no validation and only shuttles
/// the authenticated identity and opaque token onto the ambient credential
/// context. When an authenticated request carries no usable token header, the
/// resolved principal id is used as the credential token so a certificate- or
/// cookie-authenticated session still resolves to a non-anonymous caller.
/// </remarks>
internal sealed class HttpContextLatticeApiMcpCredentialBridge : ILatticeApiMcpCredentialBridge
{
    private readonly IOptions<LatticeApiMcpOptions> _options;

    /// <summary>
    /// Initialises the bridge with the resolved MCP binding options.
    /// </summary>
    public HttpContextLatticeApiMcpCredentialBridge(IOptions<LatticeApiMcpOptions> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public LatticeCredential? Resolve(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Fail-closed: only an authenticated MCP session yields a credential. An
        // anonymous / unauthenticated caller reads as null so the access gate
        // denies every read and mutation.
        var user = context.User;
        if (user?.Identity is not { IsAuthenticated: true })
        {
            return null;
        }

        var options = _options.Value;
        var scheme = options.CredentialScheme;
        var principalId = ResolvePrincipalId(user);
        var token = ResolveHeaderToken(context, options.CredentialHeaderName, scheme);

        // An authenticated session with no bearer token (for example a
        // certificate- or cookie-authenticated caller) still resolves to a
        // non-anonymous credential using the principal id as the token.
        if (string.IsNullOrEmpty(token))
        {
            token = principalId;
        }

        return string.IsNullOrEmpty(token)
            ? null
            : new LatticeCredential(
                token,
                string.IsNullOrEmpty(scheme) ? null : scheme,
                principalId);
    }

    private static string? ResolvePrincipalId(ClaimsPrincipal user)
        => user.FindFirst(ClaimTypes.NameIdentifier)?.Value
            ?? (string.IsNullOrEmpty(user.Identity?.Name) ? null : user.Identity!.Name);

    private static string? ResolveHeaderToken(HttpContext context, string? headerName, string? scheme)
    {
        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        var raw = context.Request.Headers[headerName].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var token = raw.Trim();
        if (!string.IsNullOrEmpty(scheme)
            && token.Length >= scheme.Length
            && token.AsSpan(0, scheme.Length).Equals(scheme, StringComparison.OrdinalIgnoreCase)
            && (token.Length == scheme.Length || char.IsWhiteSpace(token[scheme.Length])))
        {
            // A bare scheme with no token (for example "Bearer ") carries no
            // credential; collapse it to empty so the authenticated principal id
            // is used instead.
            token = token.Length == scheme.Length
                ? string.Empty
                : token[(scheme.Length + 1)..].Trim();
        }

        return string.IsNullOrEmpty(token) ? null : token;
    }
}
