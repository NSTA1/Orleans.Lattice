using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Default <see cref="ILatticeApiMcpCredentialBridge"/> that lifts the
/// authenticated ASP.NET Core principal onto a <see cref="LatticeCredential"/>.
/// The bridge is fail-closed: an unauthenticated request yields
/// <see langword="null"/> (an anonymous caller the access gate denies). For an
/// authenticated request it resolves the caller's principal id from the durable
/// object-id (<c>oid</c>) claim first - falling back to <c>sub</c> and then the
/// identity name - so the subject discovery introspects on matches the subject
/// the silo auth model enforces on, and reads the opaque token from the header
/// named by
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
    // The durable Entra object-id claim, keyed on first. A delegated (user)
    // access token's `sub` is a pairwise (user, client-app) identifier, so keying
    // discovery on `sub` mis-identifies the caller relative to the silo auth model
    // (which keys on `oid`). Both the raw claim name (default JWT mapping disabled)
    // and the WS-* schema URI (default JWT inbound claim mapping enabled) are
    // accepted. Cached to keep the resolution path allocation-free.
    private static readonly string[] ObjectIdClaimTypes =
    {
        "oid",
        "http://schemas.microsoft.com/identity/claims/objectidentifier",
    };

    // The subject fallback used when no `oid` is present (an authenticator that
    // carries no object id). `ClaimTypes.NameIdentifier` is the mapped form of
    // `sub` under the default JWT inbound claim mapping.
    private static readonly string[] SubjectClaimTypes =
    {
        "sub",
        ClaimTypes.NameIdentifier,
    };

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
        => FirstNonEmptyClaim(user, ObjectIdClaimTypes)
            ?? FirstNonEmptyClaim(user, SubjectClaimTypes)
            ?? (string.IsNullOrEmpty(user.Identity?.Name) ? null : user.Identity!.Name);

    private static string? FirstNonEmptyClaim(ClaimsPrincipal user, string[] claimTypes)
    {
        foreach (var claimType in claimTypes)
        {
            var value = user.FindFirst(claimType)?.Value;
            if (!string.IsNullOrEmpty(value))
            {
                return value;
            }
        }

        return null;
    }

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
