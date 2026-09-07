using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Configuration for a single <see cref="JwtCredentialAuthenticator"/> instance:
/// the issuer it owns, the audiences and signing keys it trusts, and the claim
/// types it maps into a <see cref="LatticePrincipal"/>. One authenticator is
/// registered per issuer, so a silo can trust several identity providers at
/// once.
/// </summary>
public sealed class JwtAuthenticatorOptions
{
    /// <summary>
    /// The token issuer this authenticator owns (the JWT <c>iss</c> claim). Used
    /// both to validate the token and to select this authenticator when the
    /// credential's scheme / issuer hint matches. Must be set.
    /// </summary>
    public string Issuer { get; set; } = string.Empty;

    /// <summary>
    /// Optional scheme hint (for example <c>"Bearer"</c> or a short provider
    /// name). When set, a credential whose <see cref="LatticeCredential.Scheme"/>
    /// equals this value selects this authenticator without the token being
    /// parsed. <c>null</c> to select solely by issuer.
    /// </summary>
    public string? SchemeHint { get; set; }

    /// <summary>
    /// The audiences this authenticator accepts (the JWT <c>aud</c> claim). Must
    /// be non-empty when <see cref="ValidateAudience"/> is <c>true</c>; otherwise
    /// construction throws so audience validation is never silently disabled. Set
    /// <see cref="ValidateAudience"/> to <c>false</c> to accept any audience.
    /// </summary>
    public IList<string> Audiences { get; } = new List<string>();

    /// <summary>
    /// The signing keys trusted for token-signature validation. Ignored when an
    /// explicit <see cref="ValidationParameters"/> is supplied or a subclass
    /// overrides key resolution (for example via JWKS discovery).
    /// </summary>
    public IList<SecurityKey> SigningKeys { get; } = new List<SecurityKey>();

    /// <summary>
    /// The claim types consulted, in order, to resolve the subject id. The first
    /// present claim wins. Defaults to <c>sub</c> then <c>nameid</c>.
    /// </summary>
    public IList<string> SubjectClaimTypes { get; } = new List<string> { "sub", "nameid" };

    /// <summary>
    /// The claim types whose values are collected as token-asserted group ids.
    /// Defaults to <c>groups</c>, <c>roles</c>, and <c>role</c>.
    /// </summary>
    public IList<string> GroupClaimTypes { get; } = new List<string> { "groups", "roles", "role" };

    /// <summary>
    /// The token signature algorithms this authenticator accepts, pinned via
    /// <see cref="TokenValidationParameters.ValidAlgorithms"/> (the JWT header
    /// <c>alg</c>, for example <c>RS256</c> or <c>HS256</c>). Populate it to
    /// restrict acceptance to a specific set as a defense-in-depth measure against
    /// algorithm-confusion attacks (CWE-347). Populate the collection in place; an
    /// explicit pin is always authoritative and is never widened.
    /// </summary>
    /// <remarks>
    /// When empty (the default) the allow-list is derived from the families of the
    /// configured <see cref="SigningKeys"/>: an RSA-only or EC-only key set accepts
    /// only that family's algorithms, and a symmetric-only key set accepts only the
    /// HMAC algorithms, so a deployment can never be talked into verifying a token
    /// against a key of the wrong family. The allow-list is left unrestricted only
    /// when no family can be established - an empty key set, a key set that mixes
    /// symmetric and asymmetric material, or one containing a key type the
    /// derivation does not recognise. Pin explicitly, or supply
    /// <see cref="ValidationParameters"/>, to restrict those cases.
    /// </remarks>
    public IList<string> Algorithms { get; } = new List<string>();

    /// <summary>Whether to validate the token audience. Defaults to <c>true</c>.</summary>
    public bool ValidateAudience { get; set; } = true;

    /// <summary>Whether to validate the token lifetime (<c>exp</c> / <c>nbf</c>). Defaults to <c>true</c>.</summary>
    public bool ValidateLifetime { get; set; } = true;

    /// <summary>The permitted clock skew during lifetime validation. Defaults to five minutes.</summary>
    public TimeSpan ClockSkew { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// An explicit <see cref="TokenValidationParameters"/> override. When set it
    /// is used verbatim and the issuer / audience / signing-key fields above are
    /// ignored. Provided as an extension point for OIDC / JWKS discovery and
    /// signing-key rotation, where a provider subclass supplies live parameters.
    /// </summary>
    public TokenValidationParameters? ValidationParameters { get; set; }
}
