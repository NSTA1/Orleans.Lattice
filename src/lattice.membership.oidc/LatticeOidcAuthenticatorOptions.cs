namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// Configuration for a single <see cref="OidcCredentialAuthenticator"/>: the
/// OpenID Connect authority its discovery document is fetched from, the exact
/// issuer it accepts tokens from, the audiences it trusts, and the claim names
/// it reads the subject and group membership out of. One authenticator is
/// registered per issuer, so a silo can trust several OIDC providers at once
/// alongside the Entra, basic, and anonymous authenticators.
/// </summary>
public sealed class LatticeOidcAuthenticatorOptions
{
    /// <summary>
    /// The OpenID Connect authority the discovery document is fetched from, for
    /// example <c>https://dev-123456.okta.com/oauth2/default</c> or
    /// <c>https://keycloak.example.com/realms/lattice</c>. Must be set. When
    /// <see cref="MetadataAddress"/> is unset the discovery document address is
    /// derived from this value.
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// The explicit OIDC discovery document address. When <c>null</c> it is
    /// derived from <see cref="Authority"/> by appending
    /// <c>/.well-known/openid-configuration</c>. Set it explicitly for a provider
    /// that publishes its metadata somewhere other than the conventional path.
    /// </summary>
    public string? MetadataAddress { get; set; }

    /// <summary>
    /// The exact issuer (the token <c>iss</c> claim) this authenticator accepts.
    /// Must be set. Matching is ordinal and exact - there is no prefix, wildcard,
    /// or catch-all form - so a token from any other issuer is not handled by
    /// this authenticator and resolution falls through to the next one.
    /// </summary>
    public string Issuer { get; set; } = string.Empty;

    /// <summary>
    /// The audiences accepted (the token <c>aud</c> claim), typically the OAuth
    /// client id or an API identifier registered with the provider. Must contain
    /// at least one entry: audience validation is always enforced, so an empty
    /// list is rejected rather than silently accepting every audience. Populate
    /// the collection in place.
    /// </summary>
    public IList<string> Audiences { get; } = new List<string>();

    /// <summary>
    /// The token signature algorithms this authenticator accepts (the JWT header
    /// <c>alg</c>). Leave it empty - the default - to pin the algorithms the
    /// provider advertises in its discovery document's
    /// <c>id_token_signing_alg_values_supported</c>; populate it to pin an
    /// explicit, narrower set. Pinning is always enforced: unlike the base JWT
    /// authenticator an empty list never means "accept any algorithm", and a
    /// provider that advertises no algorithms at all rejects every token rather
    /// than accepting any. This is the defense against algorithm-confusion
    /// attacks (CWE-347). Populate the collection in place.
    /// </summary>
    public IList<string> Algorithms { get; } = new List<string>();

    /// <summary>
    /// The claim types, in priority order, read for the subject identifier.
    /// Defaults to the OpenID Connect standard <c>sub</c> claim. The first claim
    /// present on the validated token wins. Populate the collection in place.
    /// </summary>
    public IList<string> SubjectClaimTypes { get; } = new List<string> { OidcClaimNames.Subject };

    /// <summary>
    /// The claim types read for group membership. Defaults to <c>groups</c>,
    /// <c>roles</c>, and <c>role</c>, which between them cover the conventions of
    /// the mainstream OpenID Connect providers. Every value found across every
    /// listed claim type is asserted. Clear the collection to disable
    /// token-asserted groups entirely. Populate the collection in place.
    /// </summary>
    public IList<string> GroupClaimTypes { get; } = new List<string>
    {
        OidcClaimNames.Groups,
        OidcClaimNames.Roles,
        OidcClaimNames.Role,
    };

    /// <summary>
    /// Optional scheme hint. When set, a credential whose
    /// <see cref="LatticeCredential.Scheme"/> equals this value (compared
    /// case-insensitively) selects this authenticator without the token being
    /// parsed. <c>null</c> to select solely by exact issuer match.
    /// </summary>
    public string? SchemeHint { get; set; }

    /// <summary>Whether to validate the token lifetime (<c>exp</c> / <c>nbf</c>). Defaults to <c>true</c>.</summary>
    public bool ValidateLifetime { get; set; } = true;

    /// <summary>The permitted clock skew during lifetime validation. Defaults to five minutes.</summary>
    public TimeSpan ClockSkew { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How often the discovered JWKS metadata is proactively refreshed. Defaults to twelve hours.</summary>
    public TimeSpan AutomaticRefreshInterval { get; set; } = TimeSpan.FromHours(12);

    /// <summary>The minimum interval between forced JWKS refreshes. Defaults to five minutes.</summary>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Resolves the OIDC discovery document address: <see cref="MetadataAddress"/>
    /// when set, otherwise derived from <see cref="Authority"/>.
    /// </summary>
    /// <returns>The metadata address, or an empty string when neither value is set.</returns>
    public string ResolveMetadataAddress()
    {
        if (!string.IsNullOrWhiteSpace(MetadataAddress))
        {
            return MetadataAddress;
        }

        if (string.IsNullOrWhiteSpace(Authority))
        {
            return string.Empty;
        }

        return $"{Authority.TrimEnd('/')}/.well-known/openid-configuration";
    }
}
