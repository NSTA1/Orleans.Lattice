using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Configuration for a single <see cref="EntraCredentialAuthenticator"/>: the
/// Entra authority it discovers metadata from, the tenant allow-list it accepts
/// tokens from, the audiences it trusts, and how it resolves overflowed group
/// membership. One authenticator is registered per Entra application, so a silo
/// can trust several Entra apps at once alongside other issuers.
/// </summary>
public sealed class LatticeEntraAuthenticatorOptions
{
    /// <summary>
    /// The default Entra v2.0 issuer template. <c>{tenantid}</c> is replaced with
    /// each token's tenant id when validating the issuer, so both single-tenant
    /// and multi-tenant tokens validate against one template.
    /// </summary>
    public const string DefaultIssuerTemplate = "https://login.microsoftonline.com/{tenantid}/v2.0";

    /// <summary>The default Entra login host used to derive the OIDC metadata address.</summary>
    public const string DefaultAuthorityHost = "https://login.microsoftonline.com";

    /// <summary>
    /// The Entra authority the OIDC metadata is discovered from, for example
    /// <c>https://login.microsoftonline.com/common/v2.0</c> for multi-tenant or
    /// <c>https://login.microsoftonline.com/{tenant-guid}/v2.0</c> for
    /// single-tenant. Must be set. When <see cref="MetadataAddress"/> is unset the
    /// discovery document address is derived from this value.
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// The explicit OIDC discovery document address. When <c>null</c> it is
    /// derived from <see cref="Authority"/> by appending
    /// <c>/.well-known/openid-configuration</c>.
    /// </summary>
    public string? MetadataAddress { get; set; }

    /// <summary>
    /// The issuer template validated against each token, with <c>{tenantid}</c>
    /// substituted by the token's tenant id. Defaults to
    /// <see cref="DefaultIssuerTemplate"/>.
    /// </summary>
    public string IssuerTemplate { get; set; } = DefaultIssuerTemplate;

    /// <summary>
    /// The tenant ids (Entra <c>tid</c> values) this authenticator accepts. A
    /// single entry is single-tenant; several entries form a multi-tenant
    /// allow-list. A token whose <c>tid</c> is not in this set is not handled and
    /// resolution falls through to the next authenticator. Must contain at least
    /// one entry.
    /// </summary>
    public IList<string> TenantIds { get; } = new List<string>();

    /// <summary>
    /// The audiences accepted (the token <c>aud</c> claim), typically the Entra
    /// application (client) id or its Application ID URI. Must contain at least
    /// one entry.
    /// </summary>
    public IList<string> Audiences { get; } = new List<string>();

    /// <summary>
    /// The default token signature algorithm Microsoft Entra ID issues v2.0
    /// tokens with (<c>RS256</c>). <see cref="Algorithms"/> is pre-populated with
    /// this value.
    /// </summary>
    public const string DefaultAlgorithm = SecurityAlgorithms.RsaSha256;

    /// <summary>
    /// The token signature algorithms this authenticator accepts (the JWT header
    /// <c>alg</c>), pinned via <see cref="TokenValidationParameters.ValidAlgorithms"/>.
    /// Defaults to <c>RS256</c>, the asymmetric algorithm Entra issues v2.0 tokens
    /// with, so the validator refuses a token advertising any other algorithm -
    /// a defense-in-depth measure against algorithm-confusion attacks (CWE-347).
    /// Clear and repopulate to accept a different set; leaving it empty disables
    /// algorithm pinning (not recommended). Populate the collection in place.
    /// </summary>
    public IList<string> Algorithms { get; } = new List<string> { DefaultAlgorithm };

    /// <summary>
    /// Optional scheme hint. When set, a credential whose
    /// <see cref="LatticeCredential.Scheme"/> equals this value selects this
    /// authenticator without the token being parsed. <c>null</c> to select solely
    /// by tenant / issuer.
    /// </summary>
    public string? SchemeHint { get; set; }

    /// <summary>How overflowed group membership is resolved. Defaults to <see cref="EntraGroupResolutionMode.TokenOnly"/>.</summary>
    public EntraGroupResolutionMode GroupResolutionMode { get; set; } = EntraGroupResolutionMode.TokenOnly;

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
