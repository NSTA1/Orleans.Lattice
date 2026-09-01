namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The well-known <see cref="IExplorerAuthMethod.SchemeId"/> values and the
/// input keys the built-in providers read from
/// <see cref="ExplorerAuthChallengeContext.Inputs"/>. Custom providers may
/// define their own scheme ids; these constants cover the schemes shipped in the
/// box and the parameter names the server advertisement uses.
/// <para>
/// The generic <see cref="Oidc"/> scheme takes its whole configuration from the
/// advertised parameters: <see cref="AuthorityParameter"/> (or
/// <see cref="MetadataAddressParameter"/> when the provider's discovery document
/// does not sit at <c>{authority}/.well-known/openid-configuration</c>),
/// <see cref="ClientIdParameter"/>, and <see cref="ScopeParameter"/>.
/// </para>
/// </summary>
/// <remarks>
/// The <see cref="Oidc"/> scheme deliberately does not derive a scope from
/// <see cref="AudienceParameter"/>. That derivation appends <c>/.default</c> to
/// the audience, which is an Entra/MSAL convention with no generic equivalent,
/// so the <see cref="Entra"/> scheme reads the audience and a conformant OpenID
/// Connect provider states its scopes explicitly instead.
/// </remarks>
public static class ExplorerAuthSchemes
{
    /// <summary>The username/password Basic scheme (the default, always available).</summary>
    public const string Basic = "basic";

    /// <summary>The Microsoft Entra ID interactive OIDC scheme.</summary>
    public const string Entra = "entra";

    /// <summary>A generic OpenID Connect scheme.</summary>
    public const string Oidc = "oidc";

    /// <summary>The <see cref="ExplorerAuthChallengeContext.Inputs"/> key carrying the Basic username.</summary>
    public const string UsernameInput = "username";

    /// <summary>The <see cref="ExplorerAuthChallengeContext.Inputs"/> key carrying the Basic password.</summary>
    public const string PasswordInput = "password";

    /// <summary>The advertised-parameter key for an OIDC authority / metadata endpoint.</summary>
    public const string AuthorityParameter = "authority";

    /// <summary>The advertised-parameter key for a directory tenant id.</summary>
    public const string TenantIdParameter = "tenantId";

    /// <summary>The advertised-parameter key for the client (application) id.</summary>
    public const string ClientIdParameter = "clientId";

    /// <summary>The advertised-parameter key for the audience / resource the token targets.</summary>
    public const string AudienceParameter = "audience";

    /// <summary>
    /// The advertised-parameter key for the scopes a client should request,
    /// consumed by the generic <see cref="Oidc"/> scheme. The value is a single
    /// space-delimited list encoded exactly as the OAuth 2.0 <c>scope</c>
    /// request parameter, for example <c>openid profile lattice.api</c>. The
    /// <see cref="Entra"/> scheme does not read this key; it derives its scope
    /// from <see cref="AudienceParameter"/> instead.
    /// </summary>
    public const string ScopeParameter = "scope";

    /// <summary>
    /// The advertised-parameter key for an explicit OpenID Connect discovery
    /// document address, consumed by the generic <see cref="Oidc"/> scheme. The
    /// value is an absolute URL to the provider's <c>openid-configuration</c>
    /// document, for a provider whose document does not sit at
    /// <c>{authority}/.well-known/openid-configuration</c>. When it is absent,
    /// the address is derived from <see cref="AuthorityParameter"/>.
    /// </summary>
    public const string MetadataAddressParameter = "metadataAddress";
}
