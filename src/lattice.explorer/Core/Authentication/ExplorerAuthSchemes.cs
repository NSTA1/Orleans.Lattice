namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The well-known <see cref="IExplorerAuthMethod.SchemeId"/> values and the
/// input keys the built-in providers read from
/// <see cref="ExplorerAuthChallengeContext.Inputs"/>. Custom providers may
/// define their own scheme ids; these constants cover the schemes shipped in the
/// box and the parameter names the server advertisement uses.
/// </summary>
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
}
