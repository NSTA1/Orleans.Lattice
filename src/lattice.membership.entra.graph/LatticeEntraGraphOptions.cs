using Azure.Core;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Configuration for the Microsoft Graph-backed <see cref="IEntraGroupResolver"/>:
/// the Entra application (client) credentials it authenticates with, the Graph
/// scopes it requests, and how it shapes the transitive-group query. The app-only
/// access token is acquired and refreshed transparently through the MSAL
/// confidential-client cache; operators never manage a Graph token directly.
/// <para>
/// Two mutually exclusive authentication modes are supported. By default the
/// resolver uses the confidential-client path, authenticating app-only with the
/// <see cref="TenantId"/>, <see cref="ClientId"/>, and <see cref="ClientSecret"/>
/// triple. Alternatively, supplying a <see cref="Credential"/> selects a
/// secret-less path where the resolver authenticates app-only with that token
/// credential (e.g. a federated managed identity) and no client secret is used.
/// </para>
/// </summary>
public sealed class LatticeEntraGraphOptions
{
    /// <summary>The default Entra login host used to build the token authority.</summary>
    public const string DefaultAuthorityHost = "https://login.microsoftonline.com";

    /// <summary>The default Graph scope for app-only access.</summary>
    public const string DefaultScope = "https://graph.microsoft.com/.default";

    /// <summary>
    /// The tenant id the app-only Graph token is issued for. Required for the
    /// confidential-client (client-secret) path; ignored when <see cref="Credential"/> is set.
    /// </summary>
    public string TenantId { get; set; } = string.Empty;

    /// <summary>
    /// The Entra application (client) id used to acquire the Graph token. Required for the
    /// confidential-client (client-secret) path; ignored when <see cref="Credential"/> is set.
    /// </summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// The Entra application client secret used to acquire the Graph token. Required for the
    /// confidential-client path; must be left unset when <see cref="Credential"/> is used.
    /// </summary>
    public string ClientSecret { get; set; } = string.Empty;

    /// <summary>
    /// An optional Azure token credential that selects the secret-less
    /// authentication path. When set, the resolver authenticates app-only with
    /// this credential (for example <c>DefaultAzureCredential</c> or a
    /// <c>ManagedIdentityCredential</c> bound to a user-assigned managed identity)
    /// and no <see cref="ClientSecret"/> is used - the shared Graph client is built
    /// directly from this credential and the configured <see cref="Scopes"/>. This
    /// mode is mutually exclusive with the client-secret path: supplying both a
    /// <see cref="Credential"/> and a <see cref="ClientSecret"/> is rejected as
    /// ambiguous. Defaults to <c>null</c> (the confidential-client path).
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// The Entra login host. Combined with <see cref="TenantId"/> to form the MSAL
    /// authority. Defaults to <see cref="DefaultAuthorityHost"/>.
    /// </summary>
    public string AuthorityHost { get; set; } = DefaultAuthorityHost;

    /// <summary>
    /// The Graph scopes requested for the app-only token. Defaults to the single
    /// <see cref="DefaultScope"/>. Must contain at least one scope.
    /// </summary>
    public IList<string> Scopes { get; } = new List<string> { DefaultScope };

    /// <summary>
    /// Whether the transitive-group query returns only security-enabled groups.
    /// Defaults to <c>false</c> (all groups and directory roles).
    /// </summary>
    public bool SecurityEnabledOnly { get; set; }

    /// <summary>
    /// How long before the token's actual expiry it is proactively refreshed, so a
    /// call never uses a token that expires mid-flight. Defaults to five minutes.
    /// </summary>
    public TimeSpan TokenRefreshSkew { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Which Entra identifier the Graph-backed
    /// <see cref="ILatticeIdentityDirectory"/> records as a
    /// <see cref="DirectoryPrincipal.Id"/>, so directory validation matches the
    /// active authenticator's subject claim. Defaults to
    /// <see cref="EntraDirectorySubjectIdSource.ObjectId"/> (the <c>oid</c>), which
    /// aligns with a typical Entra deployment whose
    /// <see cref="JwtAuthenticatorOptions.SubjectClaimTypes"/> resolves the subject
    /// to the object id.
    /// </summary>
    public EntraDirectorySubjectIdSource DirectorySubjectIdSource { get; set; } = EntraDirectorySubjectIdSource.ObjectId;

    /// <summary>Builds the MSAL authority from <see cref="AuthorityHost"/> and <see cref="TenantId"/>.</summary>
    /// <returns>The authority URL, or an empty string when either input is missing.</returns>
    public string ResolveAuthority()
    {
        if (string.IsNullOrWhiteSpace(AuthorityHost) || string.IsNullOrWhiteSpace(TenantId))
        {
            return string.Empty;
        }

        return $"{AuthorityHost.TrimEnd('/')}/{TenantId}";
    }
}
