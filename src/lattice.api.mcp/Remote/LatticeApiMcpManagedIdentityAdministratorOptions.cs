using Azure.Core;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Options for the managed-identity-backed administrator credential source that
/// keeps a long-lived remote-host MCP server supplied with a valid administrator
/// introspection token. The source acquires an Entra (Azure AD) access token for
/// <see cref="Scope"/> from <see cref="Credential"/>, caches it, and refreshes it
/// a configurable skew before expiry, so the trusted permission-introspection
/// path never presents an expired token (the failure the static
/// <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/> exhibits after
/// its ~1h lifetime). Bound and validated by
/// <see cref="LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions.AddLatticeMcpManagedIdentityAdministrator"/>.
/// </summary>
public sealed class LatticeApiMcpManagedIdentityAdministratorOptions
{
    /// <summary>
    /// The Azure credential the administrator access token is acquired from.
    /// Supply a concrete <c>Azure.Identity</c> credential such as
    /// <c>new ManagedIdentityCredential(...)</c> or
    /// <c>new DefaultAzureCredential()</c>. There is no default; a host opting into
    /// the managed-identity administrator source must set this. Only the bearer
    /// token it produces is forwarded to the remote cluster.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// The scope the administrator access token is requested for - the remote
    /// silo's audience, for example <c>api://&lt;silo-app-id&gt;/.default</c>. There
    /// is no default; must be a non-empty, non-whitespace value.
    /// </summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>
    /// How long before a cached token's expiry the source proactively acquires a
    /// fresh one, so an in-flight introspection never presents an about-to-expire
    /// token. Defaults to 5 minutes. Must be greater than or equal to
    /// <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan RefreshSkew { get; set; } = TimeSpan.FromMinutes(5);
}
