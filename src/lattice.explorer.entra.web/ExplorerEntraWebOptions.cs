using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Identity.Web;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Which token cache backs the Microsoft.Identity.Web token acquisition the
/// hosted-web Entra provider uses.
/// </summary>
public enum ExplorerWebTokenCacheKind
{
    /// <summary>
    /// A per-process in-memory token cache. Correct for a single-replica host;
    /// on a multi-replica host a user's cached token is not shared across
    /// replicas, so a request routed to a cold replica re-acquires silently.
    /// </summary>
    InMemory,

    /// <summary>
    /// A Microsoft.Identity.Web distributed token cache over the registered
    /// <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>.
    /// Register a shared cache (for example
    /// <c>Orleans.Lattice.Caching.AzureBlob</c>) so a multi-replica host shares
    /// one token cache and cookies/tokens survive a replica restart.
    /// <para>
    /// For a geo-distributed (multi-region) deployment, point every region's
    /// <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/> at
    /// a single <b>estate-global</b> container rather than a per-region one: a
    /// signed-in operator's on-behalf-of token is then acquirable on any replica in
    /// any region, so a mid-session failover across regions is seamless and never
    /// forces an interactive re-authentication.
    /// </para>
    /// </summary>
    Distributed,
}

/// <summary>
/// Configuration for the hosted-web Microsoft Entra ID (OpenID Connect) sign-in
/// provider registered by
/// <see cref="ExplorerEntraWebServiceCollectionExtensions.AddLatticeExplorerEntraWebAuth"/>.
/// It configures the ASP.NET Core OIDC app (authority, application id, redirect
/// paths) and the downstream State API token the signed-in browser user's token
/// is exchanged for.
/// </summary>
public sealed class ExplorerEntraWebOptions
{
    /// <summary>The default Entra authority instance.</summary>
    public const string DefaultInstance = "https://login.microsoftonline.com/";

    /// <summary>The default OIDC auth-code callback path.</summary>
    public const string DefaultCallbackPath = "/signin-oidc";

    /// <summary>The default OIDC signed-out callback path.</summary>
    public const string DefaultSignedOutCallbackPath = "/signout-callback-oidc";

    /// <summary>
    /// The Entra authority instance (for example
    /// <c>https://login.microsoftonline.com/</c>). Defaults to
    /// <see cref="DefaultInstance"/>.
    /// </summary>
    public string Instance { get; set; } = DefaultInstance;

    /// <summary>
    /// The directory (tenant) id the console signs users in against. Required.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// The application (client) id of the Explorer console's own Entra app
    /// registration - the confidential web app with the OIDC redirect URIs, not
    /// the State API resource app. Required.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Optional client secret for the confidential client. Leave unset to use a
    /// secret-less credential (a federated managed-identity assertion or
    /// certificate) supplied through
    /// <see cref="ConfigureMicrosoftIdentityOptions"/>; that is the recommended
    /// production configuration.
    /// </summary>
    public string? ClientSecret { get; set; }

    /// <summary>The OIDC auth-code callback path. Defaults to <see cref="DefaultCallbackPath"/>.</summary>
    public string CallbackPath { get; set; } = DefaultCallbackPath;

    /// <summary>The OIDC signed-out callback path. Defaults to <see cref="DefaultSignedOutCallbackPath"/>.</summary>
    public string SignedOutCallbackPath { get; set; } = DefaultSignedOutCallbackPath;

    /// <summary>
    /// The scopes requested for the downstream State API (for example
    /// <c>api://{tenantId}/{app}-silo/.default</c>). When empty, the provider
    /// resolves the scope at sign-in time from the audience the State API
    /// advertises, appending <c>/.default</c> when the advertised value is a bare
    /// resource id.
    /// </summary>
    public IList<string> Scopes { get; } = new List<string>();

    /// <summary>
    /// The token cache backing Microsoft.Identity.Web. Defaults to
    /// <see cref="ExplorerWebTokenCacheKind.InMemory"/>; select
    /// <see cref="ExplorerWebTokenCacheKind.Distributed"/> and register a shared
    /// <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>
    /// on a multi-replica host.
    /// </summary>
    public ExplorerWebTokenCacheKind TokenCache { get; set; } = ExplorerWebTokenCacheKind.InMemory;

    /// <summary>
    /// When <see langword="true"/> (the default) the registration installs a
    /// fallback authorization policy that requires an authenticated user, so an
    /// unauthenticated request to any endpoint is challenged into the OIDC
    /// redirect. Set to <see langword="false"/> to manage authorization yourself.
    /// </summary>
    public bool RequireAuthenticatedUser { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/> (the default) a Blazor Server circuit handler
    /// completes the State API sign-in automatically for an already
    /// browser-authenticated user, so the console connects without a manual
    /// "Sign in with Entra ID" click. Best-effort: on any failure it degrades
    /// silently to the interactive sign-in dialog. Set to <see langword="false"/>
    /// to always require the manual dialog click.
    /// </summary>
    public bool AutoSignIn { get; set; } = true;

    /// <summary>
    /// The head-relative path of the forced-interactive re-authentication endpoint
    /// (mapped by
    /// <see cref="ExplorerEntraWebEndpointRouteBuilderExtensions.MapLatticeExplorerEntraWebReauth"/>)
    /// that the explorer's re-authentication interstitial navigates to when the
    /// downstream token can no longer be renewed silently. Defaults to
    /// <see cref="ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultReauthPattern"/>.
    /// Registering the provider publishes this path to the core explorer so the UI
    /// can drive a graceful re-authentication without depending on this package.
    /// Set to <see langword="null"/> to leave the core default (a plain reload) in
    /// place.
    /// </summary>
    public string? ReauthChallengePath { get; set; } =
        ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultReauthPattern;

    /// <summary>
    /// Optional escape hatch to configure the underlying
    /// <see cref="MicrosoftIdentityOptions"/> directly - for example to attach a
    /// federated managed-identity <c>ClientCredentials</c> for secret-less auth,
    /// or to adjust the OIDC events. Invoked after the values above are applied.
    /// </summary>
    public Action<MicrosoftIdentityOptions>? ConfigureMicrosoftIdentityOptions { get; set; }

    /// <summary>
    /// Optional callback to configure the cookie authentication options (for
    /// example the session lifetime or cookie name). Invoked after
    /// Microsoft.Identity.Web applies its defaults.
    /// </summary>
    public Action<CookieAuthenticationOptions>? ConfigureCookieOptions { get; set; }

    /// <summary>
    /// Validates that the mandatory application identity is configured.
    /// </summary>
    /// <exception cref="InvalidOperationException">A required value is missing.</exception>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Instance))
        {
            throw new InvalidOperationException(
                $"{nameof(ExplorerEntraWebOptions)}.{nameof(Instance)} must not be null or empty.");
        }

        if (string.IsNullOrWhiteSpace(TenantId))
        {
            throw new InvalidOperationException(
                $"{nameof(ExplorerEntraWebOptions)}.{nameof(TenantId)} is required (the directory the console signs users in against).");
        }

        if (string.IsNullOrWhiteSpace(ClientId))
        {
            throw new InvalidOperationException(
                $"{nameof(ExplorerEntraWebOptions)}.{nameof(ClientId)} is required (the Explorer console's own Entra application id).");
        }

        if (string.IsNullOrWhiteSpace(CallbackPath))
        {
            throw new InvalidOperationException(
                $"{nameof(ExplorerEntraWebOptions)}.{nameof(CallbackPath)} must not be null or empty.");
        }
    }
}
