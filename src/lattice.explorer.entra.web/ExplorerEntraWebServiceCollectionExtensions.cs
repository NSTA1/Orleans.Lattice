using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Identity.Web;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Registration helpers for the hosted-web Microsoft Entra ID sign-in provider.
/// Calling <see cref="AddLatticeExplorerEntraWebAuth"/> wires the ASP.NET Core
/// OpenID Connect app (auth-code + PKCE, cookie session) through
/// Microsoft.Identity.Web, adds the scoped <see cref="EntraWebExplorerAuthMethod"/>
/// so the console offers an Entra sign-in when the State API advertises it, and
/// (by default) installs a fallback authorization policy plus an auto-sign-in
/// circuit handler - all without the core explorer taking any dependency on
/// Microsoft.Identity.Web.
/// </summary>
public static class ExplorerEntraWebServiceCollectionExtensions
{
    /// <summary>
    /// Registers the hosted-web Entra login provider and its Microsoft.Identity.Web
    /// OpenID Connect app.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Callback to configure <see cref="ExplorerEntraWebOptions"/>. Required (the tenant and application id have no defaults).</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">A required option is missing.</exception>
    public static IServiceCollection AddLatticeExplorerEntraWebAuth(
        this IServiceCollection services,
        Action<ExplorerEntraWebOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new ExplorerEntraWebOptions();
        configure(options);
        options.Validate();

        services.Configure(configure);
        services.AddOptions<ExplorerEntraWebOptions>();

        // Feed the authenticated user captured from HttpContext.User during the
        // initial render into the Blazor Server circuit's AuthenticationStateProvider.
        // Without this, an interactive-server circuit reports the OIDC-authenticated
        // browser user as anonymous for the life of the circuit, so the auto-sign-in
        // handler and the token acquirer (both registered below) short-circuit on the
        // anonymous principal and every downstream cluster call is made anonymously.
        services.AddCascadingAuthenticationState();

        var initialScopes = options.Scopes.Count > 0 ? options.Scopes.ToArray() : null;

        var appBuilder = services
            .AddAuthentication(OpenIdConnectDefaults.AuthenticationScheme)
            .AddMicrosoftIdentityWebApp(
                microsoftIdentityOptions =>
                {
                    microsoftIdentityOptions.Instance = options.Instance;
                    microsoftIdentityOptions.TenantId = options.TenantId;
                    microsoftIdentityOptions.ClientId = options.ClientId;
                    microsoftIdentityOptions.ClientSecret = options.ClientSecret;
                    microsoftIdentityOptions.CallbackPath = options.CallbackPath;
                    microsoftIdentityOptions.SignedOutCallbackPath = options.SignedOutCallbackPath;
                    options.ConfigureMicrosoftIdentityOptions?.Invoke(microsoftIdentityOptions);
                },
                cookieOptions => options.ConfigureCookieOptions?.Invoke(cookieOptions));

        var apiBuilder = appBuilder.EnableTokenAcquisitionToCallDownstreamApi(initialScopes);
        if (options.TokenCache == ExplorerWebTokenCacheKind.Distributed)
        {
            apiBuilder.AddDistributedTokenCaches();
        }
        else
        {
            apiBuilder.AddInMemoryTokenCaches();
        }

        // Per-circuit: the acquirer reads the circuit's authenticated user, and
        // the method depends on the acquirer.
        services.TryAddScoped<IExplorerWebTokenAcquirer, IdentityWebExplorerTokenAcquirer>();
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IExplorerAuthMethod, EntraWebExplorerAuthMethod>());

        if (options.RequireAuthenticatedUser)
        {
            services
                .AddAuthorizationBuilder()
                .SetFallbackPolicy(new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build());
        }

        if (options.AutoSignIn)
        {
            services.TryAddEnumerable(ServiceDescriptor.Scoped<CircuitHandler, ExplorerEntraWebAutoSignInCircuitHandler>());
        }

        // Publish the forced-interactive challenge path to the core explorer so its
        // re-authentication interstitial can drive a fresh OIDC sign-in without the
        // core taking any dependency on this package. Registered last so it wins
        // over the core default (the auth registration adds a no-challenge default
        // via TryAdd). Left untouched when the caller cleared the path.
        if (!string.IsNullOrWhiteSpace(options.ReauthChallengePath))
        {
            services.AddSingleton(new ExplorerReauthOptions
            {
                ChallengePath = options.ReauthChallengePath,
            });
        }

        // Publish the federated sign-out path to the core explorer so its "Sign
        // out" button posts to the endpoint mapped by
        // MapLatticeExplorerEntraWebSignOut, driving a full federated sign-out
        // (end the browser cookie + Entra session, not just drop the API
        // credential). Without this the button would only clear the local
        // credential and the fallback authorization policy would silently
        // re-authenticate the still-cookie-valid circuit. Registered last so it
        // wins over the core default (a local-only sign-out added via TryAdd).
        // Left untouched when the caller cleared the path.
        if (!string.IsNullOrWhiteSpace(options.SignOutPath))
        {
            services.AddSingleton(new ExplorerSignOutOptions
            {
                FederatedSignOutPath = options.SignOutPath,
            });
        }

        return services;
    }
}
