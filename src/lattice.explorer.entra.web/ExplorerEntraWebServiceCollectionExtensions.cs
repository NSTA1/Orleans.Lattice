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

        return services;
    }
}
