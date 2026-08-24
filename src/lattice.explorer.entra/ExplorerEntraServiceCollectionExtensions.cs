using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// Registration helpers for the Entra ID explorer login provider. Calling
/// <see cref="AddExplorerEntraAuth"/> adds the Entra
/// <see cref="IExplorerAuthMethod"/> alongside the built-in Basic provider, so
/// the explorer offers an Entra sign-in when an endpoint advertises it - without
/// the core explorer taking any dependency on MSAL.
/// </summary>
public static class ExplorerEntraServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Entra login provider (<see cref="EntraExplorerAuthMethod"/>),
    /// the MSAL-backed token acquirer, and its options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional callback to configure <see cref="ExplorerEntraOptions"/>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddExplorerEntraAuth(
        this IServiceCollection services,
        Action<ExplorerEntraOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddOptions<ExplorerEntraOptions>();

        // Per-circuit credential isolation (security invariant): the acquirer
        // owns an in-memory MSAL token cache and the auth method drives a
        // per-operator sign-in, so both must be scoped to the Blazor circuit,
        // never a process-global singleton - a shared cache would leak one
        // operator's credential to every circuit. The consuming
        // IExplorerAuthSession is itself scoped, so there is no captive
        // dependency. Registered with TryAdd* so a host may still override the
        // acquirer with its own registration.
        services.TryAddScoped<IEntraInteractiveTokenAcquirer, MsalEntraInteractiveTokenAcquirer>();
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IExplorerAuthMethod, EntraExplorerAuthMethod>());

        return services;
    }
}
