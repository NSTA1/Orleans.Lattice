using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Registration helpers for the explorer's authentication session and credential
/// store. Both heads call <see cref="AddExplorerAuth"/>; each head registers its
/// own platform-backed <see cref="ICredentialStore"/> (DPAPI on desktop, an
/// encrypted server cookie on web) before or after this call, overriding the
/// safe in-memory default.
/// </summary>
public static class ExplorerAuthServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IExplorerAuthSession"/> and a default in-memory
    /// <see cref="ICredentialStore"/>. The in-memory store is registered with
    /// <c>TryAdd</c> so a head that registers a platform store keeps it.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddExplorerAuth(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<ICredentialStore, InMemoryCredentialStore>();
        services.TryAddSingleton<IExplorerAuthSession, ExplorerAuthSession>();

        return services;
    }
}
