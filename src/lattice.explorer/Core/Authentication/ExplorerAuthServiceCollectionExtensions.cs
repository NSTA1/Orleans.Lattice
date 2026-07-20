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

        // The built-in Basic provider is always available so the original
        // username/password flow keeps working. Optional providers (Entra,
        // custom) add themselves as further IExplorerAuthMethod registrations.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IExplorerAuthMethod, BasicExplorerAuthMethod>());

        // The gRPC scheme-discovery probe. TryAdd so a head or test can override
        // it (for example with a fake advertisement).
        services.TryAddSingleton<IExplorerAuthSchemeProbe, GrpcExplorerAuthSchemeProbe>();

        // The auth session is scoped per Blazor circuit so each circuit signs in
        // (and drives its connection) independently, keyed on its own cookie
        // credential, rather than inheriting a process-global sign-in. The
        // credential store, auth methods, and scheme probe are stateless across
        // circuits and stay singletons.
        services.TryAddScoped<IExplorerAuthSession, ExplorerAuthSession>();

        return services;
    }
}
