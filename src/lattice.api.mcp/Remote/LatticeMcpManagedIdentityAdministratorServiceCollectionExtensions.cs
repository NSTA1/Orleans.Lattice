using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that registers the managed-identity-backed administrator
/// credential source for the remote-host MCP topology, replacing the static
/// <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/> with a
/// self-refreshing Entra token so a long-lived MCP server keeps a valid
/// administrator introspection credential rather than a one-shot token that
/// expires after ~1h.
/// </summary>
public static class LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions
{
    /// <summary>
    /// Binds and validates <see cref="LatticeApiMcpManagedIdentityAdministratorOptions"/>
    /// from <paramref name="configure"/> and registers the managed-identity
    /// administrator credential source consumed by the remote credential-forwarding
    /// path. Call alongside
    /// <see cref="LatticeMcpRemoteServiceCollectionExtensions.AddLatticeMcpRemote"/>;
    /// this source takes precedence over the static
    /// <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/>, which need
    /// no longer be set.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Delegate that populates <see cref="LatticeApiMcpManagedIdentityAdministratorOptions"/> -
    /// at minimum the <see cref="LatticeApiMcpManagedIdentityAdministratorOptions.Credential"/>
    /// and <see cref="LatticeApiMcpManagedIdentityAdministratorOptions.Scope"/>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeMcpManagedIdentityAdministrator(
        this IServiceCollection services,
        Action<LatticeApiMcpManagedIdentityAdministratorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IValidateOptions<LatticeApiMcpManagedIdentityAdministratorOptions>,
            LatticeApiMcpManagedIdentityAdministratorOptionsValidator>());

        // Replace any previously-registered (default static) source so the
        // managed-identity source wins regardless of registration order relative to
        // AddLatticeMcpRemote.
        services.RemoveAll<ILatticeApiMcpAdministratorCredentialSource>();
        services.AddSingleton<
            ILatticeApiMcpAdministratorCredentialSource,
            ManagedIdentityAdministratorCredentialSource>();

        return services;
    }
}
