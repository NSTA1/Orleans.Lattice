using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// Registration helper for the Explorer's tenancy seam: the tenant-administration
/// client, the operations service, the availability probe, and the controlled
/// domain model the tenancy plugins resolve.
/// </summary>
public static class ExplorerTenancyServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tenancy seam. Also calls
    /// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPluginHost"/>
    /// so the plugin host the tenancy plugins gate through exists.
    /// <para>
    /// Call after <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>,
    /// whose session and sign-in the client reads, and after
    /// <c>AddExplorerTenantView</c>, whose tenant context, switcher, and
    /// platform-operator gate this seam reuses rather than duplicating. Calling
    /// it without <c>AddExplorerTenantView</c> is legal and is the non-tenant
    /// posture: no switcher resolves, so the domain reports tenancy disabled and
    /// the availability probe reports unavailable without touching the network.
    /// </para>
    /// </summary>
    /// <remarks>
    /// Everything is scoped per Blazor circuit, because the client reads the
    /// calling scope's session and sign-in and must not be shared across
    /// circuits. <see cref="GrpcTenantAdminClient"/> owns its own Orleans
    /// serializer provider and must not be handed the application root provider
    /// (which has no <c>AddSerializer</c>), or every tenancy call would fail
    /// resolving its per-message serializers; its single constructor keeps that
    /// guarantee, so a plain type registration is safe here.
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerTenancy(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginHost();
        services.TryAddScoped<ITenantAdminClient, GrpcTenantAdminClient>();
        services.TryAddScoped<ITenantAdminService, TenantAdminService>();
        services.TryAddScoped<ITenancyAvailability, TenancyAvailability>();

        // The controlled domain model the tenancy plugins declare. Registered
        // here rather than by the head, so the one contract the host may resolve
        // for a tenancy plugin ships with the package that defines it.
        services.TryAddScoped<ITenancyDomain, TenancyDomain>();
        return services;
    }
}
