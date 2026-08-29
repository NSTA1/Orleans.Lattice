using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Registration helpers for the Access (membership &amp; access-control)
/// plugin: the auth-admin control client, the membership and policy services,
/// the single domain model its views operate against, and the four-state access
/// gate, plus the plugin registration that surfaces the area in the shell.
/// <para>
/// Both helpers ship in the plugin's own package, so a head opts the area in by
/// referencing this package and calling them, and the shared UI library carries
/// no reference to the Access feature at all (epic decision D5).
/// </para>
/// </summary>
public static class ExplorerAccessServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Access feature. Also calls
    /// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPluginHost"/>
    /// so the keyed access store the gate publishes into exists, and supplies the
    /// <see cref="IExplorerTenantOperatorGate"/> backed by the Access plugin's own
    /// administrator decision. Call after <c>AddExplorerConfiguration</c> and
    /// <c>AddExplorerAuth</c>, whose session and sign-in the auth-admin client
    /// reads.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerAccess(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerPluginHost();
        // Scoped per Blazor circuit: the auth-admin client reads the calling
        // scope's session and sign-in, so it must not be shared across circuits.
        // GrpcAuthAdminClient owns its own Orleans serializer provider; it must not
        // be handed the application root provider (which has no AddSerializer), or
        // every admin gRPC call fails resolving its per-message serializers and the
        // Access area silently greys out. Its single constructor keeps that
        // guarantee, so a plain type registration is safe here.
        services.TryAddScoped<IAuthAdminClient, GrpcAuthAdminClient>();
        services.TryAddScoped<IMembershipAdminService, MembershipAdminService>();
        services.TryAddScoped<IPolicyAdminService, PolicyAdminService>();
        services.TryAddScoped<IAuthAdminCapabilityService, AuthAdminCapabilityService>();
        // The Explorer's cross-tenant root of trust is "the auth-admin control
        // plane accepts this caller as an administrator", which is exactly the
        // Access plugin's own decision. Registering the gate here keeps that
        // knowledge inside the plugin that owns the probe, instead of in a shared
        // record every area also reads.
        services.TryAddScoped<IExplorerTenantOperatorGate, AccessExplorerTenantOperatorGate>();
        // The subject picker's search state is per-component and each carries its
        // own single in-flight debounce timer, so both are transient: every picker
        // instance resolves a fresh model over a fresh debounce.
        services.TryAddTransient<ISubjectSearchDebounce, TimerSubjectSearchDebounce>();
        services.TryAddTransient<SubjectPickerModel>();
        // The principal-label resolver caches directory display names for the
        // lifetime of the Access panel it is injected into, so it is transient:
        // each panel resolves a fresh resolver whose cache is scoped to that view.
        services.TryAddTransient<PrincipalLabelResolver>();
        // The one contract the plugin's views are handed, composed from the
        // per-circuit services above. The debounce stays a factory because each
        // picker owns its own single in-flight timer, so the injectable timing
        // seam survives the move onto the domain model.
        services.TryAddScoped<IAccessDomain>(static provider => new AccessDomain(
            provider.GetRequiredService<IMembershipAdminService>(),
            provider.GetRequiredService<IPolicyAdminService>(),
            provider.GetRequiredService<ICatalogReader>(),
            provider.GetRequiredService<IAuthAdminCapabilityService>(),
            provider.GetRequiredService<ISubjectSearchDebounce>));
        return services;
    }

    /// <summary>
    /// Registers the Access area plugin, so the shell renders a tab for it. The
    /// Access feature itself must be registered separately with
    /// <see cref="AddExplorerAccess"/> (it owns the control client, the domain
    /// model, and the access gate), and the head must have registered the plugin
    /// adapters that publish its own selection, connection, tenancy, and
    /// preference state onto the plugin contract.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerAccessPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddExplorerPlugin<AccessAreaPlugin>();
    }
}
