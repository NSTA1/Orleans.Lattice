using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Registration helpers for the explorer's Access (membership &amp;
/// access-control) area: the auth-admin control client, the membership and policy
/// services, and the plugin access gate, plus the keyed plugin access store the
/// gate publishes into.
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
        return services;
    }
}
