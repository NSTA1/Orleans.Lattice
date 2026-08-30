using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// Registration helpers for the Explorer's Tenants (platform-operator tenant
/// management) plugin: its access gate, the shared tenancy seam it operates
/// through, the tenant-view scoping it requires, and the plugin registration
/// that surfaces the area in the shell.
/// <para>
/// Both helpers ship in the plugin's own package, so a head opts the area in by
/// referencing this package and calling them, and the shared UI library carries
/// no reference to the Tenants feature at all (epic decision D5).
/// </para>
/// </summary>
public static class ExplorerTenantsServiceCollectionExtensions
{
    /// <summary>
    /// The composition-time diagnostic for a head that registered its
    /// tenant-view seam before whatever supplies the real platform-operator
    /// gate.
    /// </summary>
    internal const string MisorderedGateMessage =
        "The Explorer's tenant-view seam was registered before a platform-operator gate was "
        + "supplied, so IExplorerTenantOperatorGate resolved to the fail-closed default and no "
        + "caller can ever validate as an operator. The Tenants area would render permanently "
        + "denied to everyone. Register the surface that supplies the gate first - "
        + "AddExplorerAccess(), whose administrator decision backs it - and call "
        + "AddExplorerTenants()/AddExplorerTenantsPlugin() after it. A head supplying its own "
        + "IExplorerTenantOperatorGate must register it as an implementation type or a singleton "
        + "instance (services.AddScoped<IExplorerTenantOperatorGate, MyGate>()), not as a "
        + "factory, so this check can tell it from the fail-closed default.";

    /// <summary>
    /// The composition-time diagnostic for a head that registered no
    /// platform-operator gate at all.
    /// </summary>
    internal const string MissingGateMessage =
        "The Tenants area is reserved for platform operators, but no IExplorerTenantOperatorGate "
        + "has been registered, so no caller could ever validate as one and the area would render "
        + "permanently denied to everyone. Register the surface that supplies the gate first - "
        + "AddExplorerAccess(), whose administrator decision backs it - or register your own "
        + "IExplorerTenantOperatorGate implementation, before calling AddExplorerTenants() or "
        + "AddExplorerTenantsPlugin().";

    /// <summary>
    /// Registers the Tenants feature: the plugin's access gate, the shared
    /// tenancy seam it operates through
    /// (<see cref="ExplorerTenancyServiceCollectionExtensions.AddExplorerTenancy"/>),
    /// and the Explorer's tenant-view scoping
    /// (<see cref="ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView"/>),
    /// which the tenancy seam reads for its active tenant, switcher, and
    /// operator gate.
    /// <para>
    /// <b>Call this after the surface that supplies the platform-operator
    /// gate</b> - <c>AddExplorerAccess()</c>, whose own administrator decision
    /// backs it - and after <c>AddExplorerConfiguration</c> and
    /// <c>AddExplorerAuth</c>, whose session and sign-in the tenancy client
    /// reads. The ordering is checked rather than assumed: getting it wrong
    /// leaves a fail-closed gate that denies everyone, which would otherwise
    /// present as a Tenants area no operator can ever open and no error anywhere
    /// to explain it, so this method throws instead.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registering the tenant view here, rather than leaving it to the head, is
    /// what makes the ordering check sound: the view registers the fail-closed
    /// <c>DeniedExplorerTenantOperatorGate</c> with <c>TryAdd</c>, so if the head
    /// called it first, a later real gate silently loses the race. By asserting a
    /// real gate exists and only then turning the view on, that race cannot
    /// happen through this path.
    /// </para>
    /// <para>
    /// A head that already enabled tenant scoping keeps its own registrations:
    /// every registration involved is <c>TryAdd</c>-based, so this call never
    /// replaces a head's custom identity resolver, tenant context, or gate.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// No platform-operator gate has been registered, or the registered one is
    /// the fail-closed default because the tenant-view seam was registered
    /// first. Either way the area could never admit an operator.
    /// </exception>
    public static IServiceCollection AddExplorerTenants(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        RequirePlatformOperatorGate(services);

        // Tenant scoping, then the shared tenancy seam that reads it. Both are
        // TryAdd-based and idempotent, so a head that already called either - or
        // a sibling tenancy plugin that did - is unaffected.
        services.AddExplorerTenantView();
        services.AddExplorerTenancy();

        // The plugin's own gate, resolved by the area plugin. Scoped per Blazor
        // circuit like the domain model it probes, so one operator's decision
        // never surfaces in another's circuit.
        services.TryAddScoped<TenantsAccessGate>();
        return services;
    }

    /// <summary>
    /// Registers the Tenants area plugin, so the shell enumerates it from the
    /// container and renders its panel. Calls <see cref="AddExplorerTenants"/>
    /// for you, so it carries the same ordering requirement: register the
    /// platform-operator gate provider (<c>AddExplorerAccess()</c>) first.
    /// <para>
    /// The head is also responsible for registering the host-side plugin
    /// adapters (<c>AddExplorerPluginAdapters</c>), which live on the shell's
    /// side of the seam and are shared by every plugin. A head that calls
    /// neither helper ships no Tenants area at all, which is the whole of the
    /// opt-out.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// No usable platform-operator gate is registered; see
    /// <see cref="AddExplorerTenants"/>.
    /// </exception>
    public static IServiceCollection AddExplorerTenantsPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerTenants();
        return services.AddExplorerPlugin<TenantsAreaPlugin>();
    }

    /// <summary>
    /// Asserts that a real <see cref="IExplorerTenantOperatorGate"/> is already
    /// registered, so the operator-only area cannot be composed against a gate
    /// that denies everyone.
    /// </summary>
    /// <remarks>
    /// The last registration for a service type is the one the container
    /// resolves, so the check reads backwards and stops at the first match. A
    /// gate registered by implementation type or as an instance came from a head
    /// or from an administrative surface and is real; the only factory-registered
    /// gate in the Explorer is the fail-closed default the tenant-view seam adds
    /// when nothing better exists, which is exactly the case worth refusing.
    /// </remarks>
    private static void RequirePlatformOperatorGate(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType != typeof(IExplorerTenantOperatorGate))
            {
                continue;
            }

            if (descriptor.ImplementationType is not null || descriptor.ImplementationInstance is not null)
            {
                return;
            }

            throw new InvalidOperationException(MisorderedGateMessage);
        }

        throw new InvalidOperationException(MissingGateMessage);
    }
}
