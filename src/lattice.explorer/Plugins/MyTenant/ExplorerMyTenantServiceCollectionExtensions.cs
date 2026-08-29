using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// Registration helpers for the Explorer's My Tenant plugin: the plugin's own
/// access gate and the plugin registration that surfaces the area in the shell.
/// <para>
/// Both ship in the plugin's own package, so a head opts the area in by
/// referencing this package and calling them, and no shared library carries a
/// reference to it (epic decision D5).
/// </para>
/// </summary>
public static class ExplorerMyTenantServiceCollectionExtensions
{
    /// <summary>
    /// Registers the My Tenant feature: the plugin access gate, and - through
    /// <see cref="ExplorerTenancyServiceCollectionExtensions.AddExplorerTenancy"/>
    /// - the shared tenancy seam whose controlled domain model the plugin
    /// operates against.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Call after <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>,
    /// whose session and sign-in the tenancy client reads, and after
    /// <c>AddExplorerTenantView</c>, whose tenant context and switcher the seam
    /// reuses. Calling it on a head that never opted into tenant scoping is
    /// legal and is the non-tenant posture: the gate reports the surface
    /// unavailable without touching the network, so the area renders nothing
    /// (epic decision D9).
    /// </para>
    /// <para>
    /// <b>Ordering that matters.</b> <c>AddExplorerTenantView()</c> registers a
    /// fail-closed placeholder platform-operator gate with <c>TryAdd</c>, and an
    /// administrative plugin - <c>AddExplorerAccess()</c> - registers the real
    /// one. <c>TryAdd</c> keeps the first registration, so a head that calls
    /// <c>AddExplorerTenantView()</c> first silently keeps the placeholder and
    /// every tenant switch quietly changes nothing. This plugin detects that at
    /// probe time and files a diagnostic its Overview surface renders, so the
    /// misordering is visible instead of merely fail-closed.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerMyTenant(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // The seam this plugin's whole reach is expressed in. Registered here
        // rather than left to the head, so referencing the package is enough to
        // make the area work and there is no second call to forget.
        services.AddExplorerTenancy();

        // Scoped per Blazor circuit, like everything the tenancy seam registers:
        // the gate reads the calling scope's tenant identity and access store.
        services.TryAddScoped<IMyTenantAccessGate, MyTenantAccessGate>();

        // The Metrics surface's optional-section resolution, so the panel needs
        // no service provider of its own.
        services.TryAddScoped<MyTenantMetricsSectionAccessor>();

        return services;
    }

    /// <summary>
    /// Registers the My Tenant area plugin, so the shell enumerates it from the
    /// container and renders its panel. Call <see cref="AddExplorerMyTenant"/> as
    /// well: that registers the access gate and the tenancy seam this plugin
    /// resolves. A head that calls neither ships no My Tenant area at all, which
    /// is the whole of the opt-out.
    /// <para>
    /// The head is also responsible for registering the host-side plugin
    /// adapters (<c>AddExplorerPluginAdapters</c>), which live on the shell's
    /// side of the seam and are shared by every plugin.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerMyTenantPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddExplorerPlugin<MyTenantAreaPlugin>();
    }

    /// <summary>
    /// Registers the section the Metrics surface renders, replacing its
    /// placeholder body.
    /// <para>
    /// This is the seam the tenant-metrics work plugs into: the My Tenant area
    /// declares the Metrics tab from the start so it does not appear later and
    /// shift every tab beside it, and a head that registers no section simply
    /// gets the placeholder.
    /// </para>
    /// </summary>
    /// <typeparam name="TSection">The section to register.</typeparam>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerMyTenantMetricsSection<TSection>(
        this IServiceCollection services)
        where TSection : class, IMyTenantMetricsSection
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IMyTenantMetricsSection, TSection>();
        return services;
    }
}
