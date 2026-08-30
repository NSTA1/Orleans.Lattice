using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.MyTenant;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Registers the Explorer's telemetry seam and the surfaces built on it.
/// </summary>
public static class ExplorerTelemetryServiceCollectionExtensions
{
    /// <summary>
    /// Adds the telemetry seam: the transport client, the operations surface, the
    /// availability probe, and the controlled domain model a telemetry plugin
    /// declares.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Everything is scoped, so each circuit gets its own channel, its own sign-in,
    /// and its own remembered catalogue rather than sharing one across users.
    /// </para>
    /// <para>
    /// Every registration is a <c>TryAdd</c>, so a head that has already supplied
    /// its own client - a test double, or a head that reaches telemetry some other
    /// way - keeps it, and calling this twice registers nothing twice.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerTelemetry(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginHost();
        services.TryAddScoped<ITelemetryQueryClient, GrpcTelemetryQueryClient>();
        services.TryAddScoped<ITelemetryQueryService, TelemetryQueryService>();
        services.TryAddScoped<ITelemetryAvailability, TelemetryAvailability>();

        // Also registered as its own concrete type, because it is the gate the
        // area plugin's constructor takes. Resolved from the same scope so the
        // gate and the domain share one remembered catalogue rather than each
        // reading the cluster.
        services.TryAddScoped<TelemetryAvailability>();

        // The controlled domain model the telemetry plugins declare. Registered
        // here rather than by the head, so the one contract the host may resolve
        // for a telemetry plugin ships with the package that defines it.
        services.TryAddScoped<ITelemetryDomain, TelemetryDomain>();
        return services;
    }

    /// <summary>
    /// Registers the Telemetry area plugin, so the shell enumerates it from the
    /// container and renders its panel. Call <see cref="AddExplorerTelemetry"/>
    /// as well: that registers the seam and the gate this plugin resolves. A
    /// head that calls neither ships no Telemetry area at all, which is the
    /// whole of the opt-out.
    /// <para>
    /// The head is also responsible for registering the host-side plugin
    /// adapters (<c>AddExplorerPluginAdapters</c>), which live on the shell's
    /// side of the seam and are shared by every plugin.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerTelemetryPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddExplorerPlugin<TelemetryAreaPlugin>();
    }

    /// <summary>
    /// Fills the My Tenant area's tenant-metrics section with the telemetry
    /// panels, pinned to the caller's own tenant.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Independent of <see cref="AddExplorerTelemetryPlugin"/>: a head may ship
    /// the tenant-facing section without the operator-facing area, or either
    /// without the other. Both need <see cref="AddExplorerTelemetry"/>, and the
    /// section additionally needs the My Tenant area registered - without it
    /// there is no surface for the section to appear in, and the registration
    /// is simply inert.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerTelemetryMyTenantSection(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddExplorerMyTenantMetricsSection<TelemetryMyTenantSection>();
    }
}
