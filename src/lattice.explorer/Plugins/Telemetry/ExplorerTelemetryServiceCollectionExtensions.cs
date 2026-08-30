using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// Registers the Explorer's telemetry seam.
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

        // The controlled domain model the telemetry plugins declare. Registered
        // here rather than by the head, so the one contract the host may resolve
        // for a telemetry plugin ships with the package that defines it.
        services.TryAddScoped<ITelemetryDomain, TelemetryDomain>();
        return services;
    }
}
