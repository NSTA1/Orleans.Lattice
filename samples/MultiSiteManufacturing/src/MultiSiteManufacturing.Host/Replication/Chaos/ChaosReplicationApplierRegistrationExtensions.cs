using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// DI registration helpers that wrap the currently-registered
/// <see cref="IReplicationApplier"/> singleton with
/// <see cref="ChaosReplicationApplier"/>, so the operator-driven
/// disconnect flag short-circuits inbound apply at the same seam the
/// package's gRPC service consults.
/// </summary>
internal static class ChaosReplicationApplierRegistrationExtensions
{
    /// <summary>
    /// Decorates the currently-registered <see cref="IReplicationApplier"/>
    /// singleton with <see cref="ChaosReplicationApplier"/>. Must be
    /// called after the underlying applier has been registered (via
    /// <c>AddLatticeReplication</c>) and after
    /// <see cref="BaselineReplicationApplierRegistrationExtensions.AddBaselineReplicationApplierDecorator"/>
    /// so the chaos gate is the outermost layer the gRPC server sees.
    /// </summary>
    /// <remarks>
    /// Mirrors the descriptor-walking pattern in
    /// <see cref="ChaosReplicationTransportRegistrationExtensions.AddChaosReplicationTransportDecorator"/>
    /// and
    /// <see cref="BaselineReplicationApplierRegistrationExtensions.AddBaselineReplicationApplierDecorator"/>.
    /// </remarks>
    public static IServiceCollection AddChaosReplicationApplierDecorator(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var existing = services.LastOrDefault(d => d.ServiceType == typeof(IReplicationApplier))
            ?? throw new InvalidOperationException(
                "IReplicationApplier must be registered before decorating it; "
                + "call silo.AddLatticeReplication(...) (and any prior decorator) first.");

        services.Remove(existing);

        if (existing.ImplementationType is { } implType)
        {
            services.AddSingleton(implType);
            services.AddSingleton<IReplicationApplier>(sp =>
                new ChaosReplicationApplier(
                    (IReplicationApplier)sp.GetRequiredService(implType),
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationApplier>>()));
        }
        else if (existing.ImplementationFactory is { } factory)
        {
            services.AddSingleton<IReplicationApplier>(sp =>
                new ChaosReplicationApplier(
                    (IReplicationApplier)factory(sp),
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationApplier>>()));
        }
        else if (existing.ImplementationInstance is IReplicationApplier instance)
        {
            services.AddSingleton<IReplicationApplier>(sp =>
                new ChaosReplicationApplier(
                    instance,
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationApplier>>()));
        }
        else
        {
            throw new InvalidOperationException(
                "IReplicationApplier descriptor uses an unsupported registration shape.");
        }

        return services;
    }
}
