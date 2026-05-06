using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// DI registration helpers that wrap the package's
/// <see cref="IReplicationTransport"/> singleton with
/// <see cref="ChaosReplicationTransport"/> so the operator-driven
/// disconnect flag short-circuits outbound ship at the transport seam.
/// </summary>
internal static class ChaosReplicationTransportRegistrationExtensions
{
    /// <summary>
    /// Decorates the currently-registered <see cref="IReplicationTransport"/>
    /// singleton with <see cref="ChaosReplicationTransport"/>. Must be
    /// called after the underlying transport has been registered (for
    /// example via
    /// <c>AddLatticeReplicationGrpcPushTransport</c>); throws otherwise.
    /// </summary>
    /// <remarks>
    /// The package's gRPC push transport is registered as
    /// <c>ServiceDescriptor.Singleton&lt;IReplicationTransport, GrpcPushTransport&gt;</c>
    /// where <c>GrpcPushTransport</c> is <c>internal sealed</c>, so the
    /// decorator cannot reference it statically. This method walks the
    /// existing descriptor and re-registers the inner concrete type by
    /// <see cref="Type"/>, then registers the decorator via a factory
    /// that resolves the inner from DI. Both
    /// <see cref="ServiceDescriptor.ImplementationType"/> and
    /// <see cref="ServiceDescriptor.ImplementationFactory"/> registration
    /// shapes are handled.
    /// </remarks>
    public static IServiceCollection AddChaosReplicationTransportDecorator(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var existing = services.LastOrDefault(d => d.ServiceType == typeof(IReplicationTransport))
            ?? throw new InvalidOperationException(
                "IReplicationTransport must be registered before decorating it; "
                + "call AddLatticeReplicationGrpcPushTransport first.");

        services.Remove(existing);

        if (existing.ImplementationType is { } implType)
        {
            // Re-register the concrete transport as itself so the
            // decorator can resolve it from DI without a static
            // reference (the package type is internal sealed).
            services.TryAddSingleton(implType);

            services.AddSingleton<IReplicationTransport>(sp =>
                new ChaosReplicationTransport(
                    (IReplicationTransport)sp.GetRequiredService(implType),
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationTransport>>()));
        }
        else if (existing.ImplementationFactory is { } factory)
        {
            services.AddSingleton<IReplicationTransport>(sp =>
                new ChaosReplicationTransport(
                    (IReplicationTransport)factory(sp),
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationTransport>>()));
        }
        else if (existing.ImplementationInstance is IReplicationTransport instance)
        {
            services.AddSingleton<IReplicationTransport>(sp =>
                new ChaosReplicationTransport(
                    instance,
                    sp.GetRequiredService<IGrainFactory>(),
                    sp.GetRequiredService<ILogger<ChaosReplicationTransport>>()));
        }
        else
        {
            throw new InvalidOperationException(
                "IReplicationTransport descriptor uses an unsupported registration shape.");
        }

        return services;
    }
}
