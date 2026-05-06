using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// DI registration helpers that wrap the package's
/// <see cref="IReplicationApplier"/> singleton with
/// <see cref="BaselineReplicationApplier"/> so cross-cluster applies
/// are mirrored into the local <see cref="BaselineFactBackend"/> and
/// surfaced to the dashboard via
/// <see cref="FederationRouter.FactReplicated"/>.
/// </summary>
internal static class BaselineReplicationApplierRegistrationExtensions
{
    /// <summary>
    /// Decorates the currently-registered <see cref="IReplicationApplier"/>
    /// singleton with <see cref="BaselineReplicationApplier"/>. Must be
    /// called after the underlying applier has been registered (via
    /// <c>AddLatticeReplication</c>); throws otherwise.
    /// </summary>
    /// <remarks>
    /// The package registers its dead-letter-tracking applier as
    /// <c>ServiceDescriptor.Singleton&lt;IReplicationApplier&gt;</c>
    /// where the concrete type is <c>internal sealed</c>, so the
    /// decorator cannot reference it statically. This method walks the
    /// existing descriptor and re-registers the inner via a factory
    /// that resolves it from DI. Both
    /// <see cref="ServiceDescriptor.ImplementationType"/> and
    /// <see cref="ServiceDescriptor.ImplementationFactory"/>
    /// registration shapes are handled - mirrors the chaos-transport
    /// decorator pattern in
    /// <see cref="ChaosReplicationTransportRegistrationExtensions.AddChaosReplicationTransportDecorator"/>.
    /// </remarks>
    public static IServiceCollection AddBaselineReplicationApplierDecorator(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var existing = services.LastOrDefault(d => d.ServiceType == typeof(IReplicationApplier))
            ?? throw new InvalidOperationException(
                "IReplicationApplier must be registered before decorating it; "
                + "call silo.AddLatticeReplication(...) first.");

        services.Remove(existing);

        if (existing.ImplementationType is { } implType)
        {
            services.AddSingleton(implType);
            services.AddSingleton<IReplicationApplier>(sp =>
                new BaselineReplicationApplier(
                    (IReplicationApplier)sp.GetRequiredService(implType),
                    sp.GetRequiredService<BaselineFactBackend>(),
                    sp.GetRequiredService<FederationRouter>(),
                    sp.GetRequiredService<PartCrdtStore>(),
                    sp.GetRequiredService<ILogger<BaselineReplicationApplier>>()));
        }
        else if (existing.ImplementationFactory is { } factory)
        {
            services.AddSingleton<IReplicationApplier>(sp =>
                new BaselineReplicationApplier(
                    (IReplicationApplier)factory(sp),
                    sp.GetRequiredService<BaselineFactBackend>(),
                    sp.GetRequiredService<FederationRouter>(),
                    sp.GetRequiredService<PartCrdtStore>(),
                    sp.GetRequiredService<ILogger<BaselineReplicationApplier>>()));
        }
        else if (existing.ImplementationInstance is IReplicationApplier instance)
        {
            services.AddSingleton<IReplicationApplier>(sp =>
                new BaselineReplicationApplier(
                    instance,
                    sp.GetRequiredService<BaselineFactBackend>(),
                    sp.GetRequiredService<FederationRouter>(),
                    sp.GetRequiredService<PartCrdtStore>(),
                    sp.GetRequiredService<ILogger<BaselineReplicationApplier>>()));
        }
        else
        {
            throw new InvalidOperationException(
                "IReplicationApplier descriptor uses an unsupported registration shape.");
        }

        return services;
    }
}
