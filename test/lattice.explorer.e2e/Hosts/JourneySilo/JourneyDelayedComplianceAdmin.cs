using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The compliance engine the journey silo registers when the fault seams are on: the
/// cluster's own engine, behind <see cref="JourneyComplianceScanDelay"/>'s hold. With no hold
/// armed it adds nothing but one dictionary lookup per scan.
/// </summary>
/// <param name="inner">The cluster's engine.</param>
/// <param name="delay">The holds.</param>
internal sealed class JourneyDelayedComplianceAdmin(ILatticeSchemaComplianceAdmin inner, JourneyComplianceScanDelay delay)
    : ILatticeSchemaComplianceAdmin
{
    /// <inheritdoc />
    public async Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        await delay.HoldAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await inner.ScanComplianceAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Puts the hold in front of the engine the schema enforcement registration made. Call it
    /// after <c>AddLatticeSchemaEnforcement</c>, and only when the fault seams are on.
    /// </summary>
    /// <param name="services">The silo's services.</param>
    public static void Register(IServiceCollection services)
    {
        var engine = services.LastOrDefault(descriptor => descriptor.ServiceType == typeof(ILatticeSchemaComplianceAdmin))
            ?? throw new InvalidOperationException("The compliance engine is not registered: call AddLatticeSchemaEnforcement first.");
        var type = engine.ImplementationType
            ?? throw new InvalidOperationException("The compliance engine is registered by factory or instance; the hold expects a type.");

        services.Remove(engine);
        services.AddSingleton(sp => new JourneyComplianceScanDelay(sp.GetService<TimeProvider>() ?? TimeProvider.System));
        services.AddSingleton<ILatticeSchemaComplianceAdmin>(sp => new JourneyDelayedComplianceAdmin(
            (ILatticeSchemaComplianceAdmin)ActivatorUtilities.CreateInstance(sp, type),
            sp.GetRequiredService<JourneyComplianceScanDelay>()));
    }
}
