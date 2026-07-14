using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Views;

/// <summary>
/// Rebuilds an in-memory <see cref="ViewRegistration"/> from a durable
/// <see cref="RuntimeViewRegistration"/> by resolving the view's projection from a
/// service provider by its persisted concrete type. Shared by the hosted
/// <see cref="ViewActivationService"/> (startup re-hydration) and the
/// <see cref="ViewMaintainerGrain"/> (per-activation re-hydration when a maintainer
/// reactivates on a silo whose in-memory catalog has not yet seen the runtime
/// view).
/// <para>
/// A projection cannot be serialized, so only its identity is persisted; this
/// resolver reconstructs the instance from the service provider. It therefore
/// requires the projection's concrete type to be resolvable from DI - either
/// registered there or constructable via <see cref="ActivatorUtilities"/> with
/// DI-satisfiable constructor arguments.
/// </para>
/// <para>
/// The persisted type name is resolved through
/// <see cref="RuntimeViewProjectionAllowList"/>, which constrains it to the
/// projection types already loaded on this silo. A persisted name that is not an
/// allow-listed projection of the expected kind is rejected before the type is
/// constructed, so a tampered registry entry cannot drive re-hydration to load
/// or activate an arbitrary type.
/// </para>
/// </summary>
internal static class RuntimeViewRehydrator
{
    /// <summary>
    /// Resolves <paramref name="record"/> into a <see cref="ViewRegistration"/>, or
    /// returns <see langword="null"/> (logging a warning) when the projection type
    /// cannot be loaded, instantiated, or is not the expected projection kind.
    /// </summary>
    public static ViewRegistration? Resolve(
        RuntimeViewRegistration record,
        IServiceProvider services,
        ILogger logger)
    {
        // Constrain type resolution to the allow-list of projection types this
        // silo already has loaded, rather than resolving (and thereby loading)
        // an arbitrary assembly-qualified type named by the persisted - and, in
        // the threat model, potentially attacker-written - registry field. A
        // name that is not an allow-listed projection of the expected kind is
        // rejected before the type is ever constructed.
        var type = RuntimeViewProjectionAllowList.Resolve(record.ProjectionTypeName, record.IsAggregation);
        if (type is null)
        {
            logger.LogWarning(
                "Runtime view '{ViewName}' cannot be re-hydrated: projection type '{TypeName}' is not a known projection type configured on this silo. The view stays dormant until it is re-created.",
                record.ViewName, record.ProjectionTypeName);
            return null;
        }

        object? instance;
        try
        {
            instance = services.GetService(type) ?? ActivatorUtilities.CreateInstance(services, type);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Runtime view '{ViewName}' cannot be re-hydrated: projection type '{TypeName}' could not be resolved from the service provider. The view stays dormant until it is re-created.",
                record.ViewName, record.ProjectionTypeName);
            return null;
        }

        if (record.IsAggregation)
        {
            if (instance is ILatticeAggregationProjection aggregation)
            {
                return new ViewRegistration(record.ViewName, record.SourceTreeId, Projection: null, aggregation);
            }
        }
        else if (instance is ILatticeViewProjection projection)
        {
            return new ViewRegistration(
                record.ViewName, record.SourceTreeId, projection, Accumulative: record.Accumulative);
        }

        logger.LogWarning(
            "Runtime view '{ViewName}' cannot be re-hydrated: resolved projection type '{TypeName}' is not the expected projection kind. The view stays dormant until it is re-created.",
            record.ViewName, record.ProjectionTypeName);
        return null;
    }
}
