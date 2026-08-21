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
        RuntimeViewProjectionProviderCatalog providers,
        ILogger logger)
    {
        if (string.IsNullOrEmpty(record.ViewName)
            || string.IsNullOrEmpty(record.SourceTreeId))
        {
            logger.LogWarning(
                "A runtime view cannot be re-hydrated because its persisted view name or source tree id is empty. The view stays dormant until it is re-created.");
            return null;
        }

        try
        {
            ViewSourceTreeValidator.ThrowIfViewTree(record.SourceTreeId);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(
                ex,
                "Runtime view '{ViewName}' cannot be re-hydrated because its persisted source '{SourceTreeId}' is another materialised view. The view stays dormant until it is re-created.",
                record.ViewName,
                record.SourceTreeId);
            return null;
        }

        if (string.IsNullOrEmpty(record.ProjectionVersion))
        {
            logger.LogWarning(
                "Runtime view '{ViewName}' cannot be re-hydrated: its persisted projection version is empty. The view stays dormant until it is re-created.",
                record.ViewName);
            return null;
        }

        if ((record.ProjectionProviderKey is null) != (record.ProjectionProviderPayload is null))
        {
            logger.LogWarning(
                "Runtime view '{ViewName}' cannot be re-hydrated: its projection provider key and payload are inconsistent. The view stays dormant until it is re-created.",
                record.ViewName);
            return null;
        }

        if (record.ProjectionProviderKey is { } providerKey)
        {
            if (string.IsNullOrEmpty(providerKey))
            {
                logger.LogWarning(
                    "Runtime view '{ViewName}' cannot be re-hydrated: its projection provider key is empty. The view stays dormant until it is re-created.",
                    record.ViewName);
                return null;
            }

            var payload = record.ProjectionProviderPayload!;
            if (payload.Length > LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes)
            {
                logger.LogWarning(
                    "Runtime view '{ViewName}' cannot be re-hydrated: provider payload length {PayloadLength} exceeds the {MaxPayloadLength}-byte limit. The view stays dormant until it is re-created.",
                    record.ViewName, payload.Length, LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes);
                return null;
            }

            var provider = providers.TryGet(providerKey);
            if (provider is null)
            {
                logger.LogWarning(
                    "Runtime view '{ViewName}' cannot be re-hydrated: projection provider '{ProviderKey}' is not configured on this silo. The view stays dormant until it is re-created.",
                    record.ViewName, providerKey);
                return null;
            }

            LatticeViewDefinition definition;
            try
            {
                definition = provider.Factory(
                    services,
                    new LatticeRuntimeViewProjectionContext(record.ViewName, record.SourceTreeId, payload));
                if (definition is null)
                {
                    throw new InvalidOperationException(
                        $"Projection provider '{providerKey}' returned null.");
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Runtime view '{ViewName}' cannot be re-hydrated: projection provider '{ProviderKey}' failed. The view stays dormant until it is re-created.",
                    record.ViewName, providerKey);
                return null;
            }

            if (!string.Equals(definition.ViewName, record.ViewName, StringComparison.Ordinal))
            {
                logger.LogWarning(
                    "Runtime view '{ViewName}' cannot be re-hydrated: projection provider '{ProviderKey}' returned definition name '{DefinitionViewName}'. The view stays dormant until it is re-created.",
                    record.ViewName, providerKey, definition.ViewName);
                return null;
            }

            var registration = ToRegistration(
                definition,
                record.SourceTreeId,
                providerKey);
            return ValidatePersistedShape(record, registration, logger);
        }

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
                return ValidatePersistedShape(
                    record,
                    new ViewRegistration(record.ViewName, record.SourceTreeId, Projection: null, aggregation),
                    logger);
            }
        }
        else if (instance is ILatticeViewProjection projection)
        {
            return ValidatePersistedShape(
                record,
                new ViewRegistration(
                    record.ViewName, record.SourceTreeId, projection, Accumulative: record.Accumulative),
                logger);
        }

        logger.LogWarning(
            "Runtime view '{ViewName}' cannot be re-hydrated: resolved projection type '{TypeName}' is not the expected projection kind. The view stays dormant until it is re-created.",
            record.ViewName, record.ProjectionTypeName);
        return null;
    }

    private static ViewRegistration ToRegistration(
        LatticeViewDefinition definition,
        string sourceTreeId,
        string providerKey) =>
        definition.AggregationProjection is { } aggregation
            ? new ViewRegistration(
                definition.ViewName,
                sourceTreeId,
                Projection: null,
                aggregation,
                ProjectionProviderKey: providerKey)
            : new ViewRegistration(
                definition.ViewName,
                sourceTreeId,
                definition.Projection,
                Accumulative: definition.Accumulative,
                ProjectionProviderKey: providerKey);

    private static ViewRegistration? ValidatePersistedShape(
        RuntimeViewRegistration record,
        ViewRegistration registration,
        ILogger logger)
    {
        try
        {
            var reconstructedVersion = registration.ProjectionVersion;
            if (registration.IsAggregation != record.IsAggregation
                || registration.Accumulative != record.Accumulative
                || !string.Equals(
                    reconstructedVersion,
                    record.ProjectionVersion,
                    StringComparison.Ordinal))
            {
                logger.LogWarning(
                    "Runtime view '{ViewName}' cannot be re-hydrated: reconstructed projection shape/version does not match the persisted registration (persisted kind={PersistedKind}, accumulative={PersistedAccumulative}, version='{PersistedVersion}'; reconstructed kind={ReconstructedKind}, accumulative={ReconstructedAccumulative}, version='{ReconstructedVersion}'). The view stays dormant until it is re-created.",
                    record.ViewName,
                    record.IsAggregation,
                    record.Accumulative,
                    record.ProjectionVersion,
                    registration.IsAggregation,
                    registration.Accumulative,
                    reconstructedVersion);
                return null;
            }

            return registration;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Runtime view '{ViewName}' cannot be re-hydrated: its reconstructed projection version could not be read. The view stays dormant until it is re-created.",
                record.ViewName);
            return null;
        }
    }
}
