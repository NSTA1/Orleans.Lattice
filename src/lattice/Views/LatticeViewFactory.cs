using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="ILatticeViewFactory"/>. Captures the grain factory, the
/// view catalog, the durable runtime-view registry, the startup-declared view
/// names, and the injectable <see cref="ILatticeReplicationContext"/> seam used to
/// validate replication topology before publishing a view (views require a WAL
/// provider, registered by <c>AddLattice</c>). Each
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>
/// persists a runtime registration before publishing it to the catalog and
/// returning; the synchronous
/// <see cref="ILatticeViewFactory.Create(ILattice,string,LatticeViewDefinition)"/>
/// compatibility path persists and activates in the background.
/// <see cref="DeleteAsync"/> tears a view down completely.
/// </summary>
internal sealed class LatticeViewFactory(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
    IReadOnlyList<StartupViewRegistration> startupRegistrations,
    ILatticeReplicationContext replicationContext,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    IServiceProvider services,
    RuntimeViewProjectionProviderCatalog runtimeProviders,
    PredicateRuntimeViewProjectionCodec predicateCodec,
    ILogger<LatticeViewFactory> logger) : ILatticeViewFactory
{
    /// <inheritdoc />
    public ILatticeView Create(ILattice source, string viewName, LatticeViewDefinition definition)
    {
        var (registration, durable) = Prepare(source, viewName, definition);
        return RegisterAndActivate(viewName, registration, durable);
    }

    /// <inheritdoc />
    public async Task<ILatticeView> CreateAsync(
        ILattice source,
        string viewName,
        LatticeViewDefinition definition,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (registration, durable) = Prepare(source, viewName, definition);
        return await RegisterDurablyAndActivateAsync(viewName, registration, durable);
    }

    /// <inheritdoc />
    public ILatticeView Create(
        ILattice source,
        string viewName,
        LatticeRuntimeViewProjectionDescriptor runtimeProjection)
    {
        var (registration, durable) = Prepare(source, viewName, runtimeProjection);
        return RegisterAndActivate(viewName, registration, durable);
    }

    /// <inheritdoc />
    public async Task<ILatticeView> CreateAsync(
        ILattice source,
        string viewName,
        LatticeRuntimeViewProjectionDescriptor runtimeProjection,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (registration, durable) = Prepare(source, viewName, runtimeProjection);
        return await RegisterDurablyAndActivateAsync(viewName, registration, durable);
    }

    private (ViewRegistration Registration, RuntimeViewRegistration? Durable) Prepare(
        ILattice source,
        string viewName,
        LatticeViewDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(source);
        ViewNameValidator.ThrowIfComposedInvalid(viewName);
        ArgumentNullException.ThrowIfNull(definition);

        var sourceTreeId = source.GetPrimaryKeyString();
        ViewSourceTreeValidator.ThrowIfViewTree(sourceTreeId);
        ValidateReplicationTopology(viewName, sourceTreeId);
        var suppliedRegistration = definition.AggregationProjection is { } aggregation
            ? new ViewRegistration(viewName, sourceTreeId, Projection: null, aggregation)
            : new ViewRegistration(viewName, sourceTreeId, definition.Projection, Accumulative: definition.Accumulative);
        if (IsStartupDeclared(viewName))
        {
            return (suppliedRegistration, null);
        }

        var descriptor = ResolveRuntimeDescriptor(definition);
        var durable = BuildDurableRegistration(suppliedRegistration, descriptor);
        var registration = RuntimeViewRehydrator.Resolve(durable, services, runtimeProviders, logger)
            ?? throw new InvalidOperationException(
                $"Runtime view '{viewName}' cannot be reconstructed faithfully after a restart. Register a runtime projection provider and attach a matching {nameof(LatticeRuntimeViewProjectionDescriptor)}.");
        return (registration, durable);
    }

    private (ViewRegistration Registration, RuntimeViewRegistration? Durable) Prepare(
        ILattice source,
        string viewName,
        LatticeRuntimeViewProjectionDescriptor runtimeProjection)
    {
        ArgumentNullException.ThrowIfNull(source);
        ViewNameValidator.ThrowIfComposedInvalid(viewName);
        ArgumentNullException.ThrowIfNull(runtimeProjection);

        var provider = runtimeProviders.TryGet(runtimeProjection.ProviderKey)
            ?? throw new InvalidOperationException(
                $"Runtime view projection provider '{runtimeProjection.ProviderKey}' is not configured on this silo.");
        var sourceTreeId = source.GetPrimaryKeyString();
        ViewSourceTreeValidator.ThrowIfViewTree(sourceTreeId);
        ValidateReplicationTopology(viewName, sourceTreeId);
        var definition = provider.Factory(
            services,
            new LatticeRuntimeViewProjectionContext(
                viewName,
                sourceTreeId,
                runtimeProjection.PayloadSpan))
            ?? throw new InvalidOperationException(
                $"Runtime view projection provider '{runtimeProjection.ProviderKey}' returned null.");
        if (!string.Equals(definition.ViewName, viewName, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Runtime view projection provider '{runtimeProjection.ProviderKey}' returned definition name '{definition.ViewName}' for view '{viewName}'.");
        }

        var registration = definition.AggregationProjection is { } aggregation
            ? new ViewRegistration(
                viewName,
                sourceTreeId,
                Projection: null,
                aggregation,
                ProjectionProviderKey: runtimeProjection.ProviderKey)
            : new ViewRegistration(
                viewName,
                sourceTreeId,
                definition.Projection,
                Accumulative: definition.Accumulative,
                ProjectionProviderKey: runtimeProjection.ProviderKey);
        if (string.IsNullOrEmpty(registration.ProjectionVersion))
        {
            throw new InvalidOperationException(
                $"Runtime view projection provider '{runtimeProjection.ProviderKey}' returned an empty projection version.");
        }

        var durable = IsStartupDeclared(viewName)
            ? null
            : BuildDurableRegistration(registration, runtimeProjection);
        return (registration, durable);
    }

    private void ValidateReplicationTopology(string viewName, string sourceTreeId) =>
        ViewReplicationTopology.Resolve(
            viewName,
            sourceTreeId,
            viewOptions.Get(viewName),
            replicationContext);

    /// <inheritdoc />
    public async Task<ILatticeView?> GetAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        // Resolve whether the view exists and whether it is an aggregation view -
        // the only definition-derived fact the read handle needs - without
        // requiring the caller to re-supply the source tree or projection. Prefer
        // the in-memory catalog (startup-declared and runtime views already seen on
        // this silo), then the startup declarations (declared but not yet
        // re-hydrated into this silo's catalog), then the durable runtime registry
        // (runtime views created on another silo or before a restart).
        if (catalog.TryGet(viewName) is { } registration)
        {
            return BuildHandle(viewName, registration.IsAggregation);
        }

        if (FindStartupRegistration(viewName) is { } startup)
        {
            return BuildHandle(viewName, startup.AggregationProjectionFactory is not null);
        }

        var durable = await RegistryGrain.ListAsync();
        for (var i = 0; i < durable.Count; i++)
        {
            if (string.Equals(durable[i].ViewName, viewName, StringComparison.Ordinal))
            {
                return BuildHandle(viewName, durable[i].IsAggregation);
            }
        }

        return null;
    }

    private LatticeView BuildHandle(string viewName, bool isAggregation)
    {
        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
        var options = viewOptions.Get(viewName);
        var cacheTtl = options.ReadHandleCacheTtl > TimeSpan.Zero
            ? options.ReadHandleCacheTtl
            : LatticeViewOptions.DefaultReadHandleCacheTtl;

        // The read handle resolves the active-generation tree through the maintainer
        // (cached for cacheTtl) rather than binding a fixed tree id, so queries
        // follow a shadow-swap rebuild automatically.
        return new LatticeView(viewName, grainFactory, maintainer, cacheTtl, isAggregation);
    }

    private ILatticeView RegisterAndActivate(
        string viewName,
        ViewRegistration registration,
        RuntimeViewRegistration? durable)
    {
        catalog.Register(registration);

        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);

        // Lazy activation (Phase 1): persist the durable runtime registration (so
        // the view survives a restart) and kick the maintainer online in the
        // background. Faults are observed and logged rather than surfaced through
        // the sync Create call. The hosted ViewActivationService performs the same
        // EnsureActiveAsync with retry/backoff for startup-registered and
        // re-hydrated runtime views.
        _ = PersistAndActivateAsync(maintainer, registration, durable);

        return BuildHandle(viewName, registration.IsAggregation);
    }

    private async Task<ILatticeView> RegisterDurablyAndActivateAsync(
        string viewName,
        ViewRegistration registration,
        RuntimeViewRegistration? durable)
    {
        var handle = BuildHandle(viewName, registration.IsAggregation);
        if (durable is not null)
        {
            await RegistryGrain.RegisterAsync(durable);
        }

        catalog.Register(registration);
        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
        _ = ActivateAsync(maintainer, viewName);
        return handle;
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string viewName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        // A startup-declared view is authoritative: deleting it would only have it
        // re-created on the next silo start from its declaration, so reject the
        // call rather than silently churning.
        if (IsStartupDeclared(viewName))
        {
            throw new InvalidOperationException(
                $"View '{viewName}' is declared at startup via AddLatticeViews and cannot be deleted at runtime; the declaration would re-create it on the next silo start. Remove the startup declaration instead.");
        }

        // Idempotent no-op: a view that is neither in the catalog nor durably
        // registered was never created (or is already deleted), so there is
        // nothing to tear down - and crucially nothing to delete that would
        // otherwise materialise a phantom backing tree.
        var sourceTreeId = await ResolveSourceTreeIdAsync(viewName);
        if (sourceTreeId is null)
        {
            logger.LogDebug("DeleteAsync for view '{ViewName}' is a no-op; the view is not registered.", viewName);
            return;
        }

        // Same reasoning as the startup-declaration guard, for the views a
        // first-party add-on declares from its own initializer rather than through
        // AddLatticeViews: the add-on re-creates the view on the next silo start,
        // so a runtime delete only tears down history the add-on owns and then has
        // it silently reappear. Those views are identified by their source living
        // in the reserved system-data namespace, which no caller-created view can
        // name (the tree-administration facade refuses a reserved source id).
        if (IsLibraryOwnedSource(sourceTreeId))
        {
            throw new InvalidOperationException(
                $"View '{viewName}' is declared by a Lattice add-on over the reserved system tree '{sourceTreeId}' and cannot be deleted at runtime; the add-on would re-create it on the next silo start.");
        }

        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.DecommissionAsync(cancellationToken);

        await RegistryGrain.UnregisterAsync(viewName);
        catalog.Remove(viewName);

        logger.LogInformation("View '{ViewName}' deleted.", viewName);
    }

    private IViewRegistryGrain RegistryGrain =>
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);

    private bool IsStartupDeclared(string viewName)
    {
        for (var i = 0; i < startupRegistrations.Count; i++)
        {
            if (string.Equals(startupRegistrations[i].ViewName, viewName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private StartupViewRegistration? FindStartupRegistration(string viewName)
    {
        for (var i = 0; i < startupRegistrations.Count; i++)
        {
            if (string.Equals(startupRegistrations[i].ViewName, viewName, StringComparison.Ordinal))
            {
                return startupRegistrations[i];
            }
        }

        return null;
    }

    /// <summary>
    /// Resolves a registered view's source tree id from the silo-local catalog,
    /// falling back to the cluster-wide durable registry, and returns <c>null</c>
    /// when the view is registered in neither.
    /// </summary>
    private async Task<string?> ResolveSourceTreeIdAsync(string viewName)
    {
        if (catalog.TryGet(viewName) is { } registration)
        {
            return registration.SourceTreeId;
        }

        var durable = await RegistryGrain.ListAsync();
        for (var i = 0; i < durable.Count; i++)
        {
            if (string.Equals(durable[i].ViewName, viewName, StringComparison.Ordinal))
            {
                return durable[i].SourceTreeId;
            }
        }

        return null;
    }

    /// <summary>
    /// Reports whether a view's source tree is one the library itself owns, so the
    /// view is re-created by its owning add-on on the next silo start and must not
    /// be droppable at runtime.
    /// </summary>
    private static bool IsLibraryOwnedSource(string sourceTreeId) =>
        sourceTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
        || sourceTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

    private LatticeRuntimeViewProjectionDescriptor? ResolveRuntimeDescriptor(
        LatticeViewDefinition definition)
    {
        if (definition.RuntimeProjection is not null)
        {
            return definition.RuntimeProjection;
        }

        if (definition.Projection is PredicateLatticeViewProjection predicate
            && !predicate.HasValueSelector
            && !predicate.HasKeySelector)
        {
            return new LatticeRuntimeViewProjectionDescriptor(
                PredicateRuntimeViewProjectionCodec.ProviderKey,
                predicateCodec.Encode(predicate.Filter));
        }

        return null;
    }

    private static RuntimeViewRegistration BuildDurableRegistration(
        ViewRegistration registration,
        LatticeRuntimeViewProjectionDescriptor? descriptor)
    {
        var projection = (object?)registration.AggregationProjection ?? registration.Projection;
        var typeName = projection!.GetType().FullName!;

        return new RuntimeViewRegistration
        {
            ViewName = registration.ViewName,
            SourceTreeId = registration.SourceTreeId,
            ProjectionTypeName = typeName,
            ProjectionVersion = registration.ProjectionVersion,
            IsAggregation = registration.IsAggregation,
            Accumulative = registration.Accumulative,
            ProjectionProviderKey = descriptor?.ProviderKey,
            ProjectionProviderPayload = descriptor?.Payload,
        };
    }

    private async Task PersistAndActivateAsync(
        IViewMaintainerGrain maintainer,
        ViewRegistration registration,
        RuntimeViewRegistration? durable)
    {
        var viewName = registration.ViewName;
        try
        {
            // Startup-declared views are re-registered authoritatively by the
            // activation service on every start, so they need no durable runtime
            // record (and on a name conflict the startup declaration wins).
            if (durable is not null)
            {
                await RegistryGrain.RegisterAsync(durable);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist the durable runtime registration for view '{ViewName}'; it will not survive a restart until re-created.", viewName);
        }

        await ActivateAsync(maintainer, viewName);
    }

    private async Task ActivateAsync(IViewMaintainerGrain maintainer, string viewName)
    {
        try
        {
            await maintainer.EnsureActiveAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to activate maintainer for view '{ViewName}' from factory; the activation service will retry.", viewName);
        }
    }
}
