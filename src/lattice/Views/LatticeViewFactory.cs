using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="ILatticeViewFactory"/>. Captures the grain factory, the
/// view catalog, the durable runtime-view registry, the startup-declared view
/// names, and the injectable <see cref="ILatticeReplicationContext"/> seam (views
/// require a WAL provider, registered by <c>AddLattice</c>). Each
/// <see cref="Create"/> call registers the view in the catalog and persists its
/// runtime registration durably so the maintainer survives a silo restart, then
/// ensures the maintainer is active. <see cref="DeleteAsync"/> tears a view down
/// completely.
/// </summary>
internal sealed class LatticeViewFactory(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
    IReadOnlyList<StartupViewRegistration> startupRegistrations,
    ILatticeReplicationContext replicationContext,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    ILogger<LatticeViewFactory> logger) : ILatticeViewFactory
{
    /// <inheritdoc />
    public ILatticeView Create(ILattice source, string viewName, LatticeViewDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(definition);

        // replicationContext is captured to mirror the tag-index factory's
        // pre-wiring; Phase 1 views derive their WAL seams from it implicitly
        // (the maintainer resolves ICommitLogReader, which is only non-null when
        // a WAL provider - the in-memory baseline from AddLattice, a durable
        // provider, or replication - is present).
        _ = replicationContext;

        var sourceTreeId = source.GetPrimaryKeyString();
        ViewSourceTreeValidator.ThrowIfViewTree(sourceTreeId);
        var registration = definition.AggregationProjection is { } aggregation
            ? new ViewRegistration(viewName, sourceTreeId, Projection: null, aggregation)
            : new ViewRegistration(viewName, sourceTreeId, definition.Projection, Accumulative: definition.Accumulative);
        catalog.Register(registration);

        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);

        // Lazy activation (Phase 1): persist the durable runtime registration (so
        // the view survives a restart) and kick the maintainer online in the
        // background. Faults are observed and logged rather than surfaced through
        // the sync Create call. The hosted ViewActivationService performs the same
        // EnsureActiveAsync with retry/backoff for startup-registered and
        // re-hydrated runtime views.
        _ = PersistAndActivateAsync(maintainer, registration);

        return BuildHandle(viewName, registration.IsAggregation);
    }

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
        if (!await ViewExistsAsync(viewName))
        {
            logger.LogDebug("DeleteAsync for view '{ViewName}' is a no-op; the view is not registered.", viewName);
            return;
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

    private async Task<bool> ViewExistsAsync(string viewName)
    {
        if (catalog.TryGet(viewName) is not null)
        {
            return true;
        }

        var durable = await RegistryGrain.ListAsync();
        for (var i = 0; i < durable.Count; i++)
        {
            if (string.Equals(durable[i].ViewName, viewName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private async Task PersistAndActivateAsync(IViewMaintainerGrain maintainer, ViewRegistration registration)
    {
        var viewName = registration.ViewName;
        try
        {
            // Startup-declared views are re-registered authoritatively by the
            // activation service on every start, so they need no durable runtime
            // record (and on a name conflict the startup declaration wins).
            if (!IsStartupDeclared(viewName))
            {
                var projection = (object?)registration.AggregationProjection ?? registration.Projection;
                var typeName = projection!.GetType().AssemblyQualifiedName;
                if (typeName is not null)
                {
                    await RegistryGrain.RegisterAsync(new RuntimeViewRegistration
                    {
                        ViewName = viewName,
                        SourceTreeId = registration.SourceTreeId,
                        ProjectionTypeName = typeName,
                        ProjectionVersion = registration.ProjectionVersion,
                        IsAggregation = registration.IsAggregation,
                        Accumulative = registration.Accumulative,
                    });
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist the durable runtime registration for view '{ViewName}'; it will not survive a restart until re-created.", viewName);
        }

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
