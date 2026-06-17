using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="ILatticeViewFactory"/>. Captures the grain factory, the
/// view catalog, and the injectable <see cref="ILatticeReplicationContext"/> seam
/// (views require a WAL provider, registered by <c>AddLattice</c>). Each
/// <see cref="Create"/> call registers the view in the catalog so the maintainer
/// grain can resolve the source tree id and projection, then ensures the
/// maintainer is active.
/// </summary>
internal sealed class LatticeViewFactory(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
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
        var registration = definition.AggregationProjection is { } aggregation
            ? new ViewRegistration(viewName, sourceTreeId, Projection: null, aggregation)
            : new ViewRegistration(viewName, sourceTreeId, definition.Projection);
        catalog.Register(registration);

        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);

        // Lazy activation (Phase 1): kick the maintainer online in the background.
        // Faults are observed and logged rather than surfaced through the sync
        // Create call. The hosted ViewActivationService performs the same
        // EnsureActiveAsync with retry/backoff for startup-registered views.
        _ = EnsureActiveAsync(maintainer, viewName);

        var options = viewOptions.Get(viewName);
        var cacheTtl = options.ReadHandleCacheTtl > TimeSpan.Zero
            ? options.ReadHandleCacheTtl
            : LatticeViewOptions.DefaultReadHandleCacheTtl;

        // The read handle resolves the active-generation tree through the
        // maintainer (cached for cacheTtl) rather than binding a fixed tree id, so
        // queries follow a shadow-swap rebuild automatically.
        return new LatticeView(viewName, grainFactory, maintainer, cacheTtl, registration.IsAggregation);
    }

    private async Task EnsureActiveAsync(IViewMaintainerGrain maintainer, string viewName)
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
