using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Default <see cref="ILatticeViewFactory"/>. Captures the grain factory, the
/// view catalog, and the injectable <see cref="ILatticeReplicationContext"/> seam
/// (views require a WAL provider, which the replication package supplies). Each
/// <see cref="Create"/> call registers the view in the catalog so the maintainer
/// grain can resolve the source tree id and projection, then ensures the
/// maintainer is active.
/// </summary>
internal sealed class LatticeViewFactory(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
    ILatticeReplicationContext replicationContext,
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
        // the WAL provider the replication package registers is present).
        _ = replicationContext;

        var sourceTreeId = source.GetPrimaryKeyString();
        catalog.Register(new ViewRegistration(viewName, sourceTreeId, definition.Projection));

        var maintainer = grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
        var viewTree = grainFactory.GetGrain<ILattice>($"view-{viewName}");

        // Lazy activation (Phase 1): kick the maintainer online in the background.
        // Faults are observed and logged rather than surfaced through the sync
        // Create call. The hosted ViewActivationService performs the same
        // EnsureActiveAsync with retry/backoff for startup-registered views.
        _ = EnsureActiveAsync(maintainer, viewName);

        return new LatticeView(viewName, viewTree, maintainer);
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
