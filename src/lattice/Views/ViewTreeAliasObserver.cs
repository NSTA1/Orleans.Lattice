using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Views;

/// <summary>
/// View-side <see cref="ITreeAliasObserver"/> that turns a tree-registry
/// physical-identity swap into an immediate, event-driven rebind of every
/// materialised-view maintainer whose source is the affected logical tree. The
/// core registry fires <see cref="ITreeAliasObserver.OnTreeAliasChangedAsync"/>
/// from its single alias-mutation choke point when a shadow-cutover restore,
/// resize, or reshard repoints a logical tree onto a new physical WAL; this
/// observer fans the change out to the per-view maintainer grains
/// (<see cref="IViewMaintainerGrain.NotifySourceIdentityChangedAsync"/>) so they
/// rebind on their next drain without re-reading the registry on every idle tick.
/// <para>
/// The push is best-effort: a maintainer that misses it (transiently
/// unavailable, not yet activated, or predating this build) still heals via its
/// own backstop re-resolve
/// (<see cref="LatticeViewOptions.SourceIdentityBackstopInterval"/>), so a
/// per-view failure here is logged and does not abort the remaining views. It is
/// the same core-to-consumer inversion the cross-cluster replication shipper
/// uses, applied to the view maintainer's identical per-drain resolve.
/// </para>
/// </summary>
internal sealed class ViewTreeAliasObserver(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
    ILogger<ViewTreeAliasObserver> logger) : ITreeAliasObserver
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IViewCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    private readonly ILogger<ViewTreeAliasObserver> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken)
    {
        foreach (var registration in _catalog.All())
        {
            if (!string.Equals(registration.SourceTreeId, change.TreeId, StringComparison.Ordinal))
            {
                continue;
            }

            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await _grainFactory
                    .GetGrain<IViewMaintainerGrain>(registration.ViewName)
                    .NotifySourceIdentityChangedAsync(change.NewPhysicalTreeId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Best-effort push: the maintainer will still pick the new identity
                // up via its backstop re-resolve. Log and continue so one
                // unreachable view does not starve the others of the notify.
                _logger.LogWarning(ex,
                    "Failed to notify view maintainer '{ViewName}' of source '{Source}' identity change to '{NewPhysical}'; "
                    + "the maintainer will rebind via its backstop re-resolve.",
                    registration.ViewName, change.TreeId, change.NewPhysicalTreeId);
            }
        }
    }
}
