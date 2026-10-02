using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupCatalogRebuildService"/>. Enumerates the
/// sink's manifests and re-registers each into the catalog store, counting fresh
/// additions separately from in-place reconciliations. The whole pass runs inside
/// a single <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope so the
/// reserved <c>sys-backup-catalog</c> tree accepts the infrastructure-authored
/// writes and the sink's own system-origin scans nest cleanly.
/// </summary>
internal sealed class LatticeBackupCatalogRebuildService(
    ILatticeBackupSink sink,
    ILatticeBackupCatalogStore catalog) : ILatticeBackupCatalogRebuildService
{
    private readonly ILatticeBackupSink _sink = sink ?? throw new ArgumentNullException(nameof(sink));
    private readonly ILatticeBackupCatalogStore _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <inheritdoc />
    public async Task<BackupCatalogRebuildReport> RebuildFromSinkAsync(CancellationToken cancellationToken = default)
    {
        long scanned = 0;
        long registered = 0;
        long reconciled = 0;

        // Tracked-operation progress (#4125). The sink's manifests are streamed, so
        // their count is unknown until the walk ends and no total is invented.
        var progress = LatticeOperationProgress.Current;
        if (progress is not null)
        {
            await progress.ReportAsync(
                BackupOperationPhases.RebuildingCatalog, 0, null, BackupOperationUnits.Manifests).ConfigureAwait(false);
        }

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var manifest in _sink.ListManifestsAsync(cancellationToken).ConfigureAwait(false))
            {
                scanned++;

                // Classify the manifest as a fresh addition or an in-place
                // reconciliation before registering. RegisterAsync itself
                // reconciles against any existing row (preserving the immutable
                // capture timestamp), so this pre-read only drives the summary
                // counts and never changes what is stored.
                var existing = await _catalog.GetAsync(manifest.Id, cancellationToken).ConfigureAwait(false);
                await _catalog.RegisterAsync(manifest, cancellationToken).ConfigureAwait(false);

                if (existing is null)
                {
                    registered++;
                }
                else
                {
                    reconciled++;
                }

                if (progress is not null)
                {
                    await progress.ReportAsync(
                        BackupOperationPhases.RebuildingCatalog, scanned, null, BackupOperationUnits.Manifests)
                        .ConfigureAwait(false);
                }
            }
        }

        return new BackupCatalogRebuildReport(scanned, registered, reconciled);
    }
}
