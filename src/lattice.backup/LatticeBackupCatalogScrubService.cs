using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupCatalogScrubService"/>. Enumerates the
/// catalog, probes the sink for each row's resolvability, collects the orphans,
/// and - only when pruning is opted into - removes each orphan row. The removal
/// pass runs inside a single <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>
/// scope so the reserved <c>sys-backup-catalog</c> tree accepts the
/// infrastructure-authored deletes; the read-only probe pass needs no such scope
/// because the sink enters its own system-origin scope for the reserved
/// <c>sys-backup-store</c> tree.
/// </summary>
internal sealed class LatticeBackupCatalogScrubService(
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink) : ILatticeBackupCatalogScrubService
{
    private readonly ILatticeBackupCatalogStore _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    private readonly ILatticeBackupSink _sink = sink ?? throw new ArgumentNullException(nameof(sink));

    /// <inheritdoc />
    public async Task<BackupCatalogScrubReport> ScrubAsync(
        bool pruneOrphans = false,
        CancellationToken cancellationToken = default)
    {
        long scanned = 0;
        var orphans = new List<string>();

        // Tracked-operation progress (#4125). The catalog is streamed, so the row
        // count is unknown until the walk ends and no total is invented.
        var progress = LatticeOperationProgress.Current;
        if (progress is not null)
        {
            await progress.ReportAsync(
                BackupOperationPhases.ScrubbingCatalog, 0, null, BackupOperationUnits.Manifests).ConfigureAwait(false);
        }

        // Read pass: collect every catalog row whose sink payload is unresolvable.
        // The catalog is drained first so the (possibly destructive) second pass
        // does not mutate the collection being enumerated.
        await foreach (var manifest in _catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            scanned++;

            var resolution = await _sink.ProbeAsync(manifest.Id, cancellationToken).ConfigureAwait(false);
            if (!resolution.IsResolvable)
            {
                orphans.Add(manifest.Id);
            }

            if (progress is not null)
            {
                await progress.ReportAsync(
                    BackupOperationPhases.ScrubbingCatalog, scanned, null, BackupOperationUnits.Manifests)
                    .ConfigureAwait(false);
            }
        }

        long removed = 0;
        if (pruneOrphans && orphans.Count > 0)
        {
            if (progress is not null)
            {
                await progress.ReportAsync(
                    BackupOperationPhases.PruningOrphans, 0, orphans.Count, BackupOperationUnits.Manifests)
                    .ConfigureAwait(false);
            }

            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                long visited = 0;
                foreach (var orphanId in orphans)
                {
                    if (await _catalog.RemoveAsync(orphanId, cancellationToken).ConfigureAwait(false))
                    {
                        removed++;
                    }

                    // Every orphan visited counts, removed or already gone, so the
                    // count reaches the total.
                    if (progress is not null)
                    {
                        await progress.ReportAsync(
                            BackupOperationPhases.PruningOrphans, ++visited, orphans.Count, BackupOperationUnits.Manifests)
                            .ConfigureAwait(false);
                    }
                }
            }
        }

        return new BackupCatalogScrubReport(
            scannedCount: scanned,
            orphanCount: orphans.Count,
            removedCount: removed,
            pruned: pruneOrphans,
            orphanBackupIds: orphans);
    }
}
