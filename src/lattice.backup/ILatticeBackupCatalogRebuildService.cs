namespace Orleans.Lattice.Backup;

/// <summary>
/// Rebuilds the in-cluster backup catalog from the durable sink, making the sink
/// the single source of truth and the catalog a disposable, self-healing
/// projection over it. Scans every manifest the sink holds (via
/// <see cref="ILatticeBackupSink.ListManifestsAsync"/>, whose manifests are
/// already fully self-describing) and re-registers each into the reserved
/// <c>sys-backup-catalog</c> tree through
/// <see cref="ILatticeBackupCatalogStore.RegisterAsync"/>.
/// <para>
/// The operation is idempotent and safe to re-run: registration reconciles
/// against any existing catalog row so a manifest already present keeps its
/// immutable capture timestamp (the field the catalog index orders by) rather
/// than being re-keyed into an orphaned duplicate. It heals drift in both
/// directions - a catalog missing rows the sink has is repopulated, and a stale
/// row is overwritten with the sink's authoritative manifest.
/// </para>
/// </summary>
public interface ILatticeBackupCatalogRebuildService
{
    /// <summary>
    /// Scans every manifest in the sink and re-registers it into the catalog,
    /// returning a summary of how many manifests were scanned, freshly added, and
    /// reconciled in place. Idempotent and safe to re-run. Runs under
    /// system-origin so the reserved catalog tree accepts the writes.
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>The rebuild outcome summary.</returns>
    Task<BackupCatalogRebuildReport> RebuildFromSinkAsync(CancellationToken cancellationToken = default);
}
