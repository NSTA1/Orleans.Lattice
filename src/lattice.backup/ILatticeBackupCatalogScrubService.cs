namespace Orleans.Lattice.Backup;

/// <summary>
/// Reconciles the in-cluster backup catalog against the durable sink, cross-checks
/// every catalogued backup for a resolvable sink payload (via
/// <see cref="ILatticeBackupSink.ProbeAsync"/>: the manifest is present and every
/// referenced artifact exists and is committed), and reports orphans - catalog
/// rows whose sink payload is gone. Because the sink is the single source of truth
/// and the only tolerated drift is "catalog missing a row the sink has" (healed by
/// rebuild), a catalog row with no resolvable sink payload is a prunable orphan
/// that must never be offered as a restore point.
/// <para>
/// The pass is idempotent and <b>non-destructive by default</b>: it flags orphans
/// and returns them. Destructive pruning - removing orphan rows from the reserved
/// <c>sys-backup-catalog</c> tree under system-origin - is an explicit opt-in, so
/// an operator can inspect the flagged orphans before deleting anything and a
/// re-run after a prune reports no orphans.
/// </para>
/// </summary>
public interface ILatticeBackupCatalogScrubService
{
    /// <summary>
    /// Cross-checks every catalog row against the sink and returns a summary of the
    /// orphans found. When <paramref name="pruneOrphans"/> is <see langword="false"/>
    /// (the default) the catalog is left untouched and the orphans are only flagged;
    /// when <see langword="true"/> each orphan row is removed from the catalog under
    /// system-origin. Idempotent and safe to re-run.
    /// </summary>
    /// <param name="pruneOrphans">
    /// <see langword="true"/> to destructively remove orphan rows from the catalog;
    /// <see langword="false"/> (the default) to flag them non-destructively.
    /// </param>
    /// <param name="cancellationToken">Cancels the scrub.</param>
    /// <returns>The scrub outcome summary.</returns>
    Task<BackupCatalogScrubReport> ScrubAsync(
        bool pruneOrphans = false,
        CancellationToken cancellationToken = default);
}
