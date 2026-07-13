namespace Orleans.Lattice.Backup;

/// <summary>
/// Restores a backup into a <b>fresh</b> cluster using the durable sink as the
/// single source of truth, with zero dependency on any surviving
/// <c>sys-backup-catalog</c> tree. This is the disaster-recovery entry point: a
/// cluster that lost its grain storage (so its catalog is gone) but still has the
/// external sink can enumerate, resolve, chain-walk, and restore its backups from
/// the sink alone.
/// <para>
/// The cold path differs from <see cref="ILatticeBackupRestoreService"/> only in
/// its <i>resolution</i> and <i>orchestration</i>: it resolves the target backup
/// (and walks its <see cref="BackupManifest.BaseBackupId"/> chain) directly from
/// the sink rather than the catalog, bootstraps the reserved <c>sys-</c> trees if
/// they are absent, delegates the actual causal-preserving replay to the existing
/// restore engine, and re-projects the catalog from the sink afterwards so the
/// recovered cluster ends up with a correct, populated catalog. The replay itself
/// preserves every entry's hybrid-logical-clock, version vector, origin cluster
/// id, expiry, and tombstone flag verbatim, exactly as an ordinary restore does.
/// </para>
/// </summary>
public interface ILatticeBackupColdRestoreService
{
    /// <summary>
    /// Restores the backup identified by
    /// <see cref="LatticeRestoreRequest.BackupId"/> into a fresh cluster from the
    /// sink alone. Bootstraps the reserved <c>sys-</c> trees if they do not yet
    /// exist, resolves the target manifest and its base chain directly from the
    /// sink, verifies every referenced artifact is present and intact, replays the
    /// chain through the HLC-preserving restore engine, and re-projects the catalog
    /// from the sink so the recovered cluster is left with a correct catalog. Never
    /// reads the catalog to resolve the backup, so it works when the catalog starts
    /// empty. Idempotent: re-running the same request converges to the same state.
    /// </summary>
    /// <param name="request">The restore request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the cold restore.</param>
    /// <returns>The restore outcome.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeRestoreValidationException">
    /// No backup with the requested id exists in the sink, or the backup fails
    /// pre-apply validation (a broken base chain or a missing / tampered artifact).
    /// </exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    Task<LatticeRestoreResult> ColdRestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);
}
