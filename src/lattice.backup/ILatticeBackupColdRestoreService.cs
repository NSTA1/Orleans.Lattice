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
/// its <i>resolution</i> and <i>orchestration</i>: it resolves the target
/// backup's manifest from the sink (so a backup the catalog does not know is
/// still found), bootstraps the reserved <c>sys-</c> trees if they are absent,
/// delegates the actual causal-preserving replay to the existing restore engine -
/// which re-reads the target and walks its
/// <see cref="BackupManifest.BaseBackupId"/> chain catalog-first with a sink
/// fallback, so on a cluster whose catalog starts empty the whole chain resolves
/// from the sink - and re-projects the catalog from the sink afterwards so the
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
    /// exist, resolves the target manifest from the sink (failing when the sink
    /// does not hold it), then delegates to the restore engine, which walks the
    /// base chain catalog-first with a sink fallback, verifies every referenced
    /// artifact is present and intact, and replays the chain through the
    /// HLC-preserving restore path; finally re-projects the catalog from the sink
    /// so the recovered cluster is left with a correct catalog. Works when the
    /// catalog starts empty, because every catalog miss falls back to the sink.
    /// Idempotent: re-running the same request converges to the same state.
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
    /// <exception cref="ArgumentException">The delegated restore rejects the target tree id.</exception>
    /// <exception cref="InvalidOperationException">The target tree cannot change aliases because another alias-changing operation or a delete is in progress.</exception>
    /// <exception cref="Orleans.Lattice.LatticeTreeOwnershipDeniedException">The registered ownership guard refuses the alias swap.</exception>
    Task<LatticeRestoreResult> ColdRestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);
}
