namespace Orleans.Lattice.Backup;

/// <summary>
/// Restores a backup into a target tree, preserving the captured causal history
/// verbatim and remaining idempotent under retry. A restore reads the manifest
/// chain (a full backup, or a base plus ordered increments to a chosen point),
/// validates every referenced artifact against its recorded content digest before
/// installing anything, then replays the entries through the HLC-preserving
/// last-writer-wins merge / bulk-load seams so each entry's hybrid-logical-clock,
/// version vector, origin cluster id, expiry, and tombstone flag land bit-identical
/// to the capture. The restore is authorized fail-closed against the
/// <see cref="LatticeOperation.Restore"/> capability for the target scope.
/// </summary>
public interface ILatticeBackupRestoreService
{
    /// <summary>
    /// Restores the backup identified by <see cref="LatticeRestoreRequest.BackupId"/>
    /// into its target tree. Walks the backup's base chain, validates every
    /// artifact, then applies the entries either in place (empty-tree bulk-load fast
    /// path, or last-writer-wins merge into existing data) or via an atomic
    /// shadow-cutover, per <see cref="LatticeRestoreRequest.Mode"/>. Re-running the
    /// same request converges to the same state (a no-op in effect).
    /// </summary>
    /// <param name="request">The restore request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the restore.</param>
    /// <returns>The restore outcome.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeRestoreValidationException">The backup fails pre-apply validation.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    Task<LatticeRestoreResult> RestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts a <see cref="LatticeRestoreMode.ShadowCutover"/> restore by swapping
    /// the target tree's registry alias back to the physical tree it resolved to
    /// before the cutover (<see cref="LatticeRestoreResult.PreviousPhysicalTreeId"/>),
    /// restoring the pre-restore state. Idempotent. Rejects a result that did not
    /// come from a shadow-cutover restore.
    /// </summary>
    /// <param name="restore">The result of the shadow-cutover restore to revert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the revert.</param>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="restore"/> is not a shadow-cutover restore result.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default);
}
