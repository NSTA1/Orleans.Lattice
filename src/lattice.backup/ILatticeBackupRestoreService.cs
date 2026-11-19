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
/// <see cref="LatticeOperation.Restore"/> capability for the target scope and,
/// when the restore retargets the backup onto a tree other than the one it was
/// captured from, additionally against the
/// <see cref="LatticeOperation.Backup"/> capability for that captured source
/// scope - so a cross-tree restore can never materialize a tree the caller holds
/// nothing on.
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
    /// <exception cref="ArgumentException">The target tree id is reserved for backup infrastructure.</exception>
    /// <exception cref="LatticeRestoreValidationException">The backup fails pre-apply validation.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope, or - on a cross-tree retarget - to back up the manifest's captured source scope.</exception>
    /// <exception cref="InvalidOperationException">The target tree cannot change aliases because another alias-changing operation or a delete is in progress.</exception>
    /// <exception cref="Orleans.Lattice.LatticeTreeOwnershipDeniedException">The registered ownership guard refuses the alias swap.</exception>
    Task<LatticeRestoreResult> RestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Restores every tree in the captured backup <b>set</b> identified by
    /// <paramref name="setId"/> as a single unit. When any member tree is currently
    /// replicated the restore runs as one all-or-nothing coordinated saga across the
    /// union of the replicated members' peer sets, so every member tree flips
    /// together on every participating cluster or none does; when no member tree is
    /// replicated (or the backup package is deployed single-cluster) it runs as a
    /// plain local per-member restore. Each member is restored via
    /// <see cref="LatticeRestoreMode.ShadowCutover"/>. Re-running the same set
    /// restore converges to the same state.
    /// </summary>
    /// <param name="setId">The content-addressed id of the backup set to restore. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the restore.</param>
    /// <returns>The per-member restore results this cluster applied, one per hosted member tree.</returns>
    /// <exception cref="ArgumentException"><paramref name="setId"/> is <c>null</c> or empty, or resolves to no member trees.</exception>
    /// <exception cref="LatticeRestoreValidationException">A member backup fails pre-apply validation, or the coordinated restore aborted.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore a member tree's scope.</exception>
    /// <exception cref="InvalidOperationException">A member tree cannot change aliases because another alias-changing operation or a delete is in progress.</exception>
    /// <exception cref="Orleans.Lattice.LatticeTreeOwnershipDeniedException">The registered ownership guard refuses a member alias swap.</exception>
    Task<IReadOnlyList<LatticeRestoreResult>> RestoreSetAsync(
        string setId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts a <see cref="LatticeRestoreMode.ShadowCutover"/> restore by swapping
    /// the target tree's registry alias back to the physical tree it resolved to
    /// before the cutover (<see cref="LatticeRestoreResult.PreviousPhysicalTreeId"/>),
    /// restoring the pre-restore state. Idempotent. Rejects a result that did not
    /// come from a shadow-cutover restore, or whose shadow was superseded by an
    /// unrelated later physical copy. Only the restore the tree resolves to now is
    /// revertible: one made final - by a later restore of the tree, by
    /// <see cref="ReleaseRestoreAsync"/>, or by
    /// <see cref="LatticeBackupOptions.RestoreRevertRetention"/> elapsing - or whose
    /// previous copy has been retired is refused, and a successful revert schedules
    /// the retirement of the shadow it moved off.
    /// </summary>
    /// <param name="restore">The result of the shadow-cutover restore to revert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the revert.</param>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="restore"/> is not a shadow-cutover restore result.</exception>
    /// <exception cref="LatticeRestoreValidationException">The supplied physical tree ids do not belong to the target tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    /// <exception cref="InvalidOperationException">Another alias-changing operation or a delete is in progress, an unrelated later physical copy superseded the restore, or the restore is final or its previous copy has been retired.</exception>
    /// <exception cref="Orleans.Lattice.LatticeTreeOwnershipDeniedException">The registered ownership guard refuses the alias swap.</exception>
    Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes a <see cref="LatticeRestoreMode.ShadowCutover"/> restore final, for a
    /// caller that ends its own undo window (issue #6103): from then on
    /// <see cref="RevertRestoreAsync"/> refuses it, and the physical copy it would
    /// have reverted to is retired in the background and, after
    /// <see cref="LatticeOptions.SoftDeleteDuration"/>, purged - exactly as when a
    /// later cutover supersedes it or <see cref="LatticeBackupOptions.RestoreRevertRetention"/>
    /// elapses. Idempotent; a restore already reverted or superseded is final
    /// already, so releasing it only schedules the retirement.
    /// </summary>
    /// <param name="restore">The result of the shadow-cutover restore to release. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the release.</param>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="restore"/> is not a shadow-cutover restore result.</exception>
    /// <exception cref="LatticeRestoreValidationException">The supplied physical tree ids do not belong to the target tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    /// <exception cref="InvalidOperationException">The target tree is deleted, or another alias-changing operation holds its alias reservation.</exception>
    /// <exception cref="NotSupportedException">The implementation does not support releasing a restore.</exception>
    Task ReleaseRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This restore service does not support releasing a restore.");

    /// <summary>
    /// Releases every restore of a restored backup set, as
    /// <see cref="ReleaseRestoreAsync"/> does for one: every member's target is
    /// authorized before any is released.
    /// </summary>
    /// <param name="restores">The per-member results of the set restore, for example from <see cref="RestoreSetAsync"/>. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the release.</param>
    /// <exception cref="ArgumentNullException"><paramref name="restores"/> or one of its results is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">A result is not a shadow-cutover restore result.</exception>
    /// <exception cref="LatticeRestoreValidationException">A result's physical tree ids do not belong to its target tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore a member's target scope.</exception>
    /// <exception cref="InvalidOperationException">A member tree is deleted, or another alias-changing operation holds its alias reservation.</exception>
    /// <exception cref="NotSupportedException">The implementation does not support releasing a restore.</exception>
    Task ReleaseRestoreSetAsync(
        IReadOnlyList<LatticeRestoreResult> restores,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This restore service does not support releasing a restore.");

    /// <summary>
    /// Reports the physical copies the shadow-cutover restores of
    /// <paramref name="treeId"/> have left behind: the alias target, the copy a
    /// still-revertible restore would return to, and every copy no revert can reach
    /// (issue #6103). A read of registry state that changes nothing.
    /// </summary>
    /// <param name="treeId">The logical tree to audit. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the audit.</param>
    /// <returns>The audit.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or a reserved backup tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the tree.</exception>
    /// <exception cref="NotSupportedException">The implementation does not support auditing restore copies.</exception>
    Task<LatticeRestoreCopyAudit> AuditRestoreCopiesAsync(
        string treeId,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This restore service does not support auditing restore copies.");

    /// <summary>
    /// Schedules the retirement of every restore copy of <paramref name="treeId"/>
    /// that no revert can reach, and returns the audit it acts on (issue #6103).
    /// Retirement runs in the background, one copy per pass, so this returns
    /// before the copies are retired; audit again to follow it. The same
    /// retirement runs on its own after every restore, revert and release, and
    /// for every tree on the estate-wide sweep, so this is only needed to act
    /// sooner.
    /// </summary>
    /// <param name="treeId">The logical tree to repair. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The audit the retirement starts from.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or a reserved backup tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the tree.</exception>
    /// <exception cref="NotSupportedException">The implementation does not support repairing restore copies.</exception>
    Task<LatticeRestoreCopyAudit> RepairRestoreCopiesAsync(
        string treeId,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This restore service does not support repairing restore copies.");
}
