namespace Orleans.Lattice.Backup;

/// <summary>
/// Captures a named, timestamped, causally consistent full backup of a selected
/// scope. The capture rides the core zero-observable-writes snapshot machinery
/// for point-in-time isolation, exports every in-scope entry with its complete
/// last-writer-wins / CRDT metadata, streams the payload to the configured
/// <see cref="ILatticeBackupSink"/> without buffering the whole scope, and
/// registers a self-describing <see cref="BackupManifest"/> in the hidden
/// <see cref="ILatticeBackupCatalogStore"/>.
/// <para>
/// The capture authorizes the scope before any data is touched, fails fast when
/// the in-scope size would exceed the snapshot replay budget, and inherits the
/// core snapshot shedding / budget behaviour by opening through the public
/// snapshot cursor surface.
/// </para>
/// </summary>
public interface ILatticeBackupCaptureService
{
    /// <summary>
    /// Captures a full backup described by <paramref name="request"/> and returns
    /// the content-addressed id and manifest of the stored backup.
    /// </summary>
    /// <param name="request">The capture request (name, scope, page size). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the capture.</param>
    /// <returns>The captured backup's id and manifest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    /// <exception cref="LatticeSnapshotReplayBudgetExceededException">The in-scope size exceeds the configured snapshot replay budget.</exception>
    /// <exception cref="LatticeSaturatedException">The silo shed the snapshot open because it was saturated.</exception>
    /// <exception cref="LatticeCursorSnapshotExpiredException">The pinned snapshot expired mid-capture.</exception>
    Task<LatticeBackupCaptureResult> CaptureAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Captures a backup <i>set</i> described by <paramref name="request"/>: one
    /// full backup per scope, grouped under a single set manifest. When
    /// <see cref="LatticeBackupSetCaptureRequest.CrossTreeConsistent"/> is
    /// <c>true</c> and the set covers more than one tree, every tree is captured
    /// as of a single causal fence selected after all in-flight cross-tree atomic
    /// sagas touching the set have drained, so a cross-tree atomic write is never
    /// torn across the set boundary: for each such batch either all members are
    /// present at or under the fence, or none are. A single-tree set (or a set
    /// with the flag left <c>false</c>) issues no extra coordination and captures
    /// each member with the cheap per-tree cut.
    /// </summary>
    /// <param name="request">The set capture request (name, per-tree scopes, cross-tree flag, page size). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the capture.</param>
    /// <returns>The set manifest and the per-tree member results in scope order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up a scope in the set.</exception>
    /// <exception cref="LatticeSnapshotReplayBudgetExceededException">A scope's in-scope size exceeds the configured snapshot replay budget.</exception>
    /// <exception cref="LatticeSaturatedException">A tree shed the snapshot open because it was saturated.</exception>
    /// <exception cref="LatticeCursorSnapshotExpiredException">A pinned snapshot expired mid-capture.</exception>
    /// <exception cref="LatticeBackupCrossTreeFenceException">A stable cross-tree fence could not be established within the configured attempts or drain timeout.</exception>
    Task<LatticeBackupSetCaptureResult> CaptureSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default);
}
