namespace Orleans.Lattice.Backup;

/// <summary>
/// Fine-grained restore primitives that decompose the atomic
/// <see cref="LatticeRestoreMode.ShadowCutover"/> restore into the separate
/// phases a coordinated (cross-cluster) restore saga drives independently:
/// probe admission, build the shadow (unfenced), commit the cutover (under a
/// brief fence), and garbage-collect an orphaned shadow on abort. The single
/// atomic <see cref="ILatticeBackupRestoreService.RestoreAsync"/> entry point
/// composes these same phases for the local (single-cluster) path, so the two
/// paths share one implementation and one alias-swap.
/// <para>
/// This engine is backup-package-local and deliberately saga-unaware: it exposes
/// the mechanism (build / commit / delete / probe) without any knowledge of the
/// coordinator, the write fence, or the participant model, which live in the
/// replication package. The replication package's restore participant maps these
/// phases onto the saga; a backup-only host never uses them.
/// </para>
/// </summary>
public interface ILatticeCoordinatedRestoreEngine
{
    /// <summary>
    /// Resolves the target backup's manifest chain and reports its self-describing
    /// size and topology, without validating artifacts, fencing, or building
    /// anything. A coordinated restore probes admission first so an infeasible
    /// target is refused before any fence or shadow build.
    /// </summary>
    /// <param name="request">
    /// The restore request. <see cref="LatticeRestoreRequest.BackupId"/> selects the
    /// chain tip; <see cref="LatticeRestoreRequest.TargetTreeId"/> the target tree.
    /// Must not be <c>null</c>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The admission report.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeRestoreValidationException">The backup or a base in its chain is missing.</exception>
    Task<RestoreAdmissionReport> ProbeAdmissionAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Builds the shadow tree from the target backup's manifest chain (base full
    /// plus any increments) into a fresh physical tree, <b>without</b> swapping the
    /// registry alias and <b>without</b> fencing the live tree. Because the shadow
    /// is materialized from the backup's fixed past cut, live traffic keeps running
    /// during the (potentially long) build. Idempotent and resumable: the shard
    /// bulk-load is keyed by a deterministic operation id, so a retry after a crash
    /// resumes rather than restarting from zero. The build must be complete before
    /// the returned result is committed.
    /// </summary>
    /// <param name="request">
    /// The restore request. <see cref="LatticeRestoreRequest.Mode"/> must be
    /// <see cref="LatticeRestoreMode.ShadowCutover"/>. Must not be <c>null</c>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The shadow build result carrying the shadow physical tree id and the
    /// previous physical tree id retained for revert.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="request"/> is not a shadow-cutover request.</exception>
    /// <exception cref="LatticeRestoreValidationException">The backup fails pre-apply validation.</exception>
    Task<LatticeRestoreResult> BuildShadowAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits a previously built shadow by atomically swapping the registry alias
    /// to <see cref="LatticeRestoreResult.ShadowPhysicalTreeId"/> - the single
    /// atomic alias swap a reader sees whole-old or whole-new, never half - then
    /// refreshing the logical tree routing and converging any covering tag index.
    /// The caller engages the write fence around this call. Idempotent.
    /// </summary>
    /// <param name="shadow">The result of a prior <see cref="BuildShadowAsync"/>. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="shadow"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="shadow"/> is not a shadow-cutover build result.</exception>
    Task CommitShadowAsync(
        LatticeRestoreResult shadow,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reliably garbage-collects an orphaned shadow physical tree so a failed or
    /// aborted restore leaks no storage. Purges every shard of the shadow and
    /// removes its registry entry. Idempotent: deleting a shadow that was never
    /// built, or already deleted, is a no-op.
    /// </summary>
    /// <param name="shadowPhysicalTreeId">The shadow physical tree id to delete. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="shadowPhysicalTreeId"/> is <c>null</c> or empty.</exception>
    Task DeleteShadowAsync(
        string shadowPhysicalTreeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deterministically resolves the shadow tree id a
    /// <see cref="BuildShadowAsync"/> of <paramref name="request"/> builds into,
    /// without any I/O. Lets an aborting participant garbage-collect the shadow by
    /// id after a reactivation that lost its in-memory prepared state, without
    /// rebuilding it. Requires <see cref="LatticeRestoreRequest.TargetTreeId"/> to
    /// be set (a coordinated restore always targets an explicit tree).
    /// </summary>
    /// <param name="request">The restore request. Must not be <c>null</c> and must set an explicit target tree.</param>
    /// <returns>The deterministic shadow tree id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="request"/> does not set an explicit target tree.</exception>
    string ResolveShadowTreeId(LatticeRestoreRequest request);
}
