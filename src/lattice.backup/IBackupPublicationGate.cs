namespace Orleans.Lattice.Backup;

/// <summary>
/// The single seam through which every publish to, and every delete from, the
/// backup catalog and sink passes, so each is serialised per backup id against a
/// retention pass and against the others (issue #5418). The default routes to the
/// scope's <see cref="ILatticeBackupSchedulerGrain"/>
/// (<see cref="SchedulerBackupPublicationGate"/>).
/// </summary>
internal interface IBackupPublicationGate
{
    /// <summary>
    /// Publishes a full backup whose id is its bare content address, and returns
    /// it as published: under its final id (scope-qualified when another scope
    /// owns the bare one, issue #5620) and with its capture times reconciled.
    /// </summary>
    /// <param name="manifest">The manifest.</param>
    /// <param name="register"><see langword="false"/> to defer the catalog row to a set's membership stamp.</param>
    /// <param name="cancellationToken">Cancels waiting for a held id.</param>
    Task<BackupManifest> PublishFullAsync(BackupManifest manifest, bool register, CancellationToken cancellationToken);

    /// <summary>
    /// Publishes an increment, or returns <see langword="null"/> when its base's
    /// row is gone and it was abandoned (nothing of it remains published).
    /// </summary>
    /// <param name="manifest">The increment's manifest.</param>
    /// <param name="cancellationToken">Cancels waiting for a held id.</param>
    Task<BackupManifest?> PublishIncrementAsync(BackupManifest manifest, CancellationToken cancellationToken);

    /// <summary>
    /// Rewrites the sink manifest and registers the row of a manifest whose id is
    /// final: a set's membership stamp, or a set member registered unstamped.
    /// </summary>
    /// <param name="manifest">The manifest.</param>
    /// <param name="cancellationToken">Cancels waiting for a held id.</param>
    Task<BackupManifest> RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken);

    /// <summary>
    /// Registers a sink manifest found by a catalog rebuild, only while it is
    /// still in the sink. Returns <see langword="false"/> when it no longer is.
    /// </summary>
    /// <param name="manifest">The sink manifest.</param>
    /// <param name="cancellationToken">Cancels waiting for a held id.</param>
    Task<bool> RegisterRebuiltAsync(BackupManifest manifest, CancellationToken cancellationToken);

    /// <summary>
    /// Removes a backup as <paramref name="kind"/> directs. Returns whether
    /// anything was removed.
    /// </summary>
    /// <param name="row">The backup's catalog row, read by the caller.</param>
    /// <param name="kind">An operator delete or a scrub of an orphan row.</param>
    /// <param name="cancellationToken">Cancels waiting for a held id.</param>
    Task<bool> RetireAsync(BackupManifest row, BackupRetireKind kind, CancellationToken cancellationToken);
}
