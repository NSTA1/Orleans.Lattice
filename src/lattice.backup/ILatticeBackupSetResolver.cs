namespace Orleans.Lattice.Backup;

/// <summary>
/// Saga-unaware read seam that expands a captured backup set id into the per-tree
/// member backups it references. A backup set is captured under one initiating
/// cluster's cross-tree causal fence and references several trees; each member
/// backup is catalogued as an ordinary <see cref="BackupManifest"/> stamped with
/// the owning <see cref="BackupManifest.SetId"/>. This seam turns a set id back
/// into that member list so the restore path can restore every tree in the set as
/// one unit.
/// <para>
/// The seam is deliberately saga-unaware and lives in the backup package (the same
/// posture as <see cref="ILatticeCoordinatedRestoreEngine"/>): it exposes only the
/// set membership, never any coordinated-restore or topology concept. The
/// replication package layers the atomic multi-tree, multi-cluster restore on top
/// of it.
/// </para>
/// </summary>
public interface ILatticeBackupSetResolver
{
    /// <summary>
    /// Resolves the member backups of the set identified by
    /// <paramref name="setId"/>, in a deterministic order (by tree id). Returns an
    /// empty list when no catalogued backup carries that set id (for example when
    /// <paramref name="setId"/> is actually a single-tree backup id, not a set id),
    /// so a caller can use a non-empty result as the signal that a restore targets
    /// a set rather than a single tree.
    /// </summary>
    /// <param name="setId">The content-addressed set id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the catalog scan.</param>
    /// <returns>The member backups of the set, in tree-id order, or an empty list when the id is not a set id.</returns>
    /// <exception cref="ArgumentException"><paramref name="setId"/> is <c>null</c> or empty.</exception>
    Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync(
        string setId, CancellationToken cancellationToken = default);
}
