namespace Orleans.Lattice.Backup;

/// <summary>
/// The in-cluster index of backup manifests: the durable, introspectable catalog
/// the backup API enumerates. Manifests are persisted into the reserved
/// <c>sys-backup-catalog</c> <c>ILattice</c> tree keyed by backup id, and every
/// mutation runs through the standard write path so it is captured by the durable
/// per-key history view (enabled by default when the backup package is
/// registered). The catalog tree carries the core <c>sys-</c> prefix, so it is
/// hidden from the default cluster-state tree catalog and the backup API is the
/// sole enumeration surface for backups. This interface is the catalog storage
/// surface only; capturing and restoring backups are the responsibility of later
/// features.
/// </summary>
public interface ILatticeBackupCatalogStore
{
    /// <summary>
    /// Registers or replaces a manifest in the catalog, keyed by its
    /// <see cref="BackupManifest.Id"/>. Registering the same manifest twice is
    /// idempotent.
    /// </summary>
    /// <param name="manifest">The manifest to register. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is <c>null</c>.</exception>
    Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default);

    /// <summary>Reads a manifest by backup id, or <c>null</c> when none exists.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a manifest from the catalog by backup id. Returns <c>true</c> when a
    /// manifest was removed, <c>false</c> when none existed.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every catalogued manifest, in backup-id order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<BackupManifest> ListAsync(CancellationToken cancellationToken = default);
}
