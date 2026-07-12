using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The Backups area's view-model layer over <see cref="IBackupControlClient"/>.
/// Turns the raw client calls into UI-friendly results that fold a permission
/// denial or a transport failure into a status rather than throwing, so the Razor
/// components stay thin and never surface an unhandled error.
/// </summary>
public interface IBackupCatalogReader
{
    /// <summary>
    /// Loads one page of the backup catalog, newest-first, honouring the optional
    /// push-down <paramref name="filter"/>. A denial or a failure is returned as a
    /// non-success <see cref="BackupListView"/> rather than thrown.
    /// </summary>
    /// <param name="pageSize">The requested page size. Values &lt;= 0 defer to the server default.</param>
    /// <param name="pageToken">The continuation cursor from a prior page, or <see langword="null"/> to start.</param>
    /// <param name="filter">The filter predicates to push into the scan, or <see langword="null"/> for no filtering.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupListView> LoadPageAsync(int pageSize = 0, string? pageToken = null, BackupCatalogFilter? filter = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gathers the catalog-wide facets the Existing Backups filter row needs: the
    /// distinct kinds and scopes present, and the full standalone backups an
    /// incremental capture can build on. A denial or failure folds into the
    /// returned summary's status rather than throwing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupCatalogSummary> LoadSummaryAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes a backup chain, or returns <see langword="null"/> for an unknown
    /// id. A permission denial surfaces as
    /// <see cref="LatticeAuthorizationDeniedException"/> for the caller to fold.
    /// </summary>
    /// <param name="backupId">The backup id to describe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupChainDescription?> DescribeAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Triggers a full backup of <paramref name="scope"/>, folding a denial into the result.</summary>
    /// <param name="name">The backup name. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope to capture. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> TriggerFullAsync(string name, BackupScopeSelector scope, CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers a backup set - one full backup per tree in <paramref name="scopes"/>,
    /// grouped under a single set manifest - folding a denial into the result.
    /// </summary>
    /// <param name="name">The set name. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scopes">The per-tree scopes to capture. Must not be <see langword="null"/> or empty, and every scope must name a distinct tree.</param>
    /// <param name="crossTreeConsistent">Whether to capture every tree at a single cross-tree causal fence.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> TriggerSetAsync(string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent, CancellationToken cancellationToken = default);

    /// <summary>Triggers an incremental backup on top of <paramref name="baseBackupId"/>, folding a denial into the result.</summary>
    /// <param name="name">The backup name. Must not be <see langword="null"/> or empty.</param>
    /// <param name="scope">The scope to capture. Must not be <see langword="null"/>.</param>
    /// <param name="baseBackupId">The base backup id the increment builds on. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> TriggerIncrementalAsync(string name, BackupScopeSelector scope, string baseBackupId, CancellationToken cancellationToken = default);

    /// <summary>Restores <paramref name="backupId"/> into <paramref name="targetTreeId"/>, folding a denial into the result.</summary>
    /// <param name="backupId">The backup id to restore. Must not be <see langword="null"/> or empty.</param>
    /// <param name="targetTreeId">The target tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="mode">
    /// How the backup is installed. <see cref="LatticeRestoreMode.InPlace"/> (the
    /// default) merges the backup into the target by last-writer-wins, so writes
    /// made after the backup was taken survive. <see cref="LatticeRestoreMode.ShadowCutover"/>
    /// builds a fresh tree from the backup and swaps the alias, so the restored
    /// tree holds exactly the backup contents - the point-in-time-recovery path.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> RestoreAsync(string backupId, string targetTreeId, LatticeRestoreMode mode = LatticeRestoreMode.InPlace, CancellationToken cancellationToken = default);

    /// <summary>Deletes <paramref name="backupId"/>, folding a denial into the result.</summary>
    /// <param name="backupId">The backup id to delete. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> DeleteAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers a recurring backup schedule for <paramref name="scope"/> at the
    /// requested <paramref name="interval"/>, folding a denial into the result.
    /// </summary>
    /// <param name="scope">The scope to schedule. Must not be <see langword="null"/>.</param>
    /// <param name="incremental"><see langword="true"/> to schedule incremental captures; otherwise full captures.</param>
    /// <param name="interval">The requested cadence between captures. Must be strictly positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupOperationResult> ScheduleAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval, CancellationToken cancellationToken = default);
}
