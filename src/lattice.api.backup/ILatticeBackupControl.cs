using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Transport-agnostic backup / restore control facade. Every transport binding
/// (the gRPC service now, a future MCP surface) is a thin adapter over this
/// single surface, so the control semantics - authorization, chain walking,
/// safe deletion, and bounded-memory enumeration - are written and tested once
/// and no transport concern leaks into the control logic.
/// </summary>
/// <remarks>
/// Every operation authorizes through the backup access gate
/// (<see cref="BackupAccessAuthorizer"/>) fail-closed <i>before</i> it touches
/// data: a capture / incremental / restore authorizes its target scope, and a
/// list / describe / delete authorizes the scope carried by each manifest. List
/// and artifact export are streamed as <see cref="IAsyncEnumerable{T}"/> so a
/// large catalog or artifact enumerates with bounded memory rather than being
/// materialized whole.
/// </remarks>
internal interface ILatticeBackupControl
{
    /// <summary>
    /// Captures a full backup of the request's scope, after authorizing the
    /// scope fail-closed.
    /// </summary>
    /// <param name="request">The full-capture request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The captured backup's id and manifest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    Task<LatticeBackupCaptureResult> CreateBackupAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Captures an incremental backup layered on a base backup, after
    /// authorizing the scope fail-closed.
    /// </summary>
    /// <param name="request">The incremental-capture request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The captured backup's id and manifest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Captures a backup <i>set</i> - one full backup per scope in the request,
    /// grouped under a single set manifest - after authorizing every member
    /// scope fail-closed. When the request asks for cross-tree consistency and
    /// the set covers more than one tree, every member is captured at a single
    /// causal fence so a cross-tree atomic write is never torn across the set
    /// boundary; a single-tree set pays no extra coordination.
    /// </summary>
    /// <param name="request">The set-capture request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The set manifest and the per-tree member results in scope order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up a scope in the set.</exception>
    Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers (or updates) a recurring backup schedule for the request's
    /// scope, after authorizing the scope fail-closed with the same grant a
    /// capture requires. Each scheduled cycle captures a full or incremental
    /// backup per <see cref="LatticeBackupScheduleRequest.Incremental"/> at the
    /// request's interval (clamped up to the scheduler minimum when smaller),
    /// overriding the configured schedule cadence for the chosen kind.
    /// Idempotent.
    /// </summary>
    /// <param name="request">The schedule request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    Task ScheduleBackupAsync(
        LatticeBackupScheduleRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the catalogued backups as a deterministic, cursor-resumable page
    /// ordered by backup id, hiding any manifest whose scope the caller may not
    /// read. Pass the previous page's
    /// <see cref="BackupCatalogPage.NextPageToken"/> to continue.
    /// </summary>
    /// <param name="request">Paging request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One page of manifests plus a continuation cursor.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    Task<BackupCatalogPage> ListBackupsAsync(
        BackupCatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams every catalogued backup the caller may read, in backup-id order,
    /// with bounded memory. The raw-enumeration analog of
    /// <see cref="ListBackupsAsync"/> for a consumer that wants to drain the
    /// whole catalog without managing a page cursor.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async stream of manifests, ordered by backup id.</returns>
    IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes a single backup and its base-first restore chain, or
    /// <see langword="null"/> when no backup with the id exists. Authorizes the
    /// backup's scope fail-closed before walking the chain.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The manifest and its ordered ancestor chain, or <see langword="null"/> when absent.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the backup's scope.</exception>
    Task<BackupChainDescription?> DescribeBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a backup: removes its manifest from the catalog and the sink and
    /// deletes only the artifacts it owns that are not shared with any other
    /// retained manifest. Authorizes the backup's scope fail-closed before
    /// deleting anything. Returns <see langword="true"/> when a backup was
    /// removed, <see langword="false"/> when none existed.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when a backup was deleted; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to delete the backup's scope.</exception>
    Task<bool> DeleteBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Restores a backup into its target tree, after authorizing the target
    /// scope fail-closed.
    /// </summary>
    /// <param name="request">The restore request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The restore outcome.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    Task<LatticeRestoreResult> RestoreBackupAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts a shadow-cutover restore, after authorizing the target scope
    /// fail-closed. Idempotent.
    /// </summary>
    /// <param name="restore">The shadow-cutover restore result to revert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams one of a backup's content-addressed artifacts back chunk-wise,
    /// with bounded memory, after authorizing the backup's scope fail-closed and
    /// verifying the artifact belongs to the backup.
    /// </summary>
    /// <param name="backupId">The owning backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="artifactId">The artifact id to export. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The ordered chunk stream of the artifact bytes.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> or <paramref name="artifactId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="KeyNotFoundException">No backup with <paramref name="backupId"/> exists, or it does not reference <paramref name="artifactId"/>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the backup's scope.</exception>
    IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(
        string backupId,
        string artifactId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Builds an inventory summary of every backup the caller may read - absolute
    /// counts, byte totals, per-kind counts, and oldest / newest timestamps from
    /// the durable catalog, plus the process-lifetime failure and bytes-reclaimed
    /// tallies from the in-memory metric registry. A manifest whose scope the
    /// caller may not read is excluded from the counts.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The catalog-wide inventory report.</returns>
    Task<BackupInventoryReport> GetInventoryAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a single scope's schedule and last-run status, or
    /// <see langword="null"/> when the scope has no registered schedule and no
    /// catalogued backup. Authorizes the scope's read grant fail-closed before
    /// returning anything.
    /// </summary>
    /// <param name="scope">The scope to describe. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scope's status, or <see langword="null"/> when the scope is unknown.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the scope.</exception>
    Task<BackupScopeStatus?> GetScopeStatusAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes, with <b>no side effects</b>, which backup / restore operations the
    /// current caller may perform over <paramref name="scope"/>. Runs the same
    /// fail-closed backup access gate the real operations use but reads, captures,
    /// restores, and deletes nothing, reporting each capability as an
    /// allowed / denied flag. Unlike every other operation on this facade it never
    /// throws <see cref="LatticeAuthorizationDeniedException"/>: a denial is
    /// reported as a <see langword="false"/> flag, default-deny, so a management
    /// UI can grey out controls the caller cannot use. The reported flags are
    /// advisory; the server still authorizes each real operation on attempt.
    /// </summary>
    /// <param name="scope">The scope to probe. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed-operation set for <paramref name="scope"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default);
}
