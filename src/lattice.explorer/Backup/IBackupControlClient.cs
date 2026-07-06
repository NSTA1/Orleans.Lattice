using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The explorer's transport-facing view of the backup control API: the subset of
/// the <c>LatticeBackupApiGrpcClient</c> surface the Backups area drives, over a
/// channel built from the current endpoint and sign-in. Kept as an interface so
/// the catalog reader and capability service can be unit-tested against a fake.
/// </summary>
/// <remarks>
/// Every mutating / scope-authorized call may surface a
/// <see cref="LatticeAuthorizationDeniedException"/> when the server denies the
/// caller (the client translates the gRPC <c>PermissionDenied</c> status back to
/// this typed exception), so callers must handle it even when an advisory
/// capability flag suggested the action was allowed.
/// </remarks>
public interface IBackupControlClient
{
    /// <summary>
    /// Probes, with no side effects, which backup / restore operations the caller
    /// may perform over <paramref name="scope"/>. Never throws on a permission
    /// denial: each capability is reported as a flag.
    /// </summary>
    /// <param name="scope">The scope to probe. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default);

    /// <summary>Lists one page of the backup catalog visible to the caller.</summary>
    /// <param name="request">The catalog page request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default);

    /// <summary>Describes a backup chain, or returns <see langword="null"/> when the id is unknown.</summary>
    /// <param name="backupId">The backup id to describe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Captures a full backup of the request's scope.</summary>
    /// <param name="request">The capture request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default);

    /// <summary>Captures an incremental backup on top of a base backup.</summary>
    /// <param name="request">The incremental capture request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default);

    /// <summary>Restores a backup into a target scope.</summary>
    /// <param name="request">The restore request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default);

    /// <summary>Deletes a backup. Returns <see langword="false"/> when the id was already absent.</summary>
    /// <param name="backupId">The backup id to delete. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default);
}
