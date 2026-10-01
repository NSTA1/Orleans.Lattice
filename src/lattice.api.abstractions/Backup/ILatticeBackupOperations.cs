using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Accept-then-poll backup and restore. Each start verb authorizes exactly as its
/// blocking twin on <see cref="ILatticeBackupControl"/>, records the operation
/// durably, starts the work in the background and returns a
/// <see cref="LatticeOperationHandle"/> at once; the caller then polls
/// <see cref="ILatticeOperations.GetOperationStatusAsync"/> for progress and the
/// outcome. The work is independent of the starting call, so a caller timeout, a
/// closed browser tab or a dropped connection does not cancel it.
/// </summary>
/// <remarks>
/// <para>
/// The status, list and cancel verbs inherited from <see cref="ILatticeOperations"/>
/// are scoped to the backup operation kinds (<see cref="BackupOperationKinds"/>),
/// the caller's tenant, and the trees the caller may read. An operation outside
/// that scope is reported as not found. Cancelling requires the same grant that
/// starting the operation required.
/// </para>
/// <para>
/// Every start verb takes an optional caller-chosen <c>operationId</c> (1 to 128
/// ASCII letters, digits, <c>-</c>, <c>_</c> or <c>.</c>). Starting again with an id
/// already in use returns the existing operation with
/// <see cref="LatticeOperationHandle.Created"/> <see langword="false"/> and starts
/// nothing, so a retried start is safe. When omitted a fresh id is generated.
/// </para>
/// </remarks>
public interface ILatticeBackupOperations : ILatticeOperations
{
    /// <summary>Starts a full capture of the request's scope.</summary>
    /// <param name="request">The capture request. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartBackupAsync(
        LatticeBackupCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>Starts an incremental capture layered on a base backup.</summary>
    /// <param name="request">The incremental-capture request. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>Starts a backup-set capture, one full backup per member scope.</summary>
    /// <param name="request">The set-capture request. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up a member scope.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>Starts a restore of a catalogued backup into its target tree.</summary>
    /// <param name="request">The restore request. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id for the tracked operation (distinct from the restore's own <see cref="LatticeRestoreRequest.OperationId"/>); <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>Starts a catalog-free disaster restore resolved from the durable sink alone.</summary>
    /// <param name="request">The restore request. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id for the tracked operation; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the target scope.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartColdRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default);
}
