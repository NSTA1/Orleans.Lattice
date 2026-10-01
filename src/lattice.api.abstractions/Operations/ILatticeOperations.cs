namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// The shared read-and-cancel surface over long-running operations. A facade
/// whose verbs start operations (for example
/// <see cref="Orleans.Lattice.Api.Backup.ILatticeBackupOperations"/>) implements
/// it, scoped to the operation kinds that facade starts, so a caller polls every
/// kind of long operation the same way.
/// </summary>
/// <remarks>
/// Every member is scoped to the caller and fails closed: an operation in another
/// tenant, of a kind this facade does not own, or over trees the caller may not
/// read is reported as not found (<see langword="null"/>), never as forbidden, so
/// its existence is not disclosed.
/// </remarks>
public interface ILatticeOperations
{
    /// <summary>Reads an operation's status.</summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId,
        CancellationToken cancellationToken = default);

    /// <summary>Lists one page of the caller's operations, newest-first.</summary>
    /// <param name="request">The page request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><see cref="LatticeOperationListRequest.PageToken"/> is malformed.</exception>
    Task<LatticeOperationPage> ListOperationsAsync(
        LatticeOperationListRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests cancellation of an operation. The status stays non-terminal until
    /// the work observes the request; a terminal operation is returned unchanged.
    /// </summary>
    /// <param name="operationId">The operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status after the request, or <see langword="null"/> when no such operation is visible to the caller.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller may read the operation but not cancel it.</exception>
    Task<LatticeOperationStatus?> CancelOperationAsync(
        string operationId,
        CancellationToken cancellationToken = default);
}
