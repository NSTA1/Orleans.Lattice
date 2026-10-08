using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Accept-then-poll schema compliance scans. <see cref="StartComplianceScanAsync"/>
/// authorizes read over the tree fail-closed, records the operation durably,
/// starts the scan in the background
/// and returns a <see cref="LatticeOperationHandle"/> at once; the caller then
/// polls <see cref="ILatticeOperations.GetOperationStatusAsync"/> for progress
/// (entries scanned against the tree's live entry count) and the outcome, and
/// rebuilds the report with <see cref="SchemaComplianceScanResults.TryReadReport"/>.
/// The scan is independent of the starting call, so a caller timeout, a closed
/// browser tab or a dropped connection does not cancel it.
/// </summary>
/// <remarks>
/// The status, list and cancel verbs inherited from <see cref="ILatticeOperations"/>
/// are scoped to the compliance-scan kind (<see cref="SchemaComplianceScanOperation.Kind"/>),
/// the caller's tenant and the trees the caller may read. An operation outside that
/// scope is reported as not found. Cancelling requires read over the scanned tree,
/// the grant that starting the scan required.
/// </remarks>
public interface ILatticeSchemaComplianceOperations : ILatticeOperations
{
    /// <summary>Starts a compliance scan of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree to scan. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">
    /// An optional idempotency id (1 to 128 ASCII letters, digits, <c>-</c>, <c>_</c>
    /// or <c>.</c>); <see langword="null"/> generates one. Starting again with an id
    /// already in use returns the existing operation with
    /// <see cref="LatticeOperationHandle.Created"/> <see langword="false"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the start call only, never the started scan.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartComplianceScanAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default);
}
