using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Accept-then-poll fresh storage usage. <see cref="StartStorageUsageRefreshAsync"/>
/// authorizes exactly as <see cref="ILatticeTreeAdmin.GetStorageUsageAsync"/>
/// (cluster telemetry, fail-closed), records the operation durably, starts the
/// deep re-measure of every tree in the background and returns a
/// <see cref="LatticeOperationHandle"/> at once. The caller polls
/// <see cref="ILatticeOperations.GetOperationStatusAsync"/> for progress (trees
/// measured against the tree count) and the cluster totals
/// (<see cref="StorageUsageRefreshResults.TryReadSummary"/>), then reads the
/// refreshed per-tree figures with the cheap
/// <see cref="ILatticeTreeAdmin.GetStorageUsageAsync"/> (<c>deep: false</c>).
/// The refresh is independent of the starting call, so a caller timeout, a closed
/// browser tab or a dropped connection does not cancel it.
/// </summary>
/// <remarks>
/// The status, list and cancel verbs inherited from <see cref="ILatticeOperations"/>
/// are scoped to the refresh kind (<see cref="StorageUsageRefreshOperation.Kind"/>),
/// the caller's tenant, and callers holding cluster telemetry authority; anything
/// else is reported as not found. Cancelling requires the same authority.
/// </remarks>
public interface ILatticeStorageUsageOperations : ILatticeOperations
{
    /// <summary>Starts a deep re-measure of every tree's storage usage.</summary>
    /// <param name="operationId">
    /// An optional idempotency id (1 to 128 ASCII letters, digits, <c>-</c>, <c>_</c>
    /// or <c>.</c>); <see langword="null"/> generates one. Starting again with an id
    /// already in use returns the existing operation with
    /// <see cref="LatticeOperationHandle.Created"/> <see langword="false"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the start call only, never the started refresh.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for cluster telemetry.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(
        string? operationId = null,
        CancellationToken cancellationToken = default);
}
