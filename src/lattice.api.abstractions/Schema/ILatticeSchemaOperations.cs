using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Accept-then-poll schema remediation and eager migration. Each start verb
/// authorizes exactly as its blocking twin on <see cref="ILatticeSchemaControl"/>,
/// records the operation durably, starts the work in the background and returns a
/// <see cref="LatticeOperationHandle"/> at once; the caller then polls
/// <see cref="ILatticeOperations.GetOperationStatusAsync"/> for the phase (dry run,
/// build, cutover), the values processed and the outcome. The work is independent
/// of the starting call, so a caller timeout, a closed browser tab or a dropped
/// connection does not cancel it.
/// </summary>
/// <remarks>
/// <para>
/// The status, list and cancel verbs inherited from <see cref="ILatticeOperations"/>
/// are scoped to the schema operation kinds (<see cref="SchemaOperationKinds"/>),
/// the caller's tenant, and the trees the caller may read. An operation outside
/// that scope is reported as not found. Cancelling requires schema-management
/// authority over the tree, and takes effect only before cutover: a remediation
/// already cutting over runs on to completion.
/// </para>
/// <para>
/// A remediation that stops at a value it cannot remediate finishes as
/// <see cref="LatticeOperationState.Failed"/>, and its result map names the value
/// (<see cref="SchemaOperationResultKeys"/>); the tree's
/// <see cref="ILatticeSchemaControl.GetRemediationStatusAsync"/> report carries a
/// bounded preview of it too.
/// </para>
/// <para>
/// Every start verb takes an optional caller-chosen <c>operationId</c> (1 to 128
/// ASCII letters, digits, <c>-</c>, <c>_</c> or <c>.</c>). Starting again with an id
/// already in use returns the existing operation with
/// <see cref="LatticeOperationHandle.Created"/> <see langword="false"/> and starts
/// nothing, so a retried start is safe. When omitted a fresh id is generated. A
/// start that finds a remediation with the same parameters already in flight on the
/// tree follows that remediation rather than starting another.
/// </para>
/// </remarks>
public interface ILatticeSchemaOperations : ILatticeOperations
{
    /// <summary>
    /// Starts a remediation of <paramref name="treeId"/>: every value rewritten by
    /// <paramref name="transform"/> and checked against <paramref name="targetPolicy"/>,
    /// then the tree cut over to the rewritten copy with the policy installed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="transform">The per-value remediation transform.</param>
    /// <param name="targetPolicy">The policy the transformed values must satisfy. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, <paramref name="targetPolicy"/> carries an invalid rule, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    Task<LatticeOperationHandle> StartRemediationAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts an eager migration that re-stamps every value of
    /// <paramref name="treeId"/> to the tree's current target schema version. A tree
    /// already fully migrated to that version finishes at once, having done nothing.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered, or the id is in use by an operation of a different kind.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeOperationHandle> StartMigrationAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts an advance of <paramref name="treeId"/>'s target schema version to
    /// <paramref name="newTargetVersion"/>, then an eager migration to it. A target
    /// that does not advance fails the operation in its first phase.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered, or the id is in use by an operation of a different kind.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(
        string treeId,
        uint newTargetVersion,
        string? operationId = null,
        CancellationToken cancellationToken = default);
}
