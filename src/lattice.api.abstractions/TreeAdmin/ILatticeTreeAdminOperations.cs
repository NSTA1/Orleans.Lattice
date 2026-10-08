using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Accept-then-poll tree maintenance: a materialised-view rebuild or reconcile, a
/// tag-index reconcile sweep, a WAL partition move, and a whole-tree orphaned-leaf
/// audit or repair. Each start verb authorizes exactly as its blocking twin on
/// <see cref="ILatticeTreeAdmin"/>, records the operation durably, starts the work
/// in the background and returns a <see cref="LatticeOperationHandle"/> at once; the
/// caller then polls <see cref="ILatticeOperations.GetOperationStatusAsync"/> for
/// the phase, the units completed and the outcome. The work is independent of the
/// starting call, so a caller timeout, a closed browser tab or a dropped connection
/// does not cancel it.
/// </summary>
/// <remarks>
/// <para>
/// The status, list and cancel verbs inherited from <see cref="ILatticeOperations"/>
/// are scoped to the tree-administration kinds (<see cref="TreeAdminOperationKinds"/>),
/// the caller's tenant, and the trees the caller may read. An operation outside that
/// scope is reported as not found. Cancelling requires the same grant that starting
/// the operation required.
/// </para>
/// <para>
/// Every start verb takes an optional caller-chosen <c>operationId</c> (1 to 128
/// ASCII letters, digits, <c>-</c>, <c>_</c> or <c>.</c>). Starting again with an id
/// already in use returns the existing operation with
/// <see cref="LatticeOperationHandle.Created"/> <see langword="false"/> and starts
/// nothing, so a retried start is safe. When omitted a fresh id is generated.
/// </para>
/// <para>
/// Phases, units and result keys are documented per kind on
/// <see cref="TreeAdminOperationKinds"/>, <see cref="TreeAdminOperationPhases"/>,
/// <see cref="TreeAdminOperationUnits"/> and <see cref="TreeAdminOperationResultKeys"/>.
/// </para>
/// </remarks>
public interface ILatticeTreeAdminOperations : ILatticeOperations
{
    /// <summary>
    /// Starts a shadow-swap rebuild of a materialised view (kind
    /// <see cref="TreeAdminOperationKinds.ViewRebuild"/>), requiring whole-tree
    /// <see cref="LatticeOperation.Admin"/> over the view's source tree.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is empty, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The view subsystem is not enabled, or the id is in use by an operation of a different kind.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks admin authority over the view's source tree.</exception>
    Task<LatticeOperationHandle> StartViewRebuildAsync(
        string viewName,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a reconcile (view anti-entropy) of a materialised view (kind
    /// <see cref="TreeAdminOperationKinds.ViewReconcile"/>), authorized as the
    /// rebuild: whole-tree <see cref="LatticeOperation.Admin"/> over the view's source.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is empty, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The view subsystem is not enabled, or the id is in use by an operation of a different kind.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks admin authority over the view's source tree.</exception>
    Task<LatticeOperationHandle> StartViewReconcileAsync(
        string viewName,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a digest-gated reconcile sweep of a tag index (kind
    /// <see cref="TreeAdminOperationKinds.TagIndexReconcile"/>), authorized as
    /// whole-tree <see cref="LatticeOperation.Admin"/> over the index's backing
    /// <c>tag-{indexName}</c> tree.
    /// </summary>
    /// <param name="indexName">The logical tag-index name. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> is empty, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The tag-index subsystem is not available, or the id is in use by an operation of a different kind.</exception>
    /// <exception cref="KeyNotFoundException">No tag index named <paramref name="indexName"/> exists.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks admin authority over the index tree.</exception>
    Task<LatticeOperationHandle> StartTagIndexReconcileAsync(
        string indexName,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts an online move of one WAL partition to another storage provider key
    /// (kind <see cref="TreeAdminOperationKinds.WalMove"/>), authorized as
    /// whole-tree <see cref="LatticeOperation.TreeLifecycle"/>. The source tail is
    /// retained until <see cref="ILatticeTreeAdmin.ReclaimMovedWalSourceAsync"/>, as
    /// for the blocking move. Cancelling before the placement flip leaves the source
    /// live; once the flip has run the move is committed.
    /// </summary>
    /// <param name="treeId">The tree whose partition to move. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="partition">The WAL partition index to move.</param>
    /// <param name="targetProviderKey">The target storage provider key. Must not be <c>null</c> or empty.</param>
    /// <param name="options">Optional move tunables; <c>null</c> takes the conventional defaults.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException">A tree id or key is empty or reserved, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<LatticeOperationHandle> StartWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a whole-tree orphaned-leaf <b>audit</b> (kind
    /// <see cref="TreeAdminOperationKinds.OrphanedLeavesAudit"/>): the operation drives
    /// <see cref="ILatticeTreeAdmin.AuditOrphanedLeavesAsync"/> batch by batch to the
    /// end of the tree and records the totals. Read-only; authorized as whole-tree
    /// <see cref="LatticeOperation.Read"/>. For the per-leaf findings, page the
    /// blocking audit verb.
    /// </summary>
    /// <param name="treeId">The tree to audit. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is empty, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts a whole-tree orphaned-leaf <b>repair</b> (kind
    /// <see cref="TreeAdminOperationKinds.OrphanedLeavesRepair"/>): the operation drives
    /// <see cref="ILatticeTreeAdmin.RepairOrphanedLeavesAsync"/> batch by batch to the
    /// end of the tree and records the totals. Irreversible per repaired leaf;
    /// authorized as whole-tree <see cref="LatticeOperation.TreeLifecycle"/>. Audit
    /// again when it finishes.
    /// </summary>
    /// <param name="treeId">The tree to repair. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="operationId">An optional idempotency id; <see langword="null"/> generates one.</param>
    /// <param name="cancellationToken">Cancels the start call only, never the started operation.</param>
    /// <returns>The operation handle.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is empty or reserved, or <paramref name="operationId"/> is malformed.</exception>
    /// <exception cref="InvalidOperationException">The id is in use by an operation of a different kind.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default);
}
