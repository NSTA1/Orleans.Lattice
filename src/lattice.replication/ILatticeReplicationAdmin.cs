namespace Orleans.Lattice.Replication;

/// <summary>
/// Operator-facing admin seam for replication. Today it exposes the
/// explicit re-seed entry point used for scheduled bootstraps - new
/// peers joining, bandwidth-constrained initial sync, post-disaster
/// recovery - that complement the receiver-driven auto-bootstrap
/// path on <see cref="ILatticeFallOffLogDetector"/>. The admin seam
/// applies a per-<c>(tree, sourceClusterId)</c> rate limit
/// (governed by
/// <see cref="LatticeReplicationOptions.OperatorReseedMinInterval"/>)
/// before delegating to
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> so a
/// stuck or repeated operator command cannot flood the source
/// cluster with snapshot exports.
/// <para>
/// The default implementation tracks honoured requests in process
/// memory only; a silo restart resets the rate-limit window for
/// every pair. Cross-silo coordination is not required because the
/// underlying <see cref="ILatticeBootstrapCoordinator"/> is itself
/// idempotent under concurrent invocations against the same tree:
/// the bootstrap state machine lives in a per-tree internal grain
/// whose cluster-wide single activation absorbs the second call as
/// a no-op when both requests share a source cluster id, and rejects
/// it as <see cref="InvalidOperationException"/> when they disagree.
/// The rate limit is therefore purely a fairness / throttling
/// mechanism, not a correctness one.
/// </para>
/// </summary>
public interface ILatticeReplicationAdmin
{
    /// <summary>
    /// Requests an operator-driven snapshot re-seed of
    /// <paramref name="treeName"/> from
    /// <paramref name="sourceClusterId"/>. When the request is
    /// honoured, invokes
    /// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> on
    /// the configured coordinator and returns
    /// <see cref="OperatorReseedDecision.Triggered"/> set to
    /// <see langword="true"/>. When the per-<c>(tree, sourceClusterId)</c>
    /// rate limit denies the request, returns
    /// <see cref="OperatorReseedDecision.Triggered"/> set to
    /// <see langword="false"/> with
    /// <see cref="OperatorReseedDecision.RetryAfter"/> populated; no
    /// bootstrap is started and no exception is thrown.
    /// <para>
    /// Exceptions thrown by the underlying coordinator (for example,
    /// <see cref="InvalidOperationException"/> when a bootstrap is
    /// already in flight from a different source cluster) propagate
    /// verbatim. The admin seam updates its rate-limit timestamp
    /// only on a successful coordinator call, so a thrown exception
    /// leaves the next request honourable as soon as the underlying
    /// fault clears.
    /// </para>
    /// </summary>
    /// <param name="treeName">The logical tree id to re-seed. Must be non-null and non-empty.</param>
    /// <param name="sourceClusterId">
    /// The id of the cluster the snapshot should be produced from.
    /// Must be non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed before the rate-limit check and propagated to the underlying coordinator.</param>
    Task<OperatorReseedDecision> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        CancellationToken cancellationToken = default);
}
