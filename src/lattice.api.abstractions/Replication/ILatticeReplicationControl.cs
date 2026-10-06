namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Transport-agnostic control facade for runtime per-tree cross-cluster
/// replication configuration. Every transport binding (the gRPC service and the
/// Orleans.Lattice.Api.Mcp MCP server) is a thin adapter over this single
/// surface, so the control semantics - authorization, engine delegation, and
/// permission-scoped discovery - are written and tested once and no transport
/// concern leaks into the control logic. Mirrors the sibling
/// <c>ILatticeBackupControl</c> / <c>ILatticeSchemaControl</c> facades.
/// </summary>
/// <remarks>
/// <para>
/// Every operation authorizes fail-closed <i>before</i> it touches engine
/// state, requiring the dedicated <see cref="LatticeOperation.Replication"/>
/// capability on the target tree. An anonymous or unauthorized caller is denied
/// with <see cref="LatticeAuthorizationDeniedException"/> and the engine is
/// never consulted. <see cref="GetReplicationConfigAsync"/> is
/// permission-scoped: it reports only the trees the caller is authorized to
/// manage, so it never reveals the existence of a tree outside the caller's
/// grant.
/// </para>
/// <para>
/// Enabling replication authors config that converges across the already
/// enrolled peer set; the operator flips it once, on any cluster, and every
/// peer converges. Per-cluster propagation is <b>not</b> re-consented - the
/// trust boundary is the existing peer enrolment - so authorization gates the
/// authoring cluster only.
/// </para>
/// </remarks>
public interface ILatticeReplicationControl
{
    /// <summary>
    /// Enables replication for <paramref name="treeId"/> under
    /// <paramref name="mode"/>, after authorizing the tree fail-closed for the
    /// <see cref="LatticeOperation.Replication"/> capability. The merge mode is
    /// fixed at enable time: enabling an already-enabled tree under a different
    /// mode is rejected (disable then re-enable to change a mode). When the tree
    /// already holds data and <paramref name="bootstrapSourceClusterId"/> is
    /// supplied, a snapshot bootstrap is requested so a peer converges on the
    /// pre-existing rows the change feed will not carry.
    /// </summary>
    /// <param name="treeId">The target tree id to enable. Must not be <c>null</c> or empty.</param>
    /// <param name="mode">The wire merge mode to fix for the tree when first enabled.</param>
    /// <param name="bootstrapSourceClusterId">
    /// Optional id of the cluster to pull an initial snapshot from when the tree
    /// already holds data. <c>null</c> or empty skips the bootstrap.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome, including the fixed mode and whether a bootstrap was requested.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to configure replication for the tree.</exception>
    Task<ReplicationEnableResult> EnableReplicationAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Disables replication for <paramref name="treeId"/>, after authorizing the
    /// tree fail-closed for the <see cref="LatticeOperation.Replication"/>
    /// capability. Disabling removes the tree's resolved runtime mode but does not
    /// tear down an already-active shipper; if it keeps shipping, peers that resolve
    /// no mode drop those entries while acknowledging the batch. It never purges
    /// already-replicated peer data and keeps the tree's fixed merge mode. A later
    /// re-enable requests a snapshot bootstrap only when a source cluster is
    /// supplied and the tree already holds data. Idempotent.
    /// </summary>
    /// <param name="treeId">The target tree id to disable. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome, including whether the tree was already disabled.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to configure replication for the tree.</exception>
    Task<ReplicationDisableResult> DisableReplicationAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports the effective replicated-tree set - each tree's enrolled state,
    /// the merge mode in force, its ambiguity / convergence status, and which
    /// enrollment source put it in force. Both sources a replication-enabled
    /// host resolves against are reconciled: trees enabled through
    /// <see cref="EnableReplicationAsync"/> at runtime <b>and</b> trees declared
    /// in the static deployment-time replicated-tree map, which acts as a
    /// fallback floor on the commit path. The report is permission-scoped: it
    /// includes only the trees the caller is authorized to manage (fail-closed
    /// discovery), so a caller without a grant for a tree is not told the tree
    /// exists. Never throws on a per-tree permission denial; a denied tree is
    /// silently omitted. Each tree is named by its effective id - the bare name for
    /// a default-tenant tree, the tenant-qualified <c>t/{tenant}/{name}</c> id for a
    /// tree of an asserted, non-default tenant - which is the id
    /// <see cref="ILatticeReplicationStatus.GetPeerStatusAsync"/> reports the same
    /// tree's links by, so the two reports join on tree id.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The permission-scoped per-tree replication config report.</returns>
    Task<ReplicationConfigReport> GetReplicationConfigAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Permanently decommissions <paramref name="peerClusterId"/>, after
    /// authorizing the caller fail-closed for the cluster-wide
    /// <see cref="LatticeOperation.Replication"/> capability. Removing a peer
    /// from <c>ReplicationPeers</c> is a <i>detach</i>: every replicated tree
    /// keeps the peer's durable enrolment, so a re-add is a cheap resume rather
    /// than a fresh bootstrap, and an in-flight cross-tree decision hold keeps
    /// waiting on the peer. Decommissioning goes further: it removes the peer's
    /// enrolment from every replicated tree outright, which lets any cross-tree
    /// decision hold still waiting on that peer release. A later re-add of the
    /// same peer cluster id is therefore always a fresh bootstrap, never a
    /// resume. Fails closed with the engine-level
    /// <c>LatticeReplicationPeerStillConfiguredException</c> (an
    /// <see cref="InvalidOperationException"/>) when the peer is still present
    /// in the configured peer set - remove it from <c>ReplicationPeers</c>
    /// first. Idempotent: decommissioning an already-decommissioned peer is a
    /// no-op reported through
    /// <see cref="ReplicationDecommissionPeerResult.AlreadyDecommissioned"/>.
    /// </summary>
    /// <param name="peerClusterId">The peer cluster id to decommission. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The outcome, including how many trees the peer's enrolment was removed from.</returns>
    /// <exception cref="ArgumentException"><paramref name="peerClusterId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer replication.</exception>
    /// <exception cref="InvalidOperationException">The peer is still present in the configured replication peer set.</exception>
    Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(
        string peerClusterId,
        CancellationToken cancellationToken = default);
}
