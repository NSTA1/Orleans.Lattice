namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Transport-agnostic facade over the cluster-wide peer decommission verb.
/// Every transport binding (the gRPC service and the Orleans.Lattice.Api.Mcp
/// MCP server) adapts over this single surface. Deliberately separate from
/// <see cref="ILatticeReplicationControl"/>, which is unchanged and still owns
/// per-tree enrolment (<see cref="ILatticeReplicationControl.EnableReplicationAsync"/>
/// / <see cref="ILatticeReplicationControl.DisableReplicationAsync"/>); this
/// facade does not repeat it, and its one verb authorizes cluster-wide rather
/// than per-tree.
/// </summary>
public interface ILatticeReplicationPeerAdmin
{
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
    /// <exception cref="LatticeReplicationEngineNotHostedException">No replication engine is hosted in this process, so the peer cannot be decommissioned.</exception>
    Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(
        string peerClusterId,
        CancellationToken cancellationToken = default);
}
