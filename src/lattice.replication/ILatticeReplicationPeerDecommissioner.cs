namespace Orleans.Lattice.Replication;

/// <summary>
/// <para>
/// Engine-side seam (issue #4684) for permanently decommissioning a peer
/// cluster id: unlike removing a peer from
/// <c>LatticeReplicationOptions.ReplicationPeers</c> - a reversible detach
/// that the cross-tree decision hold deliberately keeps waiting through, on
/// the chance the peer returns - decommissioning drops the peer from every
/// tree's durable enrolment for good, which is exactly what releases the
/// hold's wait on that peer. A later re-add of the same cluster id is
/// therefore a fresh bootstrap rather than a resumed one.
/// </para>
/// <para>
/// Issue #4723: the same permanent removal also has a *receiver* half. If
/// this cluster is still waiting, as a cross-tree barrier's receiver, on an
/// operation whose origin is the decommissioned peer, that peer is never
/// coming back to acknowledge it, so the barrier must not be left waiting
/// forever. Every tree of such a barrier is a replica of that one peer, so
/// none is left to vote: decommissioning abandons every undecided
/// receiver-side barrier the peer originated without deciding it, then
/// settles every pending bucket the peer left on each tree with the verdict
/// its registry resolves - so an abandoned operation aborts on every tree
/// alike, whatever order the trees are walked in (issue #4742).
/// </para>
/// </summary>
public interface ILatticeReplicationPeerDecommissioner
{
    /// <summary>
    /// Permanently decommissions <paramref name="peerClusterId"/>: removes it
    /// from every replicated tree's durable
    /// <c>ICrossTreePeerEnrolmentGrain</c> enrolment (which is what releases
    /// any cross-tree decision hold waiting on it), forces that tree's
    /// per-peer shipper to detach from the log synchronously rather than
    /// through the async, retried detach that
    /// <c>ReplicationDriverActivationService</c> drives off
    /// <c>ReplicationPeers</c> (that path can still be in flight, or not yet
    /// started, when this call lands, and a re-add racing ahead of it would
    /// otherwise skip the detach and leave the shipper's read-log
    /// registration stale - breaking the "a re-add bootstraps fresh"
    /// guarantee), holds the read fence of every tree whose import from that
    /// peer still holds one until the peer is re-added, abandons every
    /// undecided cross-tree barrier on which this
    /// cluster is the <em>receiver</em> of operations originated by that peer
    /// and then settles every pending bucket the peer left on each tree
    /// (issues #4723, #4742), and records a durable decommissioned marker.
    /// </summary>
    /// <param name="peerClusterId">The peer cluster id to decommission. Must not be null or empty.</param>
    /// <param name="cancellationToken">Propagates cancellation of the fan-out.</param>
    /// <returns>The outcome of the decommission, including how many trees were touched.</returns>
    /// <exception cref="ArgumentException"><paramref name="peerClusterId"/> is null or empty.</exception>
    /// <exception cref="LatticeReplicationPeerStillConfiguredException">
    /// <paramref name="peerClusterId"/> is still present in
    /// <see cref="IReplicationTopology.CurrentPeers"/>. The caller must remove
    /// it from configuration first.
    /// </exception>
    Task<LatticeReplicationPeerDecommissionOutcome> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default);
}

/// <summary>
/// The outcome of a <see cref="ILatticeReplicationPeerDecommissioner.DecommissionPeerAsync"/>
/// call.
/// </summary>
/// <param name="PeerClusterId">The peer cluster id that was decommissioned.</param>
/// <param name="TreeCount">
/// The number of replicated trees whose enrolment was touched (idempotent:
/// includes a tree the peer was never enrolled on).
/// </param>
/// <param name="AlreadyDecommissioned">
/// <see langword="true"/> when the peer had already been decommissioned by an
/// earlier call; the fan-out still ran (idempotent) but recorded no new
/// marker timestamp.
/// </param>
public readonly record struct LatticeReplicationPeerDecommissionOutcome(string PeerClusterId, int TreeCount, bool AlreadyDecommissioned);
